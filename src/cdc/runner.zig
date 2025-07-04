const std = @import("std");
const log = std.log.scoped(.nats);

const vsr = @import("../vsr.zig");
const assert = std.debug.assert;
const maybe = vsr.stdx.maybe;
const fatal = @import("nats/protocol.zig").fatal;

const stdx = vsr.stdx;
const IO = vsr.io.IO;
const Tracer = vsr.trace.TracerType(vsr.time.Time);
const Storage = vsr.storage.StorageType(IO, Tracer);
const MessagePool = vsr.message_pool.MessagePool;
const MessageBus = vsr.message_bus.MessageBusClient;
const StateMachine = vsr.state_machine.StateMachineType(
    Storage,
    vsr.constants.state_machine_config,
);
const Client = vsr.ClientType(StateMachine, MessageBus, vsr.time.Time);
const TimestampRange = vsr.lsm.TimestampRange;
const tb = vsr.tigerbeetle;

pub const nats = @import("nats.zig");

/// CDC processor targeting a NATS JetStream server.
/// Producer: TigerBeetle `get_change_events` operation.
/// Consumer: NATS JetStream publisher.
/// Both consumer and producer run concurrently using `io_uring`.
/// See `DualBuffer` for more details.
pub const Runner = struct {
    const StateRecoveryMode = union(enum) {
        recover,
        override: u64,
    };

    const constants = struct {
        const tick_ms = vsr.constants.tick_ms;
        const idle_interval_ns: u63 = 1 * std.time.ns_per_s;
        const reply_timeout_ticks = @divExact(
            30 * std.time.ms_per_s,
            constants.tick_ms,
        );
        const app_id = "tigerbeetle";
        const progress_stream = "tigerbeetle.internal.progress";
        const locker_stream = "tigerbeetle.internal.locker";
        const event_stream = "tigerbeetle.events";
        const event_count_max: u32 = Client.StateMachine.operation_result_max(
            .get_change_events,
            vsr.constants.message_body_size_max,
        );
    };

    io: IO,
    idle_completion: IO.Completion = undefined,
    idle_interval_ns: u63,
    event_count_max: u32,

    message_pool: MessagePool,
    vsr_client: Client,
    buffer: DualBuffer,

    nats_client: nats.Client,
    event_subject: []const u8,
    progress_stream: []const u8,
    locker_stream: []const u8,

    connected: struct {
        /// VSR client registered.
        vsr: bool = false,
        /// NATS client connected and ready to publish.
        nats: bool = false,
    },
    /// The producer is responsible for reading events from TigerBeetle.
    producer: enum {
        idle,
        request,
        /// No events to publish,
        /// waiting for the timeout to check for new events.
        waiting,
    },

    /// The consumer is responsible to publish events on the NATS server.
    consumer: enum {
        idle,
        publish,
        progress_update,
    },

    metrics: Metrics,

    state: union(enum) {
        unknown: StateRecoveryMode,
        recovering: struct {
            timestamp_last: ?u64,
            phase: union(enum) {
                create_event_stream,
                create_locker_stream,
                create_progress_stream,
                get_progress_message,
                ack_progress_message: struct {
                    sequence: u64,
                },
            },
        },
        last: struct {
            /// Last event read from TigerBeetle.
            producer_timestamp: u64,
            /// Last event published.
            consumer_timestamp: u64,
        },
    },

    pub fn init(
        self: *Runner,
        allocator: std.mem.Allocator,
        options: struct {
            /// TigerBeetle cluster ID.
            cluster_id: u128,
            /// TigerBeetle cluster addresses.
            addresses: []const std.net.Address,
            /// NATS server address.
            host: std.net.Address,
            /// NATS credentials (optional).
            user: ?[]const u8,
            /// NATS password (optional).
            password: ?[]const u8,
            /// NATS subject for publishing events.
            event_subject: ?[]const u8,
            /// Overrides the number max of events produced/consumed each time.
            event_count_max: ?u32,
            /// Overrides the number of milliseconds to query again if there's no new events to
            /// process.
            /// Must be greater than zero.
            idle_interval_ms: ?u32,
            /// Indicates whether to recover the last timestamp published on the state
 
            recovery_mode: StateRecoveryMode,
        },
    ) !void {
        assert(options.addresses.len > 0);

        const idle_interval_ns: u63 = if (options.idle_interval_ms) |value|
            @intCast(@as(u64, value) * std.time.ns_per_ms)
        else
            constants.idle_interval_ns;
        assert(idle_interval_ns > 0);

        const event_count_max: u32 = if (options.event_count_max) |event_count_max|
            @min(event_count_max, constants.event_count_max)
        else
            constants.event_count_max;
        assert(event_count_max > 0);

        const event_subject: []const u8 = options.event_subject orelse
            try std.fmt.allocPrint(allocator, "{s}.{}", .{ constants.event_stream, options.cluster_id });
        errdefer if (options.event_subject == null) allocator.free(event_subject);
        assert(event_subject.len > 0);

        const progress_stream_owned: []const u8 = try std.fmt.allocPrint(
            allocator,
            "{s}.{}",
            .{ constants.progress_stream, options.cluster_id },
        );
        errdefer allocator.free(progress_stream_owned);
        assert(progress_stream_owned.len <= 255);

        const locker_stream_owned: []const u8 = try std.fmt.allocPrint(
            allocator,
            "{s}.{}",
            .{ constants.locker_stream, options.cluster_id },
        );
        errdefer allocator.free(locker_stream_owned);
        assert(locker_stream_owned.len <= 255);

        const dual_buffer = try DualBuffer.init(allocator, event_count_max);
        errdefer dual_buffer.deinit(allocator);

        self.* = .{
            .idle_interval_ns = idle_interval_ns,
            .event_count_max = event_count_max,
            .event_subject = event_subject,
            .progress_stream = progress_stream_owned,
            .locker_stream = locker_stream_owned,
            .connected = .{},
            .io = undefined,
            .producer = .idle,
            .consumer = .idle,
            .metrics = undefined,
            .state = .{ .unknown = options.recovery_mode },
            .buffer = dual_buffer,
            .message_pool = undefined,
            .vsr_client = undefined,
            .nats_client = undefined,
        };

        self.metrics = .{
            .producer = .{
                .timer = try std.time.Timer.start(),
            },
            .consumer = .{
                .timer = try std.time.Timer.start(),
            },
            .flush_ticks = 0,
            .flush_timeout_ticks = @divExact(30 * std.time.ms_per_s, constants.tick_ms),
        };

        self.io = try IO.init(32, 0);
        errdefer self.io.deinit();

        self.message_pool = try MessagePool.init(allocator, .client);
        errdefer self.message_pool.deinit(allocator);

        self.vsr_client = try Client.init(allocator, .{
            .id = stdx.unique_u128(),
            .cluster = options.cluster_id,
            .replica_count = @intCast(options.addresses.len),
            .time = .{},
            .message_pool = &self.message_pool,
            .message_bus_options = .{ .configuration = options.addresses, .io = &self.io },
        });
        errdefer self.vsr_client.deinit(allocator);

        self.nats_client = try nats.Client.init(allocator, .{
            .io = &self.io,
            .message_count_max = self.event_count_max,
            .message_body_size_max = Message.json_string_size_max,
            .reply_timeout_ticks = constants.reply_timeout_ticks,
        });
        errdefer self.nats_client.deinit(allocator);

        // Starting both the VSR and the NATS clients:
        try self.nats_client.connect(
            &struct {
                fn callback(context: *nats.Client) void {
                    const runner: *Runner = @alignCast(@fieldParentPtr("nats_client", context));
                    assert(!runner.connected.nats);
                    maybe(runner.connected.vsr);
                    log.info("NATS connected.", .{});
                    runner.connected.nats = true;
                    runner.recover();
                }
            }.callback,
            .{
                .host = options.host,
                .user = options.user,
                .password = options.password,
            },
        );
    }

    pub fn deinit(self: *Runner, allocator: std.mem.Allocator) void {
        self.vsr_client.deinit(allocator);
        self.message_pool.deinit(allocator);
        self.nats_client.deinit(allocator);
        self.buffer.deinit(allocator);
        allocator.free(self.progress_stream);
        allocator.free(self.locker_stream);
        if (self.event_subject.len > 0) allocator.free(self.event_subject);
    }

    /// To make the CDC stateless, JetStream streams are used to store the state:
    ///
    /// - Progress tracking stream:
    ///   A persistent stream with a maximum size of 1 message and discard-old policy.
    ///   During publishing, a message containing the last timestamp is published to this stream
    ///   at the end of each batch.
    ///   On restart, the presence of a message indicates the `timestamp_min` to resume processing.
    ///   The stream name is unique based on the `cluster_id`.
    ///   The initial timestamp can be overridden via the command line.
    ///
    /// - Locker stream:
    ///   A temporary stream used to ensure only one CDC process is publishing at a time.
    ///   A consumer with a unique name is created to act as a lock.
    fn recover(self: *Runner) void {
        assert(self.connected.nats);
        assert(self.state == .unknown);
        const recovery_mode = self.state.unknown;
        const timestamp_override: ?u64 = switch (recovery_mode) {
            .recover => null,
            .override => |timestamp| timestamp,
        };

        self.state = .{
            .recovering = .{
                .timestamp_last = timestamp_override,
                .phase = .create_event_stream,
            },
        };
        self.recover_dispatch();
    }

    fn recover_dispatch(self: *Runner) void {
        assert(self.connected.nats);
        assert(self.state == .recovering);
        switch (self.state.recovering.phase) {
            .create_event_stream => {
                self.nats_client.stream_create(
                    &struct {
                        fn callback(context: *nats.Client) void {
                            const runner: *Runner = @alignCast(@fieldParentPtr(
                                "nats_client",
                                context,
                            ));
                            assert(runner.state == .recovering);
                            const recovering = &runner.state.recovering;
                            assert(recovering.phase == .create_event_stream);
                            recovering.phase = .create_locker_stream;
                            runner.recover_dispatch();
                        }
                    }.callback,
                    .{
                        .name = self.event_subject,
                        .subjects = &.{self.event_subject},
                        .retention = .limits,
                        .max_msgs = 0,
                        .discard = .old,
                        .storage = .file,
                    },
                );
            },
            .create_locker_stream => {
                maybe(self.state.recovering.timestamp_last == null);
                self.nats_client.stream_create(
                    &struct {
                        fn callback(context: *nats.Client) void {
                            const runner: *Runner = @alignCast(@fieldParentPtr(
                                "nats_client",
                                context,
                            ));
                            switch (runner.state) {
                                .recovering => |*recovering| {
                                    assert(recovering.phase == .create_locker_stream);
                                    recovering.phase = .create_progress_stream;
                                    runner.recover_dispatch();
                                },
                                else => unreachable,
                            }
                        }
                    }.callback,
                    .{
                        .name = self.locker_stream,
                        .subjects = &.{self.locker_stream},
                        .retention = .interest,
                        .max_msgs = 0,
                        .discard = .old,
                        .storage = .memory,
                    },
                );
            },
            .create_progress_stream => {
                maybe(self.state.recovering.timestamp_last == null);
                self.nats_client.stream_create(
                    &struct {
                        fn callback(context: *nats.Client) void {
                            const runner: *Runner = @alignCast(@fieldParentPtr(
                                "nats_client",
                                context,
                            ));
                            switch (runner.state) {
                                .recovering => |*recovering| {
                                    assert(recovering.phase == .create_progress_stream);
                                    if (recovering.timestamp_last) |timestamp_override| {
                                        runner.state = .{
                                            .last = .{
                                                .consumer_timestamp = timestamp_override,
                                                .producer_timestamp = timestamp_override + 1,
                                            },
                                        };
                                        return runner.vsr_register();
                                    }
                                    recovering.phase = .get_progress_message;
                                    runner.recover_dispatch();
                                },
                                else => unreachable,
                            }
                        }
                    }.callback,
                    .{
                        .name = self.progress_stream,
                        .subjects = &.{self.progress_stream},
                        .retention = .limits,
                        .max_msgs = 1,
                        .discard = .old,
                        .storage = .file,
                    },
                );
            },
            .get_progress_message => {
                assert(self.state.recovering.timestamp_last == null);
                self.nats_client.get_message(
                    &struct {
                        fn callback(
                            context: *nats.Client,
                            found: ?nats.GetMessageResult,
                        ) nats.Error!void {
                            const runner: *Runner = @alignCast(@fieldParentPtr(
                                "nats_client",
                                context,
                            ));
                            switch (runner.state) {
                                .recovering => |*recovering| {
                                    assert(recovering.phase == .get_progress_message);
                                    assert(recovering.timestamp_last == null);

                                    if (found) |result| {
                                        const progress_tracker = try ProgressTrackerMessage.parse(
                                            result.message,
                                        );
                                        assert(TimestampRange.valid(progress_tracker.timestamp));
                                        if (vsr.constants.state_machine_config.release.value <
                                            progress_tracker.release.value)
                                        {
                                            fatal("The last event was published using a newer " ++
                                                "release (event={} current={}).", .{
                                                progress_tracker.release,
                                                vsr.constants.state_machine_config.release,
                                            });
                                        }

                                        recovering.timestamp_last = progress_tracker.timestamp;
                                        recovering.phase = .{
                                            .ack_progress_message = .{
                                                .sequence = result.sequence,
                                            },
                                        };
                                        return runner.recover_dispatch();
                                    }

                                    // No previous progress record found, start from beginning.
                                    runner.state = .{ .last = .{
                                        .consumer_timestamp = 0,
                                        .producer_timestamp = TimestampRange.timestamp_min,
                                    } };
                                    runner.vsr_register();
                                },
                                else => unreachable,
                            }
                        }
                    }.callback,
                    .{
                        .stream = self.progress_stream,
                        .consumer = "progress_consumer",
                    },
                );
            },
            .ack_progress_message => |message| {
                assert(self.state.recovering.timestamp_last != null);
                assert(TimestampRange.valid(self.state.recovering.timestamp_last.?));
                assert(message.sequence > 0);

                self.nats_client.ack_message(&struct {
                    fn callback(context: *nats.Client) void {
                        const runner: *Runner = @alignCast(@fieldParentPtr(
                            "nats_client",
                            context,
                        ));
                        switch (runner.state) {
                            .recovering => |*recovering| {
                                assert(recovering.phase == .ack_progress_message);
                                assert(recovering.timestamp_last != null);
                                assert(TimestampRange.valid(recovering.timestamp_last.?));

                                const seq = recovering.phase.ack_progress_message.sequence;
                                assert(seq > 0);

                                runner.state = .{ .last = .{
                                    .consumer_timestamp = recovering.timestamp_last.?,
                                    .producer_timestamp = recovering.timestamp_last.? + 1,
                                } };
                                runner.vsr_register();
                            },
                            else => unreachable,
                        }
                    }
                }.callback, .{
                    .stream = self.progress_stream,
                    .sequence = message.sequence,
                });
            },
        }
    }

    fn vsr_register(self: *Runner) void {
        assert(self.connected.nats);
        assert(!self.connected.vsr);
        assert(self.producer == .idle);
        assert(self.consumer == .idle);
        assert(self.state == .last);

        self.vsr_client.register(
            &struct {
                fn callback(
                    user_data: u128,
                    result: *const vsr.RegisterResult,
                ) void {
                    const runner: *Runner = @ptrFromInt(@as(usize, @intCast(user_data)));
                    assert(runner.connected.nats);
                    assert(!runner.connected.vsr);
                    log.info("VSR client registered.", .{});
                    runner.vsr_client.batch_size_limit = result.batch_size_limit;
                    runner.connected.vsr = true;

                    log.info("Starting CDC.", .{});
                    runner.produce();
                }
            }.callback,
            @as(u128, @intCast(@intFromPtr(self))),
        );
    }

    fn produce(self: *Runner) void {
        assert(self.connected.vsr);
        assert(self.connected.nats);
        assert(self.state == .last);
        assert(TimestampRange.valid(self.state.last.producer_timestamp));
        assert(self.state.last.consumer_timestamp == 0 or
            TimestampRange.valid(self.state.last.consumer_timestamp));
        assert(self.state.last.producer_timestamp > self.state.last.consumer_timestamp);
        switch (self.producer) {
            .idle => {
                if (!self.buffer.producer_begin()) {
                    assert(self.consumer != .idle);
                    assert(self.buffer.find(.ready) != null);
                    assert(self.buffer.find(.consuming) != null);
                    return;
                }
                self.producer = .request;
                self.metrics.producer.timer.reset();
                self.produce_dispatch();
            },
            else => unreachable,
        }
    }

    fn produce_dispatch(self: *Runner) void {
        assert(self.connected.vsr);
        assert(self.connected.nats);
        assert(self.state == .last);
        assert(TimestampRange.valid(self.state.last.producer_timestamp));
        assert(self.state.last.consumer_timestamp == 0 or
            TimestampRange.valid(self.state.last.consumer_timestamp));
        assert(self.state.last.producer_timestamp > self.state.last.consumer_timestamp);
        switch (self.producer) {
            .idle => unreachable,
            .request => {
                const filter: tb.ChangeEventsFilter = .{
                    .limit = self.event_count_max,
                    .timestamp_min = self.state.last.producer_timestamp,
                    .timestamp_max = 0,
                };

                self.vsr_client.request(
                    &produce_request_callback,
                    @intFromPtr(self),
                    .get_change_events,
                    std.mem.asBytes(&filter),
                );
            },
            .waiting => {
                self.io.timeout(
                    *Runner,
                    self,
                    struct {
                        fn callback(
                            runner: *Runner,
                            completion: *IO.Completion,
                            result: IO.TimeoutError!void,
                        ) void {
                            result catch unreachable;
                            _ = completion;
                            assert(runner.buffer.all_free());
                            assert(runner.consumer == .idle);
                            assert(runner.producer == .waiting);

                            const producer_begin = runner.buffer.producer_begin();
                            assert(producer_begin);
                            runner.producer = .request;
                            runner.produce_dispatch();
                        }
                    }.callback,
                    &self.idle_completion,
                    self.idle_interval_ns,
                );
            },
        }
    }

    fn produce_request_callback(
        context: u128,
        operation_vsr: vsr.Operation,
        timestamp: u64,
        result: []u8,
    ) void {
        const operation = operation_vsr.cast(Client.StateMachine);
        assert(operation == .get_change_events);
        assert(timestamp != 0);
        const runner: *Runner = @ptrFromInt(@as(usize, @intCast(context)));
        assert(runner.producer == .request);

        const source: []const tb.ChangeEvent = stdx.bytes_as_slice(.exact, tb.ChangeEvent, result);
        const target: []tb.ChangeEvent = runner.buffer.get_producer_buffer();
        assert(source.len <= target.len);

        stdx.copy_disjoint(
            .inexact,
            tb.ChangeEvent,
            target,
            source,
        );
        runner.buffer.producer_finish(@intCast(source.len));

        if (runner.buffer.all_free()) {
            assert(source.len == 0);
            assert(runner.consumer == .idle);
            runner.producer = .waiting;
            return runner.produce_dispatch();
        }

        runner.producer = .idle;
        runner.metrics.producer.record(source.len);
        assert(source.len > 0 or runner.consumer != .idle);
        if (source.len > 0) {
            const timestamp_next = source[source.len - 1].timestamp + 1;
            assert(TimestampRange.valid(timestamp_next));
            runner.state.last.producer_timestamp = timestamp_next;

            if (runner.consumer == .idle) runner.consume();
            runner.produce();
        }
    }

    fn consume(self: *Runner) void {
        assert(self.connected.vsr);
        assert(self.connected.nats);
        assert(self.state == .last);
        assert(TimestampRange.valid(self.state.last.producer_timestamp));
        assert(self.state.last.consumer_timestamp == 0 or
            TimestampRange.valid(self.state.last.consumer_timestamp));
        assert(self.state.last.producer_timestamp > self.state.last.consumer_timestamp);
        switch (self.consumer) {
            .idle => {
                if (!self.buffer.consumer_begin()) {
                    if (self.buffer.all_free()) {
                        assert(self.producer == .idle);
                    } else {
                        assert(self.producer == .request);
                        assert(self.buffer.find(.free) != null);
                        assert(self.buffer.find(.producing) != null);
                    }
                    return;
                }
                self.consumer = .publish;
                self.metrics.consumer.timer.reset();
                self.consume_dispatch();
            },
            else => unreachable,
        }
    }

    fn consume_dispatch(self: *Runner) void {
        assert(self.connected.vsr);
        assert(self.connected.nats);
        assert(self.state == .last);
        assert(TimestampRange.valid(self.state.last.producer_timestamp));
        assert(self.state.last.consumer_timestamp == 0 or
            TimestampRange.valid(self.state.last.consumer_timestamp));
        assert(self.state.last.producer_timestamp > self.state.last.consumer_timestamp);
        switch (self.consumer) {
            .idle => unreachable,
            .publish => {
                const events: []const tb.ChangeEvent = self.buffer.get_consumer_buffer();
                assert(events.len > 0);
                for (events) |*event| {
                    const message = Message.init(event);
                    self.nats_client.publish_enqueue(.{
                        .subject = self.event_subject,
                        .body = message.body(),
                        .headers = message.header(),
                    });
                }
                self.nats_client.publish_send(&struct {
                    fn callback(context: *nats.Client) void {
                        const runner: *Runner = @alignCast(@fieldParentPtr(
                            "nats_client",
                            context,
                        ));
                        assert(runner.consumer == .publish);
                        runner.consumer = .progress_update;
                        runner.consume_dispatch();
                    }
                }.callback);
            },
            .progress_update => {
                const progress_tracker: ProgressTrackerMessage = progress: {
                    const events = self.buffer.get_consumer_buffer();
                    assert(events.len > 0);
                    break :progress .{
                        .timestamp = events[events.len - 1].timestamp,
                        .release = vsr.constants.state_machine_config.release,
                    };
                };
                self.nats_client.publish_enqueue(.{
                    .subject = self.progress_stream,
                    .body = progress_tracker.body(),
                    .headers = progress_tracker.header(),
                });
                self.nats_client.publish_send(&struct {
                    fn callback(context: *nats.Client) void {
                        const runner: *Runner = @alignCast(@fieldParentPtr(
                            "nats_client",
                            context,
                        ));
                        assert(runner.consumer == .progress_update);

                        const event_count: usize, const timestamp_last: u64 = events: {
                            const events = runner.buffer.get_consumer_buffer();
                            assert(events.len > 0);
                            break :events .{ events.len, events[events.len - 1].timestamp };
                        };
                        runner.buffer.consumer_finish();
                        runner.state.last.consumer_timestamp = timestamp_last;

                        runner.consumer = .idle;
                        runner.metrics.consumer.record(event_count);
                        runner.consume();

                        if (runner.producer == .idle) runner.produce();
                    }
                }.callback);
            },
        }
    }

    pub fn tick(self: *Runner) void {
        assert(!self.vsr_client.evicted);
        self.vsr_client.tick();
        self.nats_client.tick();
        self.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms) catch unreachable;

        self.metrics.tick();
    }
};

/// Inspired by the StateMachine metrics,
/// though the current method of shipping the metrics is a temporary solution.
const Metrics = struct {
    const TimingSummary = struct {
        timer: std.time.Timer,

        duration_min_ms: ?u64 = null,
        duration_max_ms: ?u64 = null,
        duration_sum_ms: u64 = 0,
        event_count: u64 = 0,
        count: u64 = 0,

        fn record(
            metrics: *TimingSummary,
            event_count: u64,
        ) void {
            const duration_ms: u64 = @divFloor(metrics.timer.read(), std.time.ns_per_ms);

            metrics.duration_min_ms =
                @min(duration_ms, metrics.duration_min_ms orelse std.math.maxInt(u64));
            metrics.duration_max_ms = @max(duration_ms, metrics.duration_max_ms orelse 0);
            metrics.duration_sum_ms += duration_ms;
            metrics.count += 1;
            metrics.event_count += event_count;
        }
    };

    producer: TimingSummary,
    consumer: TimingSummary,
    flush_ticks: u64,
    flush_timeout_ticks: u64,

    fn tick(self: *Metrics) void {
        assert(self.flush_ticks < self.flush_timeout_ticks);
        self.flush_ticks += 1;
        if (self.flush_ticks == self.flush_timeout_ticks) {
            self.flush_ticks = 0;
            self.log_and_reset();
        }
    }

    fn log_and_reset(metrics: *Metrics) void {
        const Fields = enum { producer, consumer };
        const runner: *const Runner = @alignCast(@fieldParentPtr("metrics", metrics));
        inline for (comptime std.enums.values(Fields)) |field| {
            const summary: *TimingSummary = &@field(metrics, @tagName(field));
            if (summary.count > 0) {
                assert(runner.state == .last);
                const timestamp_last = switch (field) {
                    .consumer => runner.state.last.consumer_timestamp,
                    .producer => runner.state.last.producer_timestamp,
                };
                const event_rate = @divTrunc(
                    summary.event_count * std.time.ms_per_s,
                    summary.duration_sum_ms,
                );
                log.info("{s}: p0={?}ms mean={}ms p100={?}ms " ++
                    "event_count={} throughput={} op/s " ++
                    "last timestamp={} ({})", .{
                    @tagName(field),
                    summary.duration_min_ms,
                    @divFloor(summary.duration_sum_ms, summary.count),
                    summary.duration_max_ms,
                    summary.event_count,
                    event_rate,
                    timestamp_last,
                    stdx.DateTimeUTC.from_timestamp_ms(
                        timestamp_last / std.time.ns_per_ms,
                    ),
                });
            }
            summary.* = .{
                .timer = summary.timer,
            };
        }
    }
};

/// Buffers swapped between producer and consumer, allowing reading from TigerBeetle
/// and publishing to NATS to happen concurrently.
const DualBuffer = struct {
    const State = enum {
        free,
        producing,
        ready,
        consuming,
    };

    const Buffer = struct {
        buffer: []tb.ChangeEvent,
        state: union(State) {
            free,
            producing,
            ready: u32,
            consuming: u32,
        } = .free,
    };

    buffer_1: Buffer,
    buffer_2: Buffer,

    pub fn init(allocator: std.mem.Allocator, event_count: u32) !DualBuffer {
        assert(event_count > 0);
        assert(event_count <= Runner.constants.event_count_max);

        const buffer_1 = try allocator.alloc(tb.ChangeEvent, event_count);
        errdefer allocator.free(buffer_1);

        const buffer_2 = try allocator.alloc(tb.ChangeEvent, event_count);
        errdefer allocator.free(buffer_2);

        return .{
            .buffer_1 = .{
                .buffer = buffer_1,
                .state = .free,
            },
            .buffer_2 = .{
                .buffer = buffer_2,
                .state = .free,
            },
        };
    }

    pub fn deinit(self: *DualBuffer, allocator: std.mem.Allocator) void {
        allocator.free(self.buffer_2.buffer);
        allocator.free(self.buffer_1.buffer);
    }

    pub fn producer_begin(self: *DualBuffer) bool {
        self.assert_state();
        assert(self.find(.producing) == null);
        const buffer = self.find(.free) orelse return false;
        buffer.state = .producing;
        return true;
    }

    pub fn get_producer_buffer(self: *DualBuffer) []tb.ChangeEvent {
        self.assert_state();
        const buffer = self.find(.producing).?;
        return buffer.buffer;
    }

    pub fn producer_finish(self: *DualBuffer, count: u32) void {
        self.assert_state();
        const buffer = self.find(.producing).?;
        buffer.state = if (count == 0) .free else .{ .ready = count };
    }

    pub fn consumer_begin(self: *DualBuffer) bool {
        self.assert_state();
        assert(self.find(.consuming) == null);
        const buffer = self.find(.ready) orelse return false;
        const count = buffer.state.ready;
        buffer.state = .{ .consuming = count };
        return true;
    }

    pub fn get_consumer_buffer(self: *DualBuffer) []const tb.ChangeEvent {
        self.assert_state();
        const buffer = self.find(.consuming).?;
        return buffer.buffer[0..buffer.state.consuming];
    }

    pub fn consumer_finish(self: *DualBuffer) void {
        self.assert_state();
        const buffer = self.find(.consuming).?;
        buffer.state = .free;
    }

    pub fn all_free(self: *const DualBuffer) bool {
        return self.buffer_1.state == .free and
            self.buffer_2.state == .free;
    }

    fn find(self: *DualBuffer, state: State) ?*Buffer {
        self.assert_state();
        if (self.buffer_1.state == state) return &self.buffer_1;
        if (self.buffer_2.state == state) return &self.buffer_2;
        return null;
    }

    fn assert_state(self: *const DualBuffer) void {
        assert(!(self.buffer_1.state == .producing and self.buffer_2.state == .producing));
        assert(!(self.buffer_1.state == .consuming and self.buffer_2.state == .consuming));
        assert(!(self.buffer_1.state == .ready and self.buffer_2.state == .ready));
        maybe(self.buffer_1.state == .free and self.buffer_2.state == .free);
    }
};

/// Progress tracker message with JSON body, containing the timestamp
/// and the release version of the last acknowledged publish.
const ProgressTrackerMessage = struct {
    release: vsr.Release,
    timestamp: u64,

    fn header(self: *const ProgressTrackerMessage) nats.Encoder.Headers {
        const vtable: nats.Encoder.Headers.VTable = comptime .{
            .write = &struct {
                fn write(context: *const anyopaque, encoder: *nats.Encoder.HeadersEncoder) void {
                    const message: *const ProgressTrackerMessage = @ptrCast(@alignCast(context));
                    var release_buffer: [
                        std.fmt.count("{}", vsr.Release.from(.{
                            .major = std.math.maxInt(u16),
                            .minor = std.math.maxInt(u8),
                            .patch = std.math.maxInt(u8),
                        }))
                    ]u8 = undefined;
                    encoder.put("release", std.fmt.bufPrint(
                        &release_buffer,
                        "{}",
                        .{message.release},
                    ) catch unreachable);
                    encoder.put("timestamp", std.fmt.comptimePrint("{}", .{message.timestamp}));
                }
            }.write,
        };
        return .{ .context = self, .vtable = &vtable };
    }

    fn body(self: *const ProgressTrackerMessage) nats.Encoder.Body {
        const vtable: nats.Encoder.Body.VTable = comptime .{
            .write = &struct {
                fn write(context: *const anyopaque, buffer: []u8) usize {
                    const message: *const ProgressTrackerMessage = @ptrCast(@alignCast(context));
                    var fbs = std.io.fixedBufferStream(buffer);
                    std.json.stringify(.{
                        .release = message.release,
                        .timestamp = message.timestamp,
                    }, .{
                        .whitespace = .minified,
                    }, fbs.writer()) catch unreachable;
                    return fbs.pos;
                }
            }.write,
        };
        return .{ .context = self, .vtable = &vtable };
    }

    fn parse(message: []const u8) nats.Error!ProgressTrackerMessage {
        const parsed = try std.json.parseFromSlice(
            struct { release: []const u8, timestamp: u64 },
            std.heap.page_allocator,
            message,
            .{ .ignore_unknown_fields = true },
        );
        defer parsed.deinit();

        const release = vsr.Release.parse(parsed.value.release) catch
            fatal("Invalid progress tracker message release format.", .{});
        const timestamp = parsed.value.timestamp;
        if (!TimestampRange.valid(timestamp)) {
            fatal("Invalid progress tracker message timestamp.", .{});
        }

        return .{
            .timestamp = timestamp,
            .release = release,
        };
    }
};

/// Message with the body in the JSON schema.
pub const Message = struct {
    pub const content_type = "application/json";

    pub const json_string_size_max = size: {
        var counting_writer = std.io.countingWriter(std.io.null_writer);
        std.json.stringify(
            worse_case(Message),
            stringify_options,
            counting_writer.writer(),
        ) catch unreachable;
        break :size counting_writer.bytes_written;
    };

    const stringify_options = std.json.StringifyOptions{
        .whitespace = .minified,
        .emit_nonportable_numbers_as_strings = true,
    };

    timestamp: u64,
    type: tb.ChangeEventType,
    ledger: u32,
    transfer: struct {
        id: u128,
        amount: u128,
        pending_id: u128,
        user_data_128: u128,
        user_data_64: u64,
        user_data_32: u32,
        timeout: u32,
        code: u16,
        flags: u16,
        timestamp: u64,
    },
    debit_account: struct {
        id: u128,
        debits_pending: u128,
        debits_posted: u128,
        credits_pending: u128,
        credits_posted: u128,
        user_data_128: u128,
        user_data_64: u64,
        user_data_32: u32,
        code: u16,
        flags: u16,
        timestamp: u64,
    },
    credit_account: struct {
        id: u128,
        debits_pending: u128,
        debits_posted: u128,
        credits_pending: u128,
        credits_posted: u128,
        user_data_128: u128,
        user_data_64: u64,
        user_data_32: u32,
        code: u16,
        flags: u16,
        timestamp: u64,
    },

    pub fn init(event: *const tb.ChangeEvent) Message {
        return .{
            .timestamp = event.timestamp,
            .type = event.type,
            .ledger = event.ledger,
            .transfer = .{
                .id = event.transfer_id,
                .amount = event.transfer_amount,
                .pending_id = event.transfer_pending_id,
                .user_data_128 = event.transfer_user_data_128,
                .user_data_64 = event.transfer_user_data_64,
                .user_data_32 = event.transfer_user_data_32,
                .timeout = event.transfer_timeout,
                .code = event.transfer_code,
                .flags = @bitCast(event.transfer_flags),
                .timestamp = event.transfer_timestamp,
            },
            .debit_account = .{
                .id = event.debit_account_id,
                .debits_pending = event.debit_account_debits_pending,
                .debits_posted = event.debit_account_debits_posted,
                .credits_pending = event.debit_account_credits_pending,
                .credits_posted = event.debit_account_credits_posted,
                .user_data_128 = event.debit_account_user_data_128,
                .user_data_64 = event.debit_account_user_data_64,
                .user_data_32 = event.debit_account_user_data_32,
                .code = event.debit_account_code,
                .flags = @bitCast(event.debit_account_flags),
                .timestamp = event.debit_account_timestamp,
            },
            .credit_account = .{
                .id = event.credit_account_id,
                .debits_pending = event.credit_account_debits_pending,
                .debits_posted = event.credit_account_debits_posted,
                .credits_pending = event.credit_account_credits_pending,
                .credits_posted = event.credit_account_credits_posted,
                .user_data_128 = event.credit_account_user_data_128,
                .user_data_64 = event.credit_account_user_data_64,
                .user_data_32 = event.credit_account_user_data_32,
                .code = event.credit_account_code,
                .flags = @bitCast(event.credit_account_flags),
                .timestamp = event.credit_account_timestamp,
            },
        };
    }

    fn header(self: *const Message) nats.Encoder.Headers {
        const vtable: nats.Encoder.Headers.VTable = comptime .{
            .write = &struct {
                fn write(context: *const anyopaque, encoder: *nats.Encoder.HeadersEncoder) void {
                    const message: *const Message = @ptrCast(@alignCast(context));
                    encoder.put("event_type", @tagName(message.type));
                    encoder.put("ledger", std.fmt.comptimePrint("{}", .{message.ledger}));
                    encoder.put("transfer_code", std.fmt.comptimePrint("{}", .{message.transfer.code}));
                    encoder.put("debit_account_code", std.fmt.comptimePrint("{}", .{message.debit_account.code}));
                    encoder.put("credit_account_code", std.fmt.comptimePrint("{}", .{message.credit_account.code}));
                }
            }.write,
        };
        return .{ .context = self, .vtable = &vtable };
    }

    fn body(self: *const Message) nats.Encoder.Body {
        const vtable: nats.Encoder.Body.VTable = comptime .{
            .write = &struct {
                fn write(context: *const anyopaque, buffer: []u8) usize {
                    const message: *const Message = @ptrCast(@alignCast(context));
                    var fbs = std.io.fixedBufferStream(buffer);
                    std.json.stringify(message, .{
                        .whitespace = .minified,
                        .emit_nonportable_numbers_as_strings = true,
                    }, fbs.writer()) catch unreachable;
                    return fbs.pos;
                }
            }.write,
        };
        return .{ .context = self, .vtable = &vtable };
    }

    fn worse_case(comptime T: type) T {
        var value: T = undefined;
        for (std.meta.fields(T)) |field| {
            @field(value, field.name) = switch (@typeInfo(field.type)) {
                .int => std.math.maxInt(field.type),
                .@"enum" => max: {
                    var name: []const u8 = "";
                    for (std.enums.values(tb.ChangeEventType)) |tag| {
                        if (@tagName(tag).len > name.len) {
                            name = @tagName(tag);
                        }
                    }
                    break :max @field(field.type, name);
                },
                .@"struct" => worse_case(field.type),
                else => unreachable,
            };
        }
        return value;
    }
};

const testing = std.testing;

test "nats: DualBuffer" {
    const event_count_max = Runner.constants.event_count_max;

    var prng = stdx.PRNG.from_seed(42);
    var dual_buffer = try DualBuffer.init(testing.allocator, event_count_max);
    defer dual_buffer.deinit(testing.allocator);

    for (0..4096) |_| {
        try testing.expect(dual_buffer.all_free());

        const producer_begin = dual_buffer.producer_begin();
        try testing.expect(producer_begin);
        try testing.expect(!dual_buffer.all_free());
        try testing.expect(!dual_buffer.consumer_begin());

        const producer1_buffer = dual_buffer.get_producer_buffer();
        try testing.expectEqual(@as(usize, event_count_max), producer1_buffer.len);

        const producer1_count = prng.range_inclusive(u32, 1, event_count_max);
        prng.fill(std.mem.sliceAsBytes(producer1_buffer[0..producer1_count]));
        dual_buffer.producer_finish(producer1_count);

        const consumer_begin = dual_buffer.consumer_begin();
        try testing.expect(consumer_begin);
        try testing.expect(!dual_buffer.all_free());

        const producer_begin_concurrently = dual_buffer.producer_begin();
        try testing.expect(producer_begin_concurrently);
        try testing.expect(!dual_buffer.all_free());

        const producer2_buffer = dual_buffer.get_producer_buffer();
        try testing.expectEqual(@as(usize, event_count_max), producer2_buffer.len);

        const producer2_count = prng.range_inclusive(u32, 0, event_count_max);
        maybe(producer2_count == 0);
        prng.fill(std.mem.sliceAsBytes(producer2_buffer[0..producer2_count]));
        dual_buffer.producer_finish(producer2_count);

        const consumer_buffer = dual_buffer.get_consumer_buffer();
        try testing.expectEqual(producer1_buffer.ptr, consumer_buffer.ptr);
        try testing.expectEqual(@as(usize, producer1_count), consumer_buffer.len);
        try testing.expectEqualSlices(
            u8,
            std.mem.sliceAsBytes(producer1_buffer[0..producer1_count]),
            std.mem.sliceAsBytes(consumer_buffer),
        );
        dual_buffer.consumer_finish();

        const consumer_begin_again = dual_buffer.consumer_begin();
        if (producer2_count == 0) {
            try testing.expect(!consumer_begin_again);
            try testing.expect(dual_buffer.all_free());
            continue;
        }

        try testing.expect(consumer_begin_again);
        try testing.expect(!dual_buffer.all_free());

        const consumer2_buffer = dual_buffer.get_consumer_buffer();
        try testing.expectEqual(producer2_buffer.ptr, consumer2_buffer.ptr);
        try testing.expectEqual(@as(usize, producer2_count), consumer2_buffer.len);
        try testing.expectEqualSlices(
            u8,
            std.mem.sliceAsBytes(producer2_buffer[0..producer2_count]),
            std.mem.sliceAsBytes(consumer2_buffer),
        );

        dual_buffer.consumer_finish();
        try testing.expect(dual_buffer.all_free());
    }
}

test "nats: ProgressTrackerMessage" {
    const buffer = try testing.allocator.alloc(u8, 256);
    defer testing.allocator.free(buffer);

    const values: []const u64 = &.{
        TimestampRange.timestamp_min,
        1745055501942402250,
        TimestampRange.timestamp_max,
    };
    for (values) |value| {
        const message: ProgressTrackerMessage = .{
            .release = vsr.Release.minimum,
            .timestamp = value,
        };
        const size = message.body().write(buffer);
        const decoded_message = try ProgressTrackerMessage.parse(buffer[0..size]);
        try testing.expectEqual(message.release.value, decoded_message.release.value);
        try testing.expectEqual(message.timestamp, decoded_message.timestamp);
    }
}

test "nats: JSON message" {
    const Snap = @import("../testing/snaptest.zig").Snap;
    const snap = Snap.snap;

    const buffer = try testing.allocator.alloc(u8, Message.json_string_size_max);
    defer testing.allocator.free(buffer);

    {
        const message: Message = std.mem.zeroInit(Message, .{});
        const size = message.body().write(buffer);
        try testing.expectEqual(@as(usize, 564), size);

        try snap(@src(),
            \\{"timestamp":0,"type":"single_phase","ledger":0,"transfer":{"id":0,"amount":0,"pending_id":0,"user_data_128":0,"user_data_64":0,"user_data_32":0,"timeout":0,"code":0,"flags":0,"timestamp":0},"debit_account":{"id":0,"debits_pending":0,"debits_posted":0,"credits_pending":0,"credits_posted":0,"user_data_128":0,"user_data_64":0,"user_data_32":0,"code":0,"flags":0,"timestamp":0},"credit_account":{"id":0,"debits_pending":0,"debits_posted":0,"credits_pending":0,"credits_posted":0,"user_data_128":0,"user_data_64":0,"user_data_32":0,"code":0,"flags":0,"timestamp":0}}
        ).diff(buffer[0..size]);
    }

    {
        const message = comptime Message.worse_case(Message);
        const size = message.body().write(buffer);
        try testing.expectEqual(@as(usize, 1425), size);
        try testing.expectEqual(size, buffer.len);

        try snap(@src(),
            \\{"timestamp":"18446744073709551615","type":"two_phase_pending","ledger":4294967295,"transfer":{"id":"340282366920938463463374607431768211455","amount":"340282366920938463463374607431768211455","pending_id":"340282366920938463463374607431768211455","user_data_128":"340282366920938463463374607431768211455","user_data_64":"18446744073709551615","user_data_32":4294967295,"timeout":4294967295,"code":65535,"flags":65535,"timestamp":"18446744073709551615"},"debit_account":{"id":"340282366920938463463374607431768211455","debits_pending":"340282366920938463463374607431768211455","debits_posted":"340282366920938463463374607431768211455","credits_pending":"340282366920938463463374607431768211455","credits_posted":"340282366920938463463374607431768211455","user_data_128":"340282366920938463463374607431768211455","user_data_64":"18446744073709551615","user_data_32":4294967295,"code":65535,"flags":65535,"timestamp":"18446744073709551615"},"credit_account":{"id":"340282366920938463463374607431768211455","debits_pending":"340282366920938463463374607431768211455","debits_posted":"340282366920938463463374607431768211455","credits_pending":"340282366920938463463374607431768211455","credits_posted":"340282366920938463463374607431768211455","user_data_128":"340282366920938463463374607431768211455","user_data_64":"18446744073709551615","user_data_32":4294967295,"code":65535,"flags":65535,"timestamp":"18446744073709551615"}}
        ).diff(buffer);
    }
}
