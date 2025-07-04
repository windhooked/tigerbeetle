const std = @import("std");
const log = std.log.scoped(.nats);
const IO = @import("../vsr/io.zig").IO;
const stdx = @import("../vsr/stdx.zig");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const fatal = @import("protocol.zig").fatal;

const c = @cImport({
    @cInclude("nats/nats.h");
});

// Map natsStatus to Zig errors
pub const Error = error{
    ConnectionFailed,
    StreamCreationFailed,
    PublishFailed,
    GetMessageFailed,
    AckFailed,
    InvalidMessage,
    Timeout,
    OutOfMemory,
};

fn natsStatusToError(status: c.natsStatus) Error!void {
    switch (status) {
        c.NATS_OK => return,
        c.NATS_CONNECTION_CLOSED => return error.ConnectionFailed,
        c.NATS_TIMEOUT => return error.Timeout,
        c.NATS_NO_MEMORY => return error.OutOfMemory,
        else => fatal("NATS error: {}", .{status}),
    }
}

pub const Encoder = struct {
    pub const Headers = struct {
        context: *const anyopaque,
        vtable: *const VTable,

        pub const VTable = struct {
            write: *const fn (context: *const anyopaque, encoder: *HeadersEncoder) void,
        };

        pub fn write(self: Headers, encoder: *HeadersEncoder) void {
            self.vtable.write(self.context, encoder);
        }
    };

    pub const HeadersEncoder = struct {
        msg: *c.natsMsg,

        pub fn init(msg: *c.natsMsg) HeadersEncoder {
            return .{ .msg = msg };
        }

        pub fn put(self: *HeadersEncoder, key: []const u8, value: []const u8) void {
            const key_c = @as([*c]const u8, @ptrCast(key));
            const value_c = @as([*c]const u8, @ptrCast(value));
            const status = c.natsMsg_SetHeader(self.msg, key_c, value_c);
            if (status != c.NATS_OK) fatal("Failed to set header: {}", .{status});
        }
    };

    pub const Body = struct {
        context: *const anyopaque,
        vtable: *const VTable,

        pub const VTable = struct {
            write: *const fn (context: *const anyopaque, buffer: []u8) usize,
        };

        pub fn write(self: Body, buffer: []u8) usize {
            return self.vtable.write(self.context, buffer);
        }
    };
};

pub const GetMessageResult = struct {
    sequence: u64,
    message: []const u8,
};

pub const Client = struct {
    const Self = @This();
    const MessageQueue = std.ArrayList(struct {
        subject: []const u8,
        body: Encoder.Body,
        headers: Encoder.Headers,
    });

    io: *IO,
    message_count_max: u32,
    message_body_size_max: usize,
    reply_timeout_ticks: u64,
    conn: ?*c.natsConnection,
    messages: MessageQueue,
    completion: IO.Completion = undefined,
    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        options: struct {
            io: *IO,
            message_count_max: u32,
            message_body_size_max: usize,
            reply_timeout_ticks: u64,
        },
    ) !Self {
        assert(options.message_count_max > 0);
        assert(options.message_body_size_max > 0);
        assert(options.reply_timeout_ticks > 0);

        var messages = try MessageQueue.initCapacity(allocator, options.message_count_max);
        errdefer messages.deinit();

        return .{
            .io = options.io,
            .message_count_max = options.message_count_max,
            .message_body_size_max = options.message_body_size_max,
            .reply_timeout_ticks = options.reply_timeout_ticks,
            .conn = null,
            .messages = messages,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        if (self.conn) |conn| c.natsConnection_Destroy(conn);
        self.messages.deinit();
    }

    pub fn connect(
        self: *Self,
        callback: *const fn (context: *Self) void,
        options: struct {
            host: std.net.Address,
            user: ?[]const u8,
            password: ?[]const u8,
        },
    ) !void {
        assert(self.conn == null);

        var conn: *c.natsConnection = undefined;
        var opts: *c.natsOptions = undefined;
        try natsStatusToError(c.natsOptions_Create(&opts));
        defer c.natsOptions_Destroy(opts);

        const host_str = try std.fmt.allocPrintZ(self.allocator, "{}", .{options.host});
        defer self.allocator.free(host_str);
        try natsStatusToError(c.natsOptions_SetURL(opts, host_str.ptr));

        if (options.user != null and options.password != null) {
            try natsStatusToError(c.natsOptions_SetUserInfo(
                opts,
                options.user.?.ptr,
                options.password.?.ptr,
            ));
        }

        // Set async callback for connection events
        try natsStatusToError(c.natsOptions_SetClosedCB(
            opts,
            struct {
                fn closed_cb(conn: *c.natsConnection, data: ?*anyopaque) callconv(.C) void {
                    _ = conn;
                    _ = data;
                    fatal("NATS connection closed unexpectedly.", .{});
                }
            }.closed_cb,
            null,
        ));

        self.io.submit(
            *Self,
            self,
            struct {
                fn callback(client: *Self, completion: *IO.Completion, result: IO.SubmitError!void) void {
                    _ = completion;
                    result catch fatal("Failed to submit connection operation.", .{});

                    var status = c.natsConnection_Connect(&client.conn, opts);
                    if (status == c.NATS_OK) {
                        log.info("Connected to NATS server.", .{});
                        callback(client);
                    } else {
                        client.conn = null;
                        fatal("Failed to connect to NATS: {}", .{status});
                    }
                }
            }.callback,
            &self.completion,
        );
    }

    pub fn stream_create(
        self: *Self,
        callback: *const fn (context: *Self) void,
        options: struct {
            name: []const u8,
            subjects: []const []const u8,
            retention: enum { limits, interest },
            max_msgs: u64,
            discard: enum { old, new },
            storage: enum { file, memory },
        },
    ) void {
        assert(self.conn != null);

        var cfg: c.jsStreamConfig = std.mem.zeroes(c.jsStreamConfig);
        cfg.Name = @as([*c]u8, @ptrCast(options.name.ptr));
        cfg.Subjects = @as([*c][*c]u8, @ptrCast(options.subjects.ptr));
        cfg.SubjectsLen = @intCast(options.subjects.len);
        cfg.Retention = switch (options.retention) {
            .limits => c.js_LimitsRetention,
            .interest => c.js_InterestRetention,
        };
        cfg.Discard = switch (options.discard) {
            .old => c.js_DiscardOld,
            .new => c.js_DiscardNew,
        };
        cfg.Storage = switch (options.storage) {
            .file => c.js_FileStorage,
            .memory => c.js_MemoryStorage,
        };
        cfg.MaxMsgs = @intCast(options.max_msgs);

        self.io.submit(
            *Self,
            self,
            struct {
                fn callback(client: *Self, completion: *IO.Completion, result: IO.SubmitError!void) void {
                    _ = completion;
                    result catch fatal("Failed to submit stream creation.", .{});

                    var stream: *c.jsStream = undefined;
                    const status = c.jsStream_Create(&stream, client.conn, &cfg);
                    if (status == c.NATS_OK) {
                        c.jsStream_Destroy(stream);
                        callback(client);
                    } else {
                        fatal("Failed to create stream: {}", .{status});
                    }
                }
            }.callback,
            &self.completion,
        );
    }

    pub fn publish_enqueue(
        self: *Self,
        options: struct {
            subject: []const u8,
            body: Encoder.Body,
            headers: Encoder.Headers,
        },
    ) void {
        assert(self.messages.items.len < self.message_count_max);
        self.messages.append(.{
            .subject = options.subject,
            .body = options.body,
            .headers = options.headers,
        }) catch fatal("Failed to enqueue message.", .{});
    }

    pub fn publish_send(
        self: *Self,
        callback: *const fn (context: *Self) void,
    ) void {
        assert(self.conn != null);
        assert(self.messages.items.len > 0);

        var buffer: [1024 * 1024]u8 = undefined;

        self.io.submit(
            *Self,
            self,
            struct {
                fn callback(client: *Self, completion: *IO.Completion, result: IO.SubmitError!void) void {
                    _ = completion;
                    result catch fatal("Failed to submit publish operation.", .{});

                    for (client.messages.items) |msg| {
                        var nats_msg: *c.natsMsg = undefined;
                        const body_size = msg.body.write(buffer[0..client.message_body_size_max]);
                        const body = buffer[0..body_size];

                        var status = c.natsMsg_Create(
                            &nats_msg,
                            @as([*c]u8, @ptrCast(msg.subject.ptr)),
                            null,
                            @as([*c]u8, @ptrCast(body.ptr)),
                            @intCast(body.len),
                        );
                        if (status != c.NATS_OK) fatal("Failed to create natsMsg: {}", .{status});

                        var headers_encoder = Encoder.HeadersEncoder.init(nats_msg);
                        msg.headers.write(&headers_encoder);

                        status = c.natsConnection_PublishMsg(client.conn, nats_msg);
                        c.natsMsg_Destroy(nats_msg);
                        if (status != c.NATS_OK) fatal("Failed to publish message: {}", .{status});
                    }

                    client.messages.clearRetainingCapacity();
                    callback(client);
                }
            }.callback,
            &self.completion,
        );
    }

    pub fn get_message(
        self: *Self,
        callback: *const fn (context: *Self, found: ?GetMessageResult) Error!void,
        options: struct {
            stream: []const u8,
            consumer: []const u8,
        },
    ) void {
        assert(self.conn != null);

        self.io.submit(
            *Self,
            self,
            struct {
                fn callback(client: *Self, completion: *IO.Completion, result: IO.SubmitError!void) void {
                    _ = completion;
                    result catch fatal("Failed to submit get message operation.", .{});

                    var consumer: *c.jsConsumer = undefined;
                    var status = c.jsConsumer_Create(
                        &consumer,
                        client.conn,
                        @as([*c]u8, @ptrCast(options.stream.ptr)),
                        @as([*c]u8, @ptrCast(options.consumer.ptr)),
                        null,
                    );
                    if (status != c.NATS_OK) fatal("Failed to create consumer: {}", .{status});
                    defer c.jsConsumer_Destroy(consumer);

                    var msg: *c.natsMsg = undefined;
                    status = c.jsConsumer_FetchOne(
                        consumer,
                        &msg,
                        @intCast(client.reply_timeout_ticks * std.time.ns_per_ms),
                    );
                    if (status == c.NATS_OK) {
                        defer c.natsMsg_Destroy(msg);
                        const data = c.natsMsg_GetData(msg);
                        const data_len = c.natsMsg_GetDataLength(msg);
                        const seq = c.natsMsg_GetSequence(msg);
                        const result = GetMessageResult{
                            .sequence = seq,
                            .message = data[0..data_len],
                        };
                        callback(client, result) catch fatal("Failed to process message.", .{});
                    } else if (status == c.NATS_NOT_FOUND or status == c.NATS_TIMEOUT) {
                        callback(client, null) catch fatal("Failed to process empty message.", .{});
                    } else {
                        fatal("Failed to fetch message: {}", .{status});
                    }
                }
            }.callback,
            &self.completion,
        );
    }

    pub fn ack_message(
        self: *Self,
        callback: *const fn (context: *Self) void,
        options: struct {
            stream: []const u8,
            sequence: u64,
        },
    ) void {
        assert(self.conn != null);

        self.io.submit(
            *Self,
            self,
            struct {
                fn callback(client: *Self, completion: *IO.Completion, result: IO.SubmitError!void) void {
                    _ = completion;
                    result catch fatal("Failed to submit ack operation.", .{});

                    var js: *c.jsCtx = undefined;
                    var status = c.jsCtx_Create(&js, client.conn);
                    if (status != c.NATS_OK) fatal("Failed to create JetStream context: {}", .{status});
                    defer c.jsCtx_Destroy(js);

                    status = c.js_AckMsg(js, options.sequence);
                    if (status == c.NATS_OK) {
                        callback(client);
                    } else {
                        fatal("Failed to ack message: {}", .{status});
                    }
                }
            }.callback,
            &self.completion,
        );
    }

    pub fn tick(self: *Self) void {
        self.io.run_for_ns(0) catch |err| fatal("IO error: {}", .{err});
        if (self.conn != null) {
            var status = c.natsConnection_FlushTimeout(
                self.conn,
                @intCast(self.reply_timeout_ticks * std.time.ns_per_ms / 1000),
            );
            if (status != c.NATS_OK) fatal("Failed to flush connection: {}", .{status});
        }
    }
};
