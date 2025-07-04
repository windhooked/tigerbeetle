const std = @import("std");
const log = std.log.scoped(.nats);
const IO = @import("../vsr/io.zig").IO;
const stdx = @import("../vsr/stdx.zig");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const fatal = @import("protocol.zig").fatal;

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
        buffer: []u8,
        index: usize,

        pub fn init(buffer: []u8) HeadersEncoder {
            return .{ .buffer = buffer, .index = 0 };
        }

        pub fn put(self: *HeadersEncoder, key: []const u8, value: []const u8) void {
            const required = key.len + value.len + 4; // Key len (u16) + value len (u16)
            assert(self.index + required <= self.buffer.len);

            std.mem.writeLE(u16, self.buffer[self.index..][0..2], @intCast(key.len));
            self.index += 2;
            @memcpy(self.buffer[self.index..][0..key.len], key);
            self.index += key.len;

            std.mem.writeLE(u16, self.buffer[self.index..][0..2], @intCast(value.len));
            self.index += 2;
            @memcpy(self.buffer[self.index..][0..value.len], value);
            self.index += value.len;
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
    socket: ?std.posix.fd_t = null,
    messages: MessageQueue,
    completion: IO.Completion = undefined,

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
            .messages = messages,
        };
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        if (self.socket) |fd| std.posix.close(fd);
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
        assert(self.socket == null);
        const fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
        errdefer std.posix.close(fd);

        self.socket = fd;
        self.io.connect(
            *Self,
            self,
            struct {
                fn callback(
                    client: *Self,
                    completion: *IO.Completion,
                    result: IO.ConnectError!void,
                ) void {
                    _ = completion;
                    if (result) |_| {
                        // Simulate N nATS CONNECT command with optional auth
                        const connect_cmd = if (options.user != null and options.password != null)
                            std.fmt.allocPrint(
                                client.messages.allocator,
                                "CONNECT {{\"user\":\"{s}\",\"pass\":\"{s}\"}}\r\n",
                                .{ options.user.?, options.password.? },
                            ) catch fatal("Failed to format CONNECT command.", .{})
                        else
                            "CONNECT {}\r\n";
                        defer if (options.user != null) client.messages.allocator.free(connect_cmd);

                        client.io.send(
                            *Self,
                            client,
                            struct {
                                fn callback(
                                    c: *Self,
                                    cmp: *IO.Completion,
                                    res: IO.SendError!usize,
                                ) void {
                                    _ = cmp;
                                    res catch fatal("Failed to send CONNECT command.", .{});
                                    callback(c);
                                }
                            }.callback,
                            &client.completion,
                            client.socket.?,
                            connect_cmd,
                        );
                    } else |err| {
                        std.posix.close(client.socket.?);
                        client.socket = null;
                        fatal("Connection failed: {}", .{err});
                    }
                }
            }.callback,
            &self.completion,
            fd,
            options.host,
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
        assert(self.socket != null);
        const retention = switch (options.retention) {
            .limits => "Limits",
            .interest => "Interest",
        };
        const discard = switch (options.discard) {
            .old => "DiscardOld",
            .new => "DiscardNew",
        };
        const storage = switch (options.storage) {
            .file => "File",
            .memory => "Memory",
        };

        var subjects_buf: [1024]u8 = undefined;
        var subjects_writer = std.io.fixedBufferStream(&subjects_buf);
        subjects_writer.writer().writeAll("[") catch unreachable;
        for (options.subjects, 0..) |subject, i| {
            if (i > 0) subjects_writer.writer().writeAll(",") catch unreachable;
            subjects_writer.writer().writeAll("\"") catch unreachable;
            subjects_writer.writer().writeAll(subject) catch unreachable;
            subjects_writer.writer().writeAll("\"") catch unreachable;
        }
        subjects_writer.writer().writeAll("]") catch unreachable;

        const cmd = std.fmt.allocPrint(
            self.messages.allocator,
            "STREAM.CREATE {{\"name\":\"{s}\",\"subjects\":{s},\"retention\":\"{s}\",\"max_msgs\":{},\"discard\":\"{s}\",\"storage\":\"{s}\"}}\r\n",
            .{
                options.name,
                subjects_buf[0..subjects_writer.pos],
                retention,
                options.max_msgs,
                discard,
                storage,
            },
        ) catch fatal("Failed to format STREAM.CREATE command.", .{});
        defer self.messages.allocator.free(cmd);

        self.io.send(
            *Self,
            self,
            struct {
                fn callback(
                    client: *Self,
                    completion: *IO.Completion,
                    result: IO.SendError!usize,
                ) void {
                    _ = completion;
                    result catch fatal("Failed to send STREAM.CREATE command.", .{});
                    // Simulate receiving +OK response
                    callback(client);
                }
            }.callback,
            &self.completion,
            self.socket.?,
            cmd,
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
        assert(self.socket != null);
        assert(self.messages.items.len > 0);

        var buffer: [1024 * 1024]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buffer);
        var writer = fbs.writer();

        for (self.messages.items) |msg| {
            var headers_buf: [1024]u8 = undefined;
            var headers_encoder = Encoder.HeadersEncoder.init(&headers_buf);
            msg.headers.write(&headers_encoder);

            const body_size = msg.body.write(buffer[fbs.pos..]);
            writer.writeAll("HPUB ") catch unreachable;
            writer.writeAll(msg.subject) catch unreachable;
            writer.writeAll(" ") catch unreachable;
            writer.writeInt(u32, headers_encoder.index, .little) catch unreachable;
            writer.writeAll(" ") catch unreachable;
            writer.writeInt(u32, body_size, .little) catch unreachable;
            writer.writeAll("\r\n") catch unreachable;
            writer.writeAll(headers_buf[0..headers_encoder.index]) catch unreachable;
            writer.writeAll(buffer[fbs.pos..][0..body_size]) catch unreachable;
            writer.writeAll("\r\n") catch unreachable;
            fbs.pos += body_size;
        }

        self.io.send(
            *Self,
            self,
            struct {
                fn callback(
                    client: *Self,
                    completion: *IO.Completion,
                    result: IO.SendError!usize,
                ) void {
                    _ = completion;
                    result catch fatal("Failed to publish messages.", .{});
                    client.messages.clearRetainingCapacity();
                    callback(client);
                }
            }.callback,
            &self.completion,
            self.socket.?,
            buffer[0..fbs.pos],
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
        assert(self.socket != null);
        const cmd = std.fmt.allocPrint(
            self.messages.allocator,
            "CONSUMER.NEXT {{\"stream\":\"{s}\",\"consumer\":\"{s}\",\"max_msgs\":1}}\r\n",
            .{ options.stream, options.consumer },
        ) catch fatal("Failed to format CONSUMER.NEXT command.", .{});
        defer self.messages.allocator.free(cmd);

        self.io.send(
            *Self,
            self,
            struct {
                fn callback(
                    client: *Self,
                    completion: *IO.Completion,
                    result: IO.SendError!usize,
                ) void {
                    _ = completion;
                    result catch fatal("Failed to send CONSUMER.NEXT command.", .{});
                    // Simulate receiving message or empty response
                    var buffer: [1024]u8 = undefined;
                    const message: ?GetMessageResult = if (std.math.random.bool())
                        .{ .sequence = 1, .message = buffer[0..100] }
                    else
                        null;
                    callback(client, message) catch fatal("Failed to process message.", .{});
                }
            }.callback,
            &self.completion,
            self.socket.?,
            cmd,
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
        assert(self.socket != null);
        const cmd = std.fmt.allocPrint(
            self.messages.allocator,
            "CONSUMER.ACK {{\"stream\":\"{s}\",\"sequence\":{}}}\r\n",
            .{ options.stream, options.sequence },
        ) catch fatal("Failed to format CONSUMER.ACK command.", .{});
        defer self.messages.allocator.free(cmd);

        self.io.send(
            *Self,
            self,
            struct {
                fn callback(
                    client: *Self,
                    completion: *IO.Completion,
                    result: IO.SendError!usize,
                ) void {
                    _ = completion;
                    result catch fatal("Failed to send CONSUMER.ACK command.", .{});
                    callback(client);
                }
            }.callback,
            &self.completion,
            self.socket.?,
            cmd,
        );
    }

    pub fn tick(self: *Self) void {
        // Process any pending I/O events
        self.io.run_for_ns(0) catch |err| fatal("IO error: {}", .{err});
    }
};
