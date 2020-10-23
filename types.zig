const std = @import("std");
const assert = std.debug.assert;
const builtin = std.builtin;
const crypto = std.crypto;
const mem = std.mem;

pub const Command = packed enum(u32) {
    reserved,
    ack,
    create_accounts,
    create_transfers,
    commit_transfers,
};

pub const Account = packed struct {
                       id: u128,
                   custom: u128,
                    flags: AccountFlags,
                     unit: u64,
           debit_reserved: u64,
           debit_accepted: u64,
          credit_reserved: u64,
          credit_accepted: u64,
     debit_reserved_limit: u64,
     debit_accepted_limit: u64,
    credit_reserved_limit: u64,
    credit_accepted_limit: u64,
                  padding: u64,
                timestamp: u64,

    pub inline fn exceeds(balance: u64, amount: u64, limit: u64) bool {
        return limit > 0 and balance + amount > limit;
    }

    pub inline fn exceeds_debit_reserved_limit(self: *const Account, amount: u64) bool {
        return Account.exceeds(self.debit_reserved, amount, self.debit_reserved_limit);
    }

    pub inline fn exceeds_debit_accepted_limit(self: *const Account, amount: u64) bool {
        return Account.exceeds(self.debit_accepted, amount, self.debit_accepted_limit);
    }

    pub inline fn exceeds_credit_reserved_limit(self: *const Account, amount: u64) bool {
        return Account.exceeds(self.credit_reserved, amount, self.credit_reserved_limit);
    }

    pub inline fn exceeds_credit_accepted_limit(self: *const Account, amount: u64) bool {
        return Account.exceeds(self.credit_accepted, amount, self.credit_accepted_limit);
    }
};

pub const AccountFlags = packed struct {
    reserved: u64 = 0,
};

pub const Transfer = packed struct {
                   id: u128,
     debit_account_id: u128,
    credit_account_id: u128,
             custom_1: u128,
             custom_2: u128,
             custom_3: u128,
                flags: TransferFlags,
               amount: u64,
              timeout: u64,
            timestamp: u64,
};

pub const TransferFlags = packed struct {
         accept: bool = false,
         reject: bool = false,
    auto_commit: bool = false,
       reserved: u61 = 0,
};

pub const Commit = packed struct {
           id: u128,
     custom_1: u128,
     custom_2: u128,
     custom_3: u128,
        flags: CommitFlags,
    timestamp: u64,
};

pub const CommitFlags = packed struct {
      accept: bool = false,
      reject: bool = false,
    reserved: u62 = 0,
};

pub const CreateAccountResult = packed enum(u32) {
    ok,
    exists,
    exists_with_different_unit,
    exists_with_different_limits,
    exists_with_different_custom_field,
    exists_with_different_flags,
    reserved_field_custom,
    reserved_field_padding,
    reserved_field_timestamp,
    reserved_flag,
    exceeds_debit_reserved_limit,
    exceeds_debit_accepted_limit,
    exceeds_credit_reserved_limit,
    exceeds_credit_accepted_limit,
    debit_reserved_limit_exceeds_debit_accepted_limit,
    credit_reserved_limit_exceeds_credit_accepted_limit,
};

pub const CreateTransferResult = packed enum(u32) {
    ok,
    exists,
    exists_with_different_debit_account_id,
    exists_with_different_credit_account_id,
    exists_with_different_custom_fields,
    exists_with_different_amount,
    exists_with_different_timeout,
    exists_with_different_flags,
    exists_and_already_committed_and_accepted,
    exists_and_already_committed_and_rejected,
    reserved_field_custom,
    reserved_field_timestamp,
    reserved_flag,
    reserved_flag_accept,
    reserved_flag_reject,
    debit_account_not_found,
    credit_account_not_found,
    accounts_are_the_same,
    accounts_have_different_units,
    amount_is_zero,
    exceeds_debit_reserved_limit,
    exceeds_debit_accepted_limit,
    exceeds_credit_reserved_limit,
    exceeds_credit_accepted_limit,
    auto_commit_must_accept,
    auto_commit_cannot_timeout,
};

pub const CommitTransferResult = packed enum(u32) {
    ok,
    reserved_field_custom,
    reserved_field_timestamp,
    reserved_flag,
    commit_must_accept_or_reject,
    commit_cannot_accept_and_reject,
    transfer_not_found,
    transfer_expired,
    already_auto_committed,
    already_committed,
    already_committed_but_accepted,
    already_committed_but_rejected,
    debit_account_not_found,
    credit_account_not_found,
    debit_amount_was_not_reserved,
    credit_amount_was_not_reserved,
    exceeds_debit_accepted_limit,
    exceeds_credit_accepted_limit,
};

pub const CreateAccountResults = packed struct {
     index: u32,
    result: CreateAccountResult,
};

pub const CreateTransferResults = packed struct {
     index: u32,
    result: CreateTransferResult,
};

pub const CommitTransferResults = packed struct {
     index: u32,
    result: CommitTransferResult,
};

pub const Magic: u64 = @byteSwap(u64, 0x0a_5ca1ab1e_bee11e); // "A scalable beetle..."

pub const JournalHeader = packed struct {
                          // This hash chain root covers all entry checksums in the journal:
                          // 1. to protect against journal tampering, and
                          // 2. to show our exact history when determining consensus across nodes.
         hash_chain_root: u128 = undefined,
                          // This binds this journal entry with the previous journal entry:
                          // 1. to protect against misdirected reads/writes by hardware, and
                          // 2. to enable "relaxed lock step" quorum across the cluster, enabling
                          //    nodes to form a quorum provided their hash chain roots can be
                          //    linked together in a directed acyclic graph by a topological sort,
                          //    i.e. a node can be one hash chain root behind another to accomodate
                          //    crashes without losing quorum.
    prev_hash_chain_root: u128,
                          // This checksum covers this entry's magic, command, size and data:
                          // 1. to protect against torn writes and provide crash safety, and
                          // 2. to protect against eventual disk corruption.
                checksum: u128 = undefined,
                   magic: u64 = Magic,
                 command: Command,
                          // This is the size of this entry's header and data:
                          // 1. also covered by the checksum, and
                          // 2. excluding additional zero byte padding for disk sector alignment,
                          //    which is necessary for direct I/O, to reduce copies in the kernel,
                          //    and improve write throughput by up to 10%.
                          //    e.g. If we write a journal entry for a single transfer of 192 bytes
                          //    (64 + 128), we will actually write 4096 bytes, which is the minimum
                          //    sector size to work with Advanced Format disks. The size will be 192
                          //    bytes, covered by the checksum, and the rest will be zero bytes.
                    size: u32,

    pub fn calculate_checksum(self: *const JournalHeader, entry: []const u8) u128 {
        assert(entry.len >= @sizeOf(JournalHeader));
        assert(entry.len == self.size);

        const checksum_offset = @byteOffsetOf(JournalHeader, "checksum");
        const checksum_size = @sizeOf(@TypeOf(self.checksum));
        assert(checksum_offset == 0 + 16 + 16);
        assert(checksum_size == 16);

        var target: [32]u8 = undefined;
        crypto.hash.Blake3.hash(entry[checksum_offset + checksum_size..], target[0..], .{});
        return @bitCast(u128, target[0..checksum_size].*);
    }

    pub fn calculate_hash_chain_root(self: *const JournalHeader) u128 {
        const hash_chain_root_size = @sizeOf(@TypeOf(self.hash_chain_root));
        assert(hash_chain_root_size == 16);

        const prev_hash_chain_root_offset = @byteOffsetOf(JournalHeader, "prev_hash_chain_root");
        const prev_hash_chain_root_size = @sizeOf(@TypeOf(self.prev_hash_chain_root));
        assert(prev_hash_chain_root_offset == 0 + 16);
        assert(prev_hash_chain_root_size == 16);

        const checksum_offset = @byteOffsetOf(JournalHeader, "checksum");
        const checksum_size = @sizeOf(@TypeOf(self.checksum));
        assert(checksum_offset == 0 + 16 + 16);
        assert(checksum_size == 16);

        assert(prev_hash_chain_root_offset + prev_hash_chain_root_size == checksum_offset);
        
        const header = @bitCast([@sizeOf(JournalHeader)]u8, self.*);
        const source = header[prev_hash_chain_root_offset..checksum_offset + checksum_size];
        assert(source.len == prev_hash_chain_root_size + checksum_size);
        var target: [32]u8 = undefined;
        crypto.hash.Blake3.hash(source, target[0..], .{});
        // TODO This is a workaround for a compiler bug, where passing the array in place segfaults:
        // https://github.com/ziglang/zig/issues/6781
        var array = target[0..hash_chain_root_size].*;
        return @bitCast(u128, array);
    }

    pub fn set_checksum_and_hash_chain_root(self: *JournalHeader, entry: []const u8) void {
        self.checksum = self.calculate_checksum(entry);
        // The hash_chain_root below depends on:
        // 1. the prev_hash_chain_root having been set, and
        // 2. the checksum having been calculated and set (above).
        self.hash_chain_root = self.calculate_hash_chain_root();
    }

    pub fn valid_checksum(self: *const JournalHeader, entry: []const u8) bool {
        return self.checksum == self.calculate_checksum(entry);
    }

    pub fn valid_hash_chain_root(self: *const JournalHeader) bool {
        return self.hash_chain_root == self.calculate_hash_chain_root();
    }
};

pub const NetworkHeader = packed struct {
    checksum_meta: u128 = undefined,
    checksum_data: u128 = undefined,
               id: u128,
            magic: u64 = Magic,
          command: Command,
             size: u32,

    pub fn calculate_checksum_meta(self: *const NetworkHeader) u128 {
        const meta = @bitCast([@sizeOf(NetworkHeader)]u8, self.*);
        const checksum_size = @sizeOf(@TypeOf(self.checksum_meta));
        assert(checksum_size == 16);
        var target: [32]u8 = undefined;
        crypto.hash.Blake3.hash(meta[checksum_size..], target[0..], .{});
        return @bitCast(u128, target[0..checksum_size].*);
    }

    pub fn calculate_checksum_data(self: *const NetworkHeader, data: []const u8) u128 {
        assert(@sizeOf(NetworkHeader) + data.len == self.size);
        const checksum_size = @sizeOf(@TypeOf(self.checksum_data));
        assert(checksum_size == 16);
        var target: [32]u8 = undefined;
        crypto.hash.Blake3.hash(data[0..], target[0..], .{});
        return @bitCast(u128, target[0..checksum_size].*);
    }

    pub fn set_checksum_meta(self: *NetworkHeader) void {
        self.checksum_meta = self.calculate_checksum_meta();
    }

    pub fn set_checksum_data(self: *NetworkHeader, data: []const u8) void {
        self.checksum_data = self.calculate_checksum_data(data);
    }

    pub fn valid_checksum_meta(self: *const NetworkHeader) bool {
        return self.checksum_meta == self.calculate_checksum_meta();
    }

    pub fn valid_checksum_data(self: *const NetworkHeader, data: []const u8) bool {
        return self.checksum_data == self.calculate_checksum_data(data);
    }

    pub fn valid_size(self: *const NetworkHeader) bool {
        if (self.size < @sizeOf(NetworkHeader)) return false;
        const data_size = self.size - @sizeOf(NetworkHeader);
        const type_size: usize = switch (self.command) {
            .reserved => unreachable,
            .ack => 8,
            .create_accounts => @sizeOf(Account),
            .create_transfers => @sizeOf(Transfer),
            .commit_transfers => @sizeOf(Commit)
        };
        const min_count: usize = switch (self.command) {
            .reserved => unreachable,
            .ack => 0,
            .create_accounts => 1,
            .create_transfers => 1,
            .commit_transfers => 1
        };
        return (
            @mod(data_size, type_size) == 0 and
            @divExact(data_size, type_size) >= min_count
        );
    }
};

comptime {
    if (builtin.os.tag != .linux) @compileError("linux required for io_uring");
    
    // We require little-endian architectures everywhere for efficient network deserialization:
    if (builtin.endian != builtin.Endian.Little) @compileError("big-endian systems not supported");
}

const testing = std.testing;

test "magic" {
    testing.expectEqualSlices(
        u8,
        ([_]u8{ 0x0a, 0x5c, 0xa1, 0xab, 0x1e, 0xbe, 0xe1, 0x1e })[0..],
        mem.toBytes(Magic)[0..]
    );
}

test "data structure sizes" {
    testing.expectEqual(@as(usize, 4), @sizeOf(Command));
    testing.expectEqual(@as(usize, 8), @sizeOf(AccountFlags));
    testing.expectEqual(@as(usize, 128), @sizeOf(Account));
    testing.expectEqual(@as(usize, 8), @sizeOf(TransferFlags));
    testing.expectEqual(@as(usize, 128), @sizeOf(Transfer));
    testing.expectEqual(@as(usize, 8), @sizeOf(CommitFlags));
    testing.expectEqual(@as(usize, 80), @sizeOf(Commit));
    testing.expectEqual(@as(usize, 8), @sizeOf(CreateAccountResults));
    testing.expectEqual(@as(usize, 8), @sizeOf(CreateTransferResults));
    testing.expectEqual(@as(usize, 8), @sizeOf(CommitTransferResults));
    testing.expectEqual(@as(usize, 8), @sizeOf(@TypeOf(Magic)));
    testing.expectEqual(@as(usize, 64), @sizeOf(JournalHeader));
    testing.expectEqual(@as(usize, 64), @sizeOf(NetworkHeader));

    // We swap the network header for a journal header so they must be the same size:
    testing.expectEqual(@sizeOf(JournalHeader), @sizeOf(NetworkHeader));
}
