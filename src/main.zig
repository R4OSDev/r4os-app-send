const std = @import("std");
const r4os = @import("r4os");

const MAX_HEADER: usize = 128;
const MAX_FILE: usize = 512 * 1024;
const DEFAULT_REMOTE_NAME = "R4SEND.BIN";

const Options = struct {
    target_text: []const u8,
    target: r4os.abi.NetworkEndpoint,
    port: u16,
    path: []const u8,
    remote_name: []const u8,
    resolved: bool,
    resume_mode: bool,
    retries: u32,
};

const App = struct {
    app: *r4os.App2,
    sys: r4os.r4sys.Context,
    output: *r4os.launch_io.Output,
    network: *r4os.network_stream.Session,
    budget: *r4os.rpc_budget.Budget,
    args: []const u8,
    family: u32,

    fn write(self: *const App, value: []const u8) void {
        self.output.write(value);
    }
    fn printU64(self: *const App, value: u64) void {
        self.output.number(value);
    }
    fn fileInfo(self: *const App, path: [*:0]const u8) ?r4os.abi.FileInfo {
        return switch ((r4os.Files{ .sys = self.sys }).info(.{ .ptr = path, .len = @intCast(std.mem.span(path).len) })) {
            .value => |value| value,
            else => null,
        };
    }
};

pub fn r4_app_main(app: *r4os.App2) i32 {
    var output: r4os.launch_io.Output = undefined;
    if (r4os.launch_io.Output.init(app, &output) != 0) return 1;
    var selected_family: u32 = 0;
    const args = r4os.network_stream.arguments(app.args(), &selected_family) orelse {
        output.write("SEND: invalid argument grammar\r\n");
        _ = output.close();
        return 1;
    };
    var budget = r4os.rpc_budget.Budget.fromTimeout(output.clock.read, .{ .kind = 1, .nanoseconds = 30_000_000_000 }) catch {
        _ = output.close();
        return 1;
    };
    var network: r4os.network_stream.Session = undefined;
    const opened = r4os.network_stream.Session.open(app, &budget, &network);
    if (opened != 0) {
        output.write("SEND: TCPSVC unavailable or invalid\r\n");
        _ = output.close();
        return 1;
    }
    const ctx: App = .{ .app = app, .sys = app.system(), .output = &output, .network = &network, .budget = &budget, .args = args, .family = selected_family };
    const result = run(&ctx);
    const closed = network.close();
    if (closed != 0) output.write("SEND close: not confirmed\r\n");
    const io_closed = output.close();
    return if (output.failed or closed != 0 or io_closed != 0) 1 else result;
}

fn run(ctx: *const App) i32 {
    const args = trim(ctx.args);
    const options = parseOptions(ctx, args) orelse {
        usage(ctx);
        return 1;
    };

    var path_buf: [128:0]u8 = .{0} ** 128;
    const path_z = copyZ(path_buf[0..], options.path) orelse {
        ctx.write("SEND: path-too-long\r\n");
        return 1;
    };

    const info = ctx.fileInfo(path_z) orelse {
        ctx.write("SEND: file not found\r\n");
        return 1;
    };
    if (info.is_dir != 0) {
        ctx.write("SEND: source is directory\r\n");
        return 1;
    }
    if (info.size > MAX_FILE) {
        ctx.write("SEND: file too large for current transfer stage\r\n");
        return 1;
    }

    const file_size: usize = @intCast(info.size);
    const allocator = ctx.app.allocator() orelse {
        ctx.write("SEND: snapshot memory unavailable\r\n");
        return 1;
    };
    const snapshot = allocator.alloc(u8, file_size) catch {
        ctx.write("SEND: snapshot allocation failed\r\n");
        return 1;
    };
    defer allocator.free(snapshot);
    const sum = loadSnapshot(ctx, path_z, snapshot) orelse {
        ctx.write("SEND read: failed or source size changed\r\n");
        return 1;
    };

    var header_buf: [MAX_HEADER]u8 = undefined;
    const header = buildHeader(header_buf[0..], if (options.resume_mode) "R4SEND2" else "R4SEND1", options.remote_name, file_size, sum) orelse {
        ctx.write("SEND: header too large\r\n");
        return 1;
    };

    if (options.resolved) {
        ctx.write("SEND resolved ");
        ctx.write(options.target_text);
        ctx.write(" to ");
        writeAddress(ctx, options.target.address);
        ctx.write("\r\n");
    }

    var attempt: u32 = 0;
    while (attempt <= options.retries) : (attempt += 1) {
        if (options.retries != 0) {
            ctx.write("SEND attempt: ");
            ctx.printU64(attempt + 1);
            ctx.write("/");
            ctx.printU64(options.retries + 1);
            ctx.write("\r\n");
        }
        const result = sendOnce(ctx, options, snapshot, sum, header);
        if (result == 0) return 0;
        if (ctx.network.cleanup_failed or ctx.output.failed) return 1;
        if (result == 2 and !options.resume_mode) {
            ctx.write("SEND retry: remote effect unconfirmed, use resume reconciliation\r\n");
            return 1;
        }
        _ = ctx.budget.remainingTicks() catch return 1;
        if (attempt < options.retries) ctx.write("SEND retry: next\r\n");
    }
    return 1;
}

fn sendOnce(ctx: *const App, options: Options, snapshot: []const u8, sum: u32, header: []const u8) i32 {
    ctx.write("SEND connect ");
    writeAddress(ctx, options.target.address);
    ctx.write(":");
    ctx.printU64(options.port);
    ctx.write(": ");
    var socket: r4os.network_stream.Socket = undefined;
    if (ctx.network.create(options.target, false, ctx.budget, &socket) != 0) {
        ctx.write("failed\r\n");
        return 1;
    }
    defer socket.close();
    ctx.write("ok\r\n");
    const sent_header = socket.writeAll(header, ctx.budget) catch return 2;
    if (!r4os.network_stream.complete(sent_header)) {
        ctx.write("SEND write: header-failed prefix=");
        ctx.printU64(sent_header.bytes);
        ctx.write("\r\n");
        return 2;
    }
    var reader: r4os.network_stream.Reader = .{ .socket = &socket };
    var offset: usize = 0;
    if (options.resume_mode) {
        var control_buf: [96]u8 = undefined;
        const control_raw = (reader.line(&control_buf, ctx.budget) catch return 2) orelse {
            ctx.write("SEND resume: incomplete control\r\n");
            return 2;
        };
        const control = trim(control_raw);
        ctx.write("SEND resume: ");
        ctx.write(control);
        ctx.write("\r\n");
        if (isAck(control)) {
            const ack_info = parseAck(control, snapshot.len, sum);
            printAckResult(ctx, ack_info);
            return if (ack_info.ok) 0 else 1;
        }
        offset = parseContinue(control) orelse {
            ctx.write("SEND resume: bad-control\r\n");
            return 2;
        };
        if (offset > snapshot.len) {
            ctx.write("SEND resume: offset-too-large\r\n");
            return 2;
        }
        ctx.write("SEND resume offset: ");
        ctx.printU64(offset);
        ctx.write("\r\n");
    }
    const sent = socket.writeAll(snapshot[offset..], ctx.budget) catch return 2;
    if (!r4os.network_stream.complete(sent)) {
        ctx.write("SEND write: failed offset=");
        ctx.printU64(offset + sent.bytes);
        ctx.write("\r\n");
        return 2;
    }
    printSent(ctx, options.remote_name, snapshot.len, offset, sum);
    var reply: [96]u8 = undefined;
    const ack_raw = (reader.line(&reply, ctx.budget) catch return 2) orelse {
        ctx.write("SEND ack: unconfirmed\r\n");
        return 2;
    };
    const ack = trim(ack_raw);
    ctx.write("SEND ack: ");
    ctx.write(ack);
    ctx.write("\r\n");
    const ack_info = parseAck(ack, snapshot.len, sum);
    printAckResult(ctx, ack_info);
    return if (ack_info.ok) 0 else 1;
}

fn printSent(ctx: *const App, remote_name: []const u8, file_size: usize, sent_from: usize, sum: u32) void {
    ctx.write("SEND sent: ");
    ctx.printU64(file_size - sent_from);
    ctx.write(" bytes file=");
    ctx.write(remote_name);
    if (sent_from != 0) {
        ctx.write(" offset=");
        ctx.printU64(sent_from);
        ctx.write(" total=");
        ctx.printU64(file_size);
    }
    ctx.write(" checksum=");
    ctx.printU64(sum);
    ctx.write("\r\n");
}

fn printAckResult(ctx: *const App, ack_info: AckInfo) void {
    if (!ack_info.ok and ack_info.code.len != 0) {
        ctx.write("SEND ack-error: ");
        ctx.write(ack_info.code);
        if (ack_info.saved != 0 or ack_info.expected != 0) {
            ctx.write(" saved=");
            ctx.printU64(ack_info.saved);
            ctx.write(" expected=");
            ctx.printU64(ack_info.expected);
        }
        ctx.write("\r\n");
    }
    ctx.write("SEND result: ");
    ctx.write(if (ack_info.ok) "ok" else "failed");
    ctx.write("\r\n");
}

fn usage(ctx: *const App) void {
    ctx.write("Usage: SEND [-4|-6] host port file [remote-name] [/RESUME] [/RETRY n]\r\n");
}

fn parseOptions(ctx: *const App, args: []const u8) ?Options {
    var rest = trim(args);
    const target = takeToken(rest) orelse return null;
    rest = target.rest;
    const port_token = takeToken(rest) orelse return null;
    rest = port_token.rest;
    const file_token = takeToken(rest) orelse return null;
    rest = file_token.rest;
    var remote_name: ?[]const u8 = null;
    var resume_mode = false;
    var retries: u32 = 0;
    while (true) {
        const tok = takeToken(rest) orelse break;
        rest = tok.rest;
        if (bytesEqual(tok.token, "/RESUME") or bytesEqual(tok.token, "/R")) {
            resume_mode = true;
        } else if (bytesEqual(tok.token, "/RETRY")) {
            const retry_token = takeToken(rest) orelse return null;
            rest = retry_token.rest;
            retries = parseRetry(retry_token.token) orelse return null;
        } else if (remote_name == null) {
            remote_name = tok.token;
        } else {
            return null;
        }
    }

    const port = parsePort(port_token.token) orelse return null;
    var endpoint: r4os.abi.NetworkEndpoint = .{};
    if (ctx.network.resolve(target.token, port, ctx.family, ctx.budget, &endpoint) != 0) {
        ctx.write("SEND resolve failed for ");
        ctx.write(target.token);
        ctx.write("\r\n");
        return null;
    }
    const numeric = if (target.token.len >= 2 and target.token[0] == '[') target.token[1 .. target.token.len - 1] else target.token;
    const resolved = r4os.network_family.parseAddress(numeric) == null;

    return .{
        .target_text = target.token,
        .target = endpoint,
        .port = port,
        .path = file_token.token,
        .remote_name = remote_name orelse baseName(file_token.token),
        .resolved = resolved,
        .resume_mode = resume_mode,
        .retries = retries,
    };
}

fn buildHeader(out: []u8, magic: []const u8, name_raw: []const u8, size: usize, sum: u32) ?[]const u8 {
    const name = if (name_raw.len == 0) DEFAULT_REMOTE_NAME else name_raw;
    if (!validName(name)) return null;
    var len: usize = 0;
    if (!append(out, &len, magic)) return null;
    if (!append(out, &len, " ")) return null;
    if (!append(out, &len, name)) return null;
    if (!append(out, &len, " ")) return null;
    if (!appendU64(out, &len, size)) return null;
    if (!append(out, &len, " ")) return null;
    if (!appendU64(out, &len, sum)) return null;
    if (!append(out, &len, "\r\n")) return null;
    return out[0..len];
}

const AckInfo = struct {
    ok: bool = false,
    code: []const u8 = "",
    saved: usize = 0,
    expected: usize = 0,
};

fn parseAck(value: []const u8, expected_size: usize, expected_sum: u32) AckInfo {
    var rest = trim(value);
    const status = takeToken(rest) orelse return .{ .code = "empty-ack" };
    rest = status.rest;
    if (bytesEqual(status.token, "ERR")) {
        const code = takeToken(rest) orelse return .{ .code = "remote-error" };
        rest = code.rest;
        const saved_token = takeToken(rest);
        var saved: usize = 0;
        var expected: usize = 0;
        if (saved_token) |tok| {
            saved = parseUsize(tok.token) orelse 0;
            const expected_token = takeToken(tok.rest);
            if (expected_token) |exp| expected = parseUsize(exp.token) orelse 0;
        }
        return .{ .code = code.token, .saved = saved, .expected = expected };
    }
    if (!bytesEqual(status.token, "OK")) return .{ .code = "bad-ack" };
    const size_token = takeToken(rest) orelse return .{ .code = "bad-ack" };
    rest = size_token.rest;
    const sum_token = takeToken(rest) orelse return .{ .code = "bad-ack" };
    if (sum_token.rest.len != 0) return .{ .code = "bad-ack" };
    const size = parseUsize(size_token.token) orelse return .{ .code = "bad-size" };
    const sum = parseU32(sum_token.token) orelse return .{ .code = "bad-checksum" };
    if (size != expected_size) return .{ .code = "size-mismatch", .saved = size, .expected = expected_size };
    if (sum != expected_sum) return .{ .code = "checksum-mismatch", .saved = sum, .expected = expected_sum };
    return .{ .ok = true };
}

fn isAck(value: []const u8) bool {
    const status = takeToken(value) orelse return false;
    return bytesEqual(status.token, "OK") or bytesEqual(status.token, "ERR");
}

fn parseContinue(value: []const u8) ?usize {
    var rest = trim(value);
    const status = takeToken(rest) orelse return null;
    if (!bytesEqual(status.token, "CONT")) return null;
    rest = status.rest;
    const offset_token = takeToken(rest) orelse return null;
    if (offset_token.rest.len != 0) return null;
    return parseUsize(offset_token.token);
}

fn validName(value: []const u8) bool {
    if (value.len == 0 or value.len > 48) return false;
    for (value) |ch| {
        if (ch < 32 or ch == 127 or ch == '"' or isSpace(ch) or ch == '/' or ch == '\\' or ch == ':') return false;
    }
    return true;
}

fn baseName(path: []const u8) []const u8 {
    var pos: usize = 0;
    var i: usize = 0;
    while (i < path.len) : (i += 1) {
        if (path[i] == '\\' or path[i] == '/' or path[i] == ':') pos = i + 1;
    }
    const name = path[pos..];
    return if (name.len == 0) DEFAULT_REMOTE_NAME else name;
}

// One bounded read under the filesystem request gate produces the bytes
// used by the header, all sends, retries and resume offsets.
fn loadSnapshot(ctx: *const App, path: [*:0]const u8, snapshot: []u8) ?u32 {
    if (snapshot.len > MAX_FILE) return null;
    const before = switch (r4os.metadata2.info(&ctx.app.context, path)) {
        .found => |value| value,
        else => return null,
    };
    if (before.flags & r4os.abi.file_metadata2_flag_byte_size_valid == 0 or before.byte_size != snapshot.len) return null;
    var source = r4os.stream_transfer.adapters.FileRead.fromContext2(&ctx.app.context, path, 0) catch return null;
    const driver: r4os.stream_transfer.adapters.Driver = .{ .sys = ctx.sys, .clock = &ctx.network.clock, .budget = ctx.budget };
    const result = driver.read(snapshot, 16 * 1024, r4os.stream_transfer.adapters.FileRead.step, &source, true) catch return null;
    if (!r4os.network_stream.complete(result)) return null;
    const after = switch (r4os.metadata2.info(&ctx.app.context, path)) {
        .found => |value| value,
        else => return null,
    };
    if (!std.meta.eql(before, after)) return null;
    return checksumUpdate(0, snapshot);
}

fn checksumUpdate(seed: u32, data: []const u8) u32 {
    var out: u32 = 0;
    out = seed;
    for (data) |ch| out +%= ch;
    return out;
}

fn append(out: []u8, len: *usize, text: []const u8) bool {
    if (len.* + text.len > out.len) return false;
    @memcpy(out[len.* .. len.* + text.len], text);
    len.* += text.len;
    return true;
}

fn appendU64(out: []u8, len: *usize, value: u64) bool {
    var digits: [20]u8 = undefined;
    var count: usize = 0;
    var n = value;
    if (n == 0) {
        return append(out, len, "0");
    }
    while (n > 0) {
        digits[digits.len - 1 - count] = '0' + @as(u8, @intCast(n % 10));
        count += 1;
        n /= 10;
    }
    return append(out, len, digits[digits.len - count ..]);
}

const Token = struct { token: []const u8, rest: []const u8 };
fn takeToken(value: []const u8) ?Token {
    var failure: r4os.arguments.Failure = .{};
    var cursor = r4os.arguments.ArgCursor.init(value, .{}, &failure) catch return null;
    const token = (cursor.next() catch return null) orelse return null;
    return .{ .token = token.bytes(value) catch return null, .rest = cursor.remainingArguments() catch return null };
}

fn parsePort(value: []const u8) ?u16 {
    if (value.len == 0) return null;
    for (value) |byte| if (byte < '0' or byte > '9') return null;
    const number = std.fmt.parseInt(u16, value, 10) catch return null;
    return if (number == 0) null else number;
}

fn parseUsize(value: []const u8) ?usize {
    if (value.len == 0) return null;
    for (value) |byte| if (byte < '0' or byte > '9') return null;
    const number = std.fmt.parseInt(usize, value, 10) catch return null;
    return number;
}

fn parseU32(value: []const u8) ?u32 {
    const parsed = parseUsize(value) orelse return null;
    if (parsed > 0xFFFF_FFFF) return null;
    return @intCast(parsed);
}

fn parseRetry(value: []const u8) ?u32 {
    const parsed = parseU32(value) orelse return null;
    if (parsed > 9) return null;
    return parsed;
}

fn writeAddress(ctx: *const App, address: r4os.abi.NetworkAddress) void {
    var buffer: [80]u8 = undefined;
    ctx.write(r4os.network_family.addressText(&buffer, address) catch "unknown");
}

fn copyZ(out: [:0]u8, text: []const u8) ?[*:0]const u8 {
    if (text.len >= out.len) return null;
    @memcpy(out[0..text.len], text);
    out[text.len] = 0;
    return @ptrCast(out.ptr);
}

fn zSlice(ptr: [*:0]const u8) []const u8 {
    var len: usize = 0;
    while (ptr[len] != 0) : (len += 1) {}
    return ptr[0..len];
}

fn trim(value: []const u8) []const u8 {
    var start: usize = 0;
    var end = value.len;
    while (start < end and isSpace(value[start])) : (start += 1) {}
    while (end > start and isSpace(value[end - 1])) : (end -= 1) {}
    return value[start..end];
}

fn isSpace(ch: u8) bool {
    return ch == ' ' or ch == '\t' or ch == '\r' or ch == '\n';
}

fn bytesEqual(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    var i: usize = 0;
    while (i < a.len) : (i += 1) {
        if (a[i] != b[i]) return false;
    }
    return true;
}

test "migration send control rejects incomplete ACK and bounds resume offsets" {
    try std.testing.expect(parseAck("OK 123 456", 123, 456).ok);
    try std.testing.expect(!parseAck("OK 123 45", 123, 456).ok);
    try std.testing.expect(!parseAck("OK 123 456 trailing", 123, 456).ok);
    try std.testing.expectEqual(@as(usize, 12), parseContinue("CONT 12").?);
    try std.testing.expect(parseContinue("CONT 999999999999999999999999999") == null);
    try std.testing.expect(!validName("bad\"name.bin"));
    try std.testing.expect(validName("Grüße.bin"));
}
