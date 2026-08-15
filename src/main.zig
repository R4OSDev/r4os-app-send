const r4os = @import("r4os");

const MAX_TCP_CHUNK: usize = r4os.abi.net_service_tcp_write_max;
const MAX_HEADER: usize = 128;
const MAX_FILE: usize = 512 * 1024;
const CONTROL_WAIT_MS: u64 = 3000;
const DEFAULT_REMOTE_NAME = "R4SEND.BIN";

const Options = struct {
    target_text: []const u8,
    target_ip: [4]u8,
    port: u16,
    path: []const u8,
    remote_name: []const u8,
    resolved: bool,
    resume_mode: bool,
    retries: u32,
};

const App = struct {
    sys: r4os.r4sys.Context,
    net: r4os.r4net.Context,

    fn init(r4_app: *r4os.App) ?App {
        return .{
            .sys = r4_app.system(),
            .net = r4_app.networkLowLevel() orelse return null,
        };
    }

    fn argsRaw(self: *const App) [*:0]const u8 {
        return self.sys.argsRaw();
    }

    fn write(self: *const App, value: []const u8) void {
        self.sys.write(value);
    }

    fn putc(self: *const App, ch: u8) void {
        self.sys.putc(ch);
    }

    fn printU64(self: *const App, value: u64) void {
        self.sys.printU64(value);
    }

    fn ticksFromMilliseconds(self: *const App, ms: u64) u64 {
        return self.sys.ticksFromMilliseconds(ms);
    }

    fn fileInfo(self: *const App, path: [*:0]const u8) ?r4os.abi.FileInfo {
        return self.sys.fileInfo(path);
    }

    fn fileReadAt(self: *const App, path: [*:0]const u8, offset: u32, out: []u8) i32 {
        return self.sys.fileReadAt(path, offset, out);
    }

    fn netDnsResolveService(self: *const App, name_value: []const u8, out: *[4]u8) i32 {
        return self.net.netDnsResolveService(name_value, out);
    }

    fn netDnsResultName(self: *const App, result: i32) []const u8 {
        return self.net.netDnsResultName(result);
    }

    fn tcpConnectService(self: *const App, a: u8, b: u8, c: u8, d: u8, port: u16) i32 {
        return self.net.tcpConnectService(a, b, c, d, port);
    }

    fn tcpWriteService(self: *const App, handle: u32, data: []const u8) i32 {
        return self.net.tcpWriteService(handle, data);
    }

    fn tcpWritePacedService(self: *const App, handle: u32, data: []const u8, wait_ticks: u64) i32 {
        return self.net.tcpWritePacedService(handle, data, wait_ticks);
    }

    fn tcpReadWaitService(self: *const App, handle: u32, out: []u8, wait_ticks: u64) i32 {
        return self.net.tcpReadWaitService(handle, out, wait_ticks);
    }

    fn tcpCloseService(self: *const App, handle: u32) i32 {
        return self.net.tcpCloseService(handle);
    }
};

pub fn r4_app_main(r4_app: *r4os.App) i32 {
    const ctx = App.init(r4_app) orelse return r4os.abi.err_no_group;
    const args = trim(zSlice(ctx.argsRaw()));
    const options = parseOptions(&ctx, args) orelse {
        usage(&ctx);
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
    const sum = checksumFile(&ctx, path_z, file_size) orelse {
        ctx.write("SEND read: failed\r\n");
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
        writeIpv4(&ctx, options.target_ip);
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
        const result = sendOnce(&ctx, options, path_z, file_size, sum, header);
        if (result == 0) return 0;
        if (attempt < options.retries) ctx.write("SEND retry: next\r\n");
    }
    return 1;
}

fn sendOnce(ctx: *const App, options: Options, path_z: [*:0]const u8, file_size: usize, sum: u32, header: []const u8) i32 {
    ctx.write("SEND connect ");
    writeIpv4(ctx, options.target_ip);
    ctx.write(":");
    ctx.printU64(options.port);
    ctx.write(": ");
    const conn = ctx.tcpConnectService(options.target_ip[0], options.target_ip[1], options.target_ip[2], options.target_ip[3], options.port);
    if (conn <= 0) {
        ctx.write("failed\r\n");
        return 1;
    }
    ctx.write("ok\r\n");

    const conn_id: u32 = @intCast(conn);
    const control_wait_ticks = ctx.ticksFromMilliseconds(CONTROL_WAIT_MS);

    var offset: usize = 0;
    var sent_from: usize = 0;
    if (options.resume_mode) {
        const header_written = ctx.tcpWriteService(conn_id, header);
        if (header_written < 0 or header_written != @as(i32, @intCast(header.len))) {
            ctx.write("SEND write: header-failed\r\n");
            _ = ctx.tcpCloseService(conn_id);
            return 1;
        }

        var control_buf: [96]u8 = undefined;
        const got_control = ctx.tcpReadWaitService(conn_id, control_buf[0..], control_wait_ticks);
        if (got_control <= 0) {
            ctx.write("SEND resume: timeout\r\n");
            _ = ctx.tcpCloseService(conn_id);
            return 1;
        }
        const control = trim(control_buf[0..@intCast(got_control)]);
        ctx.write("SEND resume: ");
        ctx.write(control);
        ctx.write("\r\n");
        if (isAck(control)) {
            const ack_info = parseAck(control, file_size, sum);
            _ = ctx.tcpCloseService(conn_id);
            printAckResult(ctx, ack_info);
            return if (ack_info.ok) 0 else 1;
        }
        offset = parseContinue(control) orelse {
            ctx.write("SEND resume: bad-control\r\n");
            _ = ctx.tcpCloseService(conn_id);
            return 1;
        };
        if (offset > file_size) {
            ctx.write("SEND resume: offset-too-large\r\n");
            _ = ctx.tcpCloseService(conn_id);
            return 1;
        }
        ctx.write("SEND resume offset: ");
        ctx.printU64(offset);
        ctx.write("\r\n");
        sent_from = offset;
    } else {
        var first_buf: [MAX_TCP_CHUNK]u8 = undefined;
        @memcpy(first_buf[0..header.len], header);
        const prefix_cap = first_buf.len - header.len;
        const prefix_len = @min(prefix_cap, file_size);
        if (prefix_len != 0) {
            const got_prefix = ctx.fileReadAt(path_z, 0, first_buf[header.len .. header.len + prefix_len]);
            if (got_prefix < 0 or got_prefix != @as(i32, @intCast(prefix_len))) {
                ctx.write("SEND read: failed offset=0\r\n");
                _ = ctx.tcpCloseService(conn_id);
                return 1;
            }
        }
        const first_len = header.len + prefix_len;
        const first_written = ctx.tcpWriteService(conn_id, first_buf[0..first_len]);
        if (first_written < 0 or first_written != @as(i32, @intCast(first_len))) {
            ctx.write("SEND write: header-failed\r\n");
            _ = ctx.tcpCloseService(conn_id);
            return 1;
        }
        offset = prefix_len;
    }

    var chunk_buf: [MAX_TCP_CHUNK]u8 = undefined;
    while (offset < file_size) {
        const want = @min(chunk_buf.len, file_size - offset);
        const got_chunk = ctx.fileReadAt(path_z, @intCast(offset), chunk_buf[0..want]);
        if (got_chunk < 0 or got_chunk != @as(i32, @intCast(want))) {
            ctx.write("SEND read: failed offset=");
            ctx.printU64(offset);
            ctx.write("\r\n");
            _ = ctx.tcpCloseService(conn_id);
            return 1;
        }
        const chunk = chunk_buf[0..@intCast(got_chunk)];
        const written = ctx.tcpWritePacedService(conn_id, chunk, control_wait_ticks);
        if (written < 0 or written != got_chunk) {
            ctx.write("SEND write: failed offset=");
            ctx.printU64(offset);
            ctx.write("\r\n");
            _ = ctx.tcpCloseService(conn_id);
            return 1;
        }
        offset += chunk.len;
    }

    printSent(ctx, options.remote_name, file_size, sent_from, sum);

    var reply: [96]u8 = undefined;
    const got = ctx.tcpReadWaitService(conn_id, reply[0..], control_wait_ticks);
    _ = ctx.tcpCloseService(conn_id);
    if (got <= 0) {
        ctx.write("SEND ack: timeout\r\n");
        return 1;
    }

    const ack = trim(reply[0..@intCast(got)]);
    ctx.write("SEND ack: ");
    ctx.write(ack);
    ctx.write("\r\n");
    const ack_info = parseAck(ack, file_size, sum);
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
    ctx.write("Usage: SEND host port file [remote-name] [/RESUME] [/RETRY n]\r\n");
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
    var target_ip: [4]u8 = undefined;
    var resolved = false;
    if (parseIpv4(target.token)) |ip| {
        target_ip = ip;
    } else {
        const result = ctx.netDnsResolveService(target.token, &target_ip);
        if (result != r4os.abi.dns_result_ok) {
            ctx.write("SEND resolve failed for ");
            ctx.write(target.token);
            ctx.write(": ");
            ctx.write(ctx.netDnsResultName(result));
            ctx.write("\r\n");
            return null;
        }
        resolved = true;
    }

    return .{
        .target_text = target.token,
        .target_ip = target_ip,
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
        if (isSpace(ch) or ch == '/' or ch == '\\' or ch == ':') return false;
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

fn checksumFile(ctx: *const App, path: [*:0]const u8, size: usize) ?u32 {
    var out: u32 = 0;
    var offset: usize = 0;
    var buf: [MAX_TCP_CHUNK]u8 = undefined;
    while (offset < size) {
        const want = @min(buf.len, size - offset);
        const got = ctx.fileReadAt(path, @intCast(offset), buf[0..want]);
        if (got < 0 or got != @as(i32, @intCast(want))) return null;
        out = checksumUpdate(out, buf[0..@intCast(got)]);
        offset += @intCast(got);
    }
    return out;
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

const Token = struct {
    token: []const u8,
    rest: []const u8,
};

fn takeToken(value: []const u8) ?Token {
    const trimmed = trim(value);
    if (trimmed.len == 0) return null;
    var end: usize = 0;
    while (end < trimmed.len and !isSpace(trimmed[end])) : (end += 1) {}
    return .{
        .token = trimmed[0..end],
        .rest = if (end >= trimmed.len) "" else trim(trimmed[end..]),
    };
}

fn parsePort(value: []const u8) ?u16 {
    if (value.len == 0) return null;
    var out: u32 = 0;
    for (value) |ch| {
        if (ch < '0' or ch > '9') return null;
        out = out * 10 + @as(u32, ch - '0');
        if (out == 0 or out > 65535) return null;
    }
    return @intCast(out);
}

fn parseUsize(value: []const u8) ?usize {
    if (value.len == 0) return null;
    var out: usize = 0;
    for (value) |ch| {
        if (ch < '0' or ch > '9') return null;
        out = out * 10 + @as(usize, ch - '0');
    }
    return out;
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

fn parseIpv4(value: []const u8) ?[4]u8 {
    var out: [4]u8 = .{0} ** 4;
    var part: usize = 0;
    var accum: u16 = 0;
    var digits: usize = 0;

    var index: usize = 0;
    while (index < value.len) : (index += 1) {
        const ch = value[index];
        if (ch >= '0' and ch <= '9') {
            accum = accum * 10 + @as(u16, ch - '0');
            if (accum > 255) return null;
            digits += 1;
            if (digits > 3) return null;
        } else if (ch == '.') {
            if (digits == 0 or part >= 3) return null;
            out[part] = @intCast(accum);
            part += 1;
            accum = 0;
            digits = 0;
        } else {
            return null;
        }
    }
    if (digits == 0 or part != 3) return null;
    out[part] = @intCast(accum);
    return out;
}

fn writeIpv4(ctx: *const App, ip: [4]u8) void {
    ctx.printU64(ip[0]);
    ctx.putc('.');
    ctx.printU64(ip[1]);
    ctx.putc('.');
    ctx.printU64(ip[2]);
    ctx.putc('.');
    ctx.printU64(ip[3]);
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
