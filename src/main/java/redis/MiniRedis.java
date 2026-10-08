package redis;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/** A small, dependency-free Redis-compatible server for learning and local development. */
public final class MiniRedis {
    private static final int DEFAULT_PORT = 6379;
    private final Map<String, Entry> values = new ConcurrentHashMap<>();
    private final ExecutorService clients = Executors.newCachedThreadPool();

    private record Entry(String value, long expiresAtMillis) {
        boolean expired() { return expiresAtMillis > 0 && System.currentTimeMillis() >= expiresAtMillis; }
    }

    public static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        new MiniRedis().serve(port);
    }

    private void serve(int port) throws IOException {
        try (ServerSocket server = new ServerSocket(port)) {
            System.out.println("MiniRedis listening on port " + port);
            while (true) {
                Socket socket = server.accept();
                clients.submit(() -> handle(socket));
            }
        }
    }

    private void handle(Socket socket) {
        try (socket;
             BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
             BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream())) {
            while (!socket.isClosed()) {
                Object request = readValue(in);
                if (request == null) return;
                Object response;
                try {
                    if (!(request instanceof List<?> list) || list.isEmpty()) {
                        response = new Error("ERR expected a non-empty command array");
                    } else {
                        List<String> args = list.stream().map(String::valueOf).toList();
                        response = execute(args);
                    }
                } catch (CommandError e) {
                    response = new Error(e.getMessage());
                } catch (RuntimeException e) {
                    response = new Error("ERR " + e.getMessage());
                }
                writeValue(out, response);
            }
        } catch (IOException ignored) {
            // A client disconnect is normal; its socket is closed by try-with-resources.
        }
    }

    private Object execute(List<String> args) throws CommandError {
        String command = args.get(0).toUpperCase(Locale.ROOT);
        return switch (command) {
            case "PING" -> {
                requireCount(args, 1, 2);
                yield args.size() == 2 ? new Bulk(args.get(1)) : new Simple("PONG");
            }
            case "ECHO" -> { requireCount(args, 2, 2); yield new Bulk(args.get(1)); }
            case "SET" -> set(args);
            case "GET" -> { requireCount(args, 2, 2); Entry e = get(args.get(1)); yield e == null ? NullBulk.INSTANCE : new Bulk(e.value); }
            case "DEL" -> delete(args);
            case "EXISTS" -> exists(args);
            case "INCR" -> increment(args, 1);
            case "DECR" -> increment(args, -1);
            case "TYPE" -> { requireCount(args, 2, 2); yield new Simple(get(args.get(1)) == null ? "none" : "string"); }
            case "TTL" -> ttl(args, false);
            case "PTTL" -> ttl(args, true);
            case "KEYS" -> keys(args);
            case "FLUSHDB", "FLUSHALL" -> { requireCount(args, 1, 1); values.clear(); yield new Simple("OK"); }
            case "SELECT" -> { requireCount(args, 2, 2); if (!args.get(1).equals("0")) throw new CommandError("ERR only database 0 is supported"); yield new Simple("OK"); }
            case "QUIT" -> new Simple("OK");
            default -> throw new CommandError("ERR unknown command '" + args.get(0) + "'");
        };
    }

    private Object set(List<String> args) throws CommandError {
        if (args.size() < 3) throw new CommandError("ERR wrong number of arguments for 'set' command");
        String key = args.get(1), value = args.get(2);
        long expiry = 0;
        for (int i = 3; i < args.size(); i++) {
            String option = args.get(i).toUpperCase(Locale.ROOT);
            if ((option.equals("EX") || option.equals("PX")) && i + 1 < args.size()) {
                try {
                    long amount = Long.parseLong(args.get(++i));
                    if (amount <= 0) throw new NumberFormatException();
                    expiry = System.currentTimeMillis() + amount * (option.equals("EX") ? 1000 : 1);
                } catch (NumberFormatException e) { throw new CommandError("ERR invalid expire time in 'set' command"); }
            } else if (option.equals("NX") || option.equals("XX")) {
                Entry existing = get(key);
                if ((option.equals("NX") && existing != null) || (option.equals("XX") && existing == null)) return NullBulk.INSTANCE;
            } else if (option.equals("GET")) {
                Entry old = get(key);
                values.put(key, new Entry(value, expiry));
                return old == null ? NullBulk.INSTANCE : new Bulk(old.value);
            } else throw new CommandError("ERR syntax error");
        }
        values.put(key, new Entry(value, expiry));
        return new Simple("OK");
    }

    private Object delete(List<String> args) throws CommandError {
        if (args.size() < 2) throw new CommandError("ERR wrong number of arguments for 'del' command");
        long count = 0;
        for (int i = 1; i < args.size(); i++) if (values.remove(args.get(i)) != null) count++;
        return count;
    }

    private Object exists(List<String> args) throws CommandError {
        if (args.size() < 2) throw new CommandError("ERR wrong number of arguments for 'exists' command");
        long count = 0;
        for (int i = 1; i < args.size(); i++) if (get(args.get(i)) != null) count++;
        return count;
    }

    private Object increment(List<String> args, long by) throws CommandError {
        requireCount(args, 2, 2);
        String key = args.get(1);
        AtomicLong result = new AtomicLong();
        values.compute(key, (k, old) -> {
            if (old != null && old.expired()) old = null;
            long current;
            try { current = old == null ? 0 : Long.parseLong(old.value); }
            catch (NumberFormatException e) { throw new IllegalArgumentException("value is not an integer or out of range"); }
            try { result.set(Math.addExact(current, by)); }
            catch (ArithmeticException e) { throw new IllegalArgumentException("value is not an integer or out of range"); }
            return new Entry(Long.toString(result.get()), old == null ? 0 : old.expiresAtMillis);
        });
        return result.get();
    }

    private Object ttl(List<String> args, boolean millis) throws CommandError {
        requireCount(args, 2, 2);
        Entry e = get(args.get(1));
        if (e == null) return -2L;
        if (e.expiresAtMillis == 0) return -1L;
        long remaining = e.expiresAtMillis - System.currentTimeMillis();
        return millis ? Math.max(remaining, 0) : Math.max(remaining, 0) / 1000;
    }

    private Object keys(List<String> args) throws CommandError {
        requireCount(args, 2, 2);
        String pattern = args.get(1);
        String regex = PatternQuote.globToRegex(pattern);
        List<Object> result = values.keySet().stream().filter(k -> get(k) != null && k.matches(regex))
                .sorted().map(k -> (Object) new Bulk(k)).toList();
        return new Array(result);
    }

    private Entry get(String key) {
        Entry entry = values.get(key);
        if (entry != null && entry.expired()) { values.remove(key, entry); return null; }
        return entry;
    }

    private static void requireCount(List<String> args, int min, int max) throws CommandError {
        if (args.size() < min || args.size() > max) throw new CommandError("ERR wrong number of arguments for '" + args.get(0).toLowerCase(Locale.ROOT) + "' command");
    }

    private static Object readValue(InputStream in) throws IOException {
        int marker = in.read();
        if (marker == -1) return null;
        String line = readLine(in);
        return switch (marker) {
            case '*' -> {
                int count = Integer.parseInt(line);
                if (count < 0) yield null;
                List<Object> items = new ArrayList<>(count);
                for (int i = 0; i < count; i++) items.add(readValue(in));
                yield items;
            }
            case '$' -> {
                int length = Integer.parseInt(line);
                if (length < 0) yield null;
                byte[] bytes = in.readNBytes(length);
                if (bytes.length != length || in.read() != '\r' || in.read() != '\n') throw new EOFException("truncated bulk string");
                yield new String(bytes, StandardCharsets.UTF_8);
            }
            case '+' -> line;
            case ':' -> Long.parseLong(line);
            case '-' -> throw new IOException("client sent RESP error: " + line);
            default -> throw new IOException("unsupported RESP type: " + (char) marker);
        };
    }

    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        int previous = -1, current;
        while ((current = in.read()) != -1) {
            if (previous == '\r' && current == '\n') {
                byte[] data = bytes.toByteArray();
                return new String(data, 0, Math.max(0, data.length - 1), StandardCharsets.UTF_8);
            }
            bytes.write(current);
            previous = current;
        }
        throw new EOFException("truncated RESP line");
    }

    private static void writeValue(OutputStream out, Object value) throws IOException {
        if (value instanceof Simple s) writeLine(out, '+' + s.value);
        else if (value instanceof Error e) writeLine(out, '-' + e.value);
        else if (value instanceof Bulk b) {
            byte[] data = b.value.getBytes(StandardCharsets.UTF_8);
            writeLine(out, "$" + data.length);
            out.write(data); out.write('\r'); out.write('\n');
        } else if (value == NullBulk.INSTANCE) writeLine(out, "$-1");
        else if (value instanceof Array a) {
            writeLine(out, "*" + a.values.size());
            for (Object item : a.values) writeValue(out, item);
        } else if (value instanceof Number n) writeLine(out, ":" + n.longValue());
        else writeLine(out, "-ERR internal response error");
        out.flush();
    }

    private static void writeLine(OutputStream out, String line) throws IOException {
        out.write(line.getBytes(StandardCharsets.UTF_8)); out.write('\r'); out.write('\n');
    }

    private record Simple(String value) {}
    private record Error(String value) {}
    private record Bulk(String value) {}
    private record Array(List<Object> values) {}
    private enum NullBulk { INSTANCE }
    private static final class CommandError extends Exception { CommandError(String message) { super(message); } }

    private static final class PatternQuote {
        static String globToRegex(String glob) {
            StringBuilder out = new StringBuilder("^");
            for (char c : glob.toCharArray()) {
                if (c == '*') out.append(".*");
                else if (c == '?') out.append('.');
                else if (c == '[') out.append('[');
                else if (c == ']') out.append(']');
                else { if ("\\.()+|^$".indexOf(c) >= 0) out.append('\\'); out.append(c); }
            }
            return out.append('$').toString();
        }
    }
}
