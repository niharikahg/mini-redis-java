# MiniRedis

A small Java implementation of a Redis-style TCP server. It accepts RESP2 arrays and bulk strings, keeps data in memory, and handles multiple clients concurrently.

## Run

Requires Java 17 or newer.

```sh
javac -d out src/main/java/redis/MiniRedis.java
java -cp out redis.MiniRedis [port]
```

The default port is `6379`. Connect with `redis-cli`:

```sh
redis-cli -p 6379
SET greeting "hello world"
GET greeting
SET session abc EX 30
TTL session
```

## Commands

`PING`, `ECHO`, `SET` (`EX`, `PX`, `NX`, `XX`, and `GET` options), `GET`, `DEL`, `EXISTS`, `INCR`, `DECR`, `TYPE`, `TTL`, `PTTL`, `KEYS`, `FLUSHDB`, `FLUSHALL`, `SELECT 0`, and `QUIT`.

This is an educational in-memory implementation: it does not persist data to disk and supports only database 0 and string values.
