# 🗄️ Bitcask

A clean, educational implementation of the [Bitcask](https://riak.com/assets/bitcask-intro.pdf) key-value store in Python. Built from scratch to understand how append-only, log-structured storage engines work under the hood.

```
PUT("name", "alice")  →  append to log  →  update in-memory index
GET("name")           →  index lookup   →  single disk seek  →  "alice"
```

---

## Why Bitcask?

Bitcask is one of the simplest storage engine designs that's actually been used in production (Riak). It makes a bold tradeoff: **keep all keys in memory** so that every read is exactly one disk seek. Writes are always sequential appends — no random I/O, no complex page management, no write-ahead log needed.

This makes it an ideal first storage engine to study. The entire architecture fits in your head, yet it covers real concepts: checksumming, crash recovery, compaction, hint files, and tombstone deletion.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                         Bitcask API                          │
│                   put / get / delete / keys                  │
└──────────┬──────────────────┬────────────────────┬───────────┘
           │                  │                    │
     ┌─────▼─────┐    ┌──────▼──────┐    ┌───────▼────────┐
     │  DataFile  │    │   KeyDir    │    │   Compaction   │
     │ append-only│    │  in-memory  │    │  merge + hint  │
     │  log files │    │  hash index │    │  file creation │
     └─────┬──────┘    └──────┬──────┘    └───────┬────────┘
           │                  │                    │
     ┌─────▼──────────────────▼────────────────────▼───────┐
     │                     Recovery                         │
     │     rebuild keydir from data files + hint files      │
     └──────────────────────────────────────────────────────┘
```

### Record Format (on disk)

Every key-value pair is stored as a single binary record:

```
┌─────────┬───────────┬───────┬──────────┬────────────┬─────┬───────┐
│ CRC (4) │ Timestamp │ Flags │ Key Size │ Value Size │ Key │ Value │
│         │   (4)     │ (1)   │   (2)    │    (4)     │     │       │
└─────────┴───────────┴───────┴──────────┴────────────┴─────┴───────┘
 ◄──────────── Header (15 bytes) ────────────────────► ◄── Body ──►
```

- **CRC** — CRC32 over everything after itself. Detects corruption from crashes or disk errors.
- **Flags** — Bit 0 marks tombstones (deletes). No magic sentinel values that could collide with user data.
- **Key/Value** — Raw bytes. The `Bitcask` class handles UTF-8 encoding at the API boundary.

### How Reads Work

```
GET("name")
  └→ keydir["name"] = (file_id=2, offset=48, value_size=5, timestamp=...)
      └→ seek to byte 48 in file 2
          └→ skip header + key, read 5 bytes → "alice"
```

One dict lookup + one disk seek. Always O(1), regardless of database size.

### How Writes Work

```
PUT("name", "alice")
  └→ encode as [crc|ts|flags|5|5|name|alice]
      └→ append to active file at offset 200
          └→ fsync
              └→ keydir["name"] = (file_id=3, offset=200, ...)
```

Sequential append + fsync. Old values become garbage — they're still on disk but unreachable through the keydir.

### How Deletes Work

```
DELETE("name")
  └→ append tombstone record: [crc|ts|flags=0x01|5|0|name|]
      └→ keydir.remove("name")
```

A tombstone is a normal record with `flags` bit 0 set and an empty value. Recovery sees the tombstone and removes the key from the keydir.

### Compaction

Over time, overwritten and deleted records accumulate as garbage. Compaction reclaims this space:

1. Identify immutable files where dead bytes exceed 60% of file size.
2. Scan each file record by record.
3. **Liveness check**: does the keydir point to *this exact file and offset* for this key? If yes → live, copy it. If no → dead, skip it.
4. Write live records to a new merged file.
5. Write a **hint file** alongside it (keys + offsets, no values).
6. Delete old files.

```
BEFORE                          AFTER
┌────────────────┐              ┌────────────────┐
│ File 1         │              │ Merged File    │
│ a=1 (dead)     │              │ a=5 (live)     │
│ b=2 (dead)     │  compact →   │ c=3 (live)     │
│ c=3 (live)     │              │ d=4 (live)     │
├────────────────┤              └────────────────┘
│ File 2         │              ┌────────────────┐
│ a=5 (live)     │              │ Active File    │
│ DEL b (dead)   │              │ (untouched)    │
│ d=4 (live)     │              └────────────────┘
├────────────────┤
│ Active File    │
│ (untouched)    │
└────────────────┘
```

### Recovery

On startup, the keydir is rebuilt from disk:

1. Scan the data directory for `.bitcask.data` files.
2. For files with a `.bitcask.hint` file → load keys and offsets from the hint (fast path, no value bytes read).
3. For files without hints (including the active file) → scan record by record, validate CRC, populate keydir.
4. If the active file has a corrupt trailing record (crash mid-write) → truncate at the last valid boundary.

Hint files make recovery fast: a 10GB database with good hint coverage might only need to scan the last 256MB active file.

### Why No WAL?

Traditional databases need a write-ahead log because they do **in-place updates** — a crash mid-update leaves a corrupted page. Bitcask never modifies existing data. Every write is an append. A crash can only produce a partial record at the tail, which CRC validation catches and recovery truncates. The data file *is* the log.

## Project Structure

```
bitcask/
├── constants.py      # Header format, thresholds, flags
├── record.py         # Binary record: encode, decode, CRC, tombstone
├── keydir.py         # In-memory index with dead bytes tracking
├── datafile.py       # Single data file: append, read, iterate, rotate
├── hintfile.py       # Write/read hint files for fast recovery
├── recovery.py       # Rebuild keydir from disk on startup
├── compaction.py     # Merge old files, produce hints, reclaim space
└── bitcask.py        # Public API: put, get, delete, compact, close
```

Each file is self-contained with its own tests in `if __name__ == "__main__"`. Reading order follows the dependency chain: `constants → record → keydir → datafile → hintfile → recovery → compaction → bitcask`.

## Usage

```python
from bitcask import Bitcask

# Open (or create) a database
with Bitcask("/tmp/mydb") as db:
    # Write
    db.put("user:1", "alice")
    db.put("user:2", "bob")

    # Read
    print(db.get("user:1"))      # "alice"
    print(db.get("missing"))     # None

    # Delete
    db.delete("user:2")

    # Iterate keys
    print(db.keys())             # ["user:1"]

    # Check membership
    print("user:1" in db)        # True

    # Database stats
    print(db.stats())

    # Manual compaction
    db.force_compact()

# Data persists — reopen and it's all there
with Bitcask("/tmp/mydb") as db:
    print(db.get("user:1"))      # "alice"
```

## What This Doesn't Have (vs Production Bitcask)

This is an educational implementation. Production systems like Riak's Bitcask additionally include:

| Feature | Purpose |
|---|---|
| File locking | Prevent multiple processes from opening the same database |
| Thread safety | Read-write locks around keydir and active file |
| TTL / Expiry | Automatically expire records after a time-to-live |
| Background compaction | Scheduled merge with configurable windows and I/O rate limiting |
| Crash-safe compaction | Detect and discard incomplete merged files on recovery |
| O_DIRECT / fadvise | Bypass OS page cache for predictable I/O performance |
| Erlang NIF | C extensions for CRC and keydir on the hot path |

## References

- [Bitcask: A Log-Structured Hash Table for Fast Key/Value Data](https://riak.com/assets/bitcask-intro.pdf) — the original design paper
- [Riak Bitcask source](https://github.com/basho/bitcask) — production Erlang implementation
- *Designing Data-Intensive Applications* (Ch. 3) — Martin Kleppmann's excellent coverage of log-structured storage

## License

MIT