

PUT("foo", "bar"):
  append [crc|ts|3|3|foo|bar] to active file at offset X
  keydir["foo"] = (active_file_id, X, 3, ts)

GET("foo"):
  lookup keydir["foo"] → (file_id, offset, size, ts)
  seek to offset in file_id, read size bytes → "bar"

RECOVERY:
  for each file (oldest → newest):
    if hint file exists: load keydir entries from it
    else: scan data file, validate CRC, rebuild keydir
  discard any trailing corrupt record in active file
  
Compaction is used to clean up dead entries in closed files. 
  Only immutable (closed) files. Never the current active file, since it's still being written to.
  
  Step 1 — Iterate through each candidate file, record by record:
  Processing file1:
    Record: key="a", offset=0   → check keydir["a"] = (file2, 0)
                                    keydir points elsewhere → DEAD, skip
    Record: key="b", offset=30  → check keydir["b"] = not found (was deleted)
                                    → DEAD, skip
    Record: key="a", offset=60  → check keydir["a"] = (file2, 0)
                                    keydir points elsewhere → DEAD, skip
  
  Processing file2:
    Record: key="a", offset=0   → check keydir["a"] = (file2, 0) ← MATCH!
                                    → LIVE, copy to merged file
    Record: key="b" (tombstone) → "b" not in keydir
                                    → DEAD, skip
  The liveness check is simple: a record is live if and only if the keydir currently points to this exact file and offset for that key.
  Step 2 — Write live records to a new merged file:
  merged_file (new_file_id = 100):
    offset 0: key="a", value="5"    ← only survivor
  Step 3 — Update the keydir to point to the new location:
  keydir["a"] = (100, 0, ...)   ← was (file2, 0), now (merged_file, 0)
  Step 4 — Write a hint file alongside the merged file:
  merged_file.hint:
    key="a", file_id=100, offset=0, value_size=1, timestamp=...
  This hint file lets future recovery skip reading the actual values.
  Step 5 — Delete the old files:
  Delete: file1, file2
  Keep: merged_file (+ hint), file3 (active)
  
  for compaction let's use a simple strategy of doing compaction once every 2 mins 
----------------------------------------------------

bitcask/
├── __init__.py          ← public API facade
├── bitcask.py           ← Bitcask class (main API: get, put, delete, close)
├── datafile.py          ← DataFile class (append records, read at offset, rotate)
├── record.py            ← Record class (serialize/deserialize, CRC, tombstone)
├── keydir.py            ← KeyDir class (in-memory index, wrapper around dict)
├── hintfile.py          ← HintFile class (write/read hint entries)
├── compaction.py        ← Compactor class (merge old files, produce hints)
├── recovery.py          ← Recovery class (rebuild keydir on startup)
└── constants.py         ← magic bytes, tombstone sentinel, header sizes, thresholds

-------------------------------------------------------
Data flow for each operation:

  PUT:  bitcask → record.encode() → datafile.append() → keydir.put()
  GET:  bitcask → keydir.get() → datafile.read_at() → record.decode()
  DEL:  bitcask → record.encode(tombstone) → datafile.append() → keydir.remove()
  BOOT: bitcask → recovery.recover() → keydir + datafiles ready
  MERGE: bitcask → compactor.compact() → new files + updated keydir

Dependency graph (no circular deps):

  constants  ←  record  ←  datafile  ←  hintfile
                                      ←  recovery  ←  bitcask
                                      ←  compaction ←─┘
                keydir  ←─────────────────────────────┘
              
---------------------------------------------------------------------------

Why rotate at 256MB instead of one giant file?
  Several reasons:
    Compaction becomes impossible. Compaction works on immutable files — the active file is never compacted. If you never rotate, you have exactly one file that's always active, and it grows forever. All the dead records just accumulate with no way to reclaim them. The database only grows, never shrinks.
    Recovery gets slower over time. On startup, the active file has no hint file, so recovery must scan it record by record. A 50GB active file means reading 50GB sequentially on every restart. With rotation, the active file is at most 256MB — older files have hint files and recover in milliseconds.
    Filesystem limitations. Some filesystems degrade with very large files — metadata operations, fsync latency, and seek times can all get worse. ext4 supports up to 16TB files, but that doesn't mean it's efficient at that size.
    fsync cost scales with file size. On some filesystems, fsync flushes all dirty pages for the file, not just the ones you just wrote. A larger file means more dirty page tracking overhead.
    Concurrent reads during compaction. If compaction needs to read an old file while the active file is being written to, smaller files mean less contention and faster compaction passes.
    The 256MB threshold is a balance — large enough that you're not creating thousands of tiny files (which has its own overhead in file descriptor usage and directory scanning), small enough that no single file becomes a bottleneck.

---------------------------------------------------------------------------

Do we need wal here ?

  In a traditional database (like PostgreSQL or SQLite), you have two separate structures: the actual data pages (B-tree, heap, etc.) and a WAL that logs changes before they're applied to those pages. The WAL exists because writes to the data pages are in-place — if you crash mid-update of a B-tree node, the page is half-written and corrupted. The WAL lets you replay or undo that partial write on recovery.
  Bitcask doesn't have this problem because it never modifies existing data. Every write is an append to the end of the active file. The worst that can happen on a crash is a partial record at the tail — which our recovery detects via CRC and truncates. The data before that point is untouched and intact.

  So the append-only data file gives you the same durability guarantee a WAL would:
  
    Atomicity — a record is either fully written (valid CRC) or it isn't (truncated on recovery). No partial state.
    Durability — fsync after each append ensures the record is on disk before we return to the caller.
    Recovery — scan forward, validate CRCs, stop at corruption. Exactly what a WAL replay does.
  
  This is actually one of the key insights of log-structured designs — by making the data format itself sequential and append-only, you eliminate the need for a separate write-ahead log. LSM-trees (LevelDB, RocksDB) do use a WAL, but only because their memtable is an in-memory structure that would be lost on crash. The WAL there replays into the memtable. In Bitcask, the keydir (our "memtable") is fully reconstructable from the data files, so there's nothing to replay.

---------------------------------------------------------------------------

  fsync(fd):
    1. Find all dirty pages for this file
       → Page 1 [dirty], Page 5 [dirty], Page 12 [dirty]
  
    2. Submit write I/O for each
       → write page 1 to SSD
       → write page 5 to SSD
       → write page 12 to SSD
  
    3. Send cache flush command to SSD
       → SSD moves data from its DRAM to NAND
  
    4. Wait for all confirmations
  
    5. Mark pages as [clean]
  
    6. Return to your code

---------------------------------------------------------------------------

Further goals

  Locking — a lockfile in the data directory so only one OS process opens the database at a time. Without this, two processes appending to the same file corrupt it.
    - We can use flock in python, it is a kernel level lock that gets reset after reboot. We create a file called bitcask.lock, when second process tries to open it it sees file already created but then it'll fail to acquire the lock over the same inode. This file will remain after crash it's just that atmost only one process will have exclusive-access to this files node. when the process holding the lock gets crashed kernal automatically releases the lock. 
  
  Thread safety — read-write locks around the keydir and active file. Our implementation would break under concurrent access from multiple threads.
    - We can have read-write locks around get(), put(), delete() and compact() 
    
  Expiry/TTL — records can have a time-to-live. Expired entries are treated as dead during compaction and skipped on reads.
    - introduce another timestamp filed and have another flag to represent it's an expiry type record 
    
  Merge triggers — a background process that periodically checks dead ratios and triggers compaction automatically, with configurable schedules,    window policies (e.g., only compact during off-peak hours), and rate limiting to avoid I/O storms.
    
  Erlang NIF integration — Riak's Bitcask is written in Erlang with C NIFs for the hot path (CRC, keydir lookups). Our pure Python implementation would be orders of magnitude slower.

  Crash-safe compaction — if the process dies mid-compaction, Riak can detect the incomplete merged file on recovery and discard it. We don't handle that.

  I/O scheduling — production systems use O_DIRECT, fadvise, and careful buffer management. We're going through Python's buffered I/O, which adds overhead and unpredictability.