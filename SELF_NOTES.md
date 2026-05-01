

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
                
                