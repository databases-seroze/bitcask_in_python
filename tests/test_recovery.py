import tempfile
import shutil
import os 
import time
from bitcask.record import Record
from bitcask.hintfile import write_hint_file
from bitcask.datafile import DataFile 
from bitcask.recovery import recover
from bitcask.keydir import KeyDir, KeyDirEntry 


def test_recovery():
    test_dir = tempfile.mkdtemp()

    
    try:
        # === Test 1: Fresh database (no files) ===
        keydir, files, active_id = recover(test_dir)
        assert len(keydir) == 0
        assert len(files) == 0
        assert active_id == 0
        print("Test 1 PASSED: Fresh database returns empty state")
 
        # === Test 2: Single file recovery ===
        df = DataFile(test_dir, file_id=1)
        df.append(Record(key=b"name", value=b"alice", timestamp=100))
        df.append(Record(key=b"age", value=b"30", timestamp=101))
        df.append(Record(key=b"name", value=b"bob", timestamp=102))  # overwrite
        df.close()
 
        keydir, files, active_id = recover(test_dir)
        assert len(keydir) == 2
        assert active_id == 1
        assert keydir.get(b"name").timestamp == 102  # latest wins
        assert keydir.get(b"age").timestamp == 101
        print("Test 2 PASSED: Single file recovery with overwrite")
 
        # Cleanup
        for f in files.values():
            f.close()
 
        # === Test 3: Tombstone handling ===
        df = DataFile(test_dir, file_id=1, read_only=False)
        df.append(Record.tombstone(b"age", timestamp=103))
        df.close()
 
        keydir, files, active_id = recover(test_dir)
        assert len(keydir) == 1
        assert b"age" not in keydir
        assert keydir.get(b"name").timestamp == 102
        print("Test 3 PASSED: Tombstone removes key from keydir")
 
        for f in files.values():
            f.close()
 
        # === Test 4: Multi-file recovery ===
        # Clean up and create fresh files
        shutil.rmtree(test_dir)
        os.makedirs(test_dir)
 
        df1 = DataFile(test_dir, file_id=1)
        df1.append(Record(key=b"x", value=b"1", timestamp=100))
        df1.append(Record(key=b"y", value=b"2", timestamp=101))
        df1.close()
 
        df2 = DataFile(test_dir, file_id=2)
        df2.append(Record(key=b"x", value=b"3", timestamp=102))  # overwrite x
        df2.append(Record(key=b"z", value=b"4", timestamp=103))
        df2.close()
 
        keydir, files, active_id = recover(test_dir)
        assert active_id == 2
        assert len(keydir) == 3  # x, y, z
        assert keydir.get(b"x").file_id == 2  # latest in file 2
        assert keydir.get(b"x").timestamp == 102
        assert keydir.get(b"y").file_id == 1
        assert keydir.get(b"z").file_id == 2
        print("Test 4 PASSED: Multi-file recovery, latest value wins")
 
        for f in files.values():
            f.close()
 
        # === Test 5: Recovery from hint file ===
        shutil.rmtree(test_dir)
        os.makedirs(test_dir)
 
        # Create a data file
        df1 = DataFile(test_dir, file_id=1)
        off_a = df1.append(Record(key=b"a", value=b"alpha", timestamp=200))
        off_b = df1.append(Record(key=b"b", value=b"beta", timestamp=201))
        df1.close()
 
        # Write a hint file for it (simulating post-compaction)
        hint_entries = [
            (b"a", KeyDirEntry(file_id=1, offset=off_a, value_size=5, timestamp=200)),
            (b"b", KeyDirEntry(file_id=1, offset=off_b, value_size=4, timestamp=201)),
        ]
        write_hint_file(test_dir, file_id=1, entries=hint_entries)
 
        # Create a second file (active, no hint)
        df2 = DataFile(test_dir, file_id=2)
        df2.append(Record(key=b"c", value=b"gamma", timestamp=202))
        df2.close()
 
        keydir, files, active_id = recover(test_dir)
        assert active_id == 2
        assert len(keydir) == 3
        assert keydir.get(b"a").offset == off_a
        assert keydir.get(b"b").offset == off_b
        assert keydir.get(b"c") is not None
        print("Test 5 PASSED: Hint file used for recovery (fast path)")
 
        for f in files.values():
            f.close()
 
        # === Test 6: Corrupt trailing record (crash recovery) ===
        shutil.rmtree(test_dir)
        os.makedirs(test_dir)
 
        df = DataFile(test_dir, file_id=1)
        df.append(Record(key=b"good", value=b"data", timestamp=300))
        valid_end = df.size
 
        # Simulate a crash: write garbage bytes at the end
        with open(df.file_path, "ab") as f:
            f.write(b"\xff\xfe\xfd\xfc\xfb\xfa")
        df.close()
 
        keydir, files, active_id = recover(test_dir)
        assert len(keydir) == 1
        assert keydir.get(b"good") is not None
 
        # Active file should have been truncated to remove garbage
        assert files[1].size == valid_end
        print("Test 6 PASSED: Corrupt trailing record truncated on active file")
 
        for f in files.values():
            f.close()
 
        print("\nAll checks passed!")
 
    finally:
        shutil.rmtree(test_dir)

