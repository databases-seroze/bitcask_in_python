import tempfile
import shutil
import os
from bitcask.record import Record
from bitcask.datafile import DataFile
from bitcask.keydir import KeyDir, KeyDirEntry
from bitcask.compaction import compact
from bitcask.hintfile import has_hint_file, read_hint_file

def test_compaction():
    test_dir = tempfile.mkdtemp()
     
    try:
        # === Setup: simulate a workload that creates garbage ===
        # File 1: write some keys
        df1 = DataFile(test_dir, file_id=1)
        off_a1 = df1.append(Record(key=b"a", value=b"1", timestamp=100))
        off_b1 = df1.append(Record(key=b"b", value=b"2", timestamp=101))
        off_c1 = df1.append(Record(key=b"c", value=b"3", timestamp=102))
        df1.make_read_only()
    
        # File 2: overwrite a, delete b, add d
        df2 = DataFile(test_dir, file_id=2)
        off_a2 = df2.append(Record(key=b"a", value=b"new_a", timestamp=200))
        off_del = df2.append(Record.tombstone(b"b", timestamp=201))
        off_d2 = df2.append(Record(key=b"d", value=b"4", timestamp=202))
        df2.make_read_only()
    
        # File 3: active file
        df3 = DataFile(test_dir, file_id=3)
        off_e3 = df3.append(Record(key=b"e", value=b"5", timestamp=300))
    
        data_files = {1: df1, 2: df2, 3: df3}
    
        # Build the keydir as recovery would
        keydir = KeyDir()
        # File 1
        keydir.put(b"a", KeyDirEntry(file_id=1, offset=off_a1, value_size=1, timestamp=100))
        keydir.put(b"b", KeyDirEntry(file_id=1, offset=off_b1, value_size=1, timestamp=101))
        keydir.put(b"c", KeyDirEntry(file_id=1, offset=off_c1, value_size=1, timestamp=102))
        # File 2 overwrites
        keydir.put(b"a", KeyDirEntry(file_id=2, offset=off_a2, value_size=5, timestamp=200))
        keydir.remove(b"b")  # tombstone
        keydir.put(b"d", KeyDirEntry(file_id=2, offset=off_d2, value_size=1, timestamp=202))
        # File 3
        keydir.put(b"e", KeyDirEntry(file_id=3, offset=off_e3, value_size=1, timestamp=300))
    
        print("=== Before compaction ===")
        print(f"  Keys in keydir: {sorted(keydir.keys())}")
        print(f"  Data files: {sorted(data_files.keys())}")
        print(f"  Dead bytes: {dict(keydir.dead_bytes)}")
        for fid, df in sorted(data_files.items()):
            print(f"  File {fid}: {df.size} bytes, dead_ratio={keydir.get_dead_ratio(fid, df.size):.1%}")
    
        # === Compact files 1 and 2 ===
        merged_id = compact(
            directory=test_dir,
            keydir=keydir,
            data_files=data_files,
            file_ids_to_compact=[1, 2],
            next_file_id=4,
        )
    
        print("\n=== After compaction ===")
        print(f"  Merged file id: {merged_id}")
        print(f"  Keys in keydir: {sorted(keydir.keys())}")
        print(f"  Data files: {sorted(data_files.keys())}")
    
        # Verify: files 1 and 2 are gone
        assert 1 not in data_files
        assert 2 not in data_files
        assert not os.path.exists(os.path.join(test_dir, "1.bitcask.data"))
        assert not os.path.exists(os.path.join(test_dir, "2.bitcask.data"))
        print("  Old files deleted: YES")
    
        # Verify: merged file exists
        assert merged_id == 4
        assert 4 in data_files
        print(f"  Merged file size: {data_files[4].size} bytes")
    
        # Verify: keydir still correct
        assert len(keydir) == 4  # a, c, d, e
        assert b"b" not in keydir  # was deleted
        assert keydir.get(b"a").file_id == 4  # moved to merged
        assert keydir.get(b"a").value_size == 5  # value = "new_a"
        assert keydir.get(b"c").file_id == 4  # moved to merged
        assert keydir.get(b"d").file_id == 4  # moved to merged
        assert keydir.get(b"e").file_id == 3  # untouched (active)
        print("  Keydir correctness: VERIFIED")
    
        # Verify: hint file written
        assert has_hint_file(test_dir, 4)
        hints = list(read_hint_file(test_dir, 4))
        assert len(hints) == 3  # a, c, d (not b — deleted, not e — in active)
        hint_keys = {k for k, _ in hints}
        assert hint_keys == {b"a", b"c", b"d"}
        print(f"  Hint file entries: {sorted(hint_keys)}")
    
        # Verify: values are actually readable from merged file
        merged_df = data_files[4]
        for key in [b"a", b"c", b"d"]:
            entry = keydir.get(key)
            val = merged_df.read_value_at(entry.offset, len(key), entry.value_size)
            print(f"  Read {key.decode()}: {val.decode()}")
    
        # Verify: dead bytes tracking cleaned up
        assert 1 not in keydir.dead_bytes
        assert 2 not in keydir.dead_bytes
        print("  Dead bytes cleanup: VERIFIED")
    
        # Cleanup
        for f in data_files.values():
            f.close()
    
        print("\nAll checks passed!")
    
    finally:
        shutil.rmtree(test_dir)
