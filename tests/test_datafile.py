import tempfile
import shutil
from bitcask.datafile import DataFile
from bitcask.record import Record

def test_datafile_operations():
    test_dir = tempfile.mkdtemp()

    try:
        # Create an active file and write some records
        df = DataFile(test_dir, file_id=1)
        print(f"Created: {df}")
 
        r1 = Record(key=b"hello", value=b"world")
        off1 = df.append(r1)
        print(f"Wrote r1 at offset {off1}")
 
        r2 = Record(key=b"foo", value=b"bar")
        off2 = df.append(r2)
        print(f"Wrote r2 at offset {off2}")
 
        r3 = Record.tombstone(b"hello")
        off3 = df.append(r3)
        print(f"Wrote tombstone at offset {off3}")
 
        print(f"File size: {df.size}")
 
        # Read value directly
        val = df.read_value_at(off1, key_size=5, value_size=5)
        assert val == b"world", f"Expected b'world', got {val!r}"
        print(f"read_value_at({off1}): {val}")
 
        val2 = df.read_value_at(off2, key_size=3, value_size=3)
        assert val2 == b"bar"
        print(f"read_value_at({off2}): {val2}")
 
        # Read full record
        rec = df.read_record_at(off3)
        assert rec.is_tombstone
        assert rec.key == b"hello"
        print(f"read_record_at({off3}): {rec}")
 
        # Iterate all records
        print("\nIterating all records:")
        records = list(df.iterate_records())
        assert len(records) == 3
        for rec, off in records:
            print(f"  offset={off}: {rec}")
 
        # Make read-only
        df.make_read_only()
        print(f"\nAfter make_read_only: {df}")
        try:
            df.append(Record(key=b"x", value=b"y"))
            assert False, "Should have raised"
        except RuntimeError:
            print("Correctly rejected write to read-only file")
 
        # Find data files
        df2 = DataFile(test_dir, file_id=2)
        df2.append(Record(key=b"a", value=b"b"))
        df2.close()
 
        found = DataFile.find_data_files(test_dir)
        assert found == [1, 2], f"Expected [1, 2], got {found}"
        print(f"\nFound data files: {found}")
 
        df.close()
 
        # Truncation test
        df3 = DataFile(test_dir, file_id=3)
        off_a = df3.append(Record(key=b"keep", value=b"this"))
        off_b = df3.append(Record(key=b"drop", value=b"this"))
        size_before = df3.size
        df3.truncate(off_b)
        assert df3.size == off_b
        print(f"\nTruncation: {size_before} -> {df3.size} (dropped last record)")
 
        # Verify only first record survives
        surviving = list(df3.iterate_records())
        assert len(surviving) == 1
        assert surviving[0][0].key == b"keep"
        print(f"Surviving records after truncate: {[r.key for r, _ in surviving]}")
        df3.close()
 
        print("\nAll checks passed!")
 
    finally:
        shutil.rmtree(test_dir)
