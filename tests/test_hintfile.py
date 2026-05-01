import shutil 
import tempfile
import os 
from bitcask.hintfile import write_hint_file, hint_file_path, read_hint_file, has_hint_file, delete_hint_file, HINT_HEADER_SIZE
from bitcask.keydir import KeyDirEntry 


def test_hintfile():
    test_dir = tempfile.mkdtemp()
 
    try:
        # Write a hint file with 3 entries
        entries = [
            (b"alice", KeyDirEntry(file_id=10, offset=0, value_size=100, timestamp=1000)),
            (b"bob", KeyDirEntry(file_id=10, offset=115, value_size=50, timestamp=1001)),
            (b"charlie", KeyDirEntry(file_id=10, offset=180, value_size=200, timestamp=1002)),
        ]
 
        write_hint_file(test_dir, file_id=10, entries=entries)
        print(f"Wrote hint file with {len(entries)} entries")
 
        # Verify it exists
        assert has_hint_file(test_dir, 10)
        assert not has_hint_file(test_dir, 99)
        print("has_hint_file checks passed")
 
        # Read it back
        recovered = list(read_hint_file(test_dir, file_id=10))
        assert len(recovered) == 3
 
        for i, (key, entry) in enumerate(recovered):
            orig_key, orig_entry = entries[i]
            assert key == orig_key, f"Key mismatch: {key!r} != {orig_key!r}"
            assert entry.file_id == orig_entry.file_id
            assert entry.offset == orig_entry.offset
            assert entry.value_size == orig_entry.value_size
            assert entry.timestamp == orig_entry.timestamp
            print(f"  {key.decode()}: offset={entry.offset} value_size={entry.value_size} ts={entry.timestamp}")
 
        print("Round-trip check passed")
 
        # Check file size is compact (no values stored)
        path = hint_file_path(test_dir, 10)
        file_size = os.path.getsize(path)
        expected = sum(HINT_HEADER_SIZE + len(k) for k, _ in entries)
        assert file_size == expected, f"File size {file_size} != expected {expected}"
        print(f"Hint file size: {file_size} bytes (header-only, no values)")
 
        # Delete
        delete_hint_file(test_dir, 10)
        assert not has_hint_file(test_dir, 10)
        print("Delete check passed")
 
        print("\nAll checks passed!")
 
    finally:
        shutil.rmtree(test_dir)
