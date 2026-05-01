import pytest 
from bitcask.keydir import KeyDirEntry, KeyDir
from bitcask.constants import HEADER_SIZE

def test_keydir1():
    kd = KeyDir()
 
    # Put a key
    kd.put(b"name", KeyDirEntry(file_id=1, offset=0, value_size=5, timestamp=100))
    assert len(kd) == 1
    assert kd.get(b"name").value_size == 5
    assert kd.dead_bytes[1] == 0  # no dead data yet
    print(f"After first put: {kd}")
 
    # Overwrite — old record becomes dead
    kd.put(b"name", KeyDirEntry(file_id=1, offset=30, value_size=7, timestamp=200))
    assert len(kd) == 1
    assert kd.get(b"name").offset == 30
    # Dead bytes = HEADER_SIZE(15) + key_size(4) + old_value_size(5) = 24
    expected_dead = HEADER_SIZE + len(b"name") + 5
    assert kd.dead_bytes[1] == expected_dead, f"Expected {expected_dead}, got {kd.dead_bytes[1]}"
    print(f"After overwrite: dead_bytes[1] = {kd.dead_bytes[1]}")
 
    # Liveness check
    assert not kd.is_live(b"name", file_id=1, offset=0)   # old offset — dead
    assert kd.is_live(b"name", file_id=1, offset=30)       # current — live
    assert not kd.is_live(b"gone", file_id=1, offset=0)    # unknown key — dead
    print("Liveness checks passed")
 
    # Delete — record becomes dead
    old_dead = kd.dead_bytes[1]
    kd.remove(b"name")
    assert len(kd) == 0
    assert b"name" not in kd
    new_dead = HEADER_SIZE + len(b"name") + 7  # the overwritten record
    assert kd.dead_bytes[1] == old_dead + new_dead
    print(f"After delete: dead_bytes[1] = {kd.dead_bytes[1]}")
 
    # Dead ratio
    fake_file_size = 100
    ratio = kd.get_dead_ratio(1, fake_file_size)
    print(f"Dead ratio for file 1 (size={fake_file_size}): {ratio:.2%}")
 
    # Cleanup
    kd.clear_dead_bytes(1)
    assert 1 not in kd.dead_bytes
    print("Cleared dead bytes tracking for file 1")
 
    print("\nAll checks passed!")
