import tempfile
import shutil
from bitcask.bitcask import Bitcask
import os 

def test_bitcask():
    test_dir = tempfile.mkdtemp()
    try:
        # === Basic CRUD ===
        with Bitcask(test_dir) as db:
            # Put
            db.put("name", "alice")
            db.put("age", "30")
            db.put("city", "tokyo")
            assert len(db) == 3
            print(f"After 3 puts: {db}")
 
            # Get
            assert db.get("name") == "alice"
            assert db.get("age") == "30"
            assert db.get("city") == "tokyo"
            assert db.get("missing") is None
            print("Gets: all correct")
 
            # Overwrite
            db.put("name", "bob")
            assert db.get("name") == "bob"
            assert len(db) == 3
            print("Overwrite: correct")
 
            # Delete
            assert db.delete("age") is True
            assert db.delete("age") is False  # already deleted
            assert db.get("age") is None
            assert len(db) == 2
            print("Delete: correct")
 
            # Contains
            assert "name" in db
            assert "age" not in db
            print("Contains: correct")
 
            # Keys
            assert sorted(db.keys()) == ["city", "name"]
            print(f"Keys: {sorted(db.keys())}")
 
            # Stats
            s = db.stats()
            print(f"Stats: {s}")
 
        # === Recovery: reopen and verify ===
        with Bitcask(test_dir) as db:
            assert db.get("name") == "bob"
            assert db.get("city") == "tokyo"
            assert db.get("age") is None
            assert len(db) == 2
            print("\nRecovery: all data intact after reopen")
 
        # === Compaction ===
        # Create some garbage across multiple files
        shutil.rmtree(test_dir)
        os.makedirs(test_dir)
 
        with Bitcask(test_dir) as db:
            # Write initial data
            for i in range(10):
                db.put(f"key{i}", f"value{i}")
 
            # Overwrite half of them many times to create garbage
            for _ in range(5):
                for i in range(5):
                    db.put(f"key{i}", f"updated_{i}_{_}")
 
            # Delete a few
            db.delete("key7")
            db.delete("key8")
 
            print(f"\nBefore compaction: {db.stats()}")
 
            # Force compaction (ignoring thresholds for testing)
            merged = db.force_compact()
            print(f"After compaction: {db.stats()}")
            print(f"Merged file id: {merged}")
 
            # Verify all data still correct
            for i in range(5):
                assert db.get(f"key{i}") == f"updated_{i}_4"
            for i in range(5, 7):
                assert db.get(f"key{i}") == f"value{i}"
            assert db.get("key7") is None
            assert db.get("key8") is None
            assert db.get("key9") == "value9"
            assert len(db) == 8
            print("Post-compaction data integrity: VERIFIED")
 
        # Verify recovery after compaction
        with Bitcask(test_dir) as db:
            assert len(db) == 8
            assert db.get("key0") == "updated_0_4"
            assert db.get("key7") is None
            print("Recovery after compaction: VERIFIED")
 
        # === Context manager and closed state ===
        db = Bitcask(test_dir)
        db.close()
        try:
            db.get("key0")
            assert False, "Should have raised"
        except RuntimeError:
            print("\nClosed state check: correctly rejected operation")
 
        print("\nAll checks passed!")
 
    finally:
        shutil.rmtree(test_dir)
