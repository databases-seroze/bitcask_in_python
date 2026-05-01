
# The orchestrator, delegates everything 
# 
# 
import os
import time
 
from bitcask.constants import MAX_FILE_SIZE
from bitcask.datafile import DataFile
from bitcask.keydir import KeyDir, KeyDirEntry
from bitcask.record import Record
from bitcask.recovery import recover
from bitcask.compaction import compact, find_compactable_files


class Bitcask:
    def __init__(self, directory: str):
        """
        Bitcask key-value store.
     
        An append-only, log-structured storage engine with O(1) reads.
        All keys are held in memory; values are read from disk with a single seek.
     
        Usage:
            db = Bitcask("/path/to/data")
            db.put("name", "alice")
            print(db.get("name"))    # "alice"
            db.delete("name")
            db.close()
        """
        self.directory = directory
        os.makedirs(directory, exist_ok=True)

        # Recover state from disk
        self.keydir, self.data_files, active_id = recover(directory)

        # Determine the next file ID: max existing + 1, or 1 if fresh
        if self.data_files:
            self._next_file_id = max(self.data_files.keys()) + 1
        else:
            self._next_file_id = 1

            
        # Set up the active file
        if active_id == 0:
            # Fresh database — create the first file
            self.active_file = self._create_new_file()
        else:
            self.active_file = self.data_files[active_id]

        self._closed = False

    def _create_new_file(self) -> DataFile:
        """Create a new active data file with the next file ID."""
        file_id = self._next_file_id
        self._next_file_id += 1
 
        df = DataFile(self.directory, file_id)
        self.data_files[file_id] = df
        return df


    def _maybe_rotate(self) -> None:
        """If the active file exceeds MAX_FILE_SIZE, rotate to a new one."""
        if self.active_file.size >= MAX_FILE_SIZE:
            self.active_file.make_read_only()
            self.active_file = self._create_new_file()


    def get(self, key: str) -> str | None:
        """
        Retrieve the value for a key.
        Returns None if the key doesn't exist.
        """
        self._check_closed()
 
        key_bytes = key.encode("utf-8")
        entry = self.keydir.get(key_bytes)
        if entry is None:
            return None
 
        df = self.data_files[entry.file_id]
        value_bytes = df.read_value_at(entry.offset, len(key_bytes), entry.value_size)
        return value_bytes.decode("utf-8")

    def put(self, key: str, value: str) -> None:
        """Store a key-value pair."""
        self._check_closed()
 
        key_bytes = key.encode("utf-8")
        value_bytes = value.encode("utf-8")
        timestamp = int(time.time())
 
        record = Record(key=key_bytes, value=value_bytes, timestamp=timestamp)
 
        self._maybe_rotate()
 
        offset = self.active_file.append(record)
 
        entry = KeyDirEntry(
            file_id=self.active_file.file_id,
            offset=offset,
            value_size=len(value_bytes),
            timestamp=timestamp,
        )
        self.keydir.put(key_bytes, entry)


    def delete(self, key: str) -> bool:
        """
        Delete a key. Returns True if the key existed, False otherwise.
        """
        self._check_closed()
 
        key_bytes = key.encode("utf-8")
 
        if key_bytes not in self.keydir:
            return False
 
        timestamp = int(time.time())
        tombstone = Record.tombstone(key_bytes, timestamp=timestamp)
 
        self._maybe_rotate()
        self.active_file.append(tombstone)
        self.keydir.remove(key_bytes)
 
        return True


    def keys(self) -> list[str]:
        """Return all live keys in the database."""
        self._check_closed()
        return [k.decode("utf-8") for k in self.keydir.keys()]

    def __len__(self) -> int:
        """Number of live keys."""
        return len(self.keydir)


    def __contains__(self, key: str) -> bool:
        return key.encode("utf-8") in self.keydir


    def compact(self) -> int | None:
        """
        Manually trigger compaction.
        Merges all eligible immutable files into a new file.
        Returns the merged file ID, or None if nothing to compact.
        """
        self._check_closed()
 
        candidates = find_compactable_files(
            keydir=self.keydir,
            data_files=self.data_files,
            active_file_id=self.active_file.file_id,
        )
 
        if not candidates:
            return None
 
        merged_id = self._next_file_id
        self._next_file_id += 1
 
        return compact(
            directory=self.directory,
            keydir=self.keydir,
            data_files=self.data_files,
            file_ids_to_compact=candidates,
            next_file_id=merged_id,
        )
 
    def force_compact(self, file_ids: list[int] | None = None) -> int | None:
        """
        Force compaction on specific files (or all immutable files),
        ignoring dead ratio thresholds. Useful for testing and maintenance.
        """
        self._check_closed()
 
        if file_ids is None:
            file_ids = sorted(
                fid for fid in self.data_files
                if fid != self.active_file.file_id
            )
 
        if not file_ids:
            return None
 
        merged_id = self._next_file_id
        self._next_file_id += 1
 
        return compact(
            directory=self.directory,
            keydir=self.keydir,
            data_files=self.data_files,
            file_ids_to_compact=file_ids,
            next_file_id=merged_id,
        )


    def sync(self) -> None:
        """Flush the active file to disk."""
        self._check_closed()
        if self.active_file and not self.active_file.read_only:
            self.active_file._f.flush()
            os.fsync(self.active_file._f.fileno())

    def close(self) -> None:
        """Close all file handles. The instance is unusable after this."""
        if self._closed:
            return
        for df in self.data_files.values():
            df.close()
        self._closed = True


    def _check_closed(self) -> None:
        if self._closed:
            raise RuntimeError("Bitcask instance is closed")


    def stats(self) -> dict:
        """Return database statistics."""
        total_size = sum(df.size for df in self.data_files.values())
        total_dead = sum(self.keydir.dead_bytes.values())
 
        return {
            "num_keys": len(self.keydir),
            "num_files": len(self.data_files),
            "active_file_id": self.active_file.file_id,
            "total_size_bytes": total_size,
            "total_dead_bytes": total_dead,
            "dead_ratio": total_dead / total_size if total_size > 0 else 0.0,
        }

    def __repr__(self):
        if self._closed:
            return "<Bitcask CLOSED>"
        return (
            f"<Bitcask dir={self.directory!r} "
            f"keys={len(self.keydir)} files={len(self.data_files)}>"
        )

    def __enter__(self):
        return self
 
    def __exit__(self, *args):
        self.close()


    