from bitcask.datafile import DataFile 
from bitcask.hintfile import has_hint_file, read_hint_file
from bitcask.keydir import KeyDir, KeyDirEntry
import os 

# Startup logic, isolated from main class 
# Scans directory, finds data files and hint files 
# for files without hint files; scans data files, validates CRC, builds keydir and hint files 
# Handles corrupt trailing records in the active file (truncate)

def recover(directory: str) -> tuple[KeyDir, dict[int, DataFile]]:
    """
    Rebuild the entire database state from disk.
 
    Called once at startup. Returns:
        - keydir: fully rebuilt in-memory index
        - data_files: dict of {file_id: DataFile} for all existing files
        - active_file_id: the ID of the active (newest) file
 
    Recovery strategy:
        1. Scan directory for data files, sort by file_id (oldest first).
        2. For each file:
           - If a hint file exists: load keydir entries from it (fast path).
           - Otherwise: scan the data file record by record (slow path).
        3. The newest file becomes the active file (read-write).
           All others are opened read-only.
        4. If the active file has a corrupt trailing record (crash mid-write),
           truncate it at the last valid record boundary.
    """
    os.makedirs(directory, exist_ok=True)
    file_ids = DataFile.find_data_files(directory) # returns a sorted list of file IDs
    
    keydir = KeyDir()
    data_files: dict[int, DataFile] = {}

    if not file_ids:
        # Fresh database — no files on disk yet.
        # Caller will create the first active file.
        return keydir, data_files, 0


    # The newest file is the active one
    active_file_id = file_ids[-1]

    for file_id in file_ids:
        is_active = file_id == active_file_id 
        read_only = not is_active 
        
        data_file = DataFile(directory, file_id, read_only=read_only)
        data_files[file_id] = data_file

        if not is_active and has_hint_file(directory, file_id):
            # Faster load from hint file if available (fast path)
            _recover_from_hint(keydir, directory, file_id)
        else:
            # Slow path: scan the data file record by record
            _recover_from_data(keydir, data_file, truncate_on_corrupt=is_active)

    return keydir, data_files, active_file_id 

def _recover_from_hint(keydir: KeyDir, directory: str, file_id: int) -> None:

    """
    Rebuild keydir entries from a hint file.
    
    Hint files are written during compaction and contain
    (key, offset, value_size, timestamp) — everything the keydir
    needs, without the actual values. This is much faster than
    scanning the data file.
    """
    for key, entry in read_hint_file(directory, file_id):
        existing = keydir.get(key)
 
        # Only insert if this entry is newer than what we have.
        # Since we process files oldest-first, this should always
        # be true — but be defensive.
        if existing is None or entry.timestamp >= existing.timestamp:
            keydir.put(key, entry)

def _recover_from_data(keydir: KeyDir, data_file: DataFile, truncate_on_corrupt: bool = False) -> None:
    """
    Rebuild keydir entries from a data file by scanning record by record.

    For the active file (truncate_on_corrupt=True):
        If we hit a corrupt record, truncate the file there.
        This handles the case where the process crashed mid-write.
    
    For immutable files (truncate_on_corrupt=False):
        A corrupt record is unexpected (these files were closed cleanly
        or produced by compaction). We stop scanning but don't truncate.
    
    """
    last_valid_offset = 0
    
    for record, offset in data_file.iterate_records():
        # iterate_records already validates CRC and stops on corruption,
        # so every record we see here is valid.
        
        if record.is_tombstone:
            keydir.remove(record.key)
        else:
            entry = KeyDirEntry(
                file_id=data_file.file_id,
                offset=offset,
                value_size=len(record.value),
                timestamp=record.timestamp,
            )
 
            existing = keydir.get(record.key)
            if existing is None or entry.timestamp >= existing.timestamp:
                keydir.put(record.key, entry)
            last_valid_offset = offset + record.size
            


    # If the active file has trailing garbage (crash mid-write),
    # truncate at the last valid record boundary.
    if truncate_on_corrupt and last_valid_offset < data_file.size:
        data_file.truncate(last_valid_offset)

        

    