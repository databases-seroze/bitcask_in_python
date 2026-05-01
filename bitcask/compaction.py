
# Could be run in a background thread 

"""
Compactor(directory, keydir)
compact(immutable_files) → (new_merged_file, deleted_file_ids)
Does the copy-if-live loop
Writes the merged data file + hint file
Updates keydir in place
Deletes old files
"""
from bitcask.hintfile import delete_hint_file, write_hint_file
from bitcask.keydir import KeyDir, KeyDirEntry 
from bitcask.datafile import DataFile
from typing import List

def find_compactable_files(keydir: KeyDir, data_files: dict[int, DataFile], active_file_id: int, threshold: int, min_size: int) -> List[int]:
    """
    Identify immutable files worth compacting.
 
    A file is compactable if:
      1. It's not the active file.
      2. Its size exceeds min_size (don't bother with tiny files).
      3. Its dead bytes ratio exceeds the threshold.
 
    Returns a list of file_ids sorted oldest first.
    """
    candidates = []
    
    for file_id, df in data_files.items():
        if file_id == active_file_id:
            continue
        if df.size < min_size:
            continue
 
        dead_ratio = keydir.get_dead_ratio(file_id, df.size)
        if dead_ratio >= threshold:
            candidates.append(file_id)
 
    return sorted(candidates)

def compact(directory: str, keydir: KeyDir, data_files: dict[int, DataFile], file_ids_to_compact: list[int], next_file_id: int ) -> int | None: 
    """
    Merge the given immutable files into a single new file.
 
    Steps:
      1. Create a new merged data file.
      2. Iterate through each old file, record by record.
      3. For each record, check liveness via keydir.
      4. If live: copy to merged file, update keydir.
      5. Write a hint file for the merged file.
      6. Delete the old files (data + hint).
 
    Args:
        directory: database directory
        keydir: in-memory index (modified in place)
        data_files: dict of all open DataFiles (modified in place)
        file_ids_to_compact: which files to merge
        next_file_id: file_id to use for the merged file
 
    Returns:
        The merged file's file_id, or None if no live records were found.
    """

    if not file_ids_to_compact:
        return None

    merged_file = DataFile(directory, next_file_id)
    hint_entries: list[tuple[bytes, KeyDirEntry]] = []
    live_records = 0
    dead_records = 0 

    for file_id in file_ids_to_compact:
        df = data_files[file_id]
        for record, offset in df.iterate_records():

            if record.is_tombstone:
                # skip 
                dead_records += 1 
                continue 
            if not keydir.is_live(record.key, file_id,  offset):
                # skip 
                dead_records += 1 
                continue 

            # Live record 
            new_offset = merged_file.append(record)

            new_entry = KeyDirEntry(offset = new_offset, file_id = next_file_id, value_size = len(record.value), timestamp = record.timestamp)

            # Update keydir to point to new location.
            # Use direct dict assignment to avoid triggering dead_bytes
            # tracking — the old file is about to be deleted anyway.
            keydir._data[record.key] = new_entry

            hint_entries.append((record.key, new_entry))
            
            live_records += 1

    if live_records == 0:
        merged_file.delete()
    else:
        # Write hint 
        write_hint_file(directory, next_file_id, hint_entries)

        # Make merged file read-only. 
        merged_file.make_read_only()

        data_files[next_file_id] = merged_file  

    # Delete old files (data + hints)

    for file_id in file_ids_to_compact:
        data_files[file_id].delete()
        del data_files[file_id] 

        delete_hint_file(directory, file_id)
        keydir.clear_dead_bytes(file_id)
        
    return next_file_id if live_records>0 else None 