
# Parallel to a short data file 
# Has it's own binary format [key_size|val_size|offset|timestamp|key], this is used to compute keyDir quickly on startup
import os 
import struct 
from typing import Iterator 

from bitcask.keydir import KeyDirEntry 

# Hint entry format: [key_size(2) | value_size(4) | offset(4) | timestamp(4) | key]
# No value, no CRC — hint files are only written during compaction
# alongside a known-good merged data file

HINT_HEADER_FORMAT = "!H I I I"
HINT_HEADER_SIZE = struct.calcsize(HINT_HEADER_FORMAT)

FILE_EXTENSION = ".hint" 

def hint_file_path(directory: str, file_id: int) -> str:
    return f"{directory}/{file_id}{FILE_EXTENSION}"


def has_hint_file(directory: str, file_id: int) -> bool:
    return os.path.exists(hint_file_path(directory, file_id))


def write_hint_file(directory: str, file_id: int, entries: list[tuple[bytes, KeyDirEntry]]) -> None:

    
    """
    Writes a hint file for the given file_id and entries to the specified directory.
    """
    with open(hint_file_path(directory, file_id), "wb") as f:
        for key, entry in entries:
            key_size = len(key)
            value_size = entry.value_size
            offset = entry.offset
            timestamp = entry.timestamp
            f.write(struct.pack(HINT_HEADER_FORMAT, key_size, value_size, offset, timestamp))
            f.write(key)

        f.flush()
        os.fsync(f.fileno())


def read_hint_file(directory: str, file_id: int) -> Iterator[tuple[bytes, KeyDirEntry]]:
    with open(hint_file_path(directory, file_id), "rb") as f:
        while True:
            header = f.read(HINT_HEADER_SIZE)
            if not header:
                break

            if len(header) < HINT_HEADER_SIZE:
                # Partial header — shouldn't happen since hint files
                # are written atomically during compaction, but be safe.
                break

            key_size, value_size, offset, timestamp = struct.unpack(HINT_HEADER_FORMAT, header)
            key = f.read(key_size)

            if len(key) < key_size:
                # Partial key — shouldn't happen, but be safe.
                break


            entry = KeyDirEntry(
                file_id=file_id,
                offset=offset,
                value_size=value_size,
                timestamp=timestamp,
            )

            yield key, entry 

def delete_hint_file(directory: str, file_id: int) -> None:
    """Remove a hint file from disk."""
    path = hint_file_path(directory, file_id)
    if os.path.exists(path):
        os.remove(path)
