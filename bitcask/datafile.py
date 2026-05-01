from bitcask.constants import HEADER_SIZE
from bitcask.record import Record
from typing import Iterator 
import os 

class DataFile: 
    """
    Represents a data file on disk.
    Each file has an integer ID. The naming convention is {file_id}.bitcask.data 

    Files are either:
        - Active (read-write): the one being appended to 
        - Immutable (read-only, for compaction)
    
    """

    FILE_EXTENSION = ".bitcask.data"
    
    def __init__(self, directory: str, file_id: int, read_only: bool = False):
        self.directory = directory
        self.file_id = file_id
        self.read_only = read_only
        self.file_path = f"{directory}/{file_id}{self.FILE_EXTENSION}"

        if self.read_only:
            self._f = open(self.file_path, "rb")
        else:
            # a+b: append + read, creates if it doesn't exist
            self._f = open(self.file_path, "a+b")

        # Track current offset for appending
        self._f.seek(0, 2)  # Move to end of file
        self._write_offset = self._f.tell()

    def append(self, record: Record) -> int:
        # write to the data file and return the offset 
        if self.read_only:
            raise RuntimeError("Data file is read-only")
        
        # Write the record to the file
        offset = self._write_offset
        self._f.write(record.encode())
        self._f.flush()
        os.fsync(self._f.fileno())
        self._write_offset = self._f.tell()
        return offset
        
    def read_value_at(self, offset: int, key_size: int, value_size: int) -> bytes: 
        # read from the data file at the given offset
        value_offset = offset + HEADER_SIZE + key_size
        self._f.seek(value_offset)
        return self._f.read(value_size)

    def read_record_at(self, offset: int) -> Record:
        self._f.seek(offset)
        result = Record.decode_from_file(self._f)
        if result is None:
            return None 
        record, _ = result 
        return record 

    def iterate_records(self) -> Iterator[tuple[Record, int]]:
        """
        Sequentially scan the entire file, yielding (record, offset) pairs.
        Used by recovery (to rebuild keydir) and compaction (to find live records).
 
        Stops at EOF or at the first corrupt/partial record.
        """

        self._f.seek(0)
        while True:
            offset = self._f.tell()
            result = Record.decode_from_file(self._f)
            if result is None:
                break
            record, bytes_read = result
            if not record.validate_checksum():
                # corrupt record stop here 
                # Recovery will truncate the file at this offset 
                break
            yield record, offset


    @property
    def size(self) -> int: 
        return self._write_offset 


    def truncate(self, offset: int) -> None:
        """
        Truncate file at the given offset.
        Used by recovery to discard a corrupt trailing record after a crash.
        """
        if self.read_only:
            # because this file might be used by multiple processes Eg: compaction 
            raise RuntimeError(f"Cannot truncate read-only file {self.path}")
        self._f.seek(offset)
        self._f.truncate()
        self._f.flush()
        os.fsync(self._f.fileno())
        self._write_offset = offset
        
    def close(self) -> None:
        """Close the file handle."""
        if self._f and not self._f.closed:
            self._f.close()

    
    def delete(self) -> None:
        """Delete the data file."""
        self.close()
        if os.path.exists(self.file_path):
            os.remove(self.file_path)

    def make_read_only(self) -> None:
        """Make the data file read-only."""
        if self.read_only:
            return 

        self.close() 
        self._f = open(self.file_path, "rb")
        self.read_only = True 
        
    @staticmethod
    def find_data_files(directory: str) -> list[int]:
        """
        Scan a directory for existing data files and return their IDs, sorted.
        """
        file_ids = []
        for fname in os.listdir(directory):
            if fname.endswith(DataFile.FILE_EXTENSION):
                try:
                    file_id = int(fname.replace(DataFile.FILE_EXTENSION, ""))
                    file_ids.append(file_id)
                except ValueError:
                    continue
        return sorted(file_ids)
