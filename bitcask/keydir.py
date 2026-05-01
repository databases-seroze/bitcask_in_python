# Thin wrapper that keeps things clean 
# 

# from typing import NamedTuple

from dataclasses import dataclass 
from collections import defaultdict 
from bitcask.constants import HEADER_SIZE 

@dataclass(slots=True)
class KeyDirEntry():
    file_id: int
    offset: int 
    value_size: int 
    timestamp: int 

    @property 
    def record_size() -> int:
        raise NotImplementedError("Use record_size for(key_size) instead")
   
    def record_size_for(self, key_size: int) -> int:
        return HEADER_SIZE + key_size + self.value_size 
        
class KeyDir:
    """
    In-memory hash index: key -> (file_id, offset, value_size, timestamp).
 
    Every live key in the database has exactly one entry here, pointing
    to its most recent value on disk. This is what makes reads O(1).
 
    Also tracks dead bytes per file for compaction decisions.
    """

    def __init__(self):
        self._data : dict[bytes, KeyDirEntry] = {} 
        self.dead_bytes: dict[int, int] = defaultdict(int)
        
    def get(self, key: bytes) -> KeyDirEntry|None:
        return self._data.get(key)
        
    def put(self, key: bytes, entry: KeyDirEntry) -> None:
        old = self._data.get(key)
        if old is not None:
            dead_size = old.record_size_for(len(key))
            self.dead_bytes[old.file_id] += dead_size 
        self._data[key] = entry 
        
    def remove(self, key: bytes) -> KeyDirEntry|None:
        old = self._data.pop(key, None)
        if old is not None:
            dead_size = old.record_size_for(len(key))
            self.dead_bytes[old.file_id] += dead_size 
            
        return old 
        
    def __len__(self) -> int:
        return len(self._data)
        
    def __contains__(self, key: bytes) -> bool:
        return key in self._data
        
    def keys(self):
        return self._data.keys()

    def items(self):
        return self._data.items()

    def is_live(self, key: bytes, file_id: int, offset: int) -> bool:
        """
        Liveness check for compaction.
        Returns True only if the keydir currently points to this
        exact file and offset for this key.
        """

        entry = self._data.get(key)
        if entry is None:
            return False 
        return entry.file_id == file_id and entry.offset == offset 

    def get_dead_ratio(self, file_id: int, file_size: int) -> float:
        """
        What fraction of a file is dead data?
        Used by compaction to decide which files to merge.
        """

        if file_size == 0:
            return 0.0 

        return self.dead_bytes.get(file_id, 0) / file_size
       
    def clear_dead_bytes(self, file_id: int) -> None: 
       self.dead_bytes.pop(file_id, None)
      
    def __repr__(self):
               return f"<KeyDir entries={len(self._data)} tracked_files={len(self.dead_bytes)}>" 



    