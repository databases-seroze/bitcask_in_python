from record import Record
from typing import Iterator 

class DataFile: 
    
    def __init__(self):
        pass 
        
    def append(self, record: Record) -> offset:
        # write to the data file and return the offset 
        pass 
        
    def read_at(self, offset: int, size: int) -> bytes: 
        pass 
        
    def iterate_records(self) -> Iterator[Record, offset]:
        pass 
        
    def size(self) -> int: 
        pass 
        
    def close(self) -> bool:
        pass 
    
    def delete(self) -> bool:
        pass 
        
    
        