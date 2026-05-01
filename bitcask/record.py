
import time
import struct 
import zlib 
from bitcask.constants import HEADER_FORMAT, HEADER_SIZE, TOMBSTONE_FLAG

ALL_F = 0xFFFFFFFF

# Lowest building block known as binary format 
class Record:
    """
    Lowest-level building block. Represents a single entry in a data file.
 
    Binary layout on disk:
        [crc(4) | timestamp(4) | flags(1) | key_size(2) | value_size(4) | key | value]
         ^^^^^^                                                                      
         CRC is computed over everything AFTER the crc field itself.
    """

    def __init__(self, key: bytes, value: bytes, timestamp: int = 0, flags: int = 0 ):
        self.key = key 
        self.value = value 
        self.timestamp = timestamp or int(time.time())
        self.flags = flags 
        self._stored_crc = 0 
        
    def encode(self) -> bytes: 
        # Serialize this record into byte 
        # 
        key_len = len(self.key)
        value_len = len(self.value)
        
        # Pack everything expect crc first crc covers all of this 
        header_without_crc = struct.pack(
            HEADER_FORMAT, 
            0,
            self.timestamp, 
            self.flags, 
            key_len, 
            value_len
        )
        
        payload = header_without_crc + self.key + self.value 
        crc = zlib.crc32(payload) & ALL_F 
        
        # Pack everything expect crc first crc covers all of this 
        header_with_crc = struct.pack(
            HEADER_FORMAT, 
            crc,
            self.timestamp, 
            self.flags, 
            key_len, 
            value_len
        )
                
        return header_with_crc + self.key + self.value  
        
    @classmethod
    def decode(cls, buffer: bytes) -> "Record" : 
        """
        Deserialize bytes into a Record.
        `buffer` must contain at least the full record (header + key + value).
        """
        
        if len(buffer) < HEADER_SIZE:
            raise ValueError(f"Buffer too small: {len(buffer)} {HEADER_SIZE}")
            
        crc, timestamp, flags, key_size, val_size = struct.unpack(
            HEADER_FORMAT, 
            buffer[:HEADER_SIZE]
        )
        
        total_size = HEADER_SIZE + key_size + val_size 
        if len(buffer)!=total_size:
            raise ValueError(f"Buffer size and total size are not matching buffer_size = {len(buffer)}, total_size = {total_size}")
        
        key = buffer[HEADER_SIZE: HEADER_SIZE+key_size]
        val = buffer[HEADER_SIZE+key_size: HEADER_SIZE+key_size+val_size]
        
        record = Record(key, val, timestamp, flags)
        record._stored_crc = crc 
        return record 
        
    @classmethod
    def decode_from_file(cls, f) -> tuple["Record", int] | None: 
        """
        Read one record from a file object at its current position.
        Returns (record, bytes_read) or None if EOF.
        """

        header_data = f.read(HEADER_SIZE)
        if not header_data:
            return None

        if len(header_data) < HEADER_SIZE:
            # Partial header — likely a crash mid-write
            return None

        crc, timestamp, flags, key_size, value_size = struct.unpack(
            HEADER_FORMAT, header_data
        )

        body = f.read(key_size + value_size)
        if len(body) < key_size + value_size:
            # Partial body — crash mid-write
            return None

        key = body[:key_size]
        value = body[key_size : key_size + value_size]

        record = cls(key=key, value=value, timestamp=timestamp, flags=flags)
        record._stored_crc = crc
 
        bytes_read = HEADER_SIZE + key_size + value_size
        return record, bytes_read

        
    def validate_checksum(self) -> bool: 
        """Verify if CRC matches, only works on decoded records."""
        header_without_crc = struct.pack(
            HEADER_FORMAT,
            0,
            self.timestamp, 
            self.flags, 
            len(self.key),
            len(self.value)
        )
        
        payload = header_without_crc + self.key + self.value 
        expected_crc = zlib.crc32(payload) & ALL_F 
        
        return expected_crc == self._stored_crc 
        
    @property 
    def is_tombstone(self) -> bool: 
        return bool(self.flags & TOMBSTONE_FLAG)

    @classmethod
    def tombstone(cls, key: bytes, timestamp: int = None) -> "Record":
        """Factory method to create a delete record."""
        return cls(key=key, value=b"", timestamp=timestamp, flags=TOMBSTONE_FLAG)

    @property 
    def size(self) -> int:
        # Returns total bytes this record occupies on disk 
        return HEADER_SIZE + len(self.key) + len(self.value)
        
    def __repr__(self) -> str:
        kind = "TOMBSTONE" if self.is_tombstone else "RECORD"
        return f"<{kind} key = {self.key!r} value = {self.value!r} ts = {self.timestamp}>"