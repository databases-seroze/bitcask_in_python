"""
TOMBSTONE = b"__BITCASK_TOMBSTONE__"
HEADER_FORMAT = "!I I H I" — struct format for [crc, timestamp, key_size, value_size]
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
MAX_FILE_SIZE = 256 * 1024 * 1024 — rotation threshold
HINT_HEADER_FORMAT, HINT_HEADER_SIZE

"""
import struct 

HEADER_FORMAT = "!I I I H I" # — struct format for [crc, timestamp, flags, key_size, value_size]
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
TOMBSTONE_FLAG = 0x01


# File rotation
MAX_FILE_SIZE = 256 * 1024 * 1024  # 256 MB
 
# Compaction
COMPACTION_THRESHOLD = 0.6
MIN_COMPACTION_SIZE = 1024 * 1024  # 1 MB
 
