from bitcask.record import Record 
from bitcask.constants import HEADER_SIZE 

def test_record_encode_decode():
    # Quick sanity check
    r = Record(key=b"hello", value=b"world")
    encoded = r.encode()
    print(f"{encoded} encoded")
    print(f"Encoded size: {len(encoded)} bytes")
    print(f"Header size:  {HEADER_SIZE} bytes")
 
    decoded = Record.decode(encoded)
    assert decoded.validate_checksum()
    assert decoded.key == b"hello"
    assert decoded.value == b"world"
    assert not decoded.is_tombstone
     
    # Tombstone
    t = Record.tombstone(b"hello")
    t_enc = t.encode()
    t_dec = Record.decode(t_enc)
    assert t_dec.validate_checksum()
    assert t_dec.is_tombstone
    assert t_dec.value == b""
    print(f"Tombstone: {t_dec}")
 
    # Corrupt data detection
    corrupted = bytearray(encoded)
    corrupted[-1] ^= 0xFF  # flip a bit in the value
    bad = Record.decode(bytes(corrupted))
    assert not bad.validate_checksum()
    print("Corruption detected correctly")
 
    print("\nAll checks passed!")
