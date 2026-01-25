"""
Record serialization and deserialization.

Records are dictionaries that need to be converted to bytes for storage
and back to dictionaries for use. We use a simple format:

    [num_fields: 2 bytes]
    For each field:
        [key_len: 2 bytes][key: variable][value_type: 1 byte][value_len: 2 bytes][value: variable]

Value types:
    0 = None
    1 = int (8 bytes, signed)
    2 = float (8 bytes, double)
    3 = str (UTF-8 encoded)
    4 = bool (1 byte)
    5 = bytes (raw)
"""

import struct
from typing import Any, Dict, Optional

# Type codes for serialization
TYPE_NONE = 0
TYPE_INT = 1
TYPE_FLOAT = 2
TYPE_STR = 3
TYPE_BOOL = 4
TYPE_BYTES = 5


class RecordSerializer:
    """
    Serializes and deserializes records (dictionaries) to/from bytes.
    
    This is a simple implementation for learning purposes.
    Production databases use more sophisticated formats.
    """
    
    @staticmethod
    def serialize(record: Dict[str, Any]) -> bytes:
        """
        Convert a record dictionary to bytes.
        
        Args:
            record: Dictionary with string keys and serializable values
            
        Returns:
            Byte representation of the record
            
        Raises:
            ValueError: If a value type is not supported
        """
        parts = []
        
        # Number of fields (2 bytes, unsigned short)
        parts.append(struct.pack('>H', len(record)))
        
        for key, value in record.items():
            # Key (length-prefixed string)
            key_bytes = key.encode('utf-8')
            parts.append(struct.pack('>H', len(key_bytes)))
            parts.append(key_bytes)
            
            # Value (type + length + data)
            value_bytes, type_code = RecordSerializer._serialize_value(value)
            parts.append(struct.pack('>B', type_code))  # 1 byte type
            parts.append(struct.pack('>H', len(value_bytes)))  # 2 bytes length
            parts.append(value_bytes)
        
        return b''.join(parts)
    
    @staticmethod
    def deserialize(data: bytes) -> Dict[str, Any]:
        """
        Convert bytes back to a record dictionary.
        
        Args:
            data: Byte representation of a record
            
        Returns:
            Dictionary with the original data
        """
        offset = 0
        
        # Read number of fields
        num_fields = struct.unpack('>H', data[offset:offset+2])[0]
        offset += 2
        
        record = {}
        for _ in range(num_fields):
            # Read key
            key_len = struct.unpack('>H', data[offset:offset+2])[0]
            offset += 2
            key = data[offset:offset+key_len].decode('utf-8')
            offset += key_len
            
            # Read value type and length
            type_code = struct.unpack('>B', data[offset:offset+1])[0]
            offset += 1
            value_len = struct.unpack('>H', data[offset:offset+2])[0]
            offset += 2
            
            # Read and deserialize value
            value_bytes = data[offset:offset+value_len]
            offset += value_len
            value = RecordSerializer._deserialize_value(value_bytes, type_code)
            
            record[key] = value
        
        return record
    
    @staticmethod
    def _serialize_value(value: Any) -> tuple[bytes, int]:
        """Serialize a single value, returning (bytes, type_code)."""
        if value is None:
            return b'', TYPE_NONE
        elif isinstance(value, bool):  # Must check before int (bool is subclass of int)
            return struct.pack('>?', value), TYPE_BOOL
        elif isinstance(value, int):
            return struct.pack('>q', value), TYPE_INT  # 8-byte signed
        elif isinstance(value, float):
            return struct.pack('>d', value), TYPE_FLOAT  # 8-byte double
        elif isinstance(value, str):
            return value.encode('utf-8'), TYPE_STR
        elif isinstance(value, bytes):
            return value, TYPE_BYTES
        else:
            raise ValueError(f"Unsupported value type: {type(value)}")
    
    @staticmethod
    def _deserialize_value(data: bytes, type_code: int) -> Any:
        """Deserialize a single value from bytes."""
        if type_code == TYPE_NONE:
            return None
        elif type_code == TYPE_INT:
            return struct.unpack('>q', data)[0]
        elif type_code == TYPE_FLOAT:
            return struct.unpack('>d', data)[0]
        elif type_code == TYPE_STR:
            return data.decode('utf-8')
        elif type_code == TYPE_BOOL:
            return struct.unpack('>?', data)[0]
        elif type_code == TYPE_BYTES:
            return data
        else:
            raise ValueError(f"Unknown type code: {type_code}")
    
    @staticmethod
    def estimate_size(record: Dict[str, Any]) -> int:
        """
        Estimate the serialized size of a record without actually serializing.
        
        Useful for checking if a record will fit in available space.
        """
        size = 2  # num_fields
        
        for key, value in record.items():
            size += 2  # key length
            size += len(key.encode('utf-8'))  # key
            size += 1  # type code
            size += 2  # value length
            
            if value is None:
                size += 0
            elif isinstance(value, bool):
                size += 1
            elif isinstance(value, int):
                size += 8
            elif isinstance(value, float):
                size += 8
            elif isinstance(value, str):
                size += len(value.encode('utf-8'))
            elif isinstance(value, bytes):
                size += len(value)
        
        return size


def extract_key(record: Dict[str, Any], key_field: str) -> Any:
    """
    Extract the key value from a record.
    
    Args:
        record: The record dictionary
        key_field: Name of the field to use as key
        
    Returns:
        The key value
        
    Raises:
        KeyError: If key_field is not in the record
    """
    if key_field not in record:
        raise KeyError(f"Key field '{key_field}' not found in record: {record}")
    return record[key_field]
