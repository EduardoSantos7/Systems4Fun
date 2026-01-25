"""
Hash Index Implementation.

A hash index uses a hash table to map keys to record locations.
It provides O(1) average-case lookups but cannot support range queries.

This is similar to the hash file, but:
- Hash File: data stored directly in buckets
- Hash Index: buckets store (key, RecordLocation) pairs pointing to data elsewhere

Use cases:
- Primary key lookups
- Equality comparisons (WHERE id = 5)
- NOT suitable for: range queries, ORDER BY, LIKE patterns
"""

from typing import Any, Dict, Iterator, List, Optional, Tuple
from pathlib import Path
import hashlib
import struct

from ..core import (
    Index,
    RecordLocation,
    DiskManager,
    SlottedPage,
    PageId,
    IOStats,
)


class HashIndex(Index):
    """
    Hash index for O(1) key lookups.
    
    Maps keys to RecordLocations using a hash table stored on disk.
    Each bucket is a page that can hold multiple entries.
    
    Example:
        >>> index = HashIndex("users_pk.idx", num_buckets=64)
        >>> index.insert(1, RecordLocation(page_id=5, slot_id=2))
        >>> loc = index.search(1)  # O(1)
        >>> print(loc)
        RecordLocation(page=5, slot=2)
    """
    
    def __init__(
        self,
        filepath: str,
        num_buckets: int = 64,
        page_size: int = 4096,
        create: bool = True
    ):
        """
        Open or create a hash index.
        
        Args:
            filepath: Path to the index file
            num_buckets: Number of hash buckets
            page_size: Size of each page in bytes
            create: If True, create file if it doesn't exist
        """
        self.filepath = filepath
        self.num_buckets = num_buckets
        self.page_size = page_size
        
        self._disk = DiskManager(filepath, page_size, create)
        
        # Directory: bucket_id -> list of page_ids
        self._directory: Dict[int, List[PageId]] = {}
        self._entry_count = 0
        
        if self._disk.num_pages == 0:
            self._initialize_buckets()
        else:
            self._load_directory()
    
    def _initialize_buckets(self):
        """Create initial bucket pages."""
        for bucket_id in range(self.num_buckets):
            page_id = self._disk.allocate_page()
            page = SlottedPage(page_id, self.page_size)
            self._disk.write_page(page)
            self._directory[bucket_id] = [page_id]
    
    def _load_directory(self):
        """Load directory from existing file."""
        for bucket_id in range(min(self.num_buckets, self._disk.num_pages)):
            self._directory[bucket_id] = [bucket_id]
        
        # Count entries
        for bucket_id in range(self.num_buckets):
            for page_id in self._directory.get(bucket_id, []):
                page = self._disk.read_page(page_id)
                if page:
                    for _ in page.iter_records():
                        self._entry_count += 1
    
    def _hash_key(self, key: Any) -> int:
        """Hash a key to a bucket ID."""
        if isinstance(key, str):
            key_bytes = key.encode('utf-8')
        elif isinstance(key, int):
            key_bytes = str(key).encode('utf-8')
        else:
            key_bytes = str(key).encode('utf-8')
        
        hash_digest = hashlib.sha256(key_bytes).digest()
        hash_int = int.from_bytes(hash_digest[:8], 'big')
        return hash_int % self.num_buckets
    
    def _serialize_entry(self, key: Any, location: RecordLocation) -> bytes:
        """Serialize a (key, location) entry."""
        parts = []
        
        # Key
        if isinstance(key, int):
            parts.append(b'\x01')
            parts.append(struct.pack('>q', key))
        elif isinstance(key, str):
            parts.append(b'\x02')
            key_bytes = key.encode('utf-8')
            parts.append(struct.pack('>H', len(key_bytes)))
            parts.append(key_bytes)
        else:
            parts.append(b'\x02')
            key_bytes = str(key).encode('utf-8')
            parts.append(struct.pack('>H', len(key_bytes)))
            parts.append(key_bytes)
        
        # Location
        parts.append(struct.pack('>II', location.page_id, location.slot_id))
        
        return b''.join(parts)
    
    def _deserialize_entry(self, data: bytes) -> Tuple[Any, RecordLocation]:
        """Deserialize a (key, location) entry."""
        offset = 0
        
        # Key
        key_type = data[offset]
        offset += 1
        
        if key_type == 1:  # int
            key = struct.unpack('>q', data[offset:offset+8])[0]
            offset += 8
        elif key_type == 2:  # str
            key_len = struct.unpack('>H', data[offset:offset+2])[0]
            offset += 2
            key = data[offset:offset+key_len].decode('utf-8')
            offset += key_len
        else:
            raise ValueError(f"Unknown key type: {key_type}")
        
        # Location
        page_id, slot_id = struct.unpack('>II', data[offset:offset+8])
        location = RecordLocation(page_id, slot_id)
        
        return key, location
    
    def insert(self, key: Any, location: RecordLocation) -> None:
        """
        Insert a key-location mapping.
        
        Args:
            key: The search key
            location: Where the record is stored
        """
        bucket_id = self._hash_key(key)
        data = self._serialize_entry(key, location)
        
        # Try to insert into bucket's pages
        page_ids = self._directory.get(bucket_id, [])
        
        for page_id in page_ids:
            page = self._disk.read_page(page_id)
            if page and page.free_space >= len(data) + 4:
                slot_id = page.insert(data)
                if slot_id is not None:
                    self._disk.write_page(page)
                    self._entry_count += 1
                    return
        
        # Need overflow page
        new_page_id = self._disk.allocate_page()
        new_page = SlottedPage(new_page_id, self.page_size)
        new_page.insert(data)
        self._disk.write_page(new_page)
        
        if bucket_id not in self._directory:
            self._directory[bucket_id] = []
        self._directory[bucket_id].append(new_page_id)
        self._entry_count += 1
    
    def search(self, key: Any) -> Optional[RecordLocation]:
        """
        Search for a key.
        
        Args:
            key: The key to find
            
        Returns:
            RecordLocation if found, None otherwise
        """
        bucket_id = self._hash_key(key)
        page_ids = self._directory.get(bucket_id, [])
        
        for page_id in page_ids:
            page = self._disk.read_page(page_id)
            if page:
                for slot_id, data in page.iter_records():
                    entry_key, location = self._deserialize_entry(data)
                    if entry_key == key:
                        return location
        
        return None
    
    def delete(self, key: Any) -> bool:
        """
        Delete a key from the index.
        
        Args:
            key: The key to delete
            
        Returns:
            True if found and deleted
        """
        bucket_id = self._hash_key(key)
        page_ids = self._directory.get(bucket_id, [])
        
        for page_id in page_ids:
            page = self._disk.read_page(page_id)
            if page:
                for slot_id, data in page.iter_records():
                    entry_key, _ = self._deserialize_entry(data)
                    if entry_key == key:
                        page.delete(slot_id)
                        self._disk.write_page(page)
                        self._entry_count -= 1
                        return True
        
        return False
    
    def update(self, old_key: Any, new_key: Any, location: RecordLocation) -> bool:
        """Update a key in the index."""
        if self.delete(old_key):
            self.insert(new_key, location)
            return True
        return False
    
    def scan(self) -> Iterator[Tuple[Any, RecordLocation]]:
        """Iterate over all entries (no particular order)."""
        for bucket_id in range(self.num_buckets):
            for page_id in self._directory.get(bucket_id, []):
                page = self._disk.read_page(page_id)
                if page:
                    for slot_id, data in page.iter_records():
                        key, location = self._deserialize_entry(data)
                        yield key, location
    
    def close(self):
        """Close the index file."""
        self._disk.close()
    
    @property
    def io_stats(self) -> IOStats:
        return self._disk.stats
    
    def reset_stats(self):
        self._disk.reset_stats()
    
    @property
    def stats(self) -> Dict[str, Any]:
        """Return index statistics."""
        return {
            'filepath': str(self.filepath),
            'num_buckets': self.num_buckets,
            'entry_count': self._entry_count,
            'avg_entries_per_bucket': self._entry_count / self.num_buckets if self.num_buckets > 0 else 0,
        }
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
    
    def __repr__(self):
        return f"HashIndex('{self.filepath}', buckets={self.num_buckets}, entries={self._entry_count})"
