"""
Hash File Implementation.

A hash file uses a hash function to distribute records into buckets.
Each bucket is one or more pages. The hash of the key determines which
bucket a record belongs to.

Characteristics:
- Insert: O(1) amortized - hash to find bucket, insert into bucket's page
- Search by key: O(1) amortized - hash to find bucket, scan bucket
- Delete: O(1) with key - hash to find bucket, scan bucket
- Range queries: O(n) - must scan all buckets (hash doesn't preserve order)

Bucket Structure:
- Each bucket starts as a single page
- If a bucket overflows, we can chain additional pages (overflow pages)
- The hash directory maps bucket_id -> page_id

Hash Function:
- We use Python's built-in hash() mod num_buckets
- In production, you'd want a more stable hash function
"""

from typing import Any, Dict, Iterator, Optional, Tuple, List
from pathlib import Path
import hashlib

from ..core import (
    DataFile,
    RecordLocation,
    RecordData,
    SlottedPage,
    DiskManager,
    RecordSerializer,
    IOStats,
    extract_key,
)


class HashFile(DataFile):
    """
    Hash file - records distributed by hash(key) into buckets.
    
    Provides O(1) average-case lookups by key, but cannot efficiently
    support range queries.
    
    Example:
        >>> hfile = HashFile("users.hash", key_field="id", num_buckets=64)
        >>> loc = hfile.insert({"id": 1, "name": "Alice"})
        >>> record = hfile.search(1)  # O(1) lookup
        >>> print(record)
        {'id': 1, 'name': 'Alice'}
    """
    
    def __init__(
        self,
        filepath: str,
        key_field: str,
        num_buckets: int = 64,
        page_size: int = 4096,
        create: bool = True
    ):
        """
        Open or create a hash file.
        
        Args:
            filepath: Path to the data file
            key_field: Name of the field to use as the hash key
            num_buckets: Number of hash buckets (should be power of 2 for efficiency)
            page_size: Size of each page in bytes
            create: If True, create file if it doesn't exist
        """
        self.filepath = filepath
        self.key_field = key_field
        self.num_buckets = num_buckets
        self.page_size = page_size
        
        self._disk = DiskManager(filepath, page_size, create)
        
        # Hash directory: bucket_id -> list of page_ids (first is primary, rest are overflow)
        self._directory: Dict[int, List[int]] = {}
        
        self._record_count = 0
        
        if self._disk.num_pages == 0:
            # New file - initialize buckets
            self._initialize_buckets()
        else:
            # Existing file - load directory
            self._load_directory()
    
    def _initialize_buckets(self):
        """Create initial bucket pages."""
        for bucket_id in range(self.num_buckets):
            page_id = self._disk.allocate_page()
            page = SlottedPage(page_id, self.page_size)
            self._disk.write_page(page)
            self._directory[bucket_id] = [page_id]
    
    def _load_directory(self):
        """
        Load the hash directory from existing file.
        
        For simplicity, we assume:
        - First num_buckets pages are the primary bucket pages
        - Additional pages are overflow pages (tracked via a header record)
        
        In production, the directory would be persisted separately.
        """
        # Simple approach: first N pages are buckets 0 to N-1
        for bucket_id in range(min(self.num_buckets, self._disk.num_pages)):
            self._directory[bucket_id] = [bucket_id]
        
        # Count records and detect overflow pages
        for bucket_id, page_ids in self._directory.items():
            for page_id in page_ids:
                page = self._disk.read_page(page_id)
                if page:
                    for _ in page.iter_records():
                        self._record_count += 1
    
    def _hash_key(self, key: Any) -> int:
        """
        Hash a key to a bucket ID.
        
        Uses SHA-256 for consistent hashing across runs.
        """
        # Convert to bytes for hashing
        if isinstance(key, str):
            key_bytes = key.encode('utf-8')
        elif isinstance(key, int):
            key_bytes = str(key).encode('utf-8')
        else:
            key_bytes = str(key).encode('utf-8')
        
        # Use SHA-256 for stable hashing
        hash_digest = hashlib.sha256(key_bytes).digest()
        # Convert first 8 bytes to int
        hash_int = int.from_bytes(hash_digest[:8], 'big')
        
        return hash_int % self.num_buckets
    
    def _get_bucket_pages(self, bucket_id: int) -> List[int]:
        """Get all page IDs for a bucket (primary + overflow)."""
        return self._directory.get(bucket_id, [])
    
    def insert(self, record: RecordData) -> RecordLocation:
        """
        Insert a record into the hash file.
        
        Args:
            record: Dictionary containing the record data (must include key_field)
            
        Returns:
            Location where the record was stored
        """
        key = extract_key(record, self.key_field)
        bucket_id = self._hash_key(key)
        
        # Serialize the record
        data = RecordSerializer.serialize(record)
        
        # Try to insert into bucket's pages
        page_ids = self._get_bucket_pages(bucket_id)
        
        for page_id in page_ids:
            page = self._disk.read_page(page_id)
            if page and page.free_space >= len(data) + 4:
                slot_id = page.insert(data)
                if slot_id is not None:
                    self._disk.write_page(page)
                    self._record_count += 1
                    return RecordLocation(page_id, slot_id)
        
        # No space in existing pages - create overflow page
        new_page_id = self._disk.allocate_page()
        new_page = SlottedPage(new_page_id, self.page_size)
        slot_id = new_page.insert(data)
        self._disk.write_page(new_page)
        
        # Add to directory
        self._directory[bucket_id].append(new_page_id)
        
        self._record_count += 1
        return RecordLocation(new_page_id, slot_id)
    
    def read(self, location: RecordLocation) -> Optional[RecordData]:
        """Read a record from the given location."""
        page = self._disk.read_page(location.page_id)
        if page is None:
            return None
        
        data = page.read(location.slot_id)
        if data is None:
            return None
        
        return RecordSerializer.deserialize(data)
    
    def search(self, key: Any) -> Optional[RecordData]:
        """
        Search for a record by key.
        
        This is the primary use case for hash files - O(1) average case.
        
        Args:
            key: The key value to search for
            
        Returns:
            The record if found, None otherwise
        """
        bucket_id = self._hash_key(key)
        page_ids = self._get_bucket_pages(bucket_id)
        
        for page_id in page_ids:
            page = self._disk.read_page(page_id)
            if page:
                for slot_id, data in page.iter_records():
                    record = RecordSerializer.deserialize(data)
                    if record.get(self.key_field) == key:
                        return record
        
        return None
    
    def search_with_location(self, key: Any) -> Optional[Tuple[RecordLocation, RecordData]]:
        """Search for a record and return both location and data."""
        bucket_id = self._hash_key(key)
        page_ids = self._get_bucket_pages(bucket_id)
        
        for page_id in page_ids:
            page = self._disk.read_page(page_id)
            if page:
                for slot_id, data in page.iter_records():
                    record = RecordSerializer.deserialize(data)
                    if record.get(self.key_field) == key:
                        return RecordLocation(page_id, slot_id), record
        
        return None
    
    def update(self, location: RecordLocation, record: RecordData) -> bool:
        """Update a record at the given location."""
        page = self._disk.read_page(location.page_id)
        if page is None:
            return False
        
        data = RecordSerializer.serialize(record)
        success = page.update(location.slot_id, data)
        
        if success:
            self._disk.write_page(page)
        
        return success
    
    def delete(self, location: RecordLocation) -> bool:
        """Delete a record at the given location."""
        page = self._disk.read_page(location.page_id)
        if page is None:
            return False
        
        success = page.delete(location.slot_id)
        
        if success:
            self._disk.write_page(page)
            self._record_count -= 1
        
        return success
    
    def delete_by_key(self, key: Any) -> bool:
        """
        Delete a record by its key.
        
        Args:
            key: The key of the record to delete
            
        Returns:
            True if found and deleted
        """
        result = self.search_with_location(key)
        if result:
            location, _ = result
            return self.delete(location)
        return False
    
    def scan(self) -> Iterator[Tuple[RecordLocation, RecordData]]:
        """
        Full scan - iterate over all records.
        
        Note: Order is not guaranteed (hash-based distribution).
        
        Yields:
            (location, record) tuples
        """
        for bucket_id in range(self.num_buckets):
            for page_id in self._get_bucket_pages(bucket_id):
                page = self._disk.read_page(page_id)
                if page:
                    for slot_id, data in page.iter_records():
                        record = RecordSerializer.deserialize(data)
                        yield RecordLocation(page_id, slot_id), record
    
    def close(self):
        """Close the file."""
        self._disk.close()
    
    @property
    def stats(self) -> Dict[str, Any]:
        """Return statistics about the hash file."""
        overflow_pages = sum(len(pages) - 1 for pages in self._directory.values())
        return {
            'filepath': str(self.filepath),
            'page_size': self.page_size,
            'num_buckets': self.num_buckets,
            'num_pages': self._disk.num_pages,
            'overflow_pages': overflow_pages,
            'record_count': self._record_count,
            'avg_records_per_bucket': self._record_count / self.num_buckets if self.num_buckets > 0 else 0,
            'io_stats': self._disk.stats,
        }
    
    @property
    def io_stats(self) -> IOStats:
        """Direct access to I/O statistics."""
        return self._disk.stats
    
    def reset_stats(self):
        """Reset I/O statistics."""
        self._disk.reset_stats()
    
    def bucket_stats(self) -> Dict[int, Dict[str, int]]:
        """Get statistics per bucket (useful for analyzing hash distribution)."""
        stats = {}
        for bucket_id in range(self.num_buckets):
            page_ids = self._get_bucket_pages(bucket_id)
            record_count = 0
            for page_id in page_ids:
                page = self._disk.read_page(page_id)
                if page:
                    record_count += sum(1 for _ in page.iter_records())
            stats[bucket_id] = {
                'pages': len(page_ids),
                'records': record_count
            }
        return stats
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
    
    def __repr__(self):
        return (
            f"HashFile('{self.filepath}', key='{self.key_field}', "
            f"buckets={self.num_buckets}, records={self._record_count})"
        )
