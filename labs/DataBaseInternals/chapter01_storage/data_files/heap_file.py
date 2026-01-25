"""
Heap File Implementation.

A heap file stores records in no particular order - records go wherever
there's space. This is the simplest data file organization.

Characteristics:
- Insert: O(1) - just find a page with space and append
- Search by key: O(n) - must scan all records
- Delete: O(1) with location, O(n) to find
- Good for: Write-heavy workloads, full table scans

Page Organization Strategies:
1. Linked List: Header page points to list of full pages and list of pages with space
2. Page Directory: Separate structure tracking free space per page

This implementation uses a page directory for efficiency.
"""

from typing import Any, Dict, Iterator, Optional, Tuple
from pathlib import Path

from ..core import (
    DataFile,
    RecordLocation,
    RecordData,
    SlottedPage,
    DiskManager,
    PageDirectory,
    RecordSerializer,
    IOStats,
)


class HeapFile(DataFile):
    """
    Heap file - records stored in insertion order.
    
    Records are inserted wherever there's space. No ordering is maintained.
    Efficient for writes, but reads require full scans without an index.
    
    Example:
        >>> heap = HeapFile("users.heap")
        >>> loc = heap.insert({"id": 1, "name": "Alice"})
        >>> record = heap.read(loc)
        >>> print(record)
        {'id': 1, 'name': 'Alice'}
    """
    
    def __init__(
        self,
        filepath: str,
        page_size: int = 4096,
        create: bool = True
    ):
        """
        Open or create a heap file.
        
        Args:
            filepath: Path to the data file
            page_size: Size of each page in bytes
            create: If True, create file if it doesn't exist
        """
        self.filepath = filepath
        self.page_size = page_size
        self._disk = DiskManager(filepath, page_size, create)
        self._directory = PageDirectory()
        self._record_count = 0
        
        # Load existing pages into directory
        self._load_directory()
    
    def _load_directory(self):
        """Load page information into the directory."""
        for page_id in range(self._disk.num_pages):
            page = self._disk.read_page(page_id)
            if page:
                self._directory.register_page(page_id, page.free_space)
                # Count records
                for _ in page.iter_records():
                    self._record_count += 1
    
    def insert(self, record: RecordData) -> RecordLocation:
        """
        Insert a record into the heap file.
        
        Args:
            record: Dictionary containing the record data
            
        Returns:
            Location where the record was stored
        """
        # Serialize the record
        data = RecordSerializer.serialize(record)
        
        # Find a page with enough space
        page_id = self._directory.find_page_with_space(len(data) + 4)  # +4 for slot
        
        if page_id is None:
            # No page has space, allocate a new one
            page_id = self._disk.allocate_page()
            page = SlottedPage(page_id, self.page_size)
        else:
            page = self._disk.read_page(page_id)
        
        # Insert into page
        slot_id = page.insert(data)
        
        if slot_id is None:
            # Page was full (shouldn't happen if directory is accurate)
            # Allocate new page
            page_id = self._disk.allocate_page()
            page = SlottedPage(page_id, self.page_size)
            slot_id = page.insert(data)
        
        # Write page back
        self._disk.write_page(page)
        
        # Update directory
        self._directory.register_page(page_id, page.free_space)
        
        self._record_count += 1
        
        return RecordLocation(page_id, slot_id)
    
    def read(self, location: RecordLocation) -> Optional[RecordData]:
        """
        Read a record from the given location.
        
        Args:
            location: The RecordLocation returned from insert
            
        Returns:
            The record dictionary, or None if not found
        """
        page = self._disk.read_page(location.page_id)
        if page is None:
            return None
        
        data = page.read(location.slot_id)
        if data is None:
            return None
        
        return RecordSerializer.deserialize(data)
    
    def update(self, location: RecordLocation, record: RecordData) -> bool:
        """
        Update a record at the given location.
        
        Note: If the new record is larger, this may fail.
        A more sophisticated implementation would handle relocations.
        
        Args:
            location: Where the record is stored
            record: New record data
            
        Returns:
            True if successful
        """
        page = self._disk.read_page(location.page_id)
        if page is None:
            return False
        
        data = RecordSerializer.serialize(record)
        success = page.update(location.slot_id, data)
        
        if success:
            self._disk.write_page(page)
            self._directory.update_free_space(location.page_id, page.free_space)
        
        return success
    
    def delete(self, location: RecordLocation) -> bool:
        """
        Delete a record at the given location.
        
        Args:
            location: Where the record is stored
            
        Returns:
            True if successful
        """
        page = self._disk.read_page(location.page_id)
        if page is None:
            return False
        
        success = page.delete(location.slot_id)
        
        if success:
            self._disk.write_page(page)
            self._directory.update_free_space(location.page_id, page.free_space)
            self._record_count -= 1
        
        return success
    
    def scan(self) -> Iterator[Tuple[RecordLocation, RecordData]]:
        """
        Full table scan - iterate over all records.
        
        This reads every page and every record. O(n) in the number of pages.
        
        Yields:
            (location, record) tuples
        """
        for page_id in self._directory.get_all_pages():
            page = self._disk.read_page(page_id)
            if page:
                for slot_id, data in page.iter_records():
                    record = RecordSerializer.deserialize(data)
                    yield RecordLocation(page_id, slot_id), record
    
    def search(self, key_field: str, key_value: Any) -> Optional[Tuple[RecordLocation, RecordData]]:
        """
        Search for a record by scanning (O(n)).
        
        This is inefficient - use an index for faster lookups.
        
        Args:
            key_field: Field name to search on
            key_value: Value to find
            
        Returns:
            (location, record) if found, None otherwise
        """
        for location, record in self.scan():
            if record.get(key_field) == key_value:
                return location, record
        return None
    
    def compact(self) -> int:
        """
        Compact all pages to reclaim space from deleted records.
        
        Returns:
            Total bytes freed
        """
        total_freed = 0
        
        for page_id in self._directory.get_all_pages():
            page = self._disk.read_page(page_id)
            if page:
                freed = page.compact()
                if freed > 0:
                    self._disk.write_page(page)
                    self._directory.update_free_space(page_id, page.free_space)
                    total_freed += freed
        
        return total_freed
    
    def close(self):
        """Close the file."""
        self._disk.close()
    
    @property
    def stats(self) -> Dict[str, Any]:
        """Return statistics about the heap file."""
        return {
            'filepath': str(self.filepath),
            'page_size': self.page_size,
            'num_pages': self._disk.num_pages,
            'record_count': self._record_count,
            'io_stats': self._disk.stats,
        }
    
    @property
    def io_stats(self) -> IOStats:
        """Direct access to I/O statistics."""
        return self._disk.stats
    
    def reset_stats(self):
        """Reset I/O statistics."""
        self._disk.reset_stats()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
    
    def __repr__(self):
        return f"HeapFile('{self.filepath}', pages={self._disk.num_pages}, records={self._record_count})"
