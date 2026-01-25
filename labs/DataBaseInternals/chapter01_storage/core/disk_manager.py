"""
Disk Manager - Low-level file I/O operations.

The DiskManager handles reading and writing pages to disk files.
It abstracts away the details of file handling and provides
page-level operations.

Key concepts:
- Pages are the unit of I/O (we always read/write full pages)
- Page ID maps directly to file offset: offset = page_id * page_size
- The manager tracks I/O statistics for performance analysis
"""

import os
from typing import Optional, Type, TypeVar
from pathlib import Path

from .interfaces import PageId, IOStats
from .page import SlottedPage


T = TypeVar('T', bound=SlottedPage)


class DiskManager:
    """
    Manages reading and writing pages to a disk file.
    
    This is a simple implementation that directly reads/writes pages.
    A production system would have a buffer pool on top of this.
    
    Attributes:
        filepath: Path to the data file
        page_size: Size of each page in bytes
        stats: I/O statistics tracker
    """
    
    def __init__(
        self,
        filepath: str,
        page_size: int = 4096,
        create: bool = True
    ):
        """
        Initialize the disk manager.
        
        Args:
            filepath: Path to the data file
            page_size: Size of each page in bytes
            create: If True, create the file if it doesn't exist
        """
        self.filepath = Path(filepath)
        self.page_size = page_size
        self.stats = IOStats()
        self._file = None
        self._num_pages = 0
        
        # Ensure directory exists
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        
        if create and not self.filepath.exists():
            # Create empty file
            self.filepath.touch()
        
        # Open file for read/write
        self._file = open(self.filepath, 'r+b' if self.filepath.exists() else 'w+b')
        
        # Calculate number of existing pages
        self._file.seek(0, 2)  # Seek to end
        file_size = self._file.tell()
        self._num_pages = file_size // page_size
    
    @property
    def num_pages(self) -> int:
        """Number of pages in the file."""
        return self._num_pages
    
    def allocate_page(self) -> PageId:
        """
        Allocate a new page at the end of the file.
        
        Returns:
            The page ID of the newly allocated page
        """
        page_id = self._num_pages
        self._num_pages += 1
        
        # Write empty page to extend file
        empty_page = SlottedPage(page_id, self.page_size)
        self.write_page(empty_page)
        
        return page_id
    
    def read_page(self, page_id: PageId, page_class: Type[T] = SlottedPage) -> Optional[T]:
        """
        Read a page from disk.
        
        Args:
            page_id: The ID of the page to read
            page_class: The class to use for deserializing (default: SlottedPage)
            
        Returns:
            The page object, or None if page_id is invalid
        """
        if page_id < 0 or page_id >= self._num_pages:
            return None
        
        # Calculate file offset
        offset = page_id * self.page_size
        
        # Read page data
        self._file.seek(offset)
        data = self._file.read(self.page_size)
        
        if len(data) != self.page_size:
            return None
        
        # Track I/O
        self.stats.record_read()
        
        # Deserialize and return
        return page_class.from_bytes(data, page_id)
    
    def write_page(self, page: SlottedPage) -> bool:
        """
        Write a page to disk.
        
        Args:
            page: The page to write
            
        Returns:
            True if successful
        """
        page_id = page.page_id
        
        # Extend file if necessary
        while page_id >= self._num_pages:
            self._num_pages = page_id + 1
        
        # Calculate file offset
        offset = page_id * self.page_size
        
        # Write page data
        self._file.seek(offset)
        data = page.to_bytes()
        
        if len(data) != self.page_size:
            # Pad or truncate to page size
            data = data[:self.page_size].ljust(self.page_size, b'\x00')
        
        self._file.write(data)
        self._file.flush()  # Ensure data is written to disk
        
        # Track I/O
        self.stats.record_write()
        
        return True
    
    def sync(self):
        """Flush all buffers to disk."""
        if self._file:
            self._file.flush()
            os.fsync(self._file.fileno())
    
    def close(self):
        """Close the file."""
        if self._file:
            self.sync()
            self._file.close()
            self._file = None
    
    def reset_stats(self):
        """Reset I/O statistics."""
        self.stats.reset()
    
    def delete_file(self):
        """Close and delete the file (for cleanup in tests)."""
        self.close()
        if self.filepath.exists():
            self.filepath.unlink()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
    
    def __repr__(self):
        return (
            f"DiskManager(file='{self.filepath}', "
            f"page_size={self.page_size}, pages={self._num_pages})"
        )


class PageDirectory:
    """
    Tracks which pages have free space.
    
    This is used by heap files to efficiently find pages
    where new records can be inserted.
    
    Two strategies:
    1. Linked list: Pages form a linked list (simple but O(n) to find space)
    2. Directory: Separate structure tracking free space per page (fast but extra I/O)
    
    This implementation uses a simple in-memory directory.
    A production system would persist this to disk.
    """
    
    def __init__(self):
        # Map of page_id -> approximate free space
        self._free_space: dict[PageId, int] = {}
        # List of pages in insertion order
        self._pages: list[PageId] = []
    
    def register_page(self, page_id: PageId, free_space: int):
        """Register a page and its free space."""
        if page_id not in self._free_space:
            self._pages.append(page_id)
        self._free_space[page_id] = free_space
    
    def update_free_space(self, page_id: PageId, free_space: int):
        """Update the free space for a page."""
        self._free_space[page_id] = free_space
    
    def find_page_with_space(self, needed: int) -> Optional[PageId]:
        """
        Find a page with at least 'needed' bytes of free space.
        
        Returns:
            Page ID if found, None if no page has enough space
        """
        for page_id in self._pages:
            if self._free_space.get(page_id, 0) >= needed:
                return page_id
        return None
    
    def get_all_pages(self) -> list[PageId]:
        """Get all page IDs in order."""
        return self._pages.copy()
    
    def __len__(self) -> int:
        return len(self._pages)
    
    def __repr__(self):
        return f"PageDirectory(pages={len(self._pages)})"
