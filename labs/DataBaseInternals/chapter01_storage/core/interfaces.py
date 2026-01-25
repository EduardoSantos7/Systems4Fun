"""
Abstract base classes defining the core interfaces for storage components.

These interfaces establish contracts that different implementations must follow,
enabling us to swap implementations (e.g., HeapFile vs HashFile) transparently.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional, Tuple, List


# Type aliases for clarity
PageId = int
SlotId = int
RecordData = Dict[str, Any]


@dataclass(frozen=True)
class RecordLocation:
    """
    Identifies where a record is stored on disk.
    
    This is what indexes store - a pointer to the actual data.
    In different systems this might be:
    - (page_id, slot_id) - direct pointer
    - primary_key - for indirection via primary index
    """
    page_id: PageId
    slot_id: SlotId
    
    def __repr__(self) -> str:
        return f"RecordLocation(page={self.page_id}, slot={self.slot_id})"


class Page(ABC):
    """
    Abstract base class for a page (block) of data.
    
    A page is the unit of I/O - we always read/write entire pages.
    Typical sizes: 4KB, 8KB, 16KB (matching disk block size).
    """
    
    @property
    @abstractmethod
    def page_id(self) -> PageId:
        """Unique identifier for this page within its file."""
        pass
    
    @property
    @abstractmethod
    def free_space(self) -> int:
        """Bytes of free space available for new records."""
        pass
    
    @abstractmethod
    def insert(self, data: bytes) -> Optional[SlotId]:
        """
        Insert record data into the page.
        
        Returns:
            SlotId if successful, None if not enough space.
        """
        pass
    
    @abstractmethod
    def read(self, slot_id: SlotId) -> Optional[bytes]:
        """Read record data from the given slot."""
        pass
    
    @abstractmethod
    def delete(self, slot_id: SlotId) -> bool:
        """Mark a slot as deleted. Returns True if successful."""
        pass
    
    @abstractmethod
    def update(self, slot_id: SlotId, data: bytes) -> bool:
        """Update record in slot. May fail if new data is larger."""
        pass
    
    @abstractmethod
    def to_bytes(self) -> bytes:
        """Serialize the entire page to bytes for writing to disk."""
        pass
    
    @classmethod
    @abstractmethod
    def from_bytes(cls, data: bytes, page_id: PageId) -> 'Page':
        """Deserialize a page from bytes read from disk."""
        pass


class DataFile(ABC):
    """
    Abstract base class for data files.
    
    A data file stores the actual records. Different organizations
    (heap, hash, sorted) implement this interface differently.
    """
    
    @abstractmethod
    def insert(self, record: RecordData) -> RecordLocation:
        """
        Insert a record and return its location.
        
        The location can be used to read the record directly,
        or stored in an index for later retrieval.
        """
        pass
    
    @abstractmethod
    def read(self, location: RecordLocation) -> Optional[RecordData]:
        """Read a record given its location."""
        pass
    
    @abstractmethod
    def update(self, location: RecordLocation, record: RecordData) -> bool:
        """Update a record at the given location."""
        pass
    
    @abstractmethod
    def delete(self, location: RecordLocation) -> bool:
        """Delete a record at the given location."""
        pass
    
    @abstractmethod
    def scan(self) -> Iterator[Tuple[RecordLocation, RecordData]]:
        """
        Full table scan - iterate over all records.
        
        Yields (location, record) tuples.
        This is O(n) and reads all pages.
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Flush any buffers and close the file."""
        pass
    
    @property
    @abstractmethod
    def stats(self) -> Dict[str, Any]:
        """Return statistics about the file (pages, records, etc.)."""
        pass


class Index(ABC):
    """
    Abstract base class for indexes.
    
    An index maps search keys to record locations (or to the records
    themselves in the case of an index-organized table).
    """
    
    @abstractmethod
    def insert(self, key: Any, location: RecordLocation) -> None:
        """Add a key-location mapping to the index."""
        pass
    
    @abstractmethod
    def search(self, key: Any) -> Optional[RecordLocation]:
        """
        Find the location of a record by key.
        
        Returns None if key not found.
        """
        pass
    
    @abstractmethod
    def delete(self, key: Any) -> bool:
        """Remove a key from the index."""
        pass
    
    @abstractmethod
    def update(self, old_key: Any, new_key: Any, location: RecordLocation) -> bool:
        """Update a key in the index (e.g., when the indexed column changes)."""
        pass
    
    def range_search(self, start_key: Any, end_key: Any) -> Iterator[Tuple[Any, RecordLocation]]:
        """
        Find all keys in the range [start_key, end_key].
        
        Default implementation raises NotImplementedError.
        Hash indexes cannot support this efficiently.
        """
        raise NotImplementedError("This index does not support range queries")
    
    @abstractmethod
    def close(self) -> None:
        """Flush and close the index file."""
        pass


class IndexOrganizedStorage(ABC):
    """
    Abstract base class for index-organized tables (IOT).
    
    In an IOT, the index IS the table - records are stored directly
    in the index structure, ordered by the primary key.
    """
    
    @abstractmethod
    def insert(self, record: RecordData) -> None:
        """Insert a record (key extracted from record automatically)."""
        pass
    
    @abstractmethod
    def search(self, key: Any) -> Optional[RecordData]:
        """Find a record by its primary key."""
        pass
    
    @abstractmethod
    def delete(self, key: Any) -> bool:
        """Delete a record by its primary key."""
        pass
    
    @abstractmethod
    def update(self, key: Any, record: RecordData) -> bool:
        """Update a record (key must match)."""
        pass
    
    @abstractmethod
    def scan(self) -> Iterator[RecordData]:
        """Scan all records in key order."""
        pass
    
    @abstractmethod
    def range_scan(self, start_key: Any, end_key: Any) -> Iterator[RecordData]:
        """Scan records in the given key range."""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Flush and close."""
        pass


# Statistics tracking for benchmarks
@dataclass
class IOStats:
    """Track I/O operations for performance analysis."""
    pages_read: int = 0
    pages_written: int = 0
    
    def reset(self):
        self.pages_read = 0
        self.pages_written = 0
    
    def record_read(self):
        self.pages_read += 1
    
    def record_write(self):
        self.pages_written += 1
    
    def __repr__(self):
        return f"IOStats(reads={self.pages_read}, writes={self.pages_written})"
