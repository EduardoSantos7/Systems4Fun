"""
Slotted Page Implementation.

A slotted page is a common page format used in databases. It allows
variable-length records and efficient space management.

Page Layout:
┌─────────────────────────────────────────────────────────────────┐
│ HEADER (16 bytes)                                               │
│ ┌───────────┬───────────┬────────────┬────────────┬───────────┐│
│ │ page_id   │ num_slots │ free_start │ free_end   │ flags     ││
│ │ (4 bytes) │ (2 bytes) │ (2 bytes)  │ (2 bytes)  │ (6 bytes) ││
│ └───────────┴───────────┴────────────┴────────────┴───────────┘│
├─────────────────────────────────────────────────────────────────┤
│ SLOT DIRECTORY (grows downward →)                               │
│ Each slot: [offset: 2 bytes][length: 2 bytes] = 4 bytes         │
│ ┌──────────┬──────────┬──────────┬──────────┐                  │
│ │ Slot 0   │ Slot 1   │ Slot 2   │   ...    │                  │
│ └──────────┴──────────┴──────────┴──────────┘                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│                       FREE SPACE                                │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│ RECORDS (grow upward ←)                                         │
│ ┌──────────────────┬──────────────────┬──────────────────┐     │
│ │    Record N      │    Record 1      │    Record 0      │     │
│ └──────────────────┴──────────────────┴──────────────────┘     │
└─────────────────────────────────────────────────────────────────┘

- Slot directory grows from top to bottom
- Records grow from bottom to top
- Free space is in the middle
- Deleted records: slot.offset = 0, slot.length = 0
"""

import struct
from typing import Optional, List, Iterator, Tuple

from .interfaces import Page, PageId, SlotId


# Header format constants
HEADER_SIZE = 16
HEADER_FORMAT = '>IHHH6s'  # page_id(4), num_slots(2), free_start(2), free_end(2), flags(6)

# Slot format constants
SLOT_SIZE = 4
SLOT_FORMAT = '>HH'  # offset(2), length(2)

# Special values
DELETED_SLOT_OFFSET = 0
DELETED_SLOT_LENGTH = 0


class SlottedPage(Page):
    """
    A slotted page implementation for variable-length records.
    
    Features:
    - Variable-length records
    - Slot indirection (records can be moved within page)
    - Efficient deletion (just mark slot as deleted)
    - Compaction support
    
    Attributes:
        _page_id: Unique identifier for this page
        _page_size: Total size of the page in bytes
        _data: The raw page data as a bytearray
    """
    
    def __init__(self, page_id: PageId, page_size: int = 4096):
        """
        Create a new empty page.
        
        Args:
            page_id: Unique identifier for this page
            page_size: Size of the page in bytes (default 4KB)
        """
        self._page_id = page_id
        self._page_size = page_size
        self._data = bytearray(page_size)
        
        # Initialize header
        self._num_slots = 0
        self._free_start = HEADER_SIZE  # Just after header
        self._free_end = page_size       # End of page
        self._flags = b'\x00' * 6
        
        self._write_header()
    
    @property
    def page_id(self) -> PageId:
        return self._page_id
    
    @property
    def page_size(self) -> int:
        return self._page_size
    
    @property
    def num_slots(self) -> int:
        return self._num_slots
    
    @property
    def free_space(self) -> int:
        """
        Available space for new records.
        
        Must account for:
        - The slot directory entry (4 bytes)
        - The actual record data
        """
        # Space between slot directory end and records start
        available = self._free_end - self._free_start
        # Reserve space for a new slot entry
        return max(0, available - SLOT_SIZE)
    
    def insert(self, data: bytes) -> Optional[SlotId]:
        """
        Insert a record into the page.
        
        Args:
            data: The serialized record data
            
        Returns:
            SlotId if successful, None if not enough space
        """
        record_size = len(data)
        
        # Check if we have enough space
        needed = record_size + SLOT_SIZE  # Record + new slot entry
        if self._free_end - self._free_start < needed:
            return None
        
        # Allocate space for record (grows upward from end)
        self._free_end -= record_size
        record_offset = self._free_end
        
        # Write record data
        self._data[record_offset:record_offset + record_size] = data
        
        # Find a deleted slot to reuse, or create new one
        slot_id = self._find_free_slot()
        if slot_id is None:
            # Create new slot
            slot_id = self._num_slots
            self._num_slots += 1
            self._free_start += SLOT_SIZE
        
        # Write slot entry
        self._write_slot(slot_id, record_offset, record_size)
        
        # Update header
        self._write_header()
        
        return slot_id
    
    def read(self, slot_id: SlotId) -> Optional[bytes]:
        """
        Read a record from the given slot.
        
        Args:
            slot_id: The slot to read from
            
        Returns:
            Record data as bytes, or None if slot is invalid/deleted
        """
        if slot_id < 0 or slot_id >= self._num_slots:
            return None
        
        offset, length = self._read_slot(slot_id)
        
        # Check if slot is deleted
        if offset == DELETED_SLOT_OFFSET and length == DELETED_SLOT_LENGTH:
            return None
        
        return bytes(self._data[offset:offset + length])
    
    def delete(self, slot_id: SlotId) -> bool:
        """
        Delete a record by marking its slot as deleted.
        
        Note: This doesn't actually free the space until compaction.
        
        Args:
            slot_id: The slot to delete
            
        Returns:
            True if successful, False if slot invalid
        """
        if slot_id < 0 or slot_id >= self._num_slots:
            return False
        
        offset, length = self._read_slot(slot_id)
        
        # Already deleted?
        if offset == DELETED_SLOT_OFFSET and length == DELETED_SLOT_LENGTH:
            return False
        
        # Mark as deleted (don't actually remove data yet)
        self._write_slot(slot_id, DELETED_SLOT_OFFSET, DELETED_SLOT_LENGTH)
        
        return True
    
    def update(self, slot_id: SlotId, data: bytes) -> bool:
        """
        Update a record in place if it fits, otherwise fail.
        
        For simplicity, we only allow updates that fit in the existing space.
        A more sophisticated implementation would handle relocations.
        
        Args:
            slot_id: The slot to update
            data: New record data
            
        Returns:
            True if successful, False if slot invalid or data too large
        """
        if slot_id < 0 or slot_id >= self._num_slots:
            return False
        
        offset, length = self._read_slot(slot_id)
        
        if offset == DELETED_SLOT_OFFSET:
            return False
        
        # Check if new data fits
        if len(data) > length:
            return False  # Would need relocation
        
        # Write new data (may be smaller, leaving some wasted space)
        self._data[offset:offset + len(data)] = data
        self._write_slot(slot_id, offset, len(data))
        
        return True
    
    def compact(self) -> int:
        """
        Compact the page by removing gaps from deleted records.
        
        This moves all live records to be contiguous at the end of the page,
        freeing up space in the middle.
        
        Returns:
            Number of bytes freed
        """
        old_free = self.free_space
        
        # Collect all live records with their slot IDs
        live_records: List[Tuple[SlotId, bytes]] = []
        for slot_id in range(self._num_slots):
            offset, length = self._read_slot(slot_id)
            if offset != DELETED_SLOT_OFFSET or length != DELETED_SLOT_LENGTH:
                record_data = bytes(self._data[offset:offset + length])
                live_records.append((slot_id, record_data))
        
        # Reset free_end to page end
        self._free_end = self._page_size
        
        # Rewrite all live records
        for slot_id, record_data in live_records:
            self._free_end -= len(record_data)
            self._data[self._free_end:self._free_end + len(record_data)] = record_data
            self._write_slot(slot_id, self._free_end, len(record_data))
        
        self._write_header()
        
        return self.free_space - old_free
    
    def iter_records(self) -> Iterator[Tuple[SlotId, bytes]]:
        """
        Iterate over all live records in the page.
        
        Yields:
            (slot_id, record_data) tuples
        """
        for slot_id in range(self._num_slots):
            data = self.read(slot_id)
            if data is not None:
                yield slot_id, data
    
    def to_bytes(self) -> bytes:
        """Serialize the page to bytes for writing to disk."""
        return bytes(self._data)
    
    @classmethod
    def from_bytes(cls, data: bytes, page_id: PageId) -> 'SlottedPage':
        """
        Deserialize a page from bytes read from disk.
        
        Args:
            data: Raw page bytes
            page_id: The page ID (for verification)
            
        Returns:
            Reconstructed SlottedPage object
        """
        page = cls.__new__(cls)
        page._page_size = len(data)
        page._data = bytearray(data)
        
        # Read header
        header = struct.unpack(HEADER_FORMAT, data[:HEADER_SIZE])
        page._page_id = header[0]
        page._num_slots = header[1]
        page._free_start = header[2]
        page._free_end = header[3]
        page._flags = header[4]
        
        # Verify page_id matches
        if page._page_id != page_id:
            raise ValueError(f"Page ID mismatch: expected {page_id}, got {page._page_id}")
        
        return page
    
    def _write_header(self):
        """Write the page header to the data buffer."""
        header = struct.pack(
            HEADER_FORMAT,
            self._page_id,
            self._num_slots,
            self._free_start,
            self._free_end,
            self._flags
        )
        self._data[:HEADER_SIZE] = header
    
    def _read_slot(self, slot_id: SlotId) -> Tuple[int, int]:
        """Read a slot entry (offset, length)."""
        slot_offset = HEADER_SIZE + slot_id * SLOT_SIZE
        return struct.unpack(SLOT_FORMAT, self._data[slot_offset:slot_offset + SLOT_SIZE])
    
    def _write_slot(self, slot_id: SlotId, offset: int, length: int):
        """Write a slot entry."""
        slot_offset = HEADER_SIZE + slot_id * SLOT_SIZE
        self._data[slot_offset:slot_offset + SLOT_SIZE] = struct.pack(SLOT_FORMAT, offset, length)
    
    def _find_free_slot(self) -> Optional[SlotId]:
        """Find a deleted slot that can be reused."""
        for slot_id in range(self._num_slots):
            offset, length = self._read_slot(slot_id)
            if offset == DELETED_SLOT_OFFSET and length == DELETED_SLOT_LENGTH:
                return slot_id
        return None
    
    def __repr__(self) -> str:
        return (
            f"SlottedPage(id={self._page_id}, size={self._page_size}, "
            f"slots={self._num_slots}, free={self.free_space})"
        )
    
    def debug_dump(self) -> str:
        """Return a detailed debug representation of the page."""
        lines = [
            f"=== SlottedPage {self._page_id} ===",
            f"Size: {self._page_size} bytes",
            f"Slots: {self._num_slots}",
            f"Free start: {self._free_start}",
            f"Free end: {self._free_end}",
            f"Free space: {self.free_space} bytes",
            "",
            "Slot Directory:"
        ]
        
        for slot_id in range(self._num_slots):
            offset, length = self._read_slot(slot_id)
            if offset == DELETED_SLOT_OFFSET and length == DELETED_SLOT_LENGTH:
                lines.append(f"  [{slot_id}] DELETED")
            else:
                lines.append(f"  [{slot_id}] offset={offset}, length={length}")
        
        return '\n'.join(lines)
