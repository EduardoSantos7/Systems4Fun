"""
Index-Organized Table (IOT) Implementation.

An IOT stores records directly in the index structure (typically a B-Tree),
rather than having separate data and index files.

Characteristics:
- Records are stored sorted by primary key
- No separate heap file needed
- Search returns data directly (no second lookup)
- Range scans are efficient (data is ordered)
- Inserts may cause page splits

Trade-offs vs Heap + Index:
- Pros: Fewer disk seeks (data is in index)
- Cons: Larger index nodes, more expensive inserts

Used by: MySQL InnoDB (clustered index), Oracle IOT
"""

from typing import Any, Dict, Iterator, List, Optional, Tuple
from dataclasses import dataclass
import struct

from ..core import (
    IndexOrganizedStorage,
    RecordData,
    DiskManager,
    SlottedPage,
    PageId,
    RecordSerializer,
    IOStats,
    extract_key,
)


@dataclass 
class IOTEntry:
    """An entry in the IOT - stores the full record."""
    key: Any
    record: RecordData
    
    def serialize(self) -> bytes:
        """Serialize entry to bytes."""
        parts = []
        
        # Key
        if isinstance(self.key, int):
            parts.append(b'\x01')
            parts.append(struct.pack('>q', self.key))
        elif isinstance(self.key, str):
            parts.append(b'\x02')
            key_bytes = self.key.encode('utf-8')
            parts.append(struct.pack('>H', len(key_bytes)))
            parts.append(key_bytes)
        else:
            parts.append(b'\x02')
            key_bytes = str(self.key).encode('utf-8')
            parts.append(struct.pack('>H', len(key_bytes)))
            parts.append(key_bytes)
        
        # Record data
        record_bytes = RecordSerializer.serialize(self.record)
        parts.append(struct.pack('>I', len(record_bytes)))
        parts.append(record_bytes)
        
        return b''.join(parts)
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'IOTEntry':
        """Deserialize entry from bytes."""
        offset = 0
        
        # Key
        key_type = data[offset]
        offset += 1
        
        if key_type == 1:
            key = struct.unpack('>q', data[offset:offset+8])[0]
            offset += 8
        elif key_type == 2:
            key_len = struct.unpack('>H', data[offset:offset+2])[0]
            offset += 2
            key = data[offset:offset+key_len].decode('utf-8')
            offset += key_len
        else:
            raise ValueError(f"Unknown key type: {key_type}")
        
        # Record
        record_len = struct.unpack('>I', data[offset:offset+4])[0]
        offset += 4
        record = RecordSerializer.deserialize(data[offset:offset+record_len])
        
        return cls(key=key, record=record)


class IOTNode:
    """A node in the Index-Organized Table (B-Tree structure)."""
    
    def __init__(
        self,
        page_id: PageId,
        is_leaf: bool = True,
        entries: Optional[List[IOTEntry]] = None,
        children: Optional[List[PageId]] = None,
        next_leaf: Optional[PageId] = None
    ):
        self.page_id = page_id
        self.is_leaf = is_leaf
        self.entries = entries or []
        self.children = children or []
        self.next_leaf = next_leaf
    
    def serialize(self) -> bytes:
        """Serialize node to bytes."""
        parts = []
        
        # Header
        parts.append(struct.pack(
            '>?HIH',
            self.is_leaf,
            len(self.entries),
            self.next_leaf if self.next_leaf is not None else 0xFFFFFFFF,
            len(self.children)
        ))
        
        # Entries
        for entry in self.entries:
            entry_bytes = entry.serialize()
            parts.append(struct.pack('>H', len(entry_bytes)))
            parts.append(entry_bytes)
        
        # Children
        for child_id in self.children:
            parts.append(struct.pack('>I', child_id))
        
        return b''.join(parts)
    
    @classmethod
    def deserialize(cls, data: bytes, page_id: PageId) -> 'IOTNode':
        """Deserialize node from bytes."""
        offset = 0
        
        is_leaf, num_entries, next_leaf, num_children = struct.unpack(
            '>?HIH', data[offset:offset+9]
        )
        offset += 9
        
        next_leaf = None if next_leaf == 0xFFFFFFFF else next_leaf
        
        entries = []
        for _ in range(num_entries):
            entry_len = struct.unpack('>H', data[offset:offset+2])[0]
            offset += 2
            entry = IOTEntry.deserialize(data[offset:offset+entry_len])
            entries.append(entry)
            offset += entry_len
        
        children = []
        for _ in range(num_children):
            child_id = struct.unpack('>I', data[offset:offset+4])[0]
            children.append(child_id)
            offset += 4
        
        return cls(
            page_id=page_id,
            is_leaf=is_leaf,
            entries=entries,
            children=children,
            next_leaf=next_leaf
        )


class IndexOrganizedTable(IndexOrganizedStorage):
    """
    Index-Organized Table - data stored directly in B-Tree.
    
    Records are stored in leaf nodes of a B-Tree, sorted by primary key.
    This combines the data file and primary index into one structure.
    
    Example:
        >>> iot = IndexOrganizedTable("users.iot", key_field="id")
        >>> iot.insert({"id": 1, "name": "Alice", "email": "alice@example.com"})
        >>> record = iot.search(1)  # Returns data directly, no second lookup
        >>> print(record)
        {'id': 1, 'name': 'Alice', 'email': 'alice@example.com'}
        >>> 
        >>> # Range scans are efficient
        >>> for record in iot.range_scan(1, 100):
        ...     print(record)
    """
    
    def __init__(
        self,
        filepath: str,
        key_field: str,
        order: int = 20,  # Reduced for typical record sizes
        page_size: int = 4096,
        create: bool = True
    ):
        """
        Open or create an Index-Organized Table.
        
        Args:
            filepath: Path to the data file
            key_field: Name of the primary key field
            order: Maximum entries per node
            page_size: Size of each page in bytes
            create: If True, create file if it doesn't exist
        """
        self.filepath = filepath
        self.key_field = key_field
        self.order = order
        self.page_size = page_size
        
        self._disk = DiskManager(filepath, page_size, create)
        self._root_page_id: Optional[PageId] = None
        self._record_count = 0
        
        if self._disk.num_pages == 0:
            self._create_root()
        else:
            self._root_page_id = 0
            self._count_records()
    
    def _create_root(self):
        """Create empty root node."""
        page_id = self._disk.allocate_page()
        root = IOTNode(page_id=page_id, is_leaf=True)
        self._write_node(root)
        self._root_page_id = page_id
    
    def _count_records(self):
        """Count existing records."""
        for _ in self.scan():
            self._record_count += 1
    
    def _read_node(self, page_id: PageId) -> Optional[IOTNode]:
        """Read a node from disk."""
        page = self._disk.read_page(page_id)
        if page is None:
            return None
        
        data = page.read(0)
        if data is None:
            return None
        
        return IOTNode.deserialize(data, page_id)
    
    def _write_node(self, node: IOTNode):
        """Write a node to disk."""
        data = node.serialize()
        
        # Check if data fits in page
        if len(data) > self.page_size - 50:  # 50 bytes for header/overhead
            raise RuntimeError(
                f"Node data too large: {len(data)} bytes. "
                f"Reduce 'order' parameter or increase 'page_size'."
            )
        
        page = SlottedPage(node.page_id, self.page_size)
        slot_id = page.insert(data)
        
        if slot_id is None:
            raise RuntimeError(
                f"Failed to insert node data into page {node.page_id}. "
                f"Data size: {len(data)}, Page free space: {page.free_space}"
            )
        
        self._disk.write_page(page)
    
    def _allocate_node(self, is_leaf: bool = True) -> IOTNode:
        """Allocate a new node."""
        page_id = self._disk.allocate_page()
        return IOTNode(page_id=page_id, is_leaf=is_leaf)
    
    def _find_leaf(self, key: Any) -> IOTNode:
        """Find the leaf node where a key should be."""
        node = self._read_node(self._root_page_id)
        
        if node is None:
            raise RuntimeError(f"Could not read root node (page_id={self._root_page_id})")
        
        while not node.is_leaf:
            child_idx = 0
            for i, entry in enumerate(node.entries):
                if key < entry.key:
                    break
                child_idx = i + 1
            
            if child_idx >= len(node.children):
                child_idx = len(node.children) - 1
            
            next_node = self._read_node(node.children[child_idx])
            if next_node is None:
                raise RuntimeError(f"Could not read child node (page_id={node.children[child_idx]})")
            node = next_node
        
        return node
    
    def insert(self, record: RecordData) -> None:
        """
        Insert a record into the IOT.
        
        Args:
            record: The record data (must contain key_field)
        """
        key = extract_key(record, self.key_field)
        
        leaf = self._find_leaf(key)
        entry = IOTEntry(key=key, record=record)
        self._insert_into_leaf(leaf, entry)
        self._record_count += 1
    
    def _insert_into_leaf(self, leaf: IOTNode, entry: IOTEntry):
        """Insert entry into leaf, splitting if needed."""
        pos = 0
        for i, e in enumerate(leaf.entries):
            if entry.key < e.key:
                break
            pos = i + 1
        
        leaf.entries.insert(pos, entry)
        
        if len(leaf.entries) >= self.order:
            self._split_leaf(leaf)
        else:
            self._write_node(leaf)
    
    def _split_leaf(self, leaf: IOTNode):
        """Split a full leaf node."""
        mid = len(leaf.entries) // 2
        
        new_leaf = self._allocate_node(is_leaf=True)
        new_leaf.entries = leaf.entries[mid:]
        new_leaf.next_leaf = leaf.next_leaf
        
        leaf.entries = leaf.entries[:mid]
        leaf.next_leaf = new_leaf.page_id
        
        self._write_node(leaf)
        self._write_node(new_leaf)
        
        promoted_key = new_leaf.entries[0].key
        self._insert_into_parent(leaf, promoted_key, new_leaf)
    
    def _insert_into_parent(self, left: IOTNode, key: Any, right: IOTNode):
        """Insert into parent after split."""
        if left.page_id == self._root_page_id:
            new_root = self._allocate_node(is_leaf=False)
            # For internal nodes, we store a dummy entry with just the key
            new_root.entries = [IOTEntry(key=key, record={})]
            new_root.children = [left.page_id, right.page_id]
            self._write_node(new_root)
            self._root_page_id = new_root.page_id
            return
        
        parent = self._find_parent(self._root_page_id, left.page_id)
        if parent is None:
            return
        
        entry = IOTEntry(key=key, record={})
        pos = 0
        for i, e in enumerate(parent.entries):
            if key < e.key:
                break
            pos = i + 1
        
        parent.entries.insert(pos, entry)
        parent.children.insert(pos + 1, right.page_id)
        
        if len(parent.entries) >= self.order:
            self._split_internal(parent)
        else:
            self._write_node(parent)
    
    def _find_parent(self, current_id: PageId, child_id: PageId) -> Optional[IOTNode]:
        """Find parent of a node."""
        node = self._read_node(current_id)
        if node is None or node.is_leaf:
            return None
        
        if child_id in node.children:
            return node
        
        for c_id in node.children:
            parent = self._find_parent(c_id, child_id)
            if parent:
                return parent
        return None
    
    def _split_internal(self, node: IOTNode):
        """Split internal node."""
        mid = len(node.entries) // 2
        
        new_node = self._allocate_node(is_leaf=False)
        new_node.entries = node.entries[mid + 1:]
        new_node.children = node.children[mid + 1:]
        
        promoted_key = node.entries[mid].key
        
        node.entries = node.entries[:mid]
        node.children = node.children[:mid + 1]
        
        self._write_node(node)
        self._write_node(new_node)
        
        self._insert_into_parent(node, promoted_key, new_node)
    
    def search(self, key: Any) -> Optional[RecordData]:
        """
        Search for a record by key.
        
        Args:
            key: Primary key value
            
        Returns:
            The record if found, None otherwise
        """
        leaf = self._find_leaf(key)
        
        for entry in leaf.entries:
            if entry.key == key:
                return entry.record
        
        return None
    
    def delete(self, key: Any) -> bool:
        """
        Delete a record by key.
        
        Note: Simplified - doesn't handle underflow.
        """
        leaf = self._find_leaf(key)
        
        for i, entry in enumerate(leaf.entries):
            if entry.key == key:
                leaf.entries.pop(i)
                self._write_node(leaf)
                self._record_count -= 1
                return True
        
        return False
    
    def update(self, key: Any, record: RecordData) -> bool:
        """Update a record."""
        record_key = extract_key(record, self.key_field)
        if record_key != key:
            raise ValueError("Key in record doesn't match search key")
        
        leaf = self._find_leaf(key)
        
        for i, entry in enumerate(leaf.entries):
            if entry.key == key:
                leaf.entries[i] = IOTEntry(key=key, record=record)
                self._write_node(leaf)
                return True
        
        return False
    
    def scan(self) -> Iterator[RecordData]:
        """Iterate over all records in key order."""
        # Find leftmost leaf
        node = self._read_node(self._root_page_id)
        if node is None:
            return
        
        while not node.is_leaf:
            node = self._read_node(node.children[0])
        
        # Iterate through leaves
        while node is not None:
            for entry in node.entries:
                yield entry.record
            
            if node.next_leaf is not None:
                node = self._read_node(node.next_leaf)
            else:
                node = None
    
    def range_scan(self, start_key: Any, end_key: Any) -> Iterator[RecordData]:
        """
        Scan records in key range [start_key, end_key].
        
        This is efficient because records are sorted by key.
        """
        leaf = self._find_leaf(start_key)
        
        while leaf is not None:
            for entry in leaf.entries:
                if entry.key > end_key:
                    return
                if entry.key >= start_key:
                    yield entry.record
            
            if leaf.next_leaf is not None:
                leaf = self._read_node(leaf.next_leaf)
            else:
                leaf = None
    
    def close(self):
        """Close the file."""
        self._disk.close()
    
    @property
    def io_stats(self) -> IOStats:
        return self._disk.stats
    
    def reset_stats(self):
        self._disk.reset_stats()
    
    @property
    def stats(self) -> Dict[str, Any]:
        return {
            'filepath': str(self.filepath),
            'key_field': self.key_field,
            'record_count': self._record_count,
            'num_pages': self._disk.num_pages,
        }
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
    
    def __repr__(self):
        return f"IndexOrganizedTable('{self.filepath}', key='{self.key_field}', records={self._record_count})"
