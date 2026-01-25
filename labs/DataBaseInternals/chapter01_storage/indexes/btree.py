"""
B-Tree Index Implementation.

A B-Tree is a self-balancing tree data structure that maintains sorted data
and allows searches, sequential access, insertions, and deletions in O(log n).

Key characteristics:
- All leaves are at the same depth
- Nodes can have multiple keys (high fanout = shallow tree)
- Each node is typically one page (minimizes disk I/O)
- Keys are kept in sorted order within nodes

This implementation:
- Stores the B-Tree nodes as pages on disk
- Leaf nodes store (key, RecordLocation) pairs
- Internal nodes store (key, child_page_id) pairs
- Supports range queries via leaf-level linked list

B-Tree Properties:
- Order M means: each node has at most M children
- Each node (except root) has at least M/2 children
- Root has at least 2 children (unless it's a leaf)
"""

from typing import Any, Dict, Iterator, List, Optional, Tuple
from dataclasses import dataclass
import struct

from ..core import (
    Index,
    RecordLocation,
    DiskManager,
    SlottedPage,
    PageId,
    IOStats,
)


@dataclass
class BTreeEntry:
    """An entry in a B-Tree node."""
    key: Any
    # For leaf nodes: record location
    # For internal nodes: child page ID
    value: Any
    
    def serialize(self) -> bytes:
        """Serialize the entry to bytes."""
        # Simple format: [key_type][key_len][key][value_type][value_data]
        parts = []
        
        # Serialize key
        if isinstance(self.key, int):
            parts.append(b'\x01')  # int type
            parts.append(struct.pack('>q', self.key))
        elif isinstance(self.key, str):
            parts.append(b'\x02')  # str type
            key_bytes = self.key.encode('utf-8')
            parts.append(struct.pack('>H', len(key_bytes)))
            parts.append(key_bytes)
        else:
            # Default: convert to string
            parts.append(b'\x02')
            key_bytes = str(self.key).encode('utf-8')
            parts.append(struct.pack('>H', len(key_bytes)))
            parts.append(key_bytes)
        
        # Serialize value (RecordLocation or page_id)
        if isinstance(self.value, RecordLocation):
            parts.append(b'\x10')  # RecordLocation type
            parts.append(struct.pack('>II', self.value.page_id, self.value.slot_id))
        elif isinstance(self.value, int):
            parts.append(b'\x11')  # page_id type
            parts.append(struct.pack('>I', self.value))
        
        return b''.join(parts)
    
    @classmethod
    def deserialize(cls, data: bytes) -> Tuple['BTreeEntry', int]:
        """Deserialize an entry from bytes, return (entry, bytes_consumed)."""
        offset = 0
        
        # Read key
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
        
        # Read value
        value_type = data[offset]
        offset += 1
        
        if value_type == 0x10:  # RecordLocation
            page_id, slot_id = struct.unpack('>II', data[offset:offset+8])
            value = RecordLocation(page_id, slot_id)
            offset += 8
        elif value_type == 0x11:  # page_id
            value = struct.unpack('>I', data[offset:offset+4])[0]
            offset += 4
        else:
            raise ValueError(f"Unknown value type: {value_type}")
        
        return cls(key=key, value=value), offset


class BTreeNode:
    """
    A node in the B-Tree.
    
    Each node is stored in a single page on disk.
    """
    
    def __init__(
        self,
        page_id: PageId,
        is_leaf: bool = True,
        entries: Optional[List[BTreeEntry]] = None,
        children: Optional[List[PageId]] = None,
        prev_leaf: Optional[PageId] = None,
        next_leaf: Optional[PageId] = None
    ):
        self.page_id = page_id
        self.is_leaf = is_leaf
        self.entries = entries or []
        # For internal nodes: children[i] contains keys < entries[i].key
        # children has one more element than entries
        self.children = children or []
        # For leaf nodes: linked list for range scans
        self.prev_leaf = prev_leaf
        self.next_leaf = next_leaf
    
    @property
    def num_keys(self) -> int:
        return len(self.entries)
    
    def serialize(self) -> bytes:
        """Serialize the node to bytes."""
        parts = []
        
        # Header: is_leaf(1), num_entries(2), prev_leaf(4), next_leaf(4), num_children(2)
        parts.append(struct.pack(
            '>?HIIH',
            self.is_leaf,
            len(self.entries),
            self.prev_leaf if self.prev_leaf is not None else 0xFFFFFFFF,
            self.next_leaf if self.next_leaf is not None else 0xFFFFFFFF,
            len(self.children)
        ))
        
        # Entries
        for entry in self.entries:
            entry_bytes = entry.serialize()
            parts.append(struct.pack('>H', len(entry_bytes)))
            parts.append(entry_bytes)
        
        # Children (for internal nodes)
        for child_id in self.children:
            parts.append(struct.pack('>I', child_id))
        
        return b''.join(parts)
    
    @classmethod
    def deserialize(cls, data: bytes, page_id: PageId) -> 'BTreeNode':
        """Deserialize a node from bytes."""
        offset = 0
        
        # Read header
        is_leaf, num_entries, prev_leaf, next_leaf, num_children = struct.unpack(
            '>?HIIH', data[offset:offset+13]
        )
        offset += 13
        
        prev_leaf = None if prev_leaf == 0xFFFFFFFF else prev_leaf
        next_leaf = None if next_leaf == 0xFFFFFFFF else next_leaf
        
        # Read entries
        entries = []
        for _ in range(num_entries):
            entry_len = struct.unpack('>H', data[offset:offset+2])[0]
            offset += 2
            entry, _ = BTreeEntry.deserialize(data[offset:offset+entry_len])
            entries.append(entry)
            offset += entry_len
        
        # Read children
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
            prev_leaf=prev_leaf,
            next_leaf=next_leaf
        )


class BTreeIndex(Index):
    """
    B-Tree index implementation.
    
    Provides O(log n) search, insert, and delete operations.
    Supports range queries via leaf-level linked list.
    
    Example:
        >>> # Create index on a heap file
        >>> heap = HeapFile("data.heap")
        >>> index = BTreeIndex("data_pk.idx", key_field="id")
        >>> 
        >>> # Insert with location
        >>> loc = heap.insert({"id": 1, "name": "Alice"})
        >>> index.insert(1, loc)
        >>> 
        >>> # Search
        >>> loc = index.search(1)  # O(log n)
        >>> record = heap.read(loc)
    """
    
    def __init__(
        self,
        filepath: str,
        order: int = 100,  # Max children per node
        page_size: int = 4096,
        create: bool = True
    ):
        """
        Open or create a B-Tree index.
        
        Args:
            filepath: Path to the index file
            order: Maximum number of children per node
            page_size: Size of each page/node in bytes
            create: If True, create file if it doesn't exist
        """
        self.filepath = filepath
        self.order = order
        self.page_size = page_size
        self._min_keys = (order - 1) // 2  # Minimum keys in non-root node
        
        self._disk = DiskManager(filepath, page_size, create)
        self._root_page_id: Optional[PageId] = None
        
        if self._disk.num_pages == 0:
            # Create root node
            self._create_root()
        else:
            # Root is always page 0
            self._root_page_id = 0
    
    def _create_root(self):
        """Create the initial root node (empty leaf)."""
        page_id = self._disk.allocate_page()
        root = BTreeNode(page_id=page_id, is_leaf=True)
        self._write_node(root)
        self._root_page_id = page_id
    
    def _read_node(self, page_id: PageId) -> Optional[BTreeNode]:
        """Read a B-Tree node from disk."""
        page = self._disk.read_page(page_id)
        if page is None:
            return None
        
        # Read from first slot
        data = page.read(0)
        if data is None:
            return None
        
        return BTreeNode.deserialize(data, page_id)
    
    def _write_node(self, node: BTreeNode):
        """Write a B-Tree node to disk."""
        data = node.serialize()
        
        # Check if data fits
        if len(data) > self.page_size - 50:
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
    
    def _allocate_node(self, is_leaf: bool = True) -> BTreeNode:
        """Allocate a new node."""
        page_id = self._disk.allocate_page()
        return BTreeNode(page_id=page_id, is_leaf=is_leaf)
    
    def _find_leaf(self, key: Any) -> BTreeNode:
        """Find the leaf node where a key should be."""
        node = self._read_node(self._root_page_id)
        
        if node is None:
            raise RuntimeError(f"Could not read root node (page_id={self._root_page_id})")
        
        while not node.is_leaf:
            # Find the child to follow
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
    
    def insert(self, key: Any, location: RecordLocation) -> None:
        """
        Insert a key-location pair into the index.
        
        Args:
            key: The search key
            location: Where the record is stored
        """
        # Find the leaf where this key belongs
        leaf = self._find_leaf(key)
        
        # Insert into leaf
        entry = BTreeEntry(key=key, value=location)
        self._insert_into_leaf(leaf, entry)
    
    def _insert_into_leaf(self, leaf: BTreeNode, entry: BTreeEntry):
        """Insert an entry into a leaf node, splitting if necessary."""
        # Find insertion position
        pos = 0
        for i, e in enumerate(leaf.entries):
            if entry.key < e.key:
                break
            pos = i + 1
        
        leaf.entries.insert(pos, entry)
        
        # Check if split is needed
        if len(leaf.entries) >= self.order:
            self._split_leaf(leaf)
        else:
            self._write_node(leaf)
    
    def _split_leaf(self, leaf: BTreeNode):
        """Split a leaf node."""
        mid = len(leaf.entries) // 2
        
        # Create new leaf with right half
        new_leaf = self._allocate_node(is_leaf=True)
        new_leaf.entries = leaf.entries[mid:]
        new_leaf.next_leaf = leaf.next_leaf
        new_leaf.prev_leaf = leaf.page_id
        
        # Update old leaf
        leaf.entries = leaf.entries[:mid]
        leaf.next_leaf = new_leaf.page_id
        
        # Update the next leaf's prev pointer
        if new_leaf.next_leaf is not None:
            next_node = self._read_node(new_leaf.next_leaf)
            if next_node:
                next_node.prev_leaf = new_leaf.page_id
                self._write_node(next_node)
        
        # Write both leaves
        self._write_node(leaf)
        self._write_node(new_leaf)
        
        # Promote middle key to parent
        promoted_key = new_leaf.entries[0].key
        self._insert_into_parent(leaf, promoted_key, new_leaf)
    
    def _insert_into_parent(self, left: BTreeNode, key: Any, right: BTreeNode):
        """Insert a key into the parent node after a split."""
        if left.page_id == self._root_page_id:
            # Create new root
            new_root = self._allocate_node(is_leaf=False)
            new_root.entries = [BTreeEntry(key=key, value=right.page_id)]
            new_root.children = [left.page_id, right.page_id]
            self._write_node(new_root)
            self._root_page_id = new_root.page_id
            return
        
        # Find parent (simplified: we'd normally track path during search)
        # For now, search from root
        parent = self._find_parent(self._root_page_id, left.page_id)
        if parent is None:
            return
        
        # Insert into parent
        entry = BTreeEntry(key=key, value=right.page_id)
        pos = 0
        for i, e in enumerate(parent.entries):
            if key < e.key:
                break
            pos = i + 1
        
        parent.entries.insert(pos, entry)
        parent.children.insert(pos + 1, right.page_id)
        
        # Check if parent needs splitting
        if len(parent.entries) >= self.order:
            self._split_internal(parent)
        else:
            self._write_node(parent)
    
    def _find_parent(self, current_page_id: PageId, child_page_id: PageId) -> Optional[BTreeNode]:
        """Find the parent of a node (simplified implementation)."""
        node = self._read_node(current_page_id)
        if node is None or node.is_leaf:
            return None
        
        if child_page_id in node.children:
            return node
        
        for child_id in node.children:
            parent = self._find_parent(child_id, child_page_id)
            if parent:
                return parent
        
        return None
    
    def _split_internal(self, node: BTreeNode):
        """Split an internal node."""
        mid = len(node.entries) // 2
        
        # Create new node with right half
        new_node = self._allocate_node(is_leaf=False)
        new_node.entries = node.entries[mid + 1:]
        new_node.children = node.children[mid + 1:]
        
        # Key to promote
        promoted_key = node.entries[mid].key
        
        # Update old node
        node.entries = node.entries[:mid]
        node.children = node.children[:mid + 1]
        
        # Write both nodes
        self._write_node(node)
        self._write_node(new_node)
        
        # Promote to parent
        self._insert_into_parent(node, promoted_key, new_node)
    
    def search(self, key: Any) -> Optional[RecordLocation]:
        """
        Search for a key in the index.
        
        Args:
            key: The key to search for
            
        Returns:
            RecordLocation if found, None otherwise
        """
        leaf = self._find_leaf(key)
        
        for entry in leaf.entries:
            if entry.key == key:
                return entry.value
        
        return None
    
    def range_search(self, start_key: Any, end_key: Any) -> Iterator[Tuple[Any, RecordLocation]]:
        """
        Find all keys in the range [start_key, end_key].
        
        Uses the leaf-level linked list for efficient iteration.
        
        Yields:
            (key, location) tuples
        """
        # Find starting leaf
        leaf = self._find_leaf(start_key)
        
        # Iterate through leaves
        while leaf is not None:
            for entry in leaf.entries:
                if entry.key > end_key:
                    return
                if entry.key >= start_key:
                    yield entry.key, entry.value
            
            # Move to next leaf
            if leaf.next_leaf is not None:
                leaf = self._read_node(leaf.next_leaf)
            else:
                leaf = None
    
    def delete(self, key: Any) -> bool:
        """
        Delete a key from the index.
        
        Note: This is a simplified implementation that doesn't handle
        underflow/merging. Production B-Trees would handle this.
        """
        leaf = self._find_leaf(key)
        
        for i, entry in enumerate(leaf.entries):
            if entry.key == key:
                leaf.entries.pop(i)
                self._write_node(leaf)
                return True
        
        return False
    
    def update(self, old_key: Any, new_key: Any, location: RecordLocation) -> bool:
        """Update a key in the index."""
        if self.delete(old_key):
            self.insert(new_key, location)
            return True
        return False
    
    def scan(self) -> Iterator[Tuple[Any, RecordLocation]]:
        """Iterate over all entries in key order."""
        # Find leftmost leaf
        node = self._read_node(self._root_page_id)
        while not node.is_leaf:
            node = self._read_node(node.children[0])
        
        # Iterate through all leaves
        while node is not None:
            for entry in node.entries:
                yield entry.key, entry.value
            
            if node.next_leaf is not None:
                node = self._read_node(node.next_leaf)
            else:
                node = None
    
    def close(self):
        """Close the index file."""
        self._disk.close()
    
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
        return f"BTreeIndex('{self.filepath}', order={self.order})"
