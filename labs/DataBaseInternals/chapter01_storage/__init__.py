"""
Chapter 1: Storage Engines & File Organization

This module provides implementations of fundamental database storage concepts:

Data Files:
- HeapFile: Unordered storage, fast inserts
- HashFile: Hash-organized for O(1) key lookups

Indexes:
- BTreeIndex: Balanced tree for ordered access
- HashIndex: Hash table for point lookups
- IndexOrganizedTable: Data stored in index (IOT)

Core Components:
- SlottedPage: Variable-length record storage
- RecordSerializer: Record serialization
- DiskManager: Low-level I/O

Example Usage:
    from chapter01_storage import HeapFile, BTreeIndex, HashFile, IndexOrganizedTable
    
    # Heap file with B-Tree index
    with HeapFile("data/users.heap") as heap:
        loc = heap.insert({"id": 1, "name": "Alice"})
        
    # Hash file for O(1) lookups
    with HashFile("data/users.hash", key_field="id") as hfile:
        hfile.insert({"id": 1, "name": "Alice"})
        record = hfile.search(1)
        
    # Index-Organized Table
    with IndexOrganizedTable("data/users.iot", key_field="id") as iot:
        iot.insert({"id": 1, "name": "Alice"})
        record = iot.search(1)
"""

# Core components
from .core import (
    # Interfaces
    Page,
    DataFile,
    Index,
    IndexOrganizedStorage,
    RecordLocation,
    PageId,
    SlotId,
    RecordData,
    IOStats,
    
    # Implementations
    SlottedPage,
    RecordSerializer,
    DiskManager,
    PageDirectory,
    extract_key,
)

# Data file implementations
from .data_files import (
    HeapFile,
    HashFile,
)

# Index implementations
from .indexes import (
    BTreeIndex,
    HashIndex,
    IndexOrganizedTable,
)


__all__ = [
    # Core interfaces
    'Page',
    'DataFile',
    'Index',
    'IndexOrganizedStorage',
    'RecordLocation',
    'PageId',
    'SlotId',
    'RecordData',
    'IOStats',
    
    # Core implementations
    'SlottedPage',
    'RecordSerializer',
    'DiskManager',
    'PageDirectory',
    'extract_key',
    
    # Data files
    'HeapFile',
    'HashFile',
    
    # Indexes
    'BTreeIndex',
    'HashIndex',
    'IndexOrganizedTable',
]

__version__ = '0.1.0'
