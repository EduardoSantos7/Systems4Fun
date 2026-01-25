"""
Core components for database storage.

This module provides the fundamental building blocks:
- Page: The unit of disk I/O
- Record: Serialization for database records  
- DiskManager: Low-level file operations
- Interfaces: Abstract base classes for components
"""

from .interfaces import (
    Page,
    DataFile,
    Index,
    IndexOrganizedStorage,
    RecordLocation,
    PageId,
    SlotId,
    RecordData,
    IOStats,
)

from .page import SlottedPage

from .record import RecordSerializer, extract_key

from .disk_manager import DiskManager, PageDirectory


__all__ = [
    # Interfaces
    'Page',
    'DataFile', 
    'Index',
    'IndexOrganizedStorage',
    'RecordLocation',
    'PageId',
    'SlotId',
    'RecordData',
    'IOStats',
    
    # Implementations
    'SlottedPage',
    'RecordSerializer',
    'extract_key',
    'DiskManager',
    'PageDirectory',
]
