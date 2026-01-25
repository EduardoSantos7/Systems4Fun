"""
Data File implementations.

This module provides different ways to organize records on disk:
- HeapFile: Unordered, records go wherever there's space
- HashFile: Records distributed by hash(key) into buckets
"""

from .heap_file import HeapFile
from .hash_file import HashFile

__all__ = [
    'HeapFile',
    'HashFile',
]
