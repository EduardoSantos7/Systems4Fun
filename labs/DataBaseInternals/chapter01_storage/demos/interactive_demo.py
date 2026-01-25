#!/usr/bin/env python3
"""
Interactive Demo - Database Storage Concepts

This demo lets you experiment with different storage structures
and see how they work under the hood.

Run with: python -m chapter01_storage.demos.interactive_demo
"""

import os
import sys
import tempfile
import shutil
from typing import Dict, Any, List
import time

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from chapter01_storage import (
    HeapFile,
    HashFile,
    BTreeIndex,
    HashIndex,
    IndexOrganizedTable,
    RecordLocation,
)


class InteractiveDemo:
    """Interactive demonstration of storage concepts."""
    
    def __init__(self):
        self.temp_dir = tempfile.mkdtemp(prefix="db_demo_")
        self.storage = None
        self.storage_type = None
        print(f"\n📁 Using temporary directory: {self.temp_dir}\n")
    
    def cleanup(self):
        """Clean up temporary files."""
        if self.storage:
            self.storage.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def show_menu(self):
        """Display the main menu."""
        print("\n" + "="*60)
        print("  DATABASE STORAGE INTERNALS - Interactive Demo")
        print("="*60)
        print("\n📚 Choose a storage type to explore:\n")
        print("  1. Heap File        - Unordered, fast inserts")
        print("  2. Hash File        - O(1) lookups by key")
        print("  3. B-Tree Index     - Ordered, range queries")
        print("  4. Hash Index       - O(1) key-to-location mapping")
        print("  5. Index-Organized  - Data inside the index (IOT)")
        print("\n  6. Compare All      - Performance comparison")
        print("  7. Page Internals   - See how pages work")
        print("\n  0. Exit")
        print()
    
    def run(self):
        """Run the interactive demo."""
        try:
            while True:
                self.show_menu()
                choice = input("Enter choice (0-7): ").strip()
                
                if choice == '0':
                    print("\n👋 Goodbye!\n")
                    break
                elif choice == '1':
                    self.demo_heap_file()
                elif choice == '2':
                    self.demo_hash_file()
                elif choice == '3':
                    self.demo_btree_index()
                elif choice == '4':
                    self.demo_hash_index()
                elif choice == '5':
                    self.demo_iot()
                elif choice == '6':
                    self.demo_comparison()
                elif choice == '7':
                    self.demo_page_internals()
                else:
                    print("❌ Invalid choice. Please try again.")
        finally:
            self.cleanup()
    
    def demo_heap_file(self):
        """Demonstrate heap file operations."""
        print("\n" + "="*60)
        print("  HEAP FILE DEMONSTRATION")
        print("="*60)
        print("""
A heap file stores records in no particular order.
Records go wherever there's space - like a "heap" of data.

Characteristics:
  ✅ Insert: O(1) - just find space and write
  ❌ Search: O(n) - must scan all records
  ✅ Good for: Write-heavy workloads, bulk loading
""")
        
        filepath = os.path.join(self.temp_dir, "demo_heap.dat")
        
        with HeapFile(filepath, page_size=4096) as heap:
            # Insert some records
            print("📝 Inserting records...")
            records = [
                {"id": 1, "name": "Alice", "email": "alice@example.com"},
                {"id": 2, "name": "Bob", "email": "bob@example.com"},
                {"id": 3, "name": "Charlie", "email": "charlie@example.com"},
            ]
            
            locations = []
            for record in records:
                loc = heap.insert(record)
                locations.append(loc)
                print(f"  Inserted id={record['id']} at {loc}")
            
            print(f"\n📊 Stats: {heap.stats['num_pages']} pages, {heap.stats['record_count']} records")
            print(f"   I/O: {heap.io_stats}")
            
            # Read by location (fast)
            print("\n🔍 Reading by location (direct access - fast):")
            heap.reset_stats()
            record = heap.read(locations[0])
            print(f"  Record at {locations[0]}: {record}")
            print(f"  I/O: {heap.io_stats}")
            
            # Search by scanning (slow)
            print("\n🔍 Searching by scanning (O(n) - slow):")
            heap.reset_stats()
            result = heap.search("name", "Charlie")
            if result:
                loc, record = result
                print(f"  Found: {record}")
            print(f"  I/O: {heap.io_stats}")
            
            # Full scan
            print("\n📋 Full table scan:")
            heap.reset_stats()
            for loc, record in heap.scan():
                print(f"  {loc} -> {record}")
            print(f"  I/O: {heap.io_stats}")
        
        input("\n[Press Enter to continue...]")
    
    def demo_hash_file(self):
        """Demonstrate hash file operations."""
        print("\n" + "="*60)
        print("  HASH FILE DEMONSTRATION")
        print("="*60)
        print("""
A hash file distributes records into buckets using hash(key).
Each bucket is one or more pages.

Characteristics:
  ✅ Search by key: O(1) average
  ✅ Insert: O(1) average
  ❌ Range queries: O(n) - hash destroys order
  ❌ Must know the key to search
""")
        
        filepath = os.path.join(self.temp_dir, "demo_hash.dat")
        
        with HashFile(filepath, key_field="id", num_buckets=8) as hfile:
            print(f"📦 Created hash file with {hfile.num_buckets} buckets\n")
            
            # Insert records
            print("📝 Inserting records...")
            for i in range(1, 11):
                record = {"id": i, "name": f"User{i}", "score": i * 10}
                loc = hfile.insert(record)
                bucket = hfile._hash_key(i)
                print(f"  id={i} -> bucket {bucket} -> {loc}")
            
            print(f"\n📊 Bucket distribution:")
            for bucket_id, stats in hfile.bucket_stats().items():
                if stats['records'] > 0:
                    print(f"  Bucket {bucket_id}: {stats['records']} records")
            
            # Search (O(1))
            print("\n🔍 Searching for id=5 (O(1)):")
            hfile.reset_stats()
            record = hfile.search(5)
            print(f"  Found: {record}")
            print(f"  I/O: {hfile.io_stats}")
            
            # Search non-existent
            print("\n🔍 Searching for id=999 (not found):")
            hfile.reset_stats()
            record = hfile.search(999)
            print(f"  Found: {record}")
            print(f"  I/O: {hfile.io_stats}")
        
        input("\n[Press Enter to continue...]")
    
    def demo_btree_index(self):
        """Demonstrate B-Tree index operations."""
        print("\n" + "="*60)
        print("  B-TREE INDEX DEMONSTRATION")
        print("="*60)
        print("""
A B-Tree is a balanced tree that keeps data sorted.
Each node is a page, and nodes can have many children (high fanout).

Characteristics:
  ✅ Search: O(log n)
  ✅ Insert: O(log n)
  ✅ Range queries: Efficient (leaves are linked)
  ✅ Ordered iteration
""")
        
        heap_path = os.path.join(self.temp_dir, "btree_data.dat")
        index_path = os.path.join(self.temp_dir, "btree_index.idx")
        
        with HeapFile(heap_path) as heap, BTreeIndex(index_path) as index:
            print("📝 Inserting records and building index...")
            
            # Insert in random order to show B-Tree handles it
            ids = [5, 2, 8, 1, 9, 3, 7, 4, 6, 10]
            for i in ids:
                record = {"id": i, "name": f"User{i}"}
                loc = heap.insert(record)
                index.insert(i, loc)
                print(f"  Inserted id={i}")
            
            # Point lookup
            print("\n🔍 Point lookup (id=7):")
            index.reset_stats()
            loc = index.search(7)
            if loc:
                record = heap.read(loc)
                print(f"  Found at {loc}: {record}")
            print(f"  Index I/O: {index.io_stats}")
            
            # Range query
            print("\n🔍 Range query (id 3 to 7):")
            index.reset_stats()
            print("  Results:")
            for key, loc in index.range_search(3, 7):
                record = heap.read(loc)
                print(f"    id={key}: {record}")
            print(f"  Index I/O: {index.io_stats}")
            
            # Ordered scan
            print("\n📋 Full scan in order:")
            for key, loc in index.scan():
                record = heap.read(loc)
                print(f"  id={key}: {record['name']}")
        
        input("\n[Press Enter to continue...]")
    
    def demo_hash_index(self):
        """Demonstrate hash index operations."""
        print("\n" + "="*60)
        print("  HASH INDEX DEMONSTRATION")
        print("="*60)
        print("""
A hash index maps keys to record locations using a hash table.
Similar to hash file, but stores locations instead of data.

Characteristics:
  ✅ Point lookup: O(1)
  ❌ Range queries: Not supported
  ✅ Good for: Primary key lookups
""")
        
        heap_path = os.path.join(self.temp_dir, "hash_idx_data.dat")
        index_path = os.path.join(self.temp_dir, "hash_index.idx")
        
        with HeapFile(heap_path) as heap, HashIndex(index_path, num_buckets=16) as index:
            print("📝 Inserting records and building hash index...")
            
            for i in range(1, 11):
                record = {"id": i, "name": f"User{i}"}
                loc = heap.insert(record)
                index.insert(i, loc)
            
            print(f"  Inserted {index._entry_count} entries")
            
            # Point lookup
            print("\n🔍 Point lookup (id=5):")
            index.reset_stats()
            loc = index.search(5)
            if loc:
                record = heap.read(loc)
                print(f"  Found at {loc}: {record}")
            print(f"  Index I/O: {index.io_stats}")
            
            print("\n⚠️  Range queries not supported by hash index!")
            print("  Use B-Tree index for range queries.")
        
        input("\n[Press Enter to continue...]")
    
    def demo_iot(self):
        """Demonstrate Index-Organized Table."""
        print("\n" + "="*60)
        print("  INDEX-ORGANIZED TABLE (IOT) DEMONSTRATION")
        print("="*60)
        print("""
An IOT stores data directly in the index (B-Tree).
No separate heap file - the index IS the table.

Characteristics:
  ✅ Search: O(log n) and returns data directly
  ✅ Range scans: Very efficient (data is sorted)
  ❌ Inserts: May cause page splits
  ❌ Secondary indexes need extra lookup

Used by: MySQL InnoDB (clustered index), Oracle IOT
""")
        
        filepath = os.path.join(self.temp_dir, "demo_iot.dat")
        
        with IndexOrganizedTable(filepath, key_field="id") as iot:
            print("📝 Inserting records (stored directly in B-Tree)...")
            
            # Insert in random order
            ids = [5, 2, 8, 1, 9, 3, 7, 4, 6, 10]
            for i in ids:
                record = {"id": i, "name": f"User{i}", "email": f"user{i}@example.com"}
                iot.insert(record)
                print(f"  Inserted id={i}")
            
            # Point lookup (returns data directly!)
            print("\n🔍 Point lookup (id=7) - data returned directly:")
            iot.reset_stats()
            record = iot.search(7)
            print(f"  Found: {record}")
            print(f"  I/O: {iot.io_stats}")
            print("  Note: Only 1-2 page reads needed (no second lookup!)")
            
            # Range scan (very efficient)
            print("\n🔍 Range scan (id 3 to 7) - data in sorted order:")
            iot.reset_stats()
            for record in iot.range_scan(3, 7):
                print(f"  {record}")
            print(f"  I/O: {iot.io_stats}")
            
            # Full scan (in order)
            print("\n📋 Full scan (automatically in key order):")
            for record in iot.scan():
                print(f"  id={record['id']}: {record['name']}")
        
        input("\n[Press Enter to continue...]")
    
    def demo_comparison(self):
        """Compare performance of different storage types."""
        print("\n" + "="*60)
        print("  PERFORMANCE COMPARISON")
        print("="*60)
        
        n_records = 100_000
        print(f"\nInserting {n_records} records into each storage type...\n")
        
        results = {}
        
        # Heap File
        filepath = os.path.join(self.temp_dir, "cmp_heap.dat")
        with HeapFile(filepath) as heap:
            start = time.time()
            for i in range(n_records):
                heap.insert({"id": i, "name": f"User{i}", "data": "x" * 100})
            insert_time = time.time() - start
            
            heap.reset_stats()
            start = time.time()
            heap.search("id", n_records // 2)
            search_time = time.time() - start
            search_io = heap.io_stats.pages_read
            
            results['Heap File'] = {
                'insert_time': insert_time,
                'search_time': search_time,
                'search_io': search_io,
            }
        
        # Hash File
        filepath = os.path.join(self.temp_dir, "cmp_hash.dat")
        with HashFile(filepath, key_field="id", num_buckets=64) as hfile:
            start = time.time()
            for i in range(n_records):
                hfile.insert({"id": i, "name": f"User{i}", "data": "x" * 100})
            insert_time = time.time() - start
            
            hfile.reset_stats()
            start = time.time()
            hfile.search(n_records // 2)
            search_time = time.time() - start
            search_io = hfile.io_stats.pages_read
            
            results['Hash File'] = {
                'insert_time': insert_time,
                'search_time': search_time,
                'search_io': search_io,
            }
        
        # IOT
        filepath = os.path.join(self.temp_dir, "cmp_iot.dat")
        with IndexOrganizedTable(filepath, key_field="id") as iot:
            start = time.time()
            for i in range(n_records):
                iot.insert({"id": i, "name": f"User{i}", "data": "x" * 100})
            insert_time = time.time() - start
            
            iot.reset_stats()
            start = time.time()
            iot.search(n_records // 2)
            search_time = time.time() - start
            search_io = iot.io_stats.pages_read
            
            results['IOT'] = {
                'insert_time': insert_time,
                'search_time': search_time,
                'search_io': search_io,
            }
        
        # Display results
        print("┌────────────────┬──────────────┬──────────────┬────────────┐")
        print("│ Storage Type   │ Insert Time  │ Search Time  │ Search I/O │")
        print("├────────────────┼──────────────┼──────────────┼────────────┤")
        for name, data in results.items():
            print(f"│ {name:<14} │ {data['insert_time']*1000:>8.2f} ms │ {data['search_time']*1000:>8.2f} ms │ {data['search_io']:>6} pages │")
        print("└────────────────┴──────────────┴──────────────┴────────────┘")
        
        print("""
Key Observations:
  • Heap File: Fast inserts, but search requires full scan
  • Hash File: Fast inserts AND fast search (O(1))
  • IOT: Slower inserts (B-Tree maintenance), fast search (O(log n))
""")
        
        input("[Press Enter to continue...]")
    
    def demo_page_internals(self):
        """Show how pages work internally."""
        print("\n" + "="*60)
        print("  PAGE INTERNALS DEMONSTRATION")
        print("="*60)
        print("""
A page is the unit of disk I/O. This demo shows how a
Slotted Page stores variable-length records.
""")
        
        from chapter01_storage import SlottedPage, RecordSerializer
        
        # Create a page
        page = SlottedPage(page_id=0, page_size=256)  # Small page for demo
        print(f"Created page: {page}")
        print(f"  Free space: {page.free_space} bytes")
        
        # Insert some records
        print("\n📝 Inserting records...")
        
        records = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]
        
        for record in records:
            data = RecordSerializer.serialize(record)
            slot_id = page.insert(data)
            print(f"  Inserted {record} at slot {slot_id}")
            print(f"    Serialized size: {len(data)} bytes")
            print(f"    Free space remaining: {page.free_space} bytes")
        
        # Show page structure
        print("\n📊 Page structure:")
        print(page.debug_dump())
        
        # Delete a record
        print("\n🗑️  Deleting slot 1 (Bob)...")
        page.delete(1)
        print(page.debug_dump())
        
        # Compact
        print("\n🔧 Compacting page...")
        freed = page.compact()
        print(f"  Freed {freed} bytes")
        print(page.debug_dump())
        
        input("\n[Press Enter to continue...]")


def main():
    """Main entry point."""
    demo = InteractiveDemo()
    demo.run()


if __name__ == "__main__":
    main()
