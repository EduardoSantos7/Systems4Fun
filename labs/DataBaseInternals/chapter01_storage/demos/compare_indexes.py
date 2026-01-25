#!/usr/bin/env python3
"""
Compare different index types.

This script benchmarks and compares:
- B-Tree Index (ordered, range queries)
- Hash Index (O(1) point lookups)
- No index (full scan baseline)

Also demonstrates:
- Clustered vs Unclustered concepts
- Primary vs Secondary indexes

Run with: python -m chapter01_storage.demos.compare_indexes
"""

import os
import sys
import tempfile
import shutil
import time
import random
from typing import Dict, List, Any, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from chapter01_storage import (
    HeapFile, 
    BTreeIndex, 
    HashIndex,
    IndexOrganizedTable,
    RecordLocation,
)


def generate_records(n: int) -> List[Dict[str, Any]]:
    """Generate n test records with various fields."""
    names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]
    domains = ["gmail.com", "yahoo.com", "outlook.com", "company.com"]
    
    records = []
    for i in range(n):
        records.append({
            "id": i,
            "name": f"{random.choice(names)}_{i}",
            "email": f"user{i}@{random.choice(domains)}",
            "age": random.randint(18, 80),
            "department": random.choice(["Engineering", "Sales", "Marketing", "HR"]),
            "salary": random.randint(30000, 150000),
        })
    return records


def benchmark_no_index(heap: HeapFile, search_keys: List[int]) -> Dict:
    """Benchmark searching without an index (full scan)."""
    results = {}
    
    heap.reset_stats()
    start = time.perf_counter()
    found = 0
    for key in search_keys[:20]:  # Limit to 20 for speed
        result = heap.search("id", key)
        if result:
            found += 1
    results["search_time"] = time.perf_counter() - start
    results["searches"] = 20
    results["found"] = found
    results["page_reads"] = heap.io_stats.pages_read
    results["avg_pages_per_search"] = results["page_reads"] / 20
    
    return results


def benchmark_btree_index(heap: HeapFile, index: BTreeIndex, search_keys: List[int]) -> Dict:
    """Benchmark B-Tree index searches."""
    results = {}
    
    # Point searches
    index.reset_stats()
    heap.reset_stats()
    start = time.perf_counter()
    found = 0
    for key in search_keys:
        loc = index.search(key)
        if loc:
            record = heap.read(loc)
            if record:
                found += 1
    results["point_search_time"] = time.perf_counter() - start
    results["point_searches"] = len(search_keys)
    results["point_found"] = found
    results["point_index_reads"] = index.io_stats.pages_read
    results["point_data_reads"] = heap.io_stats.pages_read
    
    # Range search
    index.reset_stats()
    heap.reset_stats()
    n = heap.stats["record_count"]
    range_start = n // 4
    range_end = n // 4 + 100
    
    start = time.perf_counter()
    count = 0
    for key, loc in index.range_search(range_start, range_end):
        record = heap.read(loc)
        count += 1
    results["range_search_time"] = time.perf_counter() - start
    results["range_count"] = count
    results["range_index_reads"] = index.io_stats.pages_read
    results["range_data_reads"] = heap.io_stats.pages_read
    
    return results


def benchmark_hash_index(heap: HeapFile, index: HashIndex, search_keys: List[int]) -> Dict:
    """Benchmark Hash index searches."""
    results = {}
    
    index.reset_stats()
    heap.reset_stats()
    start = time.perf_counter()
    found = 0
    for key in search_keys:
        loc = index.search(key)
        if loc:
            record = heap.read(loc)
            if record:
                found += 1
    results["search_time"] = time.perf_counter() - start
    results["searches"] = len(search_keys)
    results["found"] = found
    results["index_reads"] = index.io_stats.pages_read
    results["data_reads"] = heap.io_stats.pages_read
    
    return results


def benchmark_iot(iot: IndexOrganizedTable, search_keys: List[int]) -> Dict:
    """Benchmark Index-Organized Table (clustered index)."""
    results = {}
    
    # Point searches - data returned directly!
    iot.reset_stats()
    start = time.perf_counter()
    found = 0
    for key in search_keys:
        record = iot.search(key)
        if record:
            found += 1
    results["point_search_time"] = time.perf_counter() - start
    results["point_searches"] = len(search_keys)
    results["point_found"] = found
    results["point_page_reads"] = iot.io_stats.pages_read
    
    # Range search - very efficient
    iot.reset_stats()
    n = iot.stats["record_count"]
    range_start = n // 4
    range_end = n // 4 + 100
    
    start = time.perf_counter()
    count = 0
    for record in iot.range_scan(range_start, range_end):
        count += 1
    results["range_search_time"] = time.perf_counter() - start
    results["range_count"] = count
    results["range_page_reads"] = iot.io_stats.pages_read
    
    return results


def print_results(no_idx: Dict, btree: Dict, hash_idx: Dict, iot: Dict, n: int, n_searches: int):
    """Print comparison results."""
    print("\n" + "="*90)
    print(f"  INDEX COMPARISON - {n:,} records, {n_searches} point searches")
    print("="*90)
    
    print("\n🔍 POINT SEARCH COMPARISON:")
    print("┌────────────────────┬──────────────┬──────────────┬──────────────┬──────────────┐")
    print("│ Metric             │ No Index     │ B-Tree       │ Hash Index   │ IOT          │")
    print("├────────────────────┼──────────────┼──────────────┼──────────────┼──────────────┤")
    print(f"│ Complexity         │ {'O(n)':>12} │ {'O(log n)':>12} │ {'O(1)':>12} │ {'O(log n)':>12} │")
    print(f"│ Time (total)       │ {no_idx['search_time']*1000:>8.1f} ms │ {btree['point_search_time']*1000:>8.1f} ms │ {hash_idx['search_time']*1000:>8.1f} ms │ {iot['point_search_time']*1000:>8.1f} ms │")
    print(f"│ Searches performed │ {no_idx['searches']:>12} │ {btree['point_searches']:>12} │ {hash_idx['searches']:>12} │ {iot['point_searches']:>12} │")
    print(f"│ Index page reads   │ {'N/A':>12} │ {btree['point_index_reads']:>12} │ {hash_idx['index_reads']:>12} │ {iot['point_page_reads']:>12} │")
    print(f"│ Data page reads    │ {no_idx['page_reads']:>12} │ {btree['point_data_reads']:>12} │ {hash_idx['data_reads']:>12} │ {'(included)':>12} │")
    print("└────────────────────┴──────────────┴──────────────┴──────────────┴──────────────┘")
    
    print("\n📊 RANGE SEARCH COMPARISON (100 records in range):")
    print("┌────────────────────┬──────────────┬──────────────┬──────────────┬──────────────┐")
    print("│ Metric             │ No Index     │ B-Tree       │ Hash Index   │ IOT          │")
    print("├────────────────────┼──────────────┼──────────────┼──────────────┼──────────────┤")
    print(f"│ Supported          │ {'Full scan':>12} │ {'✓ Efficient':>12} │ {'✗ Full scan':>12} │ {'✓ Efficient':>12} │")
    print(f"│ Time               │ {'~same as pt':>12} │ {btree['range_search_time']*1000:>8.1f} ms │ {'N/A':>12} │ {iot['range_search_time']*1000:>8.1f} ms │")
    print(f"│ Records found      │ {'N/A':>12} │ {btree['range_count']:>12} │ {'N/A':>12} │ {iot['range_count']:>12} │")
    print(f"│ Page reads         │ {'All pages':>12} │ {btree['range_index_reads'] + btree['range_data_reads']:>12} │ {'All pages':>12} │ {iot['range_page_reads']:>12} │")
    print("└────────────────────┴──────────────┴──────────────┴──────────────┴──────────────┘")
    
    print("\n" + "="*90)
    print("  CLUSTERED vs UNCLUSTERED INDEX")
    print("="*90)
    print("""
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                     │
│  UNCLUSTERED (Heap + B-Tree/Hash Index):                                           │
│  ┌─────────────┐      ┌─────────────┐                                              │
│  │   Index     │ ---> │  Heap File  │   Two lookups: Index → Location → Data       │
│  │ key → loc   │      │   (data)    │                                              │
│  └─────────────┘      └─────────────┘                                              │
│                                                                                     │
│  CLUSTERED (Index-Organized Table / IOT):                                          │
│  ┌─────────────────────────────────┐                                               │
│  │   B-Tree with Data in Leaves   │   One lookup: Index → Data directly            │
│  │   key → full record            │   Data physically sorted by key!               │
│  └─────────────────────────────────┘                                               │
│                                                                                     │
│  Key Insight:                                                                       │
│  • IOT has FEWER page reads for point queries (no second lookup)                   │
│  • IOT range scans read SEQUENTIAL pages (data is sorted)                          │
│  • Unclustered range scans may read RANDOM pages (data scattered)                  │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
""")
    
    print("\n" + "="*90)
    print("  WHEN TO USE EACH INDEX TYPE")
    print("="*90)
    print("""
┌─────────────────────┬────────────────────────────────────────────────────────────────┐
│ Index Type          │ Best For                                                       │
├─────────────────────┼────────────────────────────────────────────────────────────────┤
│ No Index            │ Small tables, write-heavy with rare reads, full scans anyway  │
│ B-Tree (Unclustered)│ Secondary indexes, frequent range queries on non-PK columns   │
│ Hash Index          │ Exact match only (WHERE id = X), very high read throughput    │
│ IOT / Clustered     │ Primary key access, range scans by PK, read-heavy workloads   │
└─────────────────────┴────────────────────────────────────────────────────────────────┘

Note: Most databases (PostgreSQL, MySQL InnoDB) use B-Tree indexes by default.
      Hash indexes are less common due to inability to support range queries.
      InnoDB always uses a clustered index on the primary key.
""")


def demo_secondary_index():
    """Demonstrate secondary index with indirection."""
    print("\n" + "="*90)
    print("  SECONDARY INDEX DEMONSTRATION")
    print("="*90)
    print("""
A secondary index is an index on a non-primary-key column.
It can point directly to data location OR to the primary key.

Example: Index on 'email' column, primary key is 'id'

Option A - Direct pointer:
  email_index: "alice@example.com" → (page=5, slot=2)
  Pros: One lookup to data
  Cons: Must update index if record moves

Option B - Via primary key (MySQL InnoDB style):
  email_index: "alice@example.com" → id=42
  pk_index:    id=42 → actual data
  Pros: Only PK index updates when data moves
  Cons: Two index lookups
""")
    
    temp_dir = tempfile.mkdtemp(prefix="secondary_idx_")
    
    try:
        heap_path = os.path.join(temp_dir, "users.dat")
        pk_path = os.path.join(temp_dir, "pk_index.idx")
        email_path = os.path.join(temp_dir, "email_index.idx")
        
        with HeapFile(heap_path) as heap, \
             BTreeIndex(pk_path) as pk_index, \
             BTreeIndex(email_path) as email_index:
            
            # Insert records
            records = [
                {"id": 1, "name": "Alice", "email": "alice@example.com"},
                {"id": 2, "name": "Bob", "email": "bob@example.com"},
                {"id": 3, "name": "Charlie", "email": "charlie@example.com"},
            ]
            
            print("📝 Inserting records with primary and secondary indexes:\n")
            for record in records:
                loc = heap.insert(record)
                pk_index.insert(record["id"], loc)
                # Secondary index stores primary key as "location"
                # In real impl, would need different value type
                email_index.insert(record["email"], loc)
                print(f"  {record}")
            
            print("\n🔍 Query: Find user with email 'bob@example.com'\n")
            
            # Search via secondary index
            email_index.reset_stats()
            heap.reset_stats()
            
            loc = email_index.search("bob@example.com")
            print(f"  Step 1: email_index.search('bob@example.com') → {loc}")
            print(f"          Index reads: {email_index.io_stats.pages_read}")
            
            record = heap.read(loc)
            print(f"  Step 2: heap.read({loc}) → {record}")
            print(f"          Data reads: {heap.io_stats.pages_read}")
            
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def main():
    """Run the index comparison."""
    print("\n" + "="*90)
    print("  DATABASE INDEX COMPARISON BENCHMARK")
    print("="*90)
    
    n_records = 2000
    n_searches = 100
    
    print(f"\nGenerating {n_records:,} test records...")
    records = generate_records(n_records)
    search_keys = random.sample(range(n_records), n_searches)
    
    temp_dir = tempfile.mkdtemp(prefix="idx_compare_")
    
    try:
        # Setup files
        heap_path = os.path.join(temp_dir, "data.heap")
        btree_path = os.path.join(temp_dir, "btree.idx")
        hash_path = os.path.join(temp_dir, "hash.idx")
        iot_path = os.path.join(temp_dir, "iot.dat")
        
        print("\n⏱️  Setting up storage structures...\n")
        
        # Create heap and populate
        print("  Creating Heap File...")
        heap = HeapFile(heap_path)
        locations = {}
        for record in records:
            loc = heap.insert(record)
            locations[record["id"]] = loc
        
        # Create B-Tree index
        print("  Creating B-Tree Index...")
        btree = BTreeIndex(btree_path)
        for record in records:
            btree.insert(record["id"], locations[record["id"]])
        
        # Create Hash index
        print("  Creating Hash Index...")
        hash_idx = HashIndex(hash_path, num_buckets=128)
        for record in records:
            hash_idx.insert(record["id"], locations[record["id"]])
        
        # Create IOT
        print("  Creating Index-Organized Table...")
        iot = IndexOrganizedTable(iot_path, key_field="id")
        for record in records:
            iot.insert(record)
        
        print("\n⏱️  Running benchmarks...\n")
        
        # Benchmark
        print("  [1/4] No index (full scan)...")
        no_idx_results = benchmark_no_index(heap, search_keys)
        
        print("  [2/4] B-Tree index...")
        btree_results = benchmark_btree_index(heap, btree, search_keys)
        
        print("  [3/4] Hash index...")
        hash_results = benchmark_hash_index(heap, hash_idx, search_keys)
        
        print("  [4/4] Index-Organized Table...")
        iot_results = benchmark_iot(iot, search_keys)
        
        # Cleanup
        heap.close()
        btree.close()
        hash_idx.close()
        iot.close()
        
        # Print results
        print_results(no_idx_results, btree_results, hash_results, iot_results, n_records, n_searches)
        
        # Secondary index demo
        demo_secondary_index()
        
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    print("\n✅ Index comparison complete!\n")


if __name__ == "__main__":
    main()
