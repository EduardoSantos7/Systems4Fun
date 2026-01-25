#!/usr/bin/env python3
"""
Compare different data file organizations.

This script benchmarks and compares:
- Heap File (unordered)
- Hash File (hash-organized)
- Index-Organized Table (B-Tree)

Run with: python -m chapter01_storage.demos.compare_data_files
"""

import os
import sys
import tempfile
import shutil
import time
import random
from typing import Dict, List, Any

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from chapter01_storage import HeapFile, HashFile, IndexOrganizedTable


def generate_records(n: int) -> List[Dict[str, Any]]:
    """Generate n test records."""
    records = []
    for i in range(n):
        records.append({
            "id": i,
            "name": f"User_{i:05d}",
            "email": f"user{i}@example.com",
            "age": random.randint(18, 80),
            "score": random.random() * 100,
        })
    return records


def benchmark_heap_file(temp_dir: str, records: List[Dict], search_keys: List[int]) -> Dict:
    """Benchmark heap file operations."""
    filepath = os.path.join(temp_dir, "bench_heap.dat")
    results = {}
    
    with HeapFile(filepath, page_size=4096) as heap:
        # Insert benchmark
        start = time.perf_counter()
        locations = {}
        for record in records:
            loc = heap.insert(record)
            locations[record["id"]] = loc
        results["insert_time"] = time.perf_counter() - start
        results["insert_io_writes"] = heap.io_stats.pages_written
        
        # Point search by location (fast)
        heap.reset_stats()
        start = time.perf_counter()
        for key in search_keys:
            if key in locations:
                heap.read(locations[key])
        results["read_by_loc_time"] = time.perf_counter() - start
        results["read_by_loc_io"] = heap.io_stats.pages_read
        
        # Search by scanning (slow)
        heap.reset_stats()
        start = time.perf_counter()
        for key in search_keys[:10]:  # Only 10 to avoid too slow
            heap.search("id", key)
        results["scan_search_time"] = time.perf_counter() - start
        results["scan_search_io"] = heap.io_stats.pages_read
        
        # Full scan
        heap.reset_stats()
        start = time.perf_counter()
        count = sum(1 for _ in heap.scan())
        results["full_scan_time"] = time.perf_counter() - start
        results["full_scan_io"] = heap.io_stats.pages_read
        
        results["num_pages"] = heap.stats["num_pages"]
    
    return results


def benchmark_hash_file(temp_dir: str, records: List[Dict], search_keys: List[int]) -> Dict:
    """Benchmark hash file operations."""
    filepath = os.path.join(temp_dir, "bench_hash.dat")
    results = {}
    
    num_buckets = max(16, len(records) // 20)  # ~20 records per bucket
    
    with HashFile(filepath, key_field="id", num_buckets=num_buckets, page_size=4096) as hfile:
        # Insert benchmark
        start = time.perf_counter()
        for record in records:
            hfile.insert(record)
        results["insert_time"] = time.perf_counter() - start
        results["insert_io_writes"] = hfile.io_stats.pages_written
        
        # Point search (fast - O(1))
        hfile.reset_stats()
        start = time.perf_counter()
        for key in search_keys:
            hfile.search(key)
        results["point_search_time"] = time.perf_counter() - start
        results["point_search_io"] = hfile.io_stats.pages_read
        
        # Full scan
        hfile.reset_stats()
        start = time.perf_counter()
        count = sum(1 for _ in hfile.scan())
        results["full_scan_time"] = time.perf_counter() - start
        results["full_scan_io"] = hfile.io_stats.pages_read
        
        results["num_pages"] = hfile.stats["num_pages"]
        results["num_buckets"] = num_buckets
    
    return results


def benchmark_iot(temp_dir: str, records: List[Dict], search_keys: List[int]) -> Dict:
    """Benchmark Index-Organized Table operations."""
    filepath = os.path.join(temp_dir, "bench_iot.dat")
    results = {}
    
    with IndexOrganizedTable(filepath, key_field="id", page_size=4096) as iot:
        # Insert benchmark
        start = time.perf_counter()
        for record in records:
            iot.insert(record)
        results["insert_time"] = time.perf_counter() - start
        results["insert_io_writes"] = iot.io_stats.pages_written
        
        # Point search (O(log n))
        iot.reset_stats()
        start = time.perf_counter()
        for key in search_keys:
            iot.search(key)
        results["point_search_time"] = time.perf_counter() - start
        results["point_search_io"] = iot.io_stats.pages_read
        
        # Range scan
        iot.reset_stats()
        start = time.perf_counter()
        range_start = len(records) // 4
        range_end = len(records) // 4 + 100
        count = sum(1 for _ in iot.range_scan(range_start, range_end))
        results["range_scan_time"] = time.perf_counter() - start
        results["range_scan_io"] = iot.io_stats.pages_read
        results["range_scan_count"] = count
        
        # Full scan (in order)
        iot.reset_stats()
        start = time.perf_counter()
        count = sum(1 for _ in iot.scan())
        results["full_scan_time"] = time.perf_counter() - start
        results["full_scan_io"] = iot.io_stats.pages_read
        
        results["num_pages"] = iot.stats["num_pages"]
    
    return results


def print_comparison(heap_results: Dict, hash_results: Dict, iot_results: Dict, n: int, n_searches: int):
    """Print comparison table."""
    print("\n" + "="*80)
    print(f"  DATA FILE COMPARISON - {n:,} records, {n_searches} searches")
    print("="*80)
    
    # Insert comparison
    print("\n📝 INSERT PERFORMANCE:")
    print("┌─────────────────────┬──────────────┬──────────────┬──────────────┐")
    print("│ Metric              │ Heap File    │ Hash File    │ IOT (B-Tree) │")
    print("├─────────────────────┼──────────────┼──────────────┼──────────────┤")
    print(f"│ Time                │ {heap_results['insert_time']*1000:>8.1f} ms │ {hash_results['insert_time']*1000:>8.1f} ms │ {iot_results['insert_time']*1000:>8.1f} ms │")
    print(f"│ Page writes         │ {heap_results['insert_io_writes']:>12} │ {hash_results['insert_io_writes']:>12} │ {iot_results['insert_io_writes']:>12} │")
    print(f"│ Total pages         │ {heap_results['num_pages']:>12} │ {hash_results['num_pages']:>12} │ {iot_results['num_pages']:>12} │")
    print("└─────────────────────┴──────────────┴──────────────┴──────────────┘")
    
    # Search comparison
    print("\n🔍 POINT SEARCH PERFORMANCE:")
    print("┌─────────────────────┬──────────────┬──────────────┬──────────────┐")
    print("│ Metric              │ Heap File    │ Hash File    │ IOT (B-Tree) │")
    print("├─────────────────────┼──────────────┼──────────────┼──────────────┤")
    print(f"│ Method              │ {'By location':>12} │ {'Hash O(1)':>12} │ {'B-Tree O(logn)':>12} │")
    print(f"│ Time ({n_searches} searches)  │ {heap_results['read_by_loc_time']*1000:>8.1f} ms │ {hash_results['point_search_time']*1000:>8.1f} ms │ {iot_results['point_search_time']*1000:>8.1f} ms │")
    print(f"│ Page reads          │ {heap_results['read_by_loc_io']:>12} │ {hash_results['point_search_io']:>12} │ {iot_results['point_search_io']:>12} │")
    print("└─────────────────────┴──────────────┴──────────────┴──────────────┘")
    
    # Scan comparison
    print("\n📋 FULL SCAN PERFORMANCE:")
    print("┌─────────────────────┬──────────────┬──────────────┬──────────────┐")
    print("│ Metric              │ Heap File    │ Hash File    │ IOT (B-Tree) │")
    print("├─────────────────────┼──────────────┼──────────────┼──────────────┤")
    print(f"│ Time                │ {heap_results['full_scan_time']*1000:>8.1f} ms │ {hash_results['full_scan_time']*1000:>8.1f} ms │ {iot_results['full_scan_time']*1000:>8.1f} ms │")
    print(f"│ Page reads          │ {heap_results['full_scan_io']:>12} │ {hash_results['full_scan_io']:>12} │ {iot_results['full_scan_io']:>12} │")
    print(f"│ Output order        │ {'Insertion':>12} │ {'Hash order':>12} │ {'Sorted':>12} │")
    print("└─────────────────────┴──────────────┴──────────────┴──────────────┘")
    
    # Range scan (only IOT)
    print("\n📊 RANGE SCAN (only IOT supports efficiently):")
    print(f"   IOT range scan (100 records): {iot_results['range_scan_time']*1000:.2f} ms, {iot_results['range_scan_io']} page reads")
    print(f"   Heap/Hash would require full scan: O(n)")
    
    # Summary
    print("\n" + "="*80)
    print("  SUMMARY")
    print("="*80)
    print("""
┌─────────────────┬───────────────┬───────────────┬───────────────────────────┐
│ Use Case        │ Best Choice   │ Complexity    │ Why                       │
├─────────────────┼───────────────┼───────────────┼───────────────────────────┤
│ Write-heavy     │ Heap File     │ Insert: O(1)  │ Just append, no ordering  │
│ Point lookups   │ Hash File     │ Search: O(1)  │ Direct hash to bucket     │
│ Range queries   │ IOT           │ Range: O(logn)│ Data sorted by key        │
│ Ordered output  │ IOT           │ Scan: O(n)    │ Already in order          │
│ Mixed workload  │ Heap + Index  │ Varies        │ Best of both worlds       │
└─────────────────┴───────────────┴───────────────┴───────────────────────────┘
""")


def main():
    """Run the comparison benchmark."""
    print("\n" + "="*80)
    print("  DATABASE DATA FILE ORGANIZATION COMPARISON")
    print("="*80)
    
    # Parameters
    n_records = 2000
    n_searches = 100
    
    print(f"\nGenerating {n_records:,} test records...")
    records = generate_records(n_records)
    
    # Random search keys
    search_keys = random.sample(range(n_records), min(n_searches, n_records))
    
    # Create temp directory
    temp_dir = tempfile.mkdtemp(prefix="db_compare_")
    
    try:
        print("\n⏱️  Running benchmarks (this may take a moment)...\n")
        
        print("  [1/3] Benchmarking Heap File...")
        heap_results = benchmark_heap_file(temp_dir, records, search_keys)
        
        print("  [2/3] Benchmarking Hash File...")
        hash_results = benchmark_hash_file(temp_dir, records, search_keys)
        
        print("  [3/3] Benchmarking Index-Organized Table...")
        iot_results = benchmark_iot(temp_dir, records, search_keys)
        
        # Print comparison
        print_comparison(heap_results, hash_results, iot_results, n_records, n_searches)
        
    finally:
        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    print("\n✅ Benchmark complete!\n")


if __name__ == "__main__":
    main()
