# Chapter 1: Storage Engines & File Organization

This chapter covers the fundamental building blocks of database storage systems.

## 📚 Concepts Covered

### 1. Storage Hierarchy

```
Record (row) → Page (block) → File → Table
```

- **Record**: A single row of data (e.g., one user)
- **Page**: Fixed-size block (typically 4KB-16KB), the unit of disk I/O
- **File**: Collection of pages, stored as actual file on disk
- **Table**: Logical concept backed by data files + index files

### 2. Data File Organizations

| Type | Description | Best For |
|------|-------------|----------|
| **Heap File** | Records in insertion order, no organization | Write-heavy workloads, full scans |
| **Hash File** | Records distributed by hash(key) into buckets | Point queries by key |
| **IOT** | Records stored inside B-Tree index, ordered | Read-heavy, range queries |

### 3. Index Types

| Type | Description | Operations |
|------|-------------|------------|
| **B-Tree** | Balanced tree, ordered by key | O(log n) search, range queries |
| **Hash Index** | Hash table mapping key → location | O(1) point queries |

### 4. Clustered vs Unclustered

- **Clustered**: Data physically ordered same as index (only 1 per table)
- **Unclustered**: Index order ≠ data order (can have multiple)

## 🗂️ File Structure

```
chapter01_storage/
├── __init__.py              # Public API exports
├── README.md                # This file
│
├── core/                    # Fundamental abstractions
│   ├── __init__.py
│   ├── page.py              # Page & SlottedPage
│   ├── record.py            # Record serialization
│   ├── disk_manager.py      # Low-level disk I/O
│   └── interfaces.py        # Abstract base classes
│
├── data_files/              # Data file implementations
│   ├── __init__.py
│   ├── heap_file.py         # Unordered file
│   ├── hash_file.py         # Hash-organized file
│   └── page_directory.py    # Page tracking strategies
│
├── indexes/                 # Index implementations
│   ├── __init__.py
│   ├── btree.py             # B-Tree index
│   ├── hash_index.py        # Hash index
│   └── index_organized.py   # IOT (data inside index)
│
└── demos/                   # Interactive demonstrations
    ├── interactive_demo.py  # Full interactive demo
    ├── compare_data_files.py
    └── compare_indexes.py
```

## 🚀 Usage Examples

### Basic Usage

```python
from chapter01_storage import HeapFile, BTreeIndex

# Create a heap file
heap = HeapFile("data/users.heap", page_size=4096)

# Insert records (returns record location)
loc1 = heap.insert({"id": 1, "name": "Alice", "age": 30})
loc2 = heap.insert({"id": 2, "name": "Bob", "age": 25})

# Read by location (fast - direct access)
record = heap.read(loc1)

# Full scan (slow - reads all pages)
for record in heap.scan():
    print(record)

# Create a B-Tree index on the heap
index = BTreeIndex("data/users_pk.idx", heap, key_field="id")
index.build()  # Index existing records

# Now search is O(log n)
record = index.search(1)
```

### Hash File (O(1) Lookups)

```python
from chapter01_storage import HashFile

# Hash file with 64 buckets
hfile = HashFile("data/users.hash", num_buckets=64)

# Insert (hashes key to find bucket)
hfile.insert({"id": 1, "name": "Alice"})

# Search is O(1) amortized
record = hfile.search(key=1)

# But range queries require full scan!
# hfile.range_search(1, 100)  # This would be O(n)
```

### Index-Organized Table

```python
from chapter01_storage import IndexOrganizedTable

# Data lives INSIDE the B-Tree
iot = IndexOrganizedTable("data/users.iot", key_field="id")

# Insert maintains B-Tree order
iot.insert({"id": 1, "name": "Alice"})
iot.insert({"id": 2, "name": "Bob"})

# Search is O(log n) and returns data directly (no second lookup)
record = iot.search(1)

# Range queries are efficient
for record in iot.range_scan(1, 100):
    print(record)
```

### Comparing Clustered vs Unclustered

```python
from chapter01_storage import HeapFile, BTreeIndex

heap = HeapFile("data/users.heap")

# Primary index (typically clustered in real DBs)
pk_index = BTreeIndex("users_pk.idx", heap, key_field="id", clustered=True)

# Secondary index (always unclustered)
email_index = BTreeIndex("users_email.idx", heap, key_field="email", clustered=False)

# Search by email:
# 1. Find in email_index -> get record location (or primary key)
# 2. Fetch from heap (or primary index)
location = email_index.search("alice@example.com")
record = heap.read(location)
```

## 📊 Running Benchmarks

```bash
# Compare data file types
python -m chapter01_storage.demos.compare_data_files

# Compare index types
python -m chapter01_storage.demos.compare_indexes

# Interactive exploration
python -m chapter01_storage.demos.interactive_demo
```

## 🔍 Implementation Details

### Page Format (Slotted Page)

```
┌─────────────────────────────────────────────────────────┐
│ Page Header (fixed size)                                │
│ ┌─────────────────────────────────────────────────────┐ │
│ │ page_id | num_slots | free_space_ptr | flags       │ │
│ └─────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────┤
│ Slot Directory (grows downward →)                       │
│ ┌────────┬────────┬────────┬────────┐                  │
│ │ Slot 0 │ Slot 1 │ Slot 2 │  ...   │                  │
│ │ off,len│ off,len│ off,len│        │                  │
│ └────────┴────────┴────────┴────────┘                  │
├─────────────────────────────────────────────────────────┤
│                                                         │
│              Free Space                                 │
│                                                         │
├─────────────────────────────────────────────────────────┤
│ Records (grow upward ←)                                 │
│ ┌──────────────┬──────────────┬──────────────┐         │
│ │   Record 2   │   Record 1   │   Record 0   │         │
│ └──────────────┴──────────────┴──────────────┘         │
└─────────────────────────────────────────────────────────┘
```

### B-Tree Node Format

```
┌─────────────────────────────────────────────────────────┐
│ Node Header                                             │
│ ┌─────────────────────────────────────────────────────┐ │
│ │ is_leaf | num_keys | parent_ptr                     │ │
│ └─────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────┤
│ If Internal Node:                                       │
│ ┌────┬────┬────┬────┬────┬────┬────┐                   │
│ │ P0 │ K1 │ P1 │ K2 │ P2 │ K3 │ P3 │  (pointers=keys+1)│
│ └────┴────┴────┴────┴────┴────┴────┘                   │
├─────────────────────────────────────────────────────────┤
│ If Leaf Node:                                           │
│ ┌────────────┬────────────┬────────────┐               │
│ │ K1 → Value │ K2 → Value │ K3 → Value │               │
│ └────────────┴────────────┴────────────┘               │
│ │ prev_leaf │ next_leaf │  (for range scans)          │
└─────────────────────────────────────────────────────────┘
```

## 🧪 Key Learnings

1. **Everything is pages**: Disk I/O is in page-sized chunks
2. **Trade-offs everywhere**: Fast writes vs fast reads, space vs speed
3. **Indexes are separate files**: They map keys to locations
4. **Only 1 clustered index**: Data can only have one physical order
5. **Hash = O(1) but no ranges**: Perfect for point queries only
6. **B-Trees are universal**: Good all-around performance

## ➡️ Next Chapter

[Chapter 2: B-Trees & File Formats](../chapter02_btrees/) - Deep dive into B-Tree variants, page splits, and file format design.
