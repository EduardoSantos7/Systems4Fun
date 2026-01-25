# Database Internals - Learning Repository

A hands-on implementation of database internal concepts, following the book "Database Internals" by Alex Petrov.

## 🎯 Purpose

This repository contains practical Python implementations of fundamental database concepts. Each chapter builds upon the previous one, creating a comprehensive understanding of how databases work under the hood.

## 📚 Chapters

| Chapter | Topic | Status |
|---------|-------|--------|
| [01](./chapter01_storage/) | Storage Engines & File Organization | ✅ Complete |
| 02 | B-Trees & File Formats | 🔜 Coming soon |
| 03 | Transaction Processing & Recovery | 🔜 Coming soon |
| 04 | Log-Structured Storage (LSM Trees) | 🔜 Coming soon |
| 05 | Failure Detection & Leader Election | 🔜 Coming soon |
| 06 | Replication & Consistency | 🔜 Coming soon |
| 07 | Anti-Entropy & Gossip Protocols | 🔜 Coming soon |

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      Application Layer                       │
├─────────────────────────────────────────────────────────────┤
│                        Index Layer                           │
│    ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐   │
│    │   B-Tree    │  │ Hash Index  │  │ Index-Organized │   │
│    └─────────────┘  └─────────────┘  └─────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│                     Data File Layer                          │
│    ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐   │
│    │  Heap File  │  │  Hash File  │  │       IOT       │   │
│    └─────────────┘  └─────────────┘  └─────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│                       Page Layer                             │
│         ┌─────────────────────────────────────┐             │
│         │   SlottedPage  |  PageDirectory     │             │
│         └─────────────────────────────────────┘             │
├─────────────────────────────────────────────────────────────┤
│                       Disk Layer                             │
│         ┌─────────────────────────────────────┐             │
│         │        Physical I/O Operations       │             │
│         └─────────────────────────────────────┘             │
└─────────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

```bash
# Clone the repository
git clone https://github.com/yourusername/Systems4Fun.git
cd Systems4Fun/learning/DataBaseInternals

# Run the interactive demo
python -m chapter01_storage.demos.interactive_demo

# Run specific comparisons
python -m chapter01_storage.demos.compare_data_files
python -m chapter01_storage.demos.compare_indexes
```

## 📖 Learning Path

### Chapter 1: Storage Engines

Learn the fundamental building blocks:

1. **Pages & Records** - The atomic units of storage
2. **Data File Organizations**:
   - Heap Files (unordered, fast inserts)
   - Hash Files (O(1) lookups by key)
   - Index-Organized Tables (data inside index)
3. **Index Structures**:
   - B-Tree (ordered, range queries)
   - Hash Index (exact match lookups)
4. **Clustered vs Unclustered** indexes

```python
from chapter01_storage import HeapFile, HashFile, BTreeIndex

# Create a heap file with a B-Tree index
heap = HeapFile("users.dat", page_size=4096)
index = BTreeIndex("users_pk.idx", heap, key_field="id")

# Insert records
index.insert({"id": 1, "name": "Alice", "email": "alice@example.com"})
index.insert({"id": 2, "name": "Bob", "email": "bob@example.com"})

# Query by primary key
record = index.search(1)  # O(log n) via B-Tree

# Range query
records = index.range_search(1, 100)  # Efficient with B-Tree
```

## 🔧 Requirements

- Python 3.8+
- No external dependencies (uses only standard library)

## 📊 Performance Comparisons

Each chapter includes benchmarks comparing different implementations:

```
Data File Comparison (10,000 records):
┌────────────────┬──────────┬──────────┬─────────────┐
│ Operation      │ HeapFile │ HashFile │ IOT         │
├────────────────┼──────────┼──────────┼─────────────┤
│ Insert         │ O(1)     │ O(1)     │ O(log n)    │
│ Search by Key  │ O(n)     │ O(1)*    │ O(log n)    │
│ Range Scan     │ O(n)     │ O(n)     │ O(log n + k)│
│ Full Scan      │ O(n)     │ O(n)     │ O(n)        │
└────────────────┴──────────┴──────────┴─────────────┘
* Amortized, assuming good hash distribution
```

## 🤝 Contributing

Feel free to open issues or PRs for:
- Bug fixes
- Additional implementations
- Better documentation
- Performance improvements

## 📚 References

- Petrov, A. (2019). *Database Internals*. O'Reilly Media.
- Garcia-Molina, H., Ullman, J. D., & Widom, J. (2008). *Database Systems: The Complete Book*.
- Ramakrishnan, R., & Gehrke, J. (2003). *Database Management Systems*.

## 📝 License

MIT License - Feel free to use this code for learning purposes.
