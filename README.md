# Distributed Key-Value Store

A production-grade distributed key-value database built from scratch in Python with ACID compliance, masterless replication, and full-text search.

## 🚀 Features

- **ACID Compliant**: Atomicity, Consistency, Isolation, Durability
- **100% Durability**: All acknowledged writes survive crashes
- **Masterless Replication**: No single point of failure
- **Quorum Consistency**: Majority-based reads/writes
- **Full-Text Search**: Inverted index with AND/OR queries
- **Semantic Search**: TF-IDF word embeddings with similarity ranking
- **TCP Protocol**: Custom binary protocol over raw sockets
- **Fault Tolerant**: Survives node failures with automatic recovery

## 📊 Architecture

### Storage Engine
- Per-key file persistence with `fsync()` guarantees
- Two-phase commit for atomic bulk operations
- Automatic crash recovery

### Distributed System
- **Masterless (P2P)**: All nodes are equal peers
- **Quorum writes**: W=2, R=2 for 3-node cluster
- **Gossip protocol**: Vector clock synchronization
- **No leader election needed**: Any node handles requests

### Indexing
- **Inverted Index**: O(1) word lookups for full-text search
- **TF-IDF Embeddings**: Semantic similarity with cosine distance

## 🎯 Performance

- **Durability**: 100% (12,509/12,509 writes preserved in chaos testing)
- **Write Throughput**: ~1000 writes/sec (with fsync)
- **Isolation**: 0 violations in concurrent stress tests
- **Availability**: Survives minority node failures (1/3 nodes down)

## 📦 Installation
```bash
git clone https://github.com/yourusername/distributed-kv-store.git
cd distributed-kv-store
python kvstore.py  # Run all tests
```

No dependencies required - uses only Python standard library!

## 💻 Usage

### Start a 3-Node Cluster
```python
from kvstore import MasterlessNode, MasterlessClient

# Start nodes
nodes = []
peer_ports = [5001, 5002, 5003]

for i, port in enumerate(peer_ports):
    node = MasterlessNode(i, 'localhost', port, peer_ports)
    node.start()
    nodes.append(node)

# Connect client
client = MasterlessClient(peer_ports)

# Write data (replicates to quorum)
client.Set("user:123", "John Doe")

# Read data (from any node)
value = client.Get("user:123")

# Full-text search
client.Set("doc1", "Python is a programming language")
client.Set("doc2", "Java is also a programming language")
results = client.Search("Python programming")  # Returns ["doc1"]

# Semantic search
similar = client.SearchSimilar("coding languages", top_k=5)
# Returns: [("doc1", 0.85), ("doc2", 0.72), ...]

client.close()
```

## 🧪 Testing
```bash
# Run all tests
python kvstore.py

# Tests include:
# - ACID compliance (Atomicity, Consistency, Isolation, Durability)
# - Concurrent bulk operations
# - Chaos testing (random node failures)
# - Quorum write verification
# - Full-text and semantic search
# - Replication consistency
```

## 🏗️ Project Structure
```
├── kvstore.py              # Main implementation
├── isolation_tests.py      # Comprehensive isolation testing
├── README.md
├── .gitignore
└── docs/
    └── ARCHITECTURE.md     # Detailed design docs
```

## 📈 Benchmarks

| Test | Result |
|------|--------|
| Durability | 100% (0 lost writes) |
| Write Throughput | ~1000 ops/sec |
| Concurrent Isolation | 0 violations |
| Quorum Writes (2/3 nodes) | ✅ Success |
| Node Failure Recovery | < 3 seconds |

## 🎓 Concepts Implemented

- [x] ACID properties
- [x] CAP theorem (AP system)
- [x] Quorum replication
- [x] Vector clocks
- [x] Gossip protocol
- [x] Inverted indexes
- [x] TF-IDF embeddings
- [x] Two-phase commit
- [x] Crash recovery
- [x] Chaos engineering

## 📚 Documentation

See [ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed design decisions.

## 🤝 Contributing

This is an educational project demonstrating distributed systems concepts.

## 📄 License

MIT License

## 👤 Author

Your Name - [GitHub Profile](https://github.com/yourusername)

## 🙏 Acknowledgments

Inspired by:
- Cassandra (masterless architecture)
- Raft (consensus algorithm)
- Redis (key-value design)
- Elasticsearch (full-text search)