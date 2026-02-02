# Architecture Documentation

## System Overview

This is a masterless distributed key-value store implementing:
- Quorum-based replication (W=2, R=2)
- Vector clocks for causality
- Inverted index for full-text search
- TF-IDF embeddings for semantic search

## Design Decisions

### Why Masterless?
- No single point of failure
- Better availability (any node accepts writes)
- Simpler than leader election
- Inspired by Cassandra and DynamoDB

### Why Per-Key Files?
- Atomic writes with `fsync()`
- No WAL complexity on Windows
- Simple recovery
- Trade-off: Performance for durability

### Why Quorum (W=2, R=2)?
- Guarantees consistency
- Tolerates 1 node failure
- Follows N/2 + 1 = majority rule

## Implementation Details

[Add your detailed explanations here]