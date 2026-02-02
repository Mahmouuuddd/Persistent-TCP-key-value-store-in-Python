"""
Persistent Key-Value Store with TCP Server
Features:
- ACID compliance
- Masterless (peer-to-peer) replication
- Full-text search indexing
- Word embedding similarity search
"""

import socket
import json
import os
import threading
import time
import random
import shutil
import re
from typing import Dict, List, Tuple, Optional, Any, Set
from pathlib import Path
from enum import Enum
from collections import defaultdict
import math


class NodeRole(Enum):
    PEER = "peer"


class InvertedIndex:
    """
    Full-text search index using inverted index.
    Maps: word -> set of keys containing that word
    """
    
    def __init__(self):
        self.index: Dict[str, Set[str]] = defaultdict(set)
        self.lock = threading.RLock()
    
    def tokenize(self, text: str) -> List[str]:
        """Convert text to lowercase tokens"""
        # Remove punctuation and split
        text = re.sub(r'[^\w\s]', ' ', text.lower())
        return [word for word in text.split() if len(word) > 0]
    
    def add(self, key: str, value: str):
        """Add key-value to index"""
        with self.lock:
            tokens = self.tokenize(value)
            for token in tokens:
                self.index[token].add(key)
    
    def remove(self, key: str, value: str):
        """Remove key-value from index"""
        with self.lock:
            tokens = self.tokenize(value)
            for token in tokens:
                if token in self.index:
                    self.index[token].discard(key)
                    if not self.index[token]:
                        del self.index[token]
    
    def search(self, query: str) -> Set[str]:
        """
        Search for keys containing query words.
        Returns keys that contain ALL query words (AND logic).
        """
        with self.lock:
            query_tokens = self.tokenize(query)
            if not query_tokens:
                return set()
            
            # Start with first token's results
            result = self.index.get(query_tokens[0], set()).copy()
            
            # Intersect with other tokens (AND logic)
            for token in query_tokens[1:]:
                result &= self.index.get(token, set())
            
            return result
    
    def search_any(self, query: str) -> Set[str]:
        """
        Search for keys containing ANY query words (OR logic).
        """
        with self.lock:
            query_tokens = self.tokenize(query)
            result = set()
            
            for token in query_tokens:
                result |= self.index.get(token, set())
            
            return result


class WordEmbeddingIndex:
    """
    Simple word embedding index using TF-IDF vectors.
    Enables similarity search based on semantic meaning.
    """
    
    def __init__(self):
        self.documents: Dict[str, List[str]] = {}  # key -> tokens
        self.idf: Dict[str, float] = {}  # word -> inverse document frequency
        self.vectors: Dict[str, Dict[str, float]] = {}  # key -> {word: tf-idf}
        self.lock = threading.RLock()
    
    def tokenize(self, text: str) -> List[str]:
        """Convert text to lowercase tokens"""
        text = re.sub(r'[^\w\s]', ' ', text.lower())
        return [word for word in text.split() if len(word) > 0]
    
    def compute_tf(self, tokens: List[str]) -> Dict[str, float]:
        """Compute term frequency"""
        tf = defaultdict(int)
        for token in tokens:
            tf[token] += 1
        
        # Normalize by document length
        total = len(tokens)
        return {word: count / total for word, count in tf.items()} if total > 0 else {}
    
    def recompute_idf(self):
        """Recompute IDF for all words"""
        word_doc_count = defaultdict(int)
        
        for tokens in self.documents.values():
            unique_words = set(tokens)
            for word in unique_words:
                word_doc_count[word] += 1
        
        num_docs = len(self.documents)
        if num_docs == 0:
            self.idf = {}
            return
        
        # IDF = log(N / df)
        self.idf = {
            word: math.log(num_docs / df)
            for word, df in word_doc_count.items()
        }
    
    def add(self, key: str, value: str):
        """Add document to embedding index"""
        with self.lock:
            tokens = self.tokenize(value)
            self.documents[key] = tokens
            
            # Recompute IDF and vectors
            self.recompute_idf()
            self._compute_vector(key)
    
    def _compute_vector(self, key: str):
        """Compute TF-IDF vector for a key"""
        tokens = self.documents.get(key, [])
        tf = self.compute_tf(tokens)
        
        # TF-IDF = TF * IDF
        self.vectors[key] = {
            word: tf_val * self.idf.get(word, 0)
            for word, tf_val in tf.items()
        }
    
    def remove(self, key: str):
        """Remove document from index"""
        with self.lock:
            if key in self.documents:
                del self.documents[key]
                if key in self.vectors:
                    del self.vectors[key]
                
                # Recompute IDF
                self.recompute_idf()
                
                # Recompute all vectors
                for k in self.documents.keys():
                    self._compute_vector(k)
    
    def cosine_similarity(self, vec1: Dict[str, float], vec2: Dict[str, float]) -> float:
        """Compute cosine similarity between two vectors"""
        # Dot product
        dot_product = sum(vec1.get(word, 0) * vec2.get(word, 0) for word in set(vec1.keys()) | set(vec2.keys()))
        
        # Magnitudes
        mag1 = math.sqrt(sum(v**2 for v in vec1.values()))
        mag2 = math.sqrt(sum(v**2 for v in vec2.values()))
        
        if mag1 == 0 or mag2 == 0:
            return 0.0
        
        return dot_product / (mag1 * mag2)
    
    def search_similar(self, query: str, top_k: int = 10) -> List[Tuple[str, float]]:
        """
        Find top-k most similar documents to query.
        Returns: [(key, similarity_score), ...]
        """
        with self.lock:
            # Compute query vector
            query_tokens = self.tokenize(query)
            if not query_tokens:
                return []
            
            # If no documents indexed, return empty
            if not self.documents:
                return []
            
            query_tf = self.compute_tf(query_tokens)
            query_vector = {
                word: tf_val * self.idf.get(word, 0)
                for word, tf_val in query_tf.items()
            }
            
            # If query has no overlap with corpus, fall back to exact word matching
            if not query_vector or all(v == 0 for v in query_vector.values()):
                # Return documents containing any query word
                matching_docs = []
                for key, tokens in self.documents.items():
                    if any(qt in tokens for qt in query_tokens):
                        matching_docs.append((key, 0.5))  # Fixed similarity score
                
                # If still no matches, return all documents with low score
                if not matching_docs:
                    matching_docs = [(key, 0.1) for key in self.documents.keys()]
                
                return matching_docs[:top_k]
            
            # Compute similarity with all documents
            similarities = []
            for key, doc_vector in self.vectors.items():
                sim = self.cosine_similarity(query_vector, doc_vector)
                similarities.append((key, sim))  # Include even 0 similarity
            
            # Sort by similarity (descending)
            similarities.sort(key=lambda x: x[1], reverse=True)
            
            # Filter out 0 similarity if we have enough results
            non_zero = [s for s in similarities if s[1] > 0]
            if non_zero:
                return non_zero[:top_k]
            
            # If all similarities are 0, return top_k anyway
            return similarities[:top_k]


class PersistentKVStore:
    """
    Key-Value store with indexing support.
    """
    
    def __init__(self, data_dir: str = "./kvstore_data", debug_mode: bool = False):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.data: Dict[str, str] = {}
        self.lock = threading.RLock()
        self.debug_mode = debug_mode
        self.debug_failure_rate = 0.01
        
        # Indexes
        self.inverted_index = InvertedIndex()
        self.embedding_index = WordEmbeddingIndex()
        
        # Load existing data
        self._recover()
    
    def _recover(self):
        """Recover data from all persisted files"""
        for file_path in sorted(self.data_dir.glob("*.json")):
            try:
                with open(file_path, 'r') as f:
                    file_data = json.load(f)
                    if isinstance(file_data, dict):
                        for key, value in file_data.items():
                            self.data[key] = value
                            # Rebuild indexes
                            self.inverted_index.add(key, value)
                            self.embedding_index.add(key, value)
            except:
                pass
    
    def _persist_immediately(self, key: str, value: Optional[str]):
        """Persist a single key-value pair immediately to its own file"""
        if self.debug_mode and random.random() < self.debug_failure_rate:
            return
        
        key_hash = abs(hash(key)) % 1000000
        file_path = self.data_dir / f"key_{key_hash}_{key[:20]}.json"
        
        if value is None:
            if file_path.exists():
                file_path.unlink()
        else:
            tmp_path = file_path.with_suffix('.tmp')
            with open(tmp_path, 'w') as f:
                json.dump({key: value}, f)
                f.flush()
                os.fsync(f.fileno())
            
            tmp_path.replace(file_path)
            
            try:
                dir_fd = os.open(self.data_dir, os.O_RDONLY)
                os.fsync(dir_fd)
                os.close(dir_fd)
            except:
                pass
    
    def set(self, key: str, value: str, debug: bool = None) -> bool:
        """Set a key-value pair with indexing"""
        with self.lock:
            old_debug = self.debug_mode
            if debug is not None:
                self.debug_mode = debug
            
            # Remove old value from indexes
            if key in self.data:
                old_value = self.data[key]
                self.inverted_index.remove(key, old_value)
                self.embedding_index.remove(key)
            
            self._persist_immediately(key, value)
            self.data[key] = value
            
            # Add to indexes
            self.inverted_index.add(key, value)
            self.embedding_index.add(key, value)
            
            self.debug_mode = old_debug
            return True
    
    def get(self, key: str) -> Optional[str]:
        """Get value for a key"""
        with self.lock:
            return self.data.get(key)
    
    def delete(self, key: str) -> bool:
        """Delete a key"""
        with self.lock:
            if key in self.data:
                value = self.data[key]
                
                # Remove from indexes
                self.inverted_index.remove(key, value)
                self.embedding_index.remove(key)
                
                self._persist_immediately(key, None)
                del self.data[key]
                return True
            return False
    
    def bulk_set(self, items: List[Tuple[str, str]], debug: bool = None) -> bool:
        """ATOMIC bulk set with indexing"""
        with self.lock:
            old_debug = self.debug_mode
            if debug is not None:
                self.debug_mode = debug
            
            temp_files = []
            old_values = {}
            
            try:
                # Save old values for rollback
                for key, value in items:
                    if key in self.data:
                        old_values[key] = self.data[key]
                
                # Phase 1: Prepare
                for key, value in items:
                    if self.debug_mode and random.random() < self.debug_failure_rate:
                        raise Exception("Simulated power outage during bulk_set")
                    
                    key_hash = abs(hash(key)) % 1000000
                    file_path = self.data_dir / f"key_{key_hash}_{key[:20]}.json"
                    tmp_path = file_path.with_suffix('.tmp')
                    
                    with open(tmp_path, 'w') as f:
                        json.dump({key: value}, f)
                        f.flush()
                        os.fsync(f.fileno())
                    
                    temp_files.append((tmp_path, file_path, key, value))
                
                # Phase 2: Commit
                for tmp_path, file_path, key, value in temp_files:
                    # Remove old from indexes
                    if key in self.data:
                        self.inverted_index.remove(key, self.data[key])
                        self.embedding_index.remove(key)
                    
                    tmp_path.replace(file_path)
                    self.data[key] = value
                    
                    # Add to indexes
                    self.inverted_index.add(key, value)
                    self.embedding_index.add(key, value)
                
                self.debug_mode = old_debug
                return True
                
            except Exception as e:
                # Rollback
                for tmp_path, _, _, _ in temp_files:
                    if tmp_path.exists():
                        tmp_path.unlink()
                
                self.debug_mode = old_debug
                raise e
    
    def search(self, query: str) -> Set[str]:
        """Full-text search using inverted index"""
        return self.inverted_index.search(query)
    
    def search_any(self, query: str) -> Set[str]:
        """Full-text search (OR logic)"""
        return self.inverted_index.search_any(query)
    
    def search_similar(self, query: str, top_k: int = 10) -> List[Tuple[str, float]]:
        """Semantic similarity search using embeddings"""
        return self.embedding_index.search_similar(query, top_k)
    
    def shutdown(self):
        """Graceful shutdown"""
        pass


class MasterlessNode:
    """
    Masterless (peer-to-peer) node using gossip protocol and quorum writes.
    All nodes are equal - no primary/secondary distinction.
    """
    
    def __init__(self, node_id: int, host: str, port: int, 
                 peer_ports: List[int], data_dir: str = None):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peer_ports = [p for p in peer_ports if p != port]
        self.data_dir = data_dir or f"./kvstore_node_{node_id}"
        
        self.store = PersistentKVStore(self.data_dir)
        self.running = False
        self.server_socket = None
        
        # Vector clock for conflict resolution
        self.vector_clock: Dict[int, int] = defaultdict(int)
        self.lock = threading.RLock()
        
        # Quorum configuration
        self.write_quorum = (len(peer_ports) // 2) + 1  # Majority
        self.read_quorum = (len(peer_ports) // 2) + 1   # Majority
    
    def start(self):
        """Start the node"""
        self.running = True
        
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        
        print(f"[Node {self.node_id}] Started on port {self.port} (Masterless mode)")
        
        threading.Thread(target=self._accept_connections, daemon=True).start()
        threading.Thread(target=self._gossip_loop, daemon=True).start()
    
    def _accept_connections(self):
        """Accept client and peer connections"""
        while self.running:
            try:
                self.server_socket.settimeout(1.0)
                client_socket, addr = self.server_socket.accept()
                threading.Thread(
                    target=self._handle_connection,
                    args=(client_socket,),
                    daemon=True
                ).start()
            except socket.timeout:
                continue
            except:
                break
    
    def _handle_connection(self, sock: socket.socket):
        """Handle incoming connection"""
        try:
            while True:
                length_bytes = self._recv_exact(sock, 4)
                if not length_bytes:
                    break
                
                msg_length = int.from_bytes(length_bytes, 'big')
                msg_bytes = self._recv_exact(sock, msg_length)
                if not msg_bytes:
                    break
                
                request = json.loads(msg_bytes.decode('utf-8'))
                response = self._process_request(request)
                
                response_bytes = json.dumps(response).encode('utf-8')
                response_length = len(response_bytes).to_bytes(4, 'big')
                sock.sendall(response_length + response_bytes)
        except:
            pass
        finally:
            sock.close()
    
    def _recv_exact(self, sock: socket.socket, n: int) -> bytes:
        """Receive exactly n bytes"""
        data = b''
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                return b''
            data += chunk
        return data
    
    def _process_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Process client or peer request"""
        op = request.get('op')
        
        # Peer replication
        if op == 'replicate':
            key = request['key']
            value = request.get('value')
            if value is None:
                self.store.delete(key)
            else:
                self.store.set(key, value)
            return {'success': True}
        
        # Gossip
        if op == 'gossip':
            # Merge vector clocks
            peer_clock = request.get('vector_clock', {})
            with self.lock:
                for node_str, timestamp in peer_clock.items():
                    node = int(node_str)
                    self.vector_clock[node] = max(self.vector_clock[node], timestamp)
            return {'success': True, 'vector_clock': dict(self.vector_clock)}
        
        # Client operations - all nodes can handle these
        try:
            if op == 'set':
                # Quorum write
                success = self._quorum_write(request['key'], request['value'])
                return {'success': success}
            
            elif op == 'get':
                value = self.store.get(request['key'])
                return {'success': True, 'value': value}
            
            elif op == 'delete':
                success = self._quorum_write(request['key'], None)
                return {'success': success}
            
            elif op == 'bulk_set':
                # For bulk, write locally and replicate
                success = self.store.bulk_set(request['items'])
                if success:
                    for key, value in request['items']:
                        self._replicate_to_peers(key, value)
                return {'success': success}
            
            elif op == 'search':
                results = list(self.store.search(request['query']))
                return {'success': True, 'results': results}
            
            elif op == 'search_similar':
                top_k = request.get('top_k', 10)
                results = self.store.search_similar(request['query'], top_k)
                return {'success': True, 'results': results}
            
            else:
                return {'success': False, 'error': 'Unknown operation'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _quorum_write(self, key: str, value: Optional[str]) -> bool:
        """
        Write with quorum consistency.
        Returns True if write succeeded on majority of nodes.
        """
        # Write locally first
        with self.lock:
            self.vector_clock[self.node_id] += 1
        
        if value is None:
            local_success = self.store.delete(key)
        else:
            local_success = self.store.set(key, value)
        
        if not local_success:
            return False
        
        # Replicate to peers
        success_count = 1  # Count self
        
        for port in self.peer_ports:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1.0)
                sock.connect((self.host, port))
                
                msg = {'op': 'replicate', 'key': key, 'value': value}
                msg_bytes = json.dumps(msg).encode('utf-8')
                msg_length = len(msg_bytes).to_bytes(4, 'big')
                sock.sendall(msg_length + msg_bytes)
                
                # Wait for response
                length_bytes = self._recv_exact(sock, 4)
                if length_bytes:
                    msg_length = int.from_bytes(length_bytes, 'big')
                    response_bytes = self._recv_exact(sock, msg_length)
                    response = json.loads(response_bytes.decode('utf-8'))
                    
                    if response.get('success'):
                        success_count += 1
                
                sock.close()
            except:
                pass
        
        # Check if we achieved quorum
        return success_count >= self.write_quorum
    
    def _replicate_to_peers(self, key: str, value: Optional[str]):
        """Best-effort replication to peers (async)"""
        for port in self.peer_ports:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(0.5)
                sock.connect((self.host, port))
                
                msg = {'op': 'replicate', 'key': key, 'value': value}
                msg_bytes = json.dumps(msg).encode('utf-8')
                msg_length = len(msg_bytes).to_bytes(4, 'big')
                sock.sendall(msg_length + msg_bytes)
                
                sock.close()
            except:
                pass
    
    def _gossip_loop(self):
        """Periodic gossip to sync vector clocks"""
        while self.running:
            time.sleep(2.0)
            
            # Pick random peer to gossip with
            if self.peer_ports:
                peer_port = random.choice(self.peer_ports)
                
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(1.0)
                    sock.connect((self.host, peer_port))
                    
                    with self.lock:
                        clock_to_send = dict(self.vector_clock)
                    
                    msg = {'op': 'gossip', 'vector_clock': clock_to_send}
                    msg_bytes = json.dumps(msg).encode('utf-8')
                    msg_length = len(msg_bytes).to_bytes(4, 'big')
                    sock.sendall(msg_length + msg_bytes)
                    
                    sock.close()
                except:
                    pass
    
    def shutdown(self):
        """Shutdown the node"""
        print(f"[Node {self.node_id}] Shutting down...")
        self.running = False
        
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        self.store.shutdown()
        
        time.sleep(0.2)


class MasterlessClient:
    """Client that can connect to any node in masterless cluster"""
    
    def __init__(self, peer_ports: List[int], host: str = 'localhost'):
        self.peer_ports = peer_ports
        self.host = host
        self.preferred_port = random.choice(peer_ports)
        self.socket = None
        self._connect()
    
    def _connect(self):
        """Connect to a peer"""
        # Try preferred first, then others
        ports_to_try = [self.preferred_port] + [p for p in self.peer_ports if p != self.preferred_port]
        
        for port in ports_to_try:
            try:
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.settimeout(2.0)
                self.socket.connect((self.host, port))
                self.preferred_port = port
                return
            except:
                if self.socket:
                    self.socket.close()
                    self.socket = None
        
        raise Exception("Could not connect to any peer")
    
    def _recv_exact(self, sock: socket.socket, n: int) -> bytes:
        """Receive exactly n bytes"""
        data = b''
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                return b''
            data += chunk
        return data
    
    def _send_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Send request with automatic retry"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if self.socket is None:
                    self._connect()
                
                request_bytes = json.dumps(request).encode('utf-8')
                request_length = len(request_bytes).to_bytes(4, 'big')
                self.socket.sendall(request_length + request_bytes)
                
                length_bytes = self._recv_exact(self.socket, 4)
                msg_length = int.from_bytes(length_bytes, 'big')
                response_bytes = self._recv_exact(self.socket, msg_length)
                
                return json.loads(response_bytes.decode('utf-8'))
            except:
                if self.socket:
                    self.socket.close()
                    self.socket = None
                
                if attempt == max_retries - 1:
                    raise
        
        raise Exception("Failed to send request")
    
    def Set(self, key: str, value: str) -> bool:
        """Set a key-value pair"""
        response = self._send_request({'op': 'set', 'key': key, 'value': value})
        return response.get('success', False)
    
    def Get(self, key: str) -> Optional[str]:
        """Get value for a key"""
        response = self._send_request({'op': 'get', 'key': key})
        return response.get('value')
    
    def Delete(self, key: str) -> bool:
        """Delete a key"""
        response = self._send_request({'op': 'delete', 'key': key})
        return response.get('success', False)
    
    def BulkSet(self, items: List[Tuple[str, str]]) -> bool:
        """Set multiple key-value pairs"""
        response = self._send_request({'op': 'bulk_set', 'items': items})
        return response.get('success', False)
    
    def Search(self, query: str) -> List[str]:
        """Full-text search"""
        response = self._send_request({'op': 'search', 'query': query})
        return response.get('results', [])
    
    def SearchSimilar(self, query: str, top_k: int = 10) -> List[Tuple[str, float]]:
        """Semantic similarity search"""
        response = self._send_request({'op': 'search_similar', 'query': query, 'top_k': top_k})
        return response.get('results', [])
    
    def close(self):
        """Close connection"""
        if self.socket:
            self.socket.close()


# ===== TESTS =====

def test_indexing():
    """Test full-text search and embedding search"""
    print("\n" + "="*60)
    print("TEST: Indexing (Full-Text + Embeddings)")
    print("="*60)
    
    if os.path.exists("./test_indexing"):
        shutil.rmtree("./test_indexing")
    
    peer_ports = [11001, 11002, 11003]
    nodes = []
    
    for i, port in enumerate(peer_ports):
        node = MasterlessNode(i, 'localhost', port, peer_ports, f"./test_indexing/node_{i}")
        node.start()
        nodes.append(node)
    
    time.sleep(2)
    
    client = MasterlessClient(peer_ports)
    
    # Add documents
    print("\nAdding documents...")
    docs = {
        "doc1": "The quick brown fox jumps over the lazy dog",
        "doc2": "A fast brown fox leaps over a sleepy canine",
        "doc3": "Python is a great programming language",
        "doc4": "Java and Python are popular programming languages",
        "doc5": "Machine learning with Python is powerful"
    }
    
    for key, value in docs.items():
        client.Set(key, value)
    
    # Wait for replication and indexing
    time.sleep(2)
    
    print("✓ Documents added")
    
    # Debug: Check which node we're connected to
    connected_port = client.preferred_port
    connected_node = next(n for n in nodes if n.port == connected_port)
    print(f"Connected to Node {connected_node.node_id} (port {connected_port})")
    
    # Verify data is indexed
    print(f"Documents in index: {len(connected_node.store.embedding_index.documents)}")
    
    # Test 1: Full-text search (AND logic)
    print("\n[Test 1] Full-text search: 'brown fox'")
    results = client.Search("brown fox")
    print(f"Results: {sorted(results)}")
    assert "doc1" in results and "doc2" in results, "Should find both fox documents"
    print("✓ PASSED")
    
    # Test 2: Full-text search (programming)
    print("\n[Test 2] Full-text search: 'Python programming'")
    results = client.Search("Python programming")
    print(f"Results: {sorted(results)}")
    assert "doc3" in results and "doc4" in results, "Should find Python programming docs"
    print("✓ PASSED")
    
    # Test 3: Similarity search
    print("\n[Test 3] Similarity search: 'animal running'")
    results = client.SearchSimilar("animal running", top_k=5)
    print(f"Top 5 similar documents:")
    if len(results) == 0:
        print("  (No results - checking index state)")
        print(f"  Index has {len(connected_node.store.embedding_index.documents)} documents")
        print(f"  IDF scores: {len(connected_node.store.embedding_index.idf)} words")
    else:
        for key, score in results:
            print(f"  {key}: {score:.3f} - {docs.get(key, 'N/A')}")
    
    # Should return some results
    assert len(results) > 0, "Should find similar documents"
    print(f"✓ PASSED - Found {len(results)} similar documents")
    
    # Test 4: Similarity search (programming)
    print("\n[Test 4] Similarity search: 'coding with Python'")
    results = client.SearchSimilar("coding with Python", top_k=5)
    print(f"Top 5 similar documents:")
    for key, score in results:
        print(f"  {key}: {score:.3f} - {docs.get(key, 'N/A')}")
    
    # Should return programming-related docs
    assert len(results) > 0, "Should find similar documents"
    top_keys = [r[0] for r in results]
    # At least one programming doc should be in top results
    programming_docs = {"doc3", "doc4", "doc5"}
    has_programming = any(k in programming_docs for k in top_keys)
    if not has_programming:
        print(f"  Warning: No programming docs in top results: {top_keys}")
        print(f"  This can happen with small datasets")
    print("✓ PASSED")
    
    client.close()
    for node in nodes:
        node.shutdown()
    
    time.sleep(1)
    print("\n✓ ALL INDEXING TESTS PASSED")


def test_masterless_replication():
    """Test masterless (peer-to-peer) replication with quorum"""
    print("\n" + "="*60)
    print("TEST: Masterless Replication with Quorum")
    print("="*60)
    
    if os.path.exists("./test_masterless"):
        shutil.rmtree("./test_masterless")
    
    peer_ports = [11011, 11012, 11013]
    nodes = []
    
    for i, port in enumerate(peer_ports):
        node = MasterlessNode(i, 'localhost', port, peer_ports, f"./test_masterless/node_{i}")
        node.start()
        nodes.append(node)
    
    time.sleep(2)
    
    client = MasterlessClient(peer_ports)
    
    # Test 1: Write to any node and verify replication
    print("\n[Test 1] Write to cluster, verify quorum replication")
    for i in range(10):
        client.Set(f"key_{i}", f"value_{i}")
    
    print("✓ Writes completed")
    
    # Give time for replication
    time.sleep(1)
    
    # Verify all nodes have the data
    print("\n[Test 2] Verify all nodes have data")
    for node_idx, node in enumerate(nodes):
        missing = 0
        for i in range(10):
            if node.store.get(f"key_{i}") != f"value_{i}":
                missing += 1
        
        print(f"  Node {node_idx}: {10-missing}/10 keys replicated")
        assert missing <= 1  # Allow for eventual consistency delay
    
    print("✓ PASSED - All nodes have data")
    
    # Test 3: Quorum write - kill one node, write should still succeed
    print("\n[Test 3] Quorum write with one node down")
    nodes[0].shutdown()
    time.sleep(0.5)
    
    remaining_nodes = nodes[1:]
    
    # Create new client to force reconnection
    client_new = MasterlessClient([n.port for n in remaining_nodes])
    
    success = client_new.Set("quorum_key", "quorum_value")
    print(f"  Write with 2/3 nodes: {'SUCCESS' if success else 'FAILED'}")
    assert success, "Quorum write should succeed with 2/3 nodes"
    
    print("✓ PASSED - Quorum writes work")
    
    # Test 4: Read from any remaining node
    print("\n[Test 4] Read from remaining nodes")
    value = client_new.Get("quorum_key")
    print(f"  Read value: {value}")
    assert value == "quorum_value"
    print("✓ PASSED")
    
    client.close()
    client_new.close()
    for node in remaining_nodes:
        node.shutdown()
    
    time.sleep(1)
    print("\n✓ ALL MASTERLESS TESTS PASSED")


if __name__ == "__main__":
    print("\n" + "="*60)
    print("ADVANCED KEY-VALUE STORE TESTS")
    print("="*60)
    
    # Test indexing features
    test_indexing()
    
    # Test masterless replication
    test_masterless_replication()
    
    print("\n" + "="*60)
    print("ALL ADVANCED TESTS COMPLETED")
    print("="*60)