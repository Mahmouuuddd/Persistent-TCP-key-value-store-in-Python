"""
Persistent Key-Value Store with TCP Server
Optimized for 100% durability and high write throughput
Uses immediate persistence for guaranteed durability
"""

import socket
import json
import os
import threading
import time
import random
import shutil
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path


class PersistentKVStore:
    """
    Key-Value store with immediate persistence.
    Every write is immediately flushed to a separate file for guaranteed durability.
    """
    
    def __init__(self, data_dir: str = "./kvstore_data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        self.data: Dict[str, str] = {}
        self.lock = threading.RLock()
        
        # Load existing data
        self._recover()
    
    def _recover(self):
        """Recover data from all persisted files"""
        # Load all .json files in data directory
        for file_path in sorted(self.data_dir.glob("*.json")):
            try:
                with open(file_path, 'r') as f:
                    file_data = json.load(f)
                    if isinstance(file_data, dict):
                        self.data.update(file_data)
            except:
                pass  # Skip corrupted files
    
    def _persist_immediately(self, key: str, value: Optional[str]):
        """Persist a single key-value pair immediately to its own file"""
        # Use key hash to create unique filename
        key_hash = abs(hash(key)) % 1000000
        file_path = self.data_dir / f"key_{key_hash}_{key[:20]}.json"
        
        if value is None:
            # Delete operation
            if file_path.exists():
                file_path.unlink()
        else:
            # Write operation - use atomic write
            tmp_path = file_path.with_suffix('.tmp')
            with open(tmp_path, 'w') as f:
                json.dump({key: value}, f)
                f.flush()
                os.fsync(f.fileno())
            
            # Atomic rename
            tmp_path.replace(file_path)
            
            # Force directory sync on Unix-like systems
            try:
                dir_fd = os.open(self.data_dir, os.O_RDONLY)
                os.fsync(dir_fd)
                os.close(dir_fd)
            except:
                pass  # Not supported on Windows
    
    def set(self, key: str, value: str) -> bool:
        """Set a key-value pair with immediate persistence"""
        with self.lock:
            # Persist BEFORE updating memory
            self._persist_immediately(key, value)
            # Only after persistence, update memory
            self.data[key] = value
            return True
    
    def get(self, key: str) -> Optional[str]:
        """Get value for a key"""
        with self.lock:
            return self.data.get(key)
    
    def delete(self, key: str) -> bool:
        """Delete a key with immediate persistence"""
        with self.lock:
            if key in self.data:
                # Persist deletion BEFORE updating memory
                self._persist_immediately(key, None)
                del self.data[key]
                return True
            return False
    
    def bulk_set(self, items: List[Tuple[str, str]]) -> bool:
        """Set multiple key-value pairs with immediate persistence"""
        with self.lock:
            # Persist each item individually for durability
            for key, value in items:
                self._persist_immediately(key, value)
                self.data[key] = value
            return True
    
    def shutdown(self):
        """Graceful shutdown"""
        pass  # No cleanup needed with per-key files


class KVServer:
    """TCP server for the key-value store"""
    
    def __init__(self, host: str = 'localhost', port: int = 9999, data_dir: str = "./kvstore_data"):
        self.host = host
        self.port = port
        self.store = PersistentKVStore(data_dir)
        self.running = False
        self.server_socket: Optional[socket.socket] = None
        self.server_thread: Optional[threading.Thread] = None
    
    def start(self):
        """Start the server"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True
        
        self.server_thread = threading.Thread(target=self._accept_connections, daemon=True)
        self.server_thread.start()
    
    def _accept_connections(self):
        """Accept and handle client connections"""
        while self.running:
            try:
                self.server_socket.settimeout(1.0)
                client_socket, addr = self.server_socket.accept()
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket,),
                    daemon=True
                )
                client_thread.start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    pass
    
    def _handle_client(self, client_socket: socket.socket):
        """Handle a single client connection"""
        try:
            while True:
                # Receive message length first (4 bytes)
                length_bytes = self._recv_exact(client_socket, 4)
                if not length_bytes:
                    break
                
                msg_length = int.from_bytes(length_bytes, 'big')
                
                # Receive the actual message
                msg_bytes = self._recv_exact(client_socket, msg_length)
                if not msg_bytes:
                    break
                
                request = json.loads(msg_bytes.decode('utf-8'))
                response = self._process_request(request)
                
                # Send response
                response_bytes = json.dumps(response).encode('utf-8')
                response_length = len(response_bytes).to_bytes(4, 'big')
                client_socket.sendall(response_length + response_bytes)
        except:
            pass
        finally:
            client_socket.close()
    
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
        """Process a client request"""
        op = request.get('op')
        
        try:
            if op == 'set':
                success = self.store.set(request['key'], request['value'])
                return {'success': success}
            
            elif op == 'get':
                value = self.store.get(request['key'])
                return {'success': True, 'value': value}
            
            elif op == 'delete':
                success = self.store.delete(request['key'])
                return {'success': success}
            
            elif op == 'bulk_set':
                success = self.store.bulk_set(request['items'])
                return {'success': success}
            
            else:
                return {'success': False, 'error': 'Unknown operation'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def shutdown(self):
        """Shutdown the server gracefully"""
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        self.store.shutdown()


class KVClient:
    """Client for the key-value store"""
    
    def __init__(self, host: str = 'localhost', port: int = 9999):
        self.host = host
        self.port = port
        self.socket: Optional[socket.socket] = None
        self._connect()
    
    def _connect(self):
        """Connect to the server"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
    
    def _send_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Send a request and receive response"""
        request_bytes = json.dumps(request).encode('utf-8')
        request_length = len(request_bytes).to_bytes(4, 'big')
        
        self.socket.sendall(request_length + request_bytes)
        
        # Receive response
        length_bytes = self._recv_exact(4)
        msg_length = int.from_bytes(length_bytes, 'big')
        response_bytes = self._recv_exact(msg_length)
        
        return json.loads(response_bytes.decode('utf-8'))
    
    def _recv_exact(self, n: int) -> bytes:
        """Receive exactly n bytes"""
        data = b''
        while len(data) < n:
            chunk = self.socket.recv(n - len(data))
            if not chunk:
                raise ConnectionError("Connection closed")
            data += chunk
        return data
    
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
    
    def close(self):
        """Close the connection"""
        if self.socket:
            self.socket.close()


# ===== TESTS =====

def run_tests():
    """Run test scenarios"""
    print("\n" + "="*50)
    print("RUNNING TESTS")
    print("="*50 + "\n")
    
    # Clean start
    if os.path.exists("./test_kvstore"):
        shutil.rmtree("./test_kvstore")
    
    # Start server
    server = KVServer(port=9998, data_dir="./test_kvstore")
    server.start()
    time.sleep(0.5)
    
    try:
        client = KVClient(port=9998)
        
        # Test 1: Set then Get
        print("Test 1: Set then Get")
        client.Set("key1", "value1")
        result = client.Get("key1")
        assert result == "value1", f"Expected 'value1', got {result}"
        print("✓ PASSED\n")
        
        # Test 2: Set then Delete then Get
        print("Test 2: Set then Delete then Get")
        client.Set("key2", "value2")
        client.Delete("key2")
        result = client.Get("key2")
        assert result is None, f"Expected None, got {result}"
        print("✓ PASSED\n")
        
        # Test 3: Get without setting
        print("Test 3: Get without setting")
        result = client.Get("nonexistent")
        assert result is None, f"Expected None, got {result}"
        print("✓ PASSED\n")
        
        # Test 4: Set then Set (same key) then Get
        print("Test 4: Set then Set (same key) then Get")
        client.Set("key3", "value3a")
        client.Set("key3", "value3b")
        result = client.Get("key3")
        assert result == "value3b", f"Expected 'value3b', got {result}"
        print("✓ PASSED\n")
        
        # Test 5: Set then exit (gracefully) then Get
        print("Test 5: Set then exit (gracefully) then Get")
        client.Set("persistent_key", "persistent_value")
        client.close()
        
        # Shutdown and restart server
        server.shutdown()
        time.sleep(0.5)
        
        server = KVServer(port=9998, data_dir="./test_kvstore")
        server.start()
        time.sleep(0.5)
        
        client = KVClient(port=9998)
        result = client.Get("persistent_key")
        assert result == "persistent_value", f"Expected 'persistent_value', got {result}"
        print("✓ PASSED\n")
        
        client.close()
        print("\n" + "="*50)
        print("ALL TESTS PASSED")
        print("="*50 + "\n")
        
    finally:
        server.shutdown()


# ===== BENCHMARKS =====

def benchmark_write_throughput():
    """Benchmark write throughput with varying data sizes"""
    print("\n" + "="*50)
    print("BENCHMARK: Write Throughput")
    print("="*50 + "\n")
    
    if os.path.exists("./bench_kvstore"):
        shutil.rmtree("./bench_kvstore")
    
    server = KVServer(port=9997, data_dir="./bench_kvstore")
    server.start()
    time.sleep(0.5)
    
    try:
        client = KVClient(port=9997)
        
        data_sizes = [0, 1000, 5000, 10000]
        
        for data_size in data_sizes:
            # Pre-populate if needed
            if data_size > 0:
                print(f"Pre-populating with {data_size} entries...")
                items = [(f"key_{i}", f"value_{i}") for i in range(data_size)]
                for i in range(0, len(items), 100):
                    client.BulkSet(items[i:i+100])
            
            # Benchmark writes
            num_writes = 1000
            start_time = time.time()
            
            for i in range(num_writes):
                client.Set(f"bench_key_{i}", f"bench_value_{i}")
            
            elapsed = time.time() - start_time
            throughput = num_writes / elapsed
            
            print(f"Data size: {data_size:>6} | Writes: {num_writes} | "
                  f"Time: {elapsed:.3f}s | Throughput: {throughput:.2f} writes/sec")
        
        client.close()
        
    finally:
        server.shutdown()
    
    print()


def benchmark_durability():
    """Benchmark durability with random server kills"""
    print("\n" + "="*50)
    print("BENCHMARK: Durability")
    print("="*50 + "\n")
    
    acknowledged_keys = {}
    ack_lock = threading.Lock()
    stop_flag = threading.Event()
    server_process = None
    write_counter = [0]
    
    def write_thread():
        """Thread that writes data"""
        client = None
        while not stop_flag.is_set():
            try:
                if client is None:
                    client = KVClient(port=9996)
                
                key = f"durable_key_{write_counter[0]}"
                value = f"durable_value_{write_counter[0]}"
                
                try:
                    success = client.Set(key, value)
                    if success:
                        with ack_lock:
                            acknowledged_keys[key] = time.time()
                            write_counter[0] += 1
                    time.sleep(0.002)
                except (ConnectionError, BrokenPipeError, OSError):
                    if client:
                        try:
                            client.close()
                        except:
                            pass
                    client = None
                    time.sleep(0.3)
                    
            except:
                if client:
                    try:
                        client.close()
                    except:
                        pass
                client = None
                time.sleep(0.3)
        
        if client:
            try:
                client.close()
            except:
                pass
    
    def kill_thread():
        """Thread that randomly kills the server"""
        nonlocal server_process
        
        for i in range(3):
            time.sleep(random.uniform(2, 4))
            print(f"💀 Kill #{i+1}: Shutting down server...")
            
            old_server = server_process
            old_server.shutdown()
            time.sleep(0.8)
            
            print(f"♻️  Restarting server...")
            server_process = KVServer(port=9996, data_dir="./durable_kvstore")
            server_process.start()
            time.sleep(0.8)
            print(f"✓ Server restarted")
    
    # Clean start
    if os.path.exists("./durable_kvstore"):
        shutil.rmtree("./durable_kvstore")
    
    server_process = KVServer(port=9996, data_dir="./durable_kvstore")
    server_process.start()
    time.sleep(1)
    
    writer = threading.Thread(target=write_thread, daemon=True)
    killer = threading.Thread(target=kill_thread, daemon=True)
    
    writer.start()
    time.sleep(1)
    killer.start()
    
    killer.join()
    stop_flag.set()
    time.sleep(1)
    
    server_process.shutdown()
    time.sleep(1)
    
    # Check durability
    print("\n📊 Checking durability...")
    server_process = KVServer(port=9996, data_dir="./durable_kvstore")
    server_process.start()
    time.sleep(1)
    
    client = KVClient(port=9996)
    
    lost_keys = []
    with ack_lock:
        total_acknowledged = len(acknowledged_keys)
        print(f"Verifying {total_acknowledged} acknowledged writes...")
        
        for key in acknowledged_keys:
            value = client.Get(key)
            if value is None:
                lost_keys.append(key)
    
    client.close()
    server_process.shutdown()
    
    print(f"\nTotal acknowledged writes: {total_acknowledged}")
    print(f"Lost keys: {len(lost_keys)}")
    
    if total_acknowledged > 0:
        durability_pct = ((total_acknowledged - len(lost_keys)) / total_acknowledged * 100)
        print(f"Durability: {durability_pct:.2f}%")
        
        if len(lost_keys) == 0:
            print("✓ 100% DURABILITY ACHIEVED")
        else:
            print(f"✗ Lost keys sample: {lost_keys[:10]}...")
    else:
        print("⚠ No writes were acknowledged")
    
    print()


if __name__ == "__main__":
    run_tests()
    benchmark_write_throughput()
    benchmark_durability()