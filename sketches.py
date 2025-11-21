"""
Probabilistic data structures for approximate query processing
"""
import hashlib
import struct
import math
from typing import List


class HyperLogLog:
    """HyperLogLog for cardinality estimation"""
    
    def __init__(self, b: int = 10):
        """
        Initialize HyperLogLog with 2^b registers
        
        Args:
            b: Number of bits for register selection (4-16)
        """
        if b < 4 or b > 16:
            b = 10
        
        self.b = b
        self.m = 1 << b  # 2^b registers
        self.registers = [0] * self.m
        
        # Calculate alpha constant for bias correction
        if self.m >= 128:
            self.alpha = 0.7213 / (1 + 1.079 / self.m)
        elif self.m >= 64:
            self.alpha = 0.709
        elif self.m >= 32:
            self.alpha = 0.697
        elif self.m >= 16:
            self.alpha = 0.673
        else:
            self.alpha = 0.5
    
    def add(self, value: bytes):
        """Add a value to the HyperLogLog"""
        hash_val = self._hash64(value)
        
        # Use first b bits for register selection
        j = hash_val & ((1 << self.b) - 1)
        
        # Use remaining bits for leading zero count
        w = hash_val >> self.b
        
        # Count leading zeros + 1
        leading_zeros = 1
        while w > 0 and leading_zeros <= 64 - self.b:
            if w & 1 == 1:
                break
            leading_zeros += 1
            w >>= 1
        
        # Update register with maximum leading zero count
        if leading_zeros > self.registers[j]:
            self.registers[j] = leading_zeros
    
    def add_string(self, value: str):
        """Add a string value to the HyperLogLog"""
        self.add(value.encode('utf-8'))
    
    def count(self) -> int:
        """Estimate the cardinality"""
        # Calculate raw estimate
        raw_estimate = self.alpha * (self.m ** 2) / self._harmonic_mean()
        
        # Apply small range correction
        if raw_estimate <= 2.5 * self.m:
            zeros = self._count_zeros()
            if zeros != 0:
                return int(self.m * math.log(self.m / zeros))
        
        # Apply large range correction for 32-bit hash
        if raw_estimate <= (1.0 / 30.0) * (1 << 32):
            return int(raw_estimate)
        
        return int(-1 * (1 << 32) * math.log(1 - raw_estimate / (1 << 32)))
    
    def standard_error(self) -> float:
        """Return the theoretical standard error"""
        return 1.04 / math.sqrt(self.m)
    
    def confidence_interval(self, confidence: float = 0.95) -> tuple:
        """Return approximate confidence bounds"""
        estimate = float(self.count())
        std_err = self.standard_error() * estimate
        
        # Use normal approximation
        if abs(confidence - 0.90) < 1e-9:
            z = 1.645
        elif abs(confidence - 0.95) < 1e-9:
            z = 1.96
        elif abs(confidence - 0.99) < 1e-9:
            z = 2.576
        else:
            z = 1.96  # default to 95%
        
        margin = z * std_err
        lower = max(0, estimate - margin)
        upper = estimate + margin
        
        return (int(lower), int(upper))
    
    def merge(self, other: 'HyperLogLog'):
        """Merge with another HyperLogLog"""
        if self.m != other.m or self.b != other.b:
            raise ValueError("Cannot merge HLLs with different parameters")
        
        for i in range(self.m):
            if other.registers[i] > self.registers[i]:
                self.registers[i] = other.registers[i]
    
    def serialize(self) -> bytes:
        """Serialize the HyperLogLog to bytes"""
        data = bytearray()
        data.append(self.b)
        data.extend(struct.pack('<I', self.m))
        data.extend(bytes(self.registers))
        return bytes(data)
    
    @staticmethod
    def deserialize(data: bytes) -> 'HyperLogLog':
        """Deserialize a HyperLogLog from bytes"""
        if len(data) < 5:
            raise ValueError("Insufficient data for HLL deserialization")
        
        b = data[0]
        m = struct.unpack('<I', data[1:5])[0]
        
        if len(data) != 5 + m:
            raise ValueError("Data length mismatch")
        
        hll = HyperLogLog(b)
        hll.registers = list(data[5:])
        return hll
    
    def _hash64(self, data: bytes) -> int:
        """64-bit hash function"""
        h = hashlib.sha256(data).digest()
        return struct.unpack('<Q', h[:8])[0]
    
    def _harmonic_mean(self) -> float:
        """Calculate harmonic mean of registers"""
        return sum(2 ** (-reg) for reg in self.registers)
    
    def _count_zeros(self) -> int:
        """Count zero registers"""
        return sum(1 for reg in self.registers if reg == 0)


class CountMinSketch:
    """Count-Min Sketch for frequency estimation"""
    
    def __init__(self, epsilon: float = 0.01, delta: float = 0.01):
        """
        Initialize Count-Min Sketch
        
        Args:
            epsilon: Error bound (smaller = more accurate, more space)
            delta: Failure probability (smaller = more reliable, more space)
        """
        self.w = int(math.ceil(math.e / epsilon))  # Width
        self.d = int(math.ceil(math.log(1.0 / delta)))  # Depth
        self.table = [[0] * self.w for _ in range(self.d)]
        self.epsilon = epsilon
        self.delta = delta
    
    def add(self, key: bytes, count: int = 1):
        """Add a key with count to the sketch"""
        for i in range(self.d):
            hash_val = self._hash(key, i)
            col = hash_val % self.w
            self.table[i][col] += count
    
    def add_string(self, key: str, count: int = 1):
        """Add a string key with count"""
        self.add(key.encode('utf-8'), count)
    
    def estimate(self, key: bytes) -> int:
        """Estimate the count for a key"""
        min_count = float('inf')
        for i in range(self.d):
            hash_val = self._hash(key, i)
            col = hash_val % self.w
            min_count = min(min_count, self.table[i][col])
        return int(min_count)
    
    def estimate_string(self, key: str) -> int:
        """Estimate the count for a string key"""
        return self.estimate(key.encode('utf-8'))
    
    def serialize(self) -> bytes:
        """Serialize the Count-Min Sketch to bytes"""
        data = bytearray()
        data.extend(struct.pack('<I', self.w))
        data.extend(struct.pack('<I', self.d))
        data.extend(struct.pack('<d', self.epsilon))
        data.extend(struct.pack('<d', self.delta))
        
        for row in self.table:
            for val in row:
                data.extend(struct.pack('<Q', val))
        
        return bytes(data)
    
    @staticmethod
    def deserialize(data: bytes) -> 'CountMinSketch':
        """Deserialize a Count-Min Sketch from bytes"""
        offset = 0
        w = struct.unpack('<I', data[offset:offset+4])[0]
        offset += 4
        d = struct.unpack('<I', data[offset:offset+4])[0]
        offset += 4
        epsilon = struct.unpack('<d', data[offset:offset+8])[0]
        offset += 8
        delta = struct.unpack('<d', data[offset:offset+8])[0]
        offset += 8
        
        cms = CountMinSketch(epsilon, delta)
        cms.w = w
        cms.d = d
        cms.table = [[0] * w for _ in range(d)]
        
        for i in range(d):
            for j in range(w):
                cms.table[i][j] = struct.unpack('<Q', data[offset:offset+8])[0]
                offset += 8
        
        return cms
    
    def _hash(self, data: bytes, seed: int) -> int:
        """Hash function with seed"""
        h = hashlib.sha256(data + str(seed).encode()).digest()
        return struct.unpack('<Q', h[:8])[0]
