import random 
import math

class CuckooFilter:
    def __init__(self, item_num, fpp, max_kicks=500):

        if fpp >= 0.002:
            self.bucket_size = 2
            self.capacity = int((item_num/0.84))
        else:
            self.bucket_size = 4
            self.capacity = int((item_num/0.95))   

        self.fingerprint_size = int(math.log((1/fpp), 2) + math.log((2*self.bucket_size), 2)+1)  #fingerprint_size
        self.max_kicks = max_kicks
        self.buckets = [[] for _ in range(self.capacity)]
        self.size = 0

    def add(self, item):
        self.size = self.size + 1
        fingerprint = self._fingerprint(item)
        i1, i2 = self._index_pair(item, fingerprint)
        
        if fingerprint in self.buckets[i1] or fingerprint in self.buckets[i2]:
            return True

        if len(self.buckets[i1]) < self.bucket_size:
            self.buckets[i1].append(fingerprint)
            return True
        
        if len(self.buckets[i2]) < self.bucket_size:
            self.buckets[i2].append(fingerprint)
            return True
        
        random_index = random.choice((i1, i2))
        for _ in range(self.max_kicks):
            fingerprint = self.swap(fingerprint, self.buckets[random_index])
            random_index = (random_index ^ self._hash(fingerprint)) % self.capacity
            
            if len(self.buckets[random_index]) < self.bucket_size:
                self.buckets[random_index].append(fingerprint)
                return True
        self.size = self.size - 1
        raise Exception('Filter is full')
        
    def add_by_fp(self, fp, bucket_index):
        self.size = self.size + 1
        fingerprint = fp
        index = bucket_index

        if fingerprint in self.buckets[index]:
            return True

        if len(self.buckets[index]) < self.bucket_size:
            self.buckets[index].append(fingerprint)
            return True

        for _ in range(self.max_kicks):
            fingerprint = self.swap(fingerprint, self.buckets[index])
            index = (index ^ hash(fingerprint)) % self.capacity

            if len(self.buckets[index]) < self.bucket_size:
                self.buckets[index].append(fingerprint)
                return True
        self.size = self.size - 1
        raise Exception('Filter is full')

    def check(self, item):
        fingerprint = self._fingerprint(item)
        i1, i2 = self._index_pair(item, fingerprint)
        return (fingerprint in self.buckets[i1]) or (fingerprint in self.buckets[i2])
    
    def delete(self, item):
        fingerprint = self._fingerprint(item)
        i1, i2 = self._index_pair(item, fingerprint)
        if (fingerprint in self.buckets[i1]):
            self.buckets[i1].remove(fingerprint)
            self.size = self.size - 1
            return True
        if (fingerprint in self.buckets[i2]):
            self.buckets[i2].remove(fingerprint)
            self.size = self.size - 1
            return True
        return False

    def swap(self, fingerprint, bucket):
        item_index = random.choice(range(self.bucket_size))
        fingerprint, bucket[item_index] = bucket[item_index], fingerprint
        return fingerprint
    
    def _index_pair(self, item, fingerprint):
        i1 = self._hash(item)
        i2 = (i1 ^ self._hash(fingerprint)) % self.capacity
        return i1, i2
    
    def _hash(self, item):
        index = hash(item) % self.capacity
        return index
    
    def _fingerprint(self, item):
        random.seed(hash(item))
        return random.getrandbits(self.fingerprint_size)
    
    def load_factor(self):
        return self.size / (self.capacity * self.bucket_size)