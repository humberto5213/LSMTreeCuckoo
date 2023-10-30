from math import log
from mmh3 import hash
  
class BloomFilter:

    def __init__(self, num_items, false_positive_prob): 
        self.false_positive_prob = false_positive_prob 
        self.bit_array_size = self.bit_array_size(num_items, false_positive_prob) 
        self.num_hash_fns = self.get_hash_count(self.bit_array_size, num_items) 
        self.bit_array = [0 for i in range(self.bit_array_size)] 

    def add(self, item): 
        for seed in range(self.num_hash_fns):
            # each seed creates a different digest.
            digest = hash(item, seed) % self.bit_array_size 
            self.bit_array[digest] = 1

    def check(self, item): 
        for seed in range(self.num_hash_fns): 
            digest = hash(item, seed) % self.bit_array_size 
            if self.bit_array[digest] == 0: 
                # if any bit is false, the item is not definitely present
                return False
        return True
  
    def bit_array_size(self, num_items, probability):
        m = -(num_items * log(probability)) / (log(2)**2)
        return int(m)

    def get_hash_count(self, bit_arr_size, num_items): 
        return int((bit_arr_size/num_items) * log(2))