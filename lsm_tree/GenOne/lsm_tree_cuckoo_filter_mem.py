import sys
from tools.red_black_tree import RedBlackTree
from tools.write_append_log import AppendLog
from PDS.cuckoo_filter import CuckooFilter

from pathlib import Path
from os import remove as remove_file, rename as rename_file

import pickle
import os
from datetime import datetime

class LSMTreeCuckoo():
    def __init__(self, 
                 segment_basename='LSMTreeCuckoo', 
                 segments_directory='segments/cuckoo/', 
                 wal_basename='wal_file_cuckoo', 
                 filter_dir = 'segments/cuckoo/cuckoo_filters/'):
        ''' (self, str, str, str) -> LSMTree
        Initialize a new LSM Tree with:

        - A first segment called segment_basename
        - A segments directory called segments_directory
        - A memtable write ahead log (WAL) called wal_basename
        '''
        self.segments_directory = segments_directory
        self.filter_dir = filter_dir
        self.wal_basename = wal_basename
        
        time_str = '-' + datetime.now().strftime('%Y%m%d%H%M%S%f')
        self.current_segment = segment_basename + time_str
        self._current_ckf = 'ckf'+'-'+'1'+time_str
        
        self.first_level = []
        self.second_level = []
        self.third_level = []
        self.meta_dict = dict()
        self.ckfs = []
        self.ckfs_in_memory = dict()

        # Default threshold is 100,000 items
        self._size_threshold = 100000
        self._time_threshold = 0.25/40
        self._lvl1_size = 35
        self._lvl2_size = 100
        self._count = 0
        self._memtable = RedBlackTree()
        
        # Index
        self._index = RedBlackTree()
        self._sparsity_factor = 100

        # Cuckoo Filter
        self._ckf_num_items = self._size_threshold
        self._ckf_false_pos_prob = 0.2
        self._cuckoo_filter = None

        # Create the segments directory
        if not (Path(segments_directory).exists() and Path(segments_directory).is_dir):
            Path(segments_directory).mkdir()

        # Attempt to load metadata and a pre-existing memtable
        self.load_metadata()
        self.restore_memtable()

    def db_set(self, key, value):
        ''' (self, str, str) -> None
        Stores a new key value pair in the DB
        '''
        log = self._to_log_entry(key, value)

        # Check if we can save effort by updating the memtable in place
        node = self._memtable.find_node(key)
        if node:
            self._memtable_wal().write(log)
            node.value = value
            return

        # Check if new segment needed
        additional_size = len(key) + len(value)
        if self._count+1 > self._size_threshold:
            self._flush_memtable_to_disk(self._current_segment_path(), self._current_ckf_path())

            # Update bookkeeping metadata
            self._memtable = RedBlackTree()
            self._memtable_wal().clear()

            self.first_level.append(self.current_segment)
            self.ckfs.append(self._current_ckf)
            self.meta_dict[self.current_segment] = (self._current_ckf,)
            
            new_seg_name = self.current_segment.split('-')[0]+'-'+datetime.now().strftime('%Y%m%d%H%M%S%f')
            name, number, _ = self._current_ckf.split('-')
            new_ckf_name = '-'.join([name, number, new_seg_name.split('-')[-1]])

            self.current_segment = new_seg_name
            self._current_ckf = new_ckf_name
            self._count = 0
            
        # Execute Merging 
        if len(self.first_level) > 1:
            self._merge_by_time_th(self.first_level, self.meta_dict)
            self._move_large_files(self.first_level, self.second_level, self._lvl1_size)
        if len(self.second_level) > 1:
            self._merge_by_time_th(self.second_level, self.meta_dict)
            self._move_large_files(self.second_level, self.third_level, self._lvl2_size)
        if len(self.third_level) > 4:
            self._merge_by_time_th(self.third_level, self.meta_dict)
            
        # Write to memtable write ahead log in case of crash
        self._memtable_wal().write(log)

        # Write to memtable
        self._memtable.add(key, value)
        self._count += 1
        self._memtable.total_bytes += additional_size
        
    def db_get(self, key):
        ''' (self, str) -> None
        Retrieve the value associated with key in the db
        '''
        
        # Attempt to find the key in the memtable first
        memtable_result = self._memtable.find_node(key)
        if memtable_result:
            return memtable_result.value

        return self._search_all_segments(key)

    def db_del(self, key):
        memtable_result = self._memtable.find_node(key)

        if memtable_result:
            return self._memtable.remove(key)
        
        for segment, ckf_tup in reversed(list(self.meta_dict.items())):
            for ckf in ckf_tup:
                if self.ckfs_in_memory[ckf].delete(key):
                    if self._delete_keys_from_segment(set(key.split()), self._segment_path(segment)):
                        return

    # Write helpers
    def _flush_memtable_to_disk(self, segment_path, ckf_path):
        ''' (self, str) -> None
        Writes the contents of the current memtable to disk and wipes the current memtable.

        Updates the index and adds keys to the cuckoo filter.
        '''
        sparsity_counter = self._sparsity()

        # We track the offset for each key ourself, instead of checking the file's size as we
        # write, since its faster than making sure that every new write is flushed to disk.
        key_offset = 0

        with open(segment_path, 'w') as s:
            cuckoo_filter = CuckooFilter(self._ckf_num_items, self._ckf_false_pos_prob)
            for node in self._memtable.in_order():
                log = self._to_log_entry(node.key, node.value)

                # Update sparse index
                if sparsity_counter == 1:
                    self._index.add(node.key, node.value,
                                   offset=key_offset, segment=self.current_segment)
                    sparsity_counter = self._sparsity() + 1

                # Add to cuckoo filters
                cuckoo_filter.add(node.key)
                s.write(log)
                key_offset += len(log)
                sparsity_counter -= 1
            self.ckfs_in_memory[ckf_path.split('/')[-1]] = cuckoo_filter

    def _to_log_entry(self, key, value):
        '''(str, str) -> str
        Converts a key value pair into a comma seperated newline delimited
        log entry.
        '''
        return str(key) + ',' + (value) + '\n'

    # Files compact and delete operations
    def compact(self): #revise
        ''' (self) -> None
        Reads the keys from the memtable, determines which ones probably
        have pre-existing records on disk and reclaims disk space accordingly.

        Note: this will parse every segment in self.first_level. It is intended to be
        used BEFORE flushing the memtable to disk.
        '''
        memtable_nodes = self._memtable.in_order()

        keys_on_disk = []
        for node in memtable_nodes:
            if self._cuckoo_filter.check(node.key):
                keys_on_disk.append(node.key)

        keys_on_disk = set(keys_on_disk)

        self._delete_keys_from_segments(keys_on_disk, self.first_level)

    def _delete_keys_from_segments(self, deletion_keys, segment_names):
        ''' (self, list) -> None
        Deletes all keys stored in the set deletion_keys from each segment
        listed in segment_names.
        '''
        for segment in segment_names:
            segment_path = self._segment_path(segment)
            self._delete_keys_from_segment(deletion_keys, segment_path)

    def _delete_keys_from_segment(self, deletion_keys, segment_path):
        ''' (self, set(keys), str) -> None
        Removes the lines with key in deletion_keys from the file stored at segment 
        path.

        The method achieves this by writing the desireable keys to a new 
        temporary file, then deleting the old version and replacing it with the
        temporary one. This strategy is chosen to avoid overloading memory.
        '''
        temp_path = segment_path + '_temp'
        deleted = 0
        
        with open(segment_path, "r") as input:
            with open(temp_path, "w") as output:
                for line in input:
                    key, _ = line.split(',', 1)
                    if not key in deletion_keys:
                        output.write(line)
                    else:
                        deleted += 1
        remove_file(segment_path)
        rename_file(temp_path, segment_path)
        
        if deleted == 0:
            return False
        return True
    
    # Merging Section
    def _merge_by_time_th(self, segments, dictionary):
        if self._check_seg_time(sorted(segments)[0]):
            seg1, seg2, new_seg = self._merge(sorted(segments)[0], sorted(segments)[1])
            segments.append(new_seg), segments.remove(seg1), segments.remove(seg2)
            dictionary[new_seg] = (dictionary[seg1]+dictionary[seg2])
            
            if len(dictionary[new_seg]) > 3:
                self._create_new_ckf(new_seg, dictionary[new_seg], dictionary)
            else:
                self._ckf_compresser(dictionary[new_seg], new_seg, dictionary, self.ckfs, self.ckfs_in_memory)

            dictionary.pop(seg1), dictionary.pop(seg2)
            remove_file(self._segment_path(seg1)), remove_file(self._segment_path(seg2))
        return
            
    def _move_large_files(self, from_seg_set, to_seg_set, lvl_size):
        
        size_measurer = lambda x: os.path.getsize(self._segment_path(x))/1000000
        temp_segment = [seg for seg in from_seg_set if size_measurer(seg) > lvl_size]
        
        for seg in temp_segment:
            to_seg_set.append(seg), from_seg_set.remove(seg)

    def _merge(self, segment1, segment2):
        ''' (self, str, str) -> str
        Concatenates the contents of the files represented byt segment1 and
        segment2, erases the second segment file and returns the name of the
        first segment. 
        '''
        path1 = self._segment_path(segment1)
        path2 = self._segment_path(segment2)
        
        time_str = datetime.now().strftime('%Y%m%d%H%M%S%f')
        new_name = segment1.split('-')[0] +'-'+time_str
        new_path = self.segments_directory + new_name

        with open(new_path, 'w') as s0:
            with open(path1, 'r') as s1:
                with open(path2, 'r') as s2:
                    line1, line2 = s1.readline(), s2.readline()
                    while not (line1 == '' and line2 == ''):
                        # At the end of the file stream we'll get the empty str
                        key1, key2 = line1.split(',')[0], line2.split(',')[0]
                        if key1 == '' or key1 == key2:
                            s0.write(line2)
                            line1 = s1.readline()
                            line2 = s2.readline()
                        elif key2 == '' or key1 < key2:
                            s0.write(line1)
                            line1 = s1.readline()
                        else:
                            s0.write(line2)
                            line2 = s2.readline()
        return segment1, segment2, new_name

    # Configuration methods
    def set_size_threshold(self, threshold):
        ''' (self, int) -> None
        Sets the threshold - the point at which a new segment is created
        for the database. The argument, threshold, is measured in bytes.
        '''
        self._size_threshold = threshold

    def set_sparsity_factor(self, factor):
        ''' (self, int) -> None
        Sets the sparsity factor for the database. The threshold is divided by this 
        number to yield the index's sparsity, which represents how many elements per
        segment will be stored in the index.

        The higher this number, the more records will be stored.
        '''
        self._sparsity_factor = factor
        
    def set_time_threshold(self, time_threshold):
        ''' (self, int) -> None
        Sets the maximum time at which segments are merged
        in the database. The argument, max time, is measured in hours.
        '''
        self._time_threshold = time_threshold

    def set_levels_threshold(self, lvl1_size, lvl2_size):
        ''' (self, int) -> None
        Sets the max level size at which a segment is moved in between levels
        for the database. The argument, lvl1_size and lvl2_size is measured in megabytes.
        '''
        self._lvl1_size = lvl1_size
        self._lvl2_size = lvl2_size

    ### Helper methods
    def _memtable_wal(self):
        ''' (self) -> str
        Returns an instance of the write ahead log.
        '''
        return AppendLog.instance(self._memtable_wal_path())

    def _search_all_segments(self, key):
        ''' (self, str) -> str
        Searches all segments on disk for key by checking
        cuckoo filters firts.
        '''
        for segment, ckf_tup in reversed(list(self.meta_dict.items())):
            for ckf in ckf_tup:
                if self.ckfs_in_memory[ckf].check(key):
                    value = self._search_segment(key, segment)
                    if value != None:
                        return value
                    
    def _search_segment(self, key, segment_name):
        ''' (self, str) -> str
        Returns the value associated with key in the segment represented
        by segment_name, if it exists. Otherwise return None.
        '''
        with open(self._segment_path(segment_name), 'r') as s:
            pairs = [line.strip() for line in s]

            while len(pairs):
                ptr = (len(pairs) - 1) // 2
                k, v = pairs[ptr].split(',', 1)

                if k == key:
                    return v

                if key < k:
                    pairs = pairs[0:ptr]
                else:
                    pairs = pairs[ptr+1:]

    def _check_seg_time(self, seg_name):
        ''' (self) -> str
        Returns True or False if the time for merging is passed.
        It will merge the two oldest files of the system.
        '''
        mod_time = os.path.getmtime(self.segments_directory+seg_name)
        mod_date = datetime.fromtimestamp(mod_time)
        current_time = datetime.now()                
        time_diff = current_time-mod_date                
        if self._time_threshold < time_diff.total_seconds()/(60*60):                
            return True
        return False

    # Cuckoo filter
    def set_ckf_fpp(self, probability):
        ''' (self, int) -> None
        Sets the desired probability of generating a false positive for the cuckoo filter.

        Warning - this operation re-initializes the structure.
        '''
        self._ckf_false_pos_prob = probability
        self._cuckoo_filter = CuckooFilter(self._ckf_num_items, self._ckf_false_pos_prob)
        
    def _ckf_compresser(self, ckf_tuple, new_seg, dictionary,ckfs, ckfs_in_memory):

        # Check load
        total_load = ckfs_in_memory[ckf_tuple[0]].load_factor() + ckfs_in_memory[ckf_tuple[1]].load_factor()
        if total_load > 0.50:
            return
        
        # Make new a cuckoo filter name
        numb_list = [numb.split('-')[-2] for numb in ckf_tuple]
        new_ckf_name = f'ckf-{sum(int(x) for x in numb_list)}'+'-'+new_seg.split('-')[-1]
        # Append fingerprints of the 2nd ckf to the 1st ckf
        for index, bucket in enumerate(ckfs_in_memory[ckf_tuple[1]].buckets):
            for fp in bucket:
                ckfs_in_memory[ckf_tuple[0]].add_by_fp(fp, index)
        
        #Update Instances
        ckfs_in_memory[new_ckf_name] = ckfs_in_memory[ckf_tuple[0]]
        ckfs_in_memory.pop(ckf_tuple[0])
        ckfs_in_memory.pop(ckf_tuple[1])
        ckfs.remove(ckf_tuple[0]), ckfs.remove(ckf_tuple[1]), ckfs.append(new_ckf_name)
        dictionary[new_seg] = (new_ckf_name,)
            
    def _create_new_ckf(self, segment_name, ckf_tuple, dictionary):
        numb_list = [numb.split('-')[-2] for numb in ckf_tuple]
        total_filter = sum(int(x) for x in numb_list)
        cuckoo_filter = CuckooFilter(self._ckf_num_items*total_filter, self._ckf_false_pos_prob)
        new_ckf_name = f'ckf-{total_filter}'+'-'+segment_name.split('-')[-1]
        with open(self._segment_path(segment_name), "r") as s:
            for line in s:
                key, _ = line.split(',', 1)
                cuckoo_filter.add(key)
        for ckf in ckf_tuple:
            self.ckfs.remove(ckf)
            self.ckfs_in_memory.pop(ckf)
        self.ckfs.append(new_ckf_name)
        self.ckfs_in_memory[new_ckf_name] = cuckoo_filter
        dictionary[segment_name] = (new_ckf_name,)
    
    def save_ckfs(self):
        for ck in self.ckfs:
            with open(self.filter_dir + ck, 'wb') as cuckoo:
                pickle.dump(self.ckfs_in_memory[ck], cuckoo)

    # Index helpers
    def _sparsity(self):
        ''' (self) -> int
        Returns the sparsity of the index. This represents the number of records per
        segment that will be stored in the index. The value is always rounded down.
        '''
        return self._size_threshold // self._sparsity_factor

    def repopulate_index(self):
        '''(self) -> None
        Repopulates the index stored in the database by parsing each segment
        on disk.
        '''
        self._index = RedBlackTree()
        for segment in self.first_level:
            path = self._segment_path(segment)

            counter = self._sparsity()
            bytes = 0
            with open(path, 'r') as s:
                for line in s:
                    key, val = line.strip().split(',', 1)
                    if counter == 1:
                        self._index.add(key, val, offset=bytes, segment=segment)
                        counter = self._sparsity() + 1

                    bytes += len(line)
                    counter -= 1

    def restore_memtable(self):
        ''' (self) -> None
        Re-populates the memtable from the disk backup.
        '''
        if Path(self._memtable_wal_path()).exists():
            with open(self._memtable_wal_path(), 'r') as s:
                for line in s:
                    key, value = line.strip().split(',', 1)
                    self._memtable.add(key, value)
                    self._memtable.total_bytes += len(key) + len(value)

    # Path generators
    def _current_segment_path(self):
        return self.segments_directory + self.current_segment
    
    def _current_ckf_path(self):
        return self.segments_directory + self._current_ckf

    def _memtable_wal_path(self):
        ''' (self) -> str
        Returns the path to the memtable write ahead log.
        '''
        return self.segments_directory + self.wal_basename

    def _segment_path(self, segment_name):
        ''' (self, str) -> str
        Returns the path to the given segment_name.
        '''
        return self.segments_directory + segment_name
    
    def _ckf_path(self, ckf_name):
        ''' (self, str) -> str
        Returns the path to the given ckf_name.
        '''
        return self.segments_directory + ckf_name +'.pickle'

    def metadata_path(self):
        ''' (self) -> str
        Returns the path to the metadata backup file.
        '''
        return self.segments_directory + 'database_metadata'

    # Metadata and initialization helpers
    def load_metadata(self):
        ''' (self) -> None
        Checks to see if any metadata or memtable logs are present from the previous
        session, and load them into the system.
        '''
        if Path(self.metadata_path()).exists():
            with open(self.metadata_path(), 'rb') as s:
                metadata = pickle.load(s)
                self.first_level = metadata['first_level']
                self.second_level = metadata['second_level']
                self.meta_dict = metadata['meta_dict']
                self._count = metadata['count']
                self._time_threshold = metadata['_time_threshold']
                self.current_segment = metadata['current_segment']
                self._current_ckf = metadata['current_ckf']
                self._cuckoo_filter = metadata['cuckoo_filter']
                self._ckf_num_items = metadata['ckf_num_items']
                self._ckf_false_pos_prob = metadata['ckf_false_pos']
                self._index = metadata['index']

    def save_metadata(self):
        ''' (self) -> None
        Save necessary bookkeeping information.
        '''
        bookkeeping_info = {
            'first_level': self.first_level,
            'second_level': self.second_level,
            'meta_dict': self.meta_dict, 
            'count': self._count,
            '_time_threshold': self._time_threshold,
            'current_segment': self.current_segment,
            'current_ckf': self._current_ckf,
            'cuckoo_filter': self._cuckoo_filter,
            'ckf_num_items': self._ckf_num_items,
            'ckf_false_pos': self._ckf_false_pos_prob,
            'index': self._index
        }

        with open(self.metadata_path(), 'wb') as s:
            pickle.dump(bookkeeping_info, s)

    