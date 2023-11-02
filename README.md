# lsm-cuckoo
This study aims to develop an LSM tree that uses two different kinds of probabilistic data structures (PDS). These two data structures are the Bloom Filters and the state-of-the-art Cuckoo Filter released in 2014 by Fan. Cuckoo filters are a perfect choice for saving space and also for deleting keys that are not needed in the algorithm so the filters are synchronized with the keys of all the SSTables. The implementation in this study of Cuckoo filters showed three main advantages (1) Keys that are deleted in the SSTables are immediately reflected in the Cuckoo Filters (2) When SSTables are merged, Cuckoo Filters are also merged by measuring its load factor for availability (3) The performance of Cuckoo Filters compared to Bloom filters after deleting and query shows better performance despite that cuckoo filters are also being updated. This approach is expected to reduce storage space and the amount of time for queries and deletions on the overall data structure. This new optimized LSM tree is expected to be implemented in HBase as future work to check its performance with a real-world database for more empirical results.

For further references, you can download this system's paper through this [link](https://www.researchsquare.com/article/rs-3226421/v1).


## LSM Cuckoo Directory Guide

This directory is composed of the next folders:

- Data
    
    Here test data is attached to run the Jupyter Notebook for testing the algorithm’s performance
    
- lsm_tree
    
    This folder contains the main two algorithms used for the paper “LSM Tree Read-Deletion Operations Optimization Through The Implementation of Cuckoo Filters”. Inside is a subfolder called “GenOne” that states the first development of the LSM trees of this study. Further development of GenTwo is being conducted and uploading of the new algorithms is expected to occur. Their corresponding names are (1) lsm_tree_bloom_filter_mem and (2) lsm_tree_cuckoo_filter_mem.
    
- PDS
    
    Contains the probabilistic data structures used for the study (bloom and cuckoo filters)
    
- segments
    
    This directory is used for saving the SSTables and filters of the algorithms. Inside here, two subfolders (bloom and cuckoo) are contained, and each folder is also another folder that is used to save the filters separately.
    
- tools
    
    This folder contains the two sub-classes used in the lsm_tree scrips for indexing and ensuring data is not lost due to a possible system malfunction.
    

## LSM Cuckoo Jupyter Notebooks

There are a total of 4 Jupyter Notebooks in the main directory

- Gen 1 CompoundPerformanceTest
    
    This jupyter notebook generates data on the performance of the LSM Tree. This Notebook is only intended to be used for bloom and cuckoo filters performance test
    
- Gen 1 Data Visualization
    
    This jupyter notebook visualized the generated data of the performance of the LSM Tree. This Notebook is only intended to be used for the bloom and cuckoo filters performance test.
    
- Gen 1 LSM 3 Kind Test
    
    This jupyter notebook tests the generated data of the performance of the LSM Tree. This Notebook is intended to test the performance of the LSM tree with and without the implementation of filters.
    
- Gen 1 LSM 3 Kind Data Visualization
    
    This jupyter notebook visualized the generated data of the performance of the LSM Tree. This Notebook is intended to visualize the performance data of the LSM tree with and without the implementation of filters.
