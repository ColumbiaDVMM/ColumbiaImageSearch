import os
import sys
import time
import datetime
sys.path.append('..')
import cu_image_search
from cu_image_search.indexer.hbase_indexer import HBaseIndexer


if __name__=="__main__":
    """ Refresh indexer based on `conf_file` given as parameter
    """
    if len(sys.argv)<2:
        print "python refresh_indexer.py conf_file"
        exit(-1)
    global_conf_file = sys.argv[1]
    HBI = HBaseIndexer(global_conf_file)
    HBI.refresh_hash_index()
