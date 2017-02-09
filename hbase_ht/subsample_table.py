import MySQLdb
import happybase
from Queue import *
from threading import Thread
import json
import time
import os
import numpy as np
import sys
sys.path.insert(0, os.path.abspath('../memex_tools'))

hbase_conn_timeout = None
nb_threads = 1
pool = happybase.ConnectionPool(size=nb_threads,host='10.1.94.57',timeout=hbase_conn_timeout)
global_var = json.load(open('../../conf/global_var_all.json'))

#import sha1_tools
#sha1_tools.pool = pool
#sha1_tools.global_var = global_var
#sha1_tools.tab_aaron_name = tab_aaron_name

batch_size = 100

def copy_batch(tab_in_name,start_row,batch_size,tab_out_name):
    try:
        batch_start = time.time()
        batch,count,new_start_row = read_batch(tab_in_name,start_row,batch_size)
        write_batch(batch,tab_out_name)
        tel = time.time()-batch_start
        print "Batch of {} rows from row {} copied in: {}s.".format(len(batch),start_row,tel)
        return count,new_start_row
    except Exception as inst:
        print "Copy batch failed. {}".format(inst)
        return None,None

def read_batch(tab_in_name,start_row,batch_size):
    batch = []
    new_start_row = start_row
    with pool.connection() as connection:
        tab_in = connection.table(tab_in_name)
        for one_row in tab_in.scan(row_start=start_row):
            if len(batch)<batch_size:
                batch.append(one_row)
                new_start_row = one_row[0]
            else:
                return batch,len(batch),new_start_row
        # If scan ends before filling batch, return partial final batch
        return batch,len(batch),new_start_row

def write_batch(batch,tab_out_name):
    with pool.connection() as connection:
        tab_out = connection.table(tab_out_name)
        batch_write = tab_out.batch()
        for row in batch:
            batch_write.put(row[0],row[1])
        batch_write.send()

def subsample_table(tab_in_name,start_row,batch_size,tab_out_name):
    start_time = time.time()
    copy_count = 0
    while copy_count<rows_count:
        if copy_count+batch_size<=rows_count:
            tmp_batch_size = batch_size
        else:
            tmp_batch_size = rows_count-copy_count
        count,new_start_row = copy_batch(tab_in_name,start_row,tmp_batch_size,tab_out_name)
        if count is None:
            print "Did not copy any rows. Leaving."
            break
        else:
            if count==tmp_batch_size: # full batch
                copy_count += count
                start_row = new_start_row
            else: # incomplete last batch
                copy_count += count
                start_row = new_start_row
                print "Only managed to get {} out of {} rows requested.".format(copy_count,rows_count)
                break
    tel = time.time()-start_time
    if copy_count>0:
        print "Copied {} rows total, out of {} requested. Average time per row is: {}. Total time is: {}.".format(copy_count,rows_count,tel/copy_count,tel)
    

if __name__ == '__main__':
    start_row = None
    if len(sys.argv)<4:
        print "Usage: subsample_table.py tab_in tab_out rows_count"
        sys.exit(-1)
    else:
        tab_in_name = sys.argv[1]
        tab_out_name = sys.argv[2]
        rows_count = int(sys.argv[3])
    subsample_table(tab_in_name,start_row,batch_size,tab_out_name)
