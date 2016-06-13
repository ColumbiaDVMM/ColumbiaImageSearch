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

def get_sha1(tab_sha1_name,rows_ids):
    sha1s_list = [None]*len(rows_ids) 
    with pool.connection() as connection:
        tab_sha1 = connection.table(tab_sha1_name)
        rows_sha1 = tab_sha1.rows(rows_ids)
        for row in rows_sha1:
            pos = rows_ids.index(row[0])
            im_sha1 = None
            nd_sha1 = None
            ndd_sha1 = None
            if "meta:sha1" in row[1]:
                im_sha1 = row[1]["meta:sha1"]
            if "meta:columbia_near_dups_sha1" in row[1]:
                nd_sha1 = row[1]["meta:columbia_near_dups_sha1"]
            if "meta:columbia_near_dups_sha1_dist" in row[1]:
                ndd_sha1 = row[1]["meta:columbia_near_dups_sha1_dist"]
            sha1s_list[pos]=[im_sha1,nd_sha1,ndd_sha1]
    return sha1s_list

def get_image_ids_url(batch):
    #list_images_ids_url = [None]*len(batch)
    list_images_ids_url = []
    for i,row in enumerate(batch):
        iid = None
        url = None
        if "info:crawl_data.image_id" in row[1]:
            iid = row[1]["info:crawl_data.image_id"].strip()
        if "info:obj_stored_url" in row[1]:
            url = row[1]["info:obj_stored_url"].strip()
        if iid and url:
            list_images_ids_url.append([row[0].strip(),iid,url])
    return list_images_ids_url

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
    # If scan ends before filling batch (or never started)
    return batch,len(batch),new_start_row

def write_batch(batch,tab_out_name):
    with pool.connection() as connection:
        tab_out = connection.table(tab_out_name)
        batch_write = tab_out.batch()
        for row in batch:
            batch_write.put(row[0],row[1])
        batch_write.send()

def fill_sha1_info_onebatch(tab_escimg_name,tab_sha1_name,batch_size,start_row):
    batch,c_read,new_start_row = read_batch(tab_in_name,start_row,batch_size)
    if c_read>0:
        list_images_ids_url = get_image_ids_url(batch)
        sha1s_list = get_sha1(tab_sha1_name,[x[1] for x in list_images_ids_url])
        # sha1s_list[pos]=[im_sha1,nd_sha1,ndd_sha1]
        out_batch = [x[0],{"info:sha1": sha1s_list[i][0], "info:columbia_near_dups_sha1": sha1s_list[i][1],  "info:columbia_near_dups_sha1_dist": sha1s_list[i][2]} for i,x in enumerate(list_images_ids_url) if sha1s_list[i] and sha1s_list[i]!=[None,None,None]]
        write_batch(out_batch,tab_escimg_name)
        return c_read,new_start_row
    else:
        return c_read,None

def fill_sha1_info(tab_escimg_name,tab_sha1_name,batch_size,start_row):
    start_time = time.time()
    done = False
    while not done:
        count_read,new_start_row = fill_sha1_info_onebatch(tab_escimg_name,tab_sha1_name,batch_size,start_row)
        if new_start_row==None:
            print "Did not get any rows. Leaving."
            done = True
    tel = time.time()-start_time
    if count_read>0:
        print "Read {} rows total. Average time per row is: {}. Total time is: {}.".format(count_read,tel/count_read,tel)
    

if __name__ == '__main__':
    start_row = None
    tab_sha1_name = "aaron_memex_ht-images"
    tab_escimg_name = "escorts_images_cdrid_infos_dev"
    fill_sha1_info(tab_escimg_name,tab_sha1_name,batch_size,start_row)
