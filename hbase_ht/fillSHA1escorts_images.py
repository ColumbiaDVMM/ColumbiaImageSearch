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
import sha1_tools
hbase_conn_timeout = None
#tab_aaron_name = 'aaron_memex_ht-images'
tab_hash_name = 'image_hash'
tab_escorts_images_name = 'escorts_images_cdrid_infos_dev'
nb_threads = 2
pool = happybase.ConnectionPool(size=nb_threads,host='10.1.94.57',timeout=hbase_conn_timeout)
sha1_tools.pool = pool
global_var = json.load(open('../../conf/global_var_all.json'))
sha1_tools.global_var = global_var
#sha1_tools.tab_aaron_name = tab_aaron_name
row_count = 0
missing_sha1_count = 0
missing_sim_count = 0

batch_size = 10000

### fill sha1 'tab_escorts_images_name'
# scan tab_escorts_images_name
# get sha1 of row-key from 'tab_hash_name'
# put it in info:sha1

def process_one_row(one_row):
    global missing_sha1_count
    # should indicate row being already processed
    if 'info:sha1' in one_row[1].keys():
        return
    # we cannot get sha1 from tab_hash if we don't have the 'image_id'
    if 'info:crawl_data.image_id' not in one_row[1].keys():
        return
    row_sha1 = get_row_sha1(one_row[1]['info:crawl_data.image_id'])
    if not row_sha1:
        #print "Could not get sha1 for image with cdr_id {} and image_id {}.".format(one_row[0],one_row[1]['info:crawl_data.image_id'])
        missing_sha1_count+=1
        return
    with pool.connection() as connection:
        tab_escorts_images = connection.table(tab_escorts_images_name)
        tab_escorts_images.put(one_row[0],{'info:sha1': str(row_sha1)})
    return

def process_batch_rows(list_rows):
    for one_row in list_rows:
        process_one_row(one_row)

def process_batch_worker():
    while True:
        batch_start = time.time()
        tupInp = q.get()
        try:
            #print "Starting to process batch of rows {}.".format([x[0] for x in tupInp[0]])
            process_batch_rows(tupInp[0])
            tel = time.time()-batch_start
            print "Batch from row {} (count: {}) done in: {}.".format(tupInp[0][0][0],tupInp[1],tel)
            q.task_done()
        except Exception as inst:
            print "Batch from row {} (count: {}) FAILED.".format(tupInp[0][0][0],tupInp[1])
            exc_type, exc_obj, exc_tb = sys.exc_info()  
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print "{} in {} line {}.\n".format(exc_type, fname, exc_tb.tb_lineno)
            print "[process_batch_worker] {}.".format(inst)

def get_row_sha1(image_id):
    row_sha1 = sha1_tools.get_SHA1_from_hbase_imagehash(image_id,tab_hash_name)
    return row_sha1

if __name__ == '__main__':
    start_time = time.time()
    last_row = None
    #issue_file = "issue_start_row.txt"
    #fif = open(issue_file,"rt")
    done = False
    list_rows = []

    # Prepare queue
    q = Queue()
    for i in range(nb_threads):
        t=Thread(target=process_batch_worker)
        t.daemon=True
        t.start()

    while not done:
        try:
            with pool.connection() as connection:
                tab_escorts_images = connection.table(tab_escorts_images_name)
                # to do filter to select only columns needed
                for one_row in tab_escorts_images.scan(row_start=last_row):
                #for one_row in tab_aaron.scan(row_start=fif.readline()):
                    row_count += 1
                    list_rows.append(one_row)
                    has_slept = False
                    if row_count%(batch_size)==0:
                        while q.qsize()>nb_threads+2:
                            print "Queue seems quite full. Waiting 30 seconds."
                            sys.stdout.flush()
                            time.sleep(30)
                            has_slept = True
                        print "Pushing batch starting from row {}.".format(list_rows[0][0])
                        print "Scanned {} rows so far (misssing sha1: {}).".format(row_count,missing_sha1_count)
                        q.put((list_rows,row_count))
                        last_row = list_rows[-1][0]
                        list_rows = []
                        sys.stdout.flush()
                        # should we break after sleeping? scan may have timed out...
                        if has_slept:
                            break
                if has_slept:
                    raise ValueError("Waited to long. Just restart scanning with new connection to avoid error.") 
                done = True
                if list_rows:
                    # push last batch
                    print "Pushing batch starting from row {}.".format(list_rows[0][0])
                    print "Scanned {} rows so far (misssing sha1: {}).".format(row_count,missing_sha1_count)
                    q.put((list_rows,row_count))
                    list_rows = []
                    sys.stdout.flush()
        except Exception as inst:
            print "[Caught error] {}\n".format(inst)
            exc_type, exc_obj, exc_tb = sys.exc_info()  
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print "{} in {} line {}.\n".format(exc_type, fname, exc_tb.tb_lineno)
            time.sleep(2)
            # Should we reinitialize the pool?
            pool = happybase.ConnectionPool(size=nb_threads,host='10.1.94.57',timeout=hbase_conn_timeout)
            sha1_tools.pool = pool
        if done:
            q.join()
            tel = time.time()-start_time
            print "Scanned {} rows total (misssing sha1: {}). Average time per row is: {}. Total time is: {}.".format(row_count,missing_sha1_count,tel/row_count,tel)
