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
tab_aaron_name = 'aaron_memex_ht-images'
tab_hash_name = 'image_hash'
tab_missing_sha1_name = 'ht-images_missing_sha1'
tab_missing_sim_name = 'ht-images_missing_sim_images'
nb_threads = 12
pool = happybase.ConnectionPool(size=nb_threads,host='10.1.94.57',timeout=hbase_conn_timeout)
sha1_tools.pool = pool
global_var = json.load(open('../../conf/global_var_all.json'))
sha1_tools.global_var = global_var
sha1_tools.tab_aaron_name = tab_aaron_name
row_count = 0
missing_sha1_count = 0
missing_sim_count = 0

batch_size = 10000

### fill sha1 sim in aaron_memex_ht-images
# scan aaron_memex_ht-images
# get sha1 of row-key
# get sha1 for each image in meta:columbia_near_dups
# compact meta:columbia_near_dups into meta:columbia_near_dups_sha1 and maintain distances info of corresponding images from meta:columbia_near_dups_dist in meta:columbia_near_dups_sha1_dist

def process_one_row(one_row):
    global missing_sha1_count,missing_sim_count
    # should indicate row being already processed
    if 'meta:sha1' in one_row[1].keys():
        return
    row_sha1, from_url = get_row_sha1(one_row)
    if not row_sha1:
        #print "Could not get sha1 for image_id {}.".format(one_row[0])
        missing_sha1_count += 1
        # push to missig sha1
        sha1_tools.save_missing_SHA1_to_hbase_missing_sha1([one_row[0]],tab_missing_sha1_name)
        return
    if from_url:
        #print "Computed new sha1 for image_id {}.".format(one_row[0])
        # push to image_hash
        sha1_tools.save_SHA1_to_hbase_imagehash(one_row[0],row_sha1,tab_hash_name)
    # add sha1 to row
    if 'meta:columbia_near_dups' not in one_row[1].keys():
        #print "Similar images not computed for image_id {}.".format(one_row[0])
        missing_sim_count += 1
        save_missing_sim_images(one_row[0])
        return
    sim_image_ids = [str(x) for x in one_row[1]['meta:columbia_near_dups'].split(',')]
    sim_sha1s, missing_sim_iids, new_sha1s = sha1_tools.get_batch_SHA1_from_imageids(sim_image_ids)
    dists = one_row[1]['meta:columbia_near_dups_dist'].split(',')
    if new_sha1s:
        sha1_tools.save_batch_SHA1_to_hbase_image_hash(new_sha1s,tab_hash_name)
    if missing_sim_iids:
        # push missing sha1
        sha1_tools.save_missing_SHA1_to_hbase_missing_sha1(missing_sim_iids,tab_missing_sha1_name)
        # realign dists
        dists = [d for d,i in enumerate(dists) if sim_image_ids[i] not in missing_sim_iids]
    unique_sim_sha1s, sim_sha1s_pos = np.unique(sim_sha1s,return_index=True)                
    #print row_count, one_row[0], row_sha1, from_url, sim_sha1s, missing_sim_sha1s, new_sha1s
    dists = np.asarray([np.float32(x) for x in dists])
    sim_sha1s_sorted_pos = np.argsort(dists[sim_sha1s_pos])
    with pool.connection() as connection:
        tab_aaron = connection.table(tab_aaron_name)
        tab_aaron.put(one_row[0],{'meta:sha1': str(row_sha1), 'meta:columbia_near_dups_sha1': ','.join([str(x) for x in list(unique_sim_sha1s[sim_sha1s_sorted_pos])]), 'meta:columbia_near_dups_sha1_dist': ','.join([str(x) for x in list(dists[sim_sha1s_pos[sim_sha1s_sorted_pos]])])})
    return

def process_batch_rows(list_rows):
    for row in list_rows:
        process_one_row(one_row)

def process_batch_worker():
    while True:
        batch_start = time.time()
        tupInp = q.get()
        process_batch_rows(tupInp[0])
        tel = time.time()-batch_start
        print "Batch from row {} (count: {}) done in: {}.".format(tupInp[0][0][0],tupInp[1],tel)
        q.task_done()

def save_missing_sim_images(image_id,tab_missing_sim_name=tab_missing_sim_name):
    with pool.connection(timeout=hbase_conn_timeout) as connection:
        tab_missing_sim = connection.table(tab_missing_sim_name)
        tab_missing_sim.put(str(image_id), {'info:missing_sim': image_id})

def get_row_sha1(row):
    row_sha1 = sha1_tools.get_SHA1_from_hbase_imagehash(row[0])
    from_url = False
    if not row_sha1 and 'meta:location' in row[1].keys():
        row_sha1 = sha1_tools.get_SHA1_from_URL(row[1]['meta:location'])
        from_url = True
    return row_sha1, from_url

if __name__ == '__main__':
    start_time = time.time()
    last_row = None
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
                tab_aaron = connection.table(tab_aaron_name)
                # to do filter to select only columns needed
                for one_row in tab_aaron.scan(row_start=last_row):
                    row_count += 1
                    list_rows.append(one_row)
                    if row_count%(batch_size)==0:
                        while q.qsize()>nb_threads*2:
                            print "Queue seems quite full. Waiting 10 seconds."
                            time.sleep(10)
                        print "Scanned {} rows so far. Pushing batch starting from row {}.".format(row_count,list_rows[0][0])
                        q.put((list_rows,row_count))
                        list_rows = []
                        sys.stdout.flush()
                done = True
                if list_rows:
                    # push last batch
                    print "Scanned {} rows so far. Pushing batch starting from row {}.".format(row_count,list_rows[0][0])
                    q.put((list_rows,row_count))
                    list_rows = []
                    sys.stdout.flush()
        except Exception as inst:
            print "[Caught error] {}\n".format(inst)
            exc_type, exc_obj, exc_tb = sys.exc_info()  
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print "{} in {} line {}.\n".format(exc_type, fname, exc_tb.tb_lineno)
            time.sleep(2)
        q.join()
        tel = time.time()-start_time
        print "Scanned {} rows total (misssing sha1: {}, sim: {}). Average time per row is: {}. Total time is: {}.".format(row_count,missing_sha1_count,missing_sim_count,tel/row_count,tel)
