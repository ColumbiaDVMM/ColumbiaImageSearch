import MySQLdb
import happybase
import json
import time
import os
import sys
sys.path.insert(0, os.path.abspath('../memex_tools'))
import sha1_tools
hbase_conn_timeout = None
pool = happybase.ConnectionPool(size=6,host='10.1.94.57',timeout=hbase_conn_timeout)
sha1_tools.pool = pool
global_var = json.load(open('../../conf/global_var_all.json'))
sha1_tools.global_var = global_var

connection = happybase.Connection(host='10.1.94.57')
tab_hash = connection.table('image_hash')
tab_aaron = connection.table('aaron_memex_ht-images')
batch_size = 100

### fill sha1 sim in aaron_memex_ht-images
# scan aaron_memex_ht-images
# get sha1 of row-key
# get sha1 for each image in meta:columbia_near_dups
# compact meta:columbia_near_dups into meta:columbia_near_dups_sha1 and maintain distances info of corresponding images from meta:columbia_near_dups_dist in meta:columbia_near_dups_sha1_dist

def get_row_sha1(row):
    row_sha1 = sha1_tools.get_SHA1_from_hbase_imagehash(row[0])
    from_url = False
    if not row_sha1:
        row_sha1 = sha1_tools.get_SHA1_from_URL(row[1]['meta:location'])
        from_url = True
    return row_sha1, from_url

if __name__ == '__main__':
    start_time = time.time()
    last_row = None
    row_count = 0

    # # Prepare queue
    # q = Queue()
    # for i in range(nb_threads):
    #     t=Thread(target=worker)
    #     t.daemon=True
    #     t.start()

    try:
        with pool.connection() as connection:
            tab_aaron = connection.table('aaron_memex_ht-images')
            for one_row in tab_aaron.scan(row_start=last_row):
                row_count = row_count+1
                row_sha1, from_url = get_row_sha1(row)
                sim_sha1s, missing_sim_sha1s, new_sha1s = sha1_tools.get_batch_SHA1_from_imageids([str(x) for x in row[1]['meta:columbia_near_dups'].split(',')])
                print row_count, row[0], row_sha1, from_url, sim_sha1s, missing_sim_sha1s, new_sha1s
                if row_count%(batch_size/10)==0:
                    print "Scanned {} rows so far.".format(row_count)
                    sys.stdout.flush()
                    time.sleep(60)
    except Exception as inst:
        print "[Caught error] {}\n".format(inst)
        exc_type, exc_obj, exc_tb = sys.exc_info()  
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print "{} in {} line {}.\n".format(exc_type, fname, exc_tb.tb_lineno)
        time.sleep(2)


