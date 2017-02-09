import os
import sys
import time
import traceback
from datetime import datetime

from hbase_indexer_fortest import HBaseIndexer

lasttime = 0
interval = 10

def run_update(hbase_indexer):
        """ Runs an update.
        """
        try:
            update_id, str_list_sha1s = hbase_indexer.get_next_batch()
            if update_id:
                list_sha1s = str_list_sha1s.split(',')
                print("[run_update: log] Update {} has {} images.".format(update_id, len(list_sha1s)))
                batch_indexed = hbase_indexer.index_batch_sha1(list_sha1s, update_id)
                if not batch_indexed:
                    print("[run_update: log] Update {} failed.".format(update_id))
                    return False
                return True
            else:
                print("[Updater.run_update: log] Nothing to update!")
                return False
        except Exception as inst:
            print "[Updater.run_udpate: error] {}".format(inst)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print "*** print_tb:"
            traceback.print_tb(exc_traceback, file=sys.stdout)
            print "*** print_exception:"
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            return False


if __name__=="__main__":
    """ Run update based on `conf_file` given as parameter
    """
    if len(sys.argv)<2:
        print "python test_hbase_timeout.py conf_file"
        exit(-1)

    global_conf_file = sys.argv[1]
    hbase_indexer = HBaseIndexer(global_conf_file)
    hbase_indexer.read_conf()
    while True:
        try:
            ctime = time.time()
            time_lapse = ctime - lasttime
            if time_lapse < interval:
                print('[test_hbase_timeout] sleeping for {} seconds...'.format(interval-time_lapse))
                sys.stdout.flush()
                time.sleep(interval-time_lapse)
            lasttime = time.time()
            update_ok = run_update(hbase_indexer)
            print('[test_hbase_timeout] Update took {} seconds.'.format(time.time()-lasttime))            
            if not update_ok:
                exit(-1)
        except Exception as inst:
            print "Update failed at {} with error {}.".format(datetime.utcnow(),inst)

