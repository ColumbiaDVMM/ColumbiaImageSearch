import os
import sys
import time
import happybase

sys.path.append('..')
import cu_image_search
from cu_image_search.update import updater_hbase

batch_update_count = 0
batch_size = 100

if __name__=="__main__":
    """ Count number of updates to index based on `conf_file` given as parameter
    """
    if len(sys.argv)<2:
        print "python check_lag_update_hbase.py conf_file"
        exit(-1)
    global_conf_file = sys.argv[1]
    up_obj = updater_hbase.Updater(global_conf_file)

    connection = happybase.Connection(host=up_obj.indexer.hbase_host)
    table_updateinfos = connection.table(up_obj.indexer.table_updateinfos_name)
    # need to specify batch size to avoid timeout
    for row in table_updateinfos.scan(row_start='index_update_', row_stop='index_update_~', batch_size=batch_size):
        if "info:indexed" not in row[1]:
            batch_update_count += 1

    if batch_update_count > 0:
        print "We are lagging by {} update.".format(batch_update_count)
    else:
        print "We are up to date!"

