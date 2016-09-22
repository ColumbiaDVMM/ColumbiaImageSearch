import os
import sys
import time
import datetime
sys.path.append('..')
import cu_image_search
from cu_image_search.update import updater_hbase

lasttime = 0
interval = 60

if __name__=="__main__":
    """ Run updater based on `conf_file` given as parameter
    """
    if len(sys.argv)<2:
        print "python update.py conf_file"
        exit(-1)
    global_conf_file = sys.argv[1]
    up_obj = updater_hbase.Updater(global_conf_file)
    while True:
        try:
            ctime = time.time()
            time_lapse = ctime-lasttime
            if time_lapse < interval:
                print 'sleep for',interval-time_lapse, 'seconds...'
                time.sleep(interval-time_lapse)
            lasttime = time.time()
            up_obj.run_update()
        except Exception as inst:
            print "Update failed at {} with error {}.".format(datetime.now(),inst)

