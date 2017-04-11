import os
import sys
import time
import subprocess
from datetime import datetime
sys.path.append('..')
import cu_image_search
from cu_image_search.update import updater_hbase

lasttime = 0
interval = 300

if __name__=="__main__":
    """ Run updater based on `conf_file` given as parameter
    """
    if len(sys.argv)<2:
        print "python continuous_update_hbase.py conf_file"
        exit(-1)
    global_conf_file = sys.argv[1]
    up_obj = updater_hbase.Updater(global_conf_file)
    while True:
        try:
            ctime = time.time()
            time_lapse = ctime - lasttime
            if time_lapse < interval:
                if up_obj.has_indexed:
                    print('[continuous_update_hbase] Update took {} seconds.'.format(time.time()-lasttime))
                    # we should refresh the API, only call that when totally finished update?
                    print('[continuous_update_hbase] Refreshing the API.')
                    up_obj.refresh_API()
                    print('[continuous_update_hbase] Refreshed the API.')
                    # need to call ../cache.sh also
                    print('[continuous_update_hbase] Refreshing cache.')
                    proc = subprocess.Popen('../cache.sh', stdout=subprocess.PIPE)
                    (out, err) = proc.communicate()
                    print('[continuous_update_hbase] Refreshed cache. output was: {}, error was: {}'.format(out, err))
                    up_obj.has_indexed = False
                else:
                    print('[continuous_update_hbase] Sleeping for {} seconds...'.format(interval-time_lapse))
                    sys.stdout.flush()
                    time.sleep(interval-time_lapse)
            lasttime = time.time()
            up_obj.run_update()
        except Exception as inst:
            print "Update failed at {} with error {}.".format(datetime.now(),inst)

