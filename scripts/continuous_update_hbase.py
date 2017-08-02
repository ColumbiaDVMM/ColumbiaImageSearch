import os
import sys
import time
import json
import subprocess
from datetime import datetime
sys.path.append(os.path.join(os.path.dirname(__file__),'..'))
from cu_image_search.update import updater, updater_hbase

lasttime = 0
interval = 300

if __name__=="__main__":
    """ Run updater based on `conf_file` given as parameter
    """
    if len(sys.argv)<2:
        print "python continuous_update_hbase.py conf_file"
        exit(-1)

    global_conf_file = sys.argv[1]
    up_obj = None
    conf = json.load(open(global_conf_file, 'rt'))
    cache_path = os.path.join('/home/ubuntu/memex/ColumbiaImageSearch', 'cache.sh')
    if 'UP_updater' in conf:
        if conf['UP_updater'] == "updater":
            up_obj = updater.Updater(global_conf_file)
    if up_obj is None:
        up_obj = updater_hbase.Updater(global_conf_file)

    while True:
        print "[continuous_update_hbase] looping..."
        sys.stdout.flush()
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
                    proc = subprocess.Popen(cache_path, stdout=subprocess.PIPE)
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
            print "Update failed at {} with error {}.".format(datetime.now(), inst)
            sys.stdout.flush()

