import os
import sys
import grp
import pwd
import time
import shutil
import datetime
sys.path.append('..')
sys.path.append('../..')
import cu_image_search
from cu_image_search.search import searcher_mysqllocal, searcher_hbaseremote

if __name__=="__main__":
    """ Run search based on `conf_file` and `image_list` given as parameter
    """
    if len(sys.argv)<3:
        print "python search.py global_conf_file image_list [outputname]"
        exit(-1)
    sys.stdout = open('tmp_log_search.txt', 'w')
    sys.stderr = sys.stdout
    global_conf_file = sys.argv[1]
    image_list = sys.argv[2]
    final_outputname = None
    if len(sys.argv) == 4:
        final_outputname = sys.argv[3]
    #search_obj = searcher_mysqllocal.Searcher(global_conf_file)
    print '[search] Creating searcher from configuration {}'.format(global_conf_file)
    search_obj = searcher_hbaseremote.Searcher(global_conf_file)
    start_time = time.time()
    print '[search] Searching for images in {}'.format(image_list)
    outputname = search_obj.search_image_filelist(image_list)
    if final_outputname:
        print '[search] Moving {} to {}.'.format(outputname, final_outputname)
        shutil.move(outputname, final_outputname)
        os.chown(final_outputname, pwd.getpwnam("www-data").pw_uid, grp.getgrnam("www-data").gr_gid)
    else:
        final_outputname = outputname
    print '[search] final_outputname is {}'.format(final_outputname)
    print '[search] Query time: ', time.time() - start_time
    sys.stdout.close()
