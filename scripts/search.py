import os
import sys
import time
import datetime
sys.path.append('..')
import cu_image_search
from cu_image_search.search import searcher_mysqllocal, searcher_hbaseremote

if __name__=="__main__":
    """ Run search based on `conf_file` and `image_list` given as parameter
    """
    if len(sys.argv)<3:
        print "python search.py global_conf_file image_list"
        exit(-1)
    global_conf_file = sys.argv[1]
    image_list = sys.argv[2]
    #search_obj = searcher_mysqllocal.Searcher(global_conf_file)
    search_obj = searcher_hbaseremote.Searcher(global_conf_file)
    start_time = time.time()
    outputname = search_obj.search_image_list(image_list)
    print '[search] outputname is {}'.format(outputname)
    print '[search] Query time: ', time.time() - start_time

