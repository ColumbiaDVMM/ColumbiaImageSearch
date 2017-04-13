import os
import sys
import time
import datetime
import happybase

sys.path.append('..')
import cu_image_search
from cu_image_search.search import searcher_hbaseremote

time_sleep = 60

def write_subsampled_update(subsampled_update_id, subsample_list_sha1s, searcher):
    load = dict()
    load[searcher.indexer.index_start_marker] = 'True'
    load[searcher.indexer.index_end_marker] = 'True'
    load[searcher.indexer.batch_list_sha1_column] = ','.join(subsample_list_sha1s)
    searcher.indexer.write_batch([(subsampled_update_id, load)], searcher.indexer.table_updateinfos_name)
    print "[subsampler: log] Writing subsampled update {}".format(subsampled_update_id)
    sys.stdout.flush()
            

def subsampler(global_conf_file, nb_subsamples=50, limit_batches=100):
    # set limit_batches to None to go on forever
    searcher_subsampler = searcher_hbaseremote.Searcher(global_conf_file)
    print "[subsampler: log] Subsampler ready."
    
    total_batches = 0
    need_break = False

    while True:
        if need_break:
            break

        start_get_batch = time.time()
        nb_list_sha1s = 0
        while nb_list_sha1s<=nb_subsamples:
            update_id, str_list_sha1s = searcher_subsampler.indexer.get_next_batch_precomp_sim()
            list_sha1s = str_list_sha1s.split(',')
            nb_list_sha1s = len(list_sha1s)
        
        if update_id is None:
            print "[subsampler: log] No more update to process."
            sys.stdout.flush()
            break
        else:
            print "[subsampler: log] Got update {} in {}s".format(update_id, time.time() - start_get_batch)
            sys.stdout.flush()
        
            start_subsample = time.time()
            subsample_id = 0
            subsample_list_sha1s = []
            
            for i,sha1 in enumerate(list_sha1s):
                if i%nb_subsamples==0 and i>0:
                    subsampled_update_id = update_id+"_"+str(subsample_id)
                    write_subsampled_update(subsampled_update_id, subsample_list_sha1s, searcher_subsampler)
                    subsample_id += 1
                    total_batches += 1
                    if limit_batches is not None and total_batches>limit_batches:
                        need_break = True
                        break
                    subsample_list_sha1s = [sha1]
                else:
                    subsample_list_sha1s.append(sha1)
            if len(subsample_list_sha1s)>0:
                subsampled_update_id = update_id+"_"+str(subsample_id)
                write_subsampled_update(subsampled_update_id, subsample_list_sha1s, searcher_subsampler)
                total_batches += 1
                if limit_batches is not None and total_batches>limit_batches:
                    need_break = True
                    
        if not need_break:
            print "[subsampler: log] Sleeping {}s before getting next update.".format(time_sleep)     
            time.sleep(time_sleep)
            

if __name__ == "__main__":
    
    """ Run precompute similar images based on `conf_file` given as parameter
    """
    if len(sys.argv)<2:
        print "python precompute_similar_images_parallel.py conf_file"
        exit(-1)
    global_conf_file = sys.argv[1]
    
    subsampler(global_conf_file, limit_batches=1000)
    
    
    