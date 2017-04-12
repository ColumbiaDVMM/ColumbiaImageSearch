import os
import sys
import time
import datetime
import happybase

sys.path.append('..')
import cu_image_search
from cu_image_search.update import updater_hbase
from cu_image_search.search import searcher_hbaseremote



def get_week(today=datetime.datetime.now()):
    return today.strftime("%W")

def get_year(today=datetime.datetime.now()):
    return today.strftime("%Y")

def get_week_year(today=datetime.datetime.now()):
    week = get_week(today)
    year = get_year(today)
    return week, year

def check_indexed_noprecomp(up_obj, list_sha1s):
    columns_check = [up_obj.indexer.cu_feat_id_column, up_obj.indexer.precomp_sim_column]
    rows = up_obj.indexer.get_columns_from_sha1_rows(list_sha1s, columns=columns_check)
    not_indexed_sha1s = []
    precomp_sim_sha1s = []
    for row in rows:
    	#print row
        # check up_obj.indexer.cu_feat_id_column exists
        if up_obj.indexer.cu_feat_id_column not in row[1]:
            not_indexed_sha1s.append(str(row[0]))
        # check up_obj.indexer.precomp_sim_column does not exist
        if up_obj.indexer.precomp_sim_column in row[1]:
            precomp_sim_sha1s.append(str(row[0]))
    valid_sha1s = list(set(list_sha1s) - set(not_indexed_sha1s) - set(precomp_sim_sha1s))
    msg = "{} valid sha1s, {} not indexed sha1s, {} already precomputed similarities sha1s."
    print("[check_indexed_noprecomp: log] "+msg.format(len(valid_sha1s), len(not_indexed_sha1s), len(precomp_sim_sha1s)))
    return valid_sha1s, not_indexed_sha1s, precomp_sim_sha1s

def process_one_update(up_obj, searcher):
    update_id, str_list_sha1s = up_obj.indexer.get_next_batch_precomp_sim()
    if update_id:
    	print("[process_one_update: log] Processing update {}".format(update_id))
    	#print str_list_sha1s
        # mark info:precomp_started in escorts_images_updates_dev
        #up_obj.indexer.write_batch([(update_id, {up_obj.indexer.precomp_started: 'True'})], up_obj.indexer.table_updateinfos_name)

        # check that sha1s of batch have no precomputed similarities already in sha1_infos table
        valid_sha1s, not_indexed_sha1s, precomp_sim_sha1s = check_indexed_noprecomp(up_obj, str_list_sha1s.split(','))

        # precompute similarities using searcher 
        # [need to finalize search from sha1 method search_from_sha1_list, just precomp part of search_from_image_filenames]
        outp, outputname = searcher.search_from_sha1_list(valid_sha1s, update_id)
        print outp
        print outputname
        # push similarities to escorts_images_similar_row_dev
        # push similarities to an hbase table escorts_images_similar_row_dev_2017_weekXX for Amandeep?
        # mark precomp_sim true in escorts_images_sha1_infos_dev

        # mark info:precomp_finish in escorts_images_updates_dev
        #up_obj.indexer.write_batch([(update_id, {up_obj.indexer.precomp_finished: 'True'})], up_obj.indexer.table_updateinfos_name)


if __name__ == "__main__":
    
    week, year = get_week_year()
    print week,year

    """ Run precompute similar images based on `conf_file` given as parameter
    """
    if len(sys.argv)<2:
        print "python precompute_similar_images.py conf_file"
        exit(-1)
    global_conf_file = sys.argv[1]
    # we have two indexers then...
    up_obj = updater_hbase.Updater(global_conf_file)
    searcher = searcher_hbaseremote.Searcher(global_conf_file)
    process_one_update(up_obj, searcher)

    
    