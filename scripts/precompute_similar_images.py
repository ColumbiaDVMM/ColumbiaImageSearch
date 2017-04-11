import os
import sys
import time
import datetime
import happybase

sys.path.append('..')
import cu_image_search
from cu_image_search.update import updater_hbase

def get_week(today=datetime.datetime.now()):
	return today.strftime("%W")

def get_year(today=datetime.datetime.now()):
	return today.strftime("%Y")

def get_week_year(today=datetime.datetime.now()):
	week = get_week(today)
	year = get_year(today)
	return week, year

def process_one_update(up_obj):
	update_id, str_list_sha1s = up_obj.indexer.get_next_batch_precomp_sim()
	if update_id:
		# mark info:precomp_started in escorts_images_updates_dev
		up_obj.indexer.write_batch([(update_id, {up_obj.indexer.precomp_started: 'True'})], up_obj.indexer.table_updateinfos_name)
		
		# precompute similarities using searcher [need to write a search from sha1 method, just precomp part of search_from_image_filenames]
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
    up_obj = updater_hbase.Updater(global_conf_file)

    
    