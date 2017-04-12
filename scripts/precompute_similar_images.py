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


def read_sim_precomp(simname, up_obj, near_dup_th):
    # intialization
    sim = []
    sim_score = []
    
    if simname is not None:
        # read similar images
        count = 0
        f = open(simname);
        for line in f:
            #sim_index.append([])
            nums = line.replace(' \n','').split(' ')
            #filter near duplicate here
            nums = self.filter_near_dup(nums, near_dup_th)
            #print nums
            onum = len(nums)/2
            n = min(self.sim_limit,onum)
            #print n
            if n==0: # no returned images, e.g. no near duplicate
                sim.append(())
                sim_score.append([])
                continue
            # get the sha1s of similar images
            sim_infos = [up_obj.indexer.sha1_featid_mapping[int(i)] for i in nums[0:n]]
            # beware, need to make sure sim and sim_score are still aligned
            print("[read_sim] got {} sim_infos from {} samples".format(len(sim_infos), n))
            sim.append(sim_infos)
            sim_score.append(nums[onum:onum+n])
            count = count + 1
            if count == nb_query:
                break
        f.close()
    return sim, sim_score

    

def format_batch_sim(simname, valid_sha1s, corrupted, up_obj, near_dup_th):
    sim, sim_score = read_sim_precomp(simname, up_obj, near_dup_th)
    # batch_sim: should be a list of sha1 row key, dict of all "s:similar_sha1": dist_value
    batch_sim = []
    # batch_mark_precomp_sim: should be a list of sha1 row key, dict of precomp_sim_column: True
    batch_mark_precomp_sim = []
    if len(sim) != len(valid_sha1s) or len(sim_score) != len(valid_sha1s):
    	print "[format_batch_sim: warning] similarities and queries count are different."
    	print "[format_batch_sim: warning] corrupted is: {}.".format(corrupted)
    # deal with corrupted
    i_img = 0
    for i,sha1 in enumerate(valid_sha1s):
    	if sha1 in corrupted:
    		continue
    	sim_columns = dict()
        for i_sim,sim_img in enumerate(sim[i_img]):
            sim_columns["s:"+str(sim_img)] = str(sim_score[i_img][i_sim])
        sim_row = (sha1, sim_columns)
        batch_sim.append(sim_row)
        batch_mark_precomp_sim.append((sha1,{up_obj.indexer.precomp_sim_column: 'True'}))
        i_img += 1
    print batch_sim
    print batch_mark_precomp_sim
    return batch_sim, batch_mark_precomp_sim


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
        # we don't need the formatting step (especially the s3 urls). 
        # we just need sha1q, sha1sim, dist. get simname and do something like in read_sim?
        simname, corrupted = searcher.search_from_sha1_list_get_simname(valid_sha1s, update_id)
        #print corrupted
        # format for saving in HBase:
        # - batch_sim: should be a list of sha1 row key, dict of "s:similar_sha1": dist_value
        # - batch_mark_precomp_sim: should be a list of sha1 row key, dict of precomp_sim_column: True
        batch_sim, batch_mark_precomp_sim = format_batch_sim(simname, valid_sha1s, corrupted, up_obj, searcher.near_dup_th)

        # push similarities to HBI_table_sim (escorts_images_similar_row_dev) using up_obj.indexer.write_batch
        up_obj.indexer.write_batch(batch_sim, up_obj.indexer.table_sim_name)
        # push to weekly update table for Amandeep to integrate in DIG
        week, year = get_week_year()
        weekly_sim_table_name = up_obj.indexer.table_sim_name+"_Y{}W{}".format(year, week)
        print "[process_one_update: log] weekly table name: {}".format(weekly_sim_table_name)
        weekly_sim_table = up_obj.indexer.get_create_table(weekly_sim_table_name, families={'s': dict()})
        # check if weekly_sim_table_name is not none?
        # push similarities to an hbase table HBI_table_sim+"_Y{}W{}".format(year, week) for Amandeep?
        up_obj.indexer.write_batch(batch_sim, weekly_sim_table_name)

        # mark precomp_sim true in escorts_images_sha1_infos_dev
        up_obj.indexer.write_batch(batch_mark_precomp_sim, up_obj.indexer.table_sha1infos_name)
        # mark info:precomp_finish in escorts_images_updates_dev
        if not corrupted: # do not mark finished if we faced some issue? mark as corrupted?
        	up_obj.indexer.write_batch([(update_id, {up_obj.indexer.precomp_finished: 'True'})], up_obj.indexer.table_updateinfos_name)
        # TODO clean up
        # remove simname and features file


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

    
    