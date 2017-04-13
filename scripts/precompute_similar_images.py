import os
import sys
import time
import datetime
import happybase

sys.path.append('..')
import cu_image_search
from cu_image_search.search import searcher_hbaseremote


def get_week(today=datetime.datetime.now()):
    return today.strftime("%W")


def get_year(today=datetime.datetime.now()):
    return today.strftime("%Y")


def get_week_year(today=datetime.datetime.now()):
    week = get_week(today)
    year = get_year(today)
    return week, year


def check_indexed_noprecomp(searcher, list_sha1s):
    columns_check = [searcher.indexer.cu_feat_id_column, searcher.indexer.precomp_sim_column]
    rows = searcher.indexer.get_columns_from_sha1_rows(list_sha1s, columns=columns_check)
    not_indexed_sha1s = []
    precomp_sim_sha1s = []
    for row in rows:
        #print row
        # check searcher.indexer.cu_feat_id_column exists
        if searcher.indexer.cu_feat_id_column not in row[1]:
            not_indexed_sha1s.append(str(row[0]))
        # check searcher.indexer.precomp_sim_column does not exist
        if searcher.indexer.precomp_sim_column in row[1]:
            precomp_sim_sha1s.append(str(row[0]))
    valid_sha1s = list(set(list_sha1s) - set(not_indexed_sha1s) - set(precomp_sim_sha1s))
    msg = "{} valid sha1s, {} not indexed sha1s, {} already precomputed similarities sha1s."
    print("[check_indexed_noprecomp: log] "+msg.format(len(valid_sha1s), len(not_indexed_sha1s), len(precomp_sim_sha1s)))
    return valid_sha1s, not_indexed_sha1s, precomp_sim_sha1s


def read_sim_precomp(simname, nb_query, searcher):
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
            nums = searcher.filter_near_dup(nums, searcher.near_dup_th)
            #print nums
            onum = len(nums)/2
            n = onum
            #print n
            if onum==0: # no returned images, e.g. no near duplicate
                sim.append(())
                sim_score.append([])
                continue
            # get the sha1s of similar images
            sim_infos = [searcher.indexer.sha1_featid_mapping[int(i)] for i in nums[0:n]]
            # beware, need to make sure sim and sim_score are still aligned
            #print("[read_sim] got {} sim_infos from {} samples".format(len(sim_infos), n))
            sim.append(sim_infos)
            sim_score.append(nums[onum:onum+n])
            count = count + 1
            if count == nb_query:
                break
        f.close()
    return sim, sim_score
    

def format_batch_sim(simname, valid_sha1s, corrupted, searcher):
    nb_query = len(valid_sha1s) - len(corrupted)
    sim, sim_score = read_sim_precomp(simname, nb_query, searcher)
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
        batch_mark_precomp_sim.append((sha1,{searcher.indexer.precomp_sim_column: 'True'}))
        i_img += 1
    #print batch_sim
    #print batch_mark_precomp_sim
    return batch_sim, batch_mark_precomp_sim


def process_one_update(searcher):
    update_id, str_list_sha1s = searcher.indexer.get_next_batch_precomp_sim()
    if update_id:
        start_precomp = time.time()
        print("[process_one_update: log] Processing update {}".format(update_id))
        #print str_list_sha1s
        # mark info:precomp_started in escorts_images_updates_dev
        searcher.indexer.write_batch([(update_id, {searcher.indexer.precomp_start_marker: 'True'})], searcher.indexer.table_updateinfos_name)

        # check that sha1s of batch have no precomputed similarities already in sha1_infos table
        valid_sha1s, not_indexed_sha1s, precomp_sim_sha1s = check_indexed_noprecomp(searcher, str_list_sha1s.split(','))

        # precompute similarities using searcher 
        simname, corrupted = searcher.search_from_sha1_list_get_simname(valid_sha1s, update_id)
        #print corrupted
        # format for saving in HBase:
        # - batch_sim: should be a list of sha1 row key, dict of "s:similar_sha1": dist_value
        # - batch_mark_precomp_sim: should be a list of sha1 row key, dict of precomp_sim_column: True
        batch_sim, batch_mark_precomp_sim = format_batch_sim(simname, valid_sha1s, corrupted, searcher)

        # push similarities to HBI_table_sim (escorts_images_similar_row_dev) using searcher.indexer.write_batch
        searcher.indexer.write_batch(batch_sim, searcher.indexer.table_sim_name)
        # push to weekly update table for Amandeep to integrate in DIG
        week, year = get_week_year()
        weekly_sim_table_name = searcher.indexer.table_sim_name+"_Y{}W{}".format(year, week)
        print "[process_one_update: log] weekly table name: {}".format(weekly_sim_table_name)
        weekly_sim_table = searcher.indexer.get_create_table(weekly_sim_table_name, families={'s': dict()})
        # check if weekly_sim_table_name is not none?
        # push similarities to an hbase table HBI_table_sim+"_Y{}W{}".format(year, week) for Amandeep?
        searcher.indexer.write_batch(batch_sim, weekly_sim_table_name)

        # mark precomp_sim true in escorts_images_sha1_infos_dev
        searcher.indexer.write_batch(batch_mark_precomp_sim, searcher.indexer.table_sha1infos_name)
        # mark info:precomp_finish in escorts_images_updates_dev
        if not corrupted: # do not mark finished if we faced some issue? mark as corrupted?
            searcher.indexer.write_batch([(update_id, {searcher.indexer.precomp_end_marker: 'True'})], searcher.indexer.table_updateinfos_name)
        print("[process_one_update: log] Processed update {} in {}s.".format(update_id, time.time() - start_precomp))
        # TODO clean up
        if simname:
            # remove simname 
            os.remove(simname)
            # remove features file
            featfirst = simname.split('-')[0]
            featfn = featfirst+'.dat'
            #print "[process_one_update: log] Removing file {}".format(featfn)
            os.remove(featfn)


if __name__ == "__main__":
    
    week, year = get_week_year()
    print week,year

    """ Run precompute similar images based on `conf_file` given as parameter
    """
    if len(sys.argv)<2:
        print "python precompute_similar_images.py conf_file"
        exit(-1)
    global_conf_file = sys.argv[1]
    searcher = searcher_hbaseremote.Searcher(global_conf_file)
    process_one_update(searcher)

    
    