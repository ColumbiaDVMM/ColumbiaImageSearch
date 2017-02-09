import sys
sys.path.append('../..')
import time
import base64
import happybase
import numpy as np

from cu_image_search.indexer.local_indexer import LocalIndexer
from cu_image_search.memex_tools.sha1_tools import get_SHA1_from_file

nb_threads = 2
pool = happybase.ConnectionPool(size=nb_threads,host='10.1.94.57')

def prepare_batch(list_ids,unique_ids,res,dup_ids):
    batch = []
    for i,one_id in enumerate(list_ids):
        #try:
            tmp_feat = res[0][0][res[0][1].index(i)]
            tmp_feat_b64 = base64.b64encode(tmp_feat)
            tmp_hashcode = res[1][0][res[1][1].index(i)]
            tmp_hashcode_b64 = base64.b64encode(tmp_hashcode)
            tmp_sha1 = unique_ids[i][0]
            tmp_url = unique_ids[i][2]
            all_htids = [str(x[0]) for x in dup_ids if x[1]==tmp_sha1]
            batch.append((tmp_sha1,{'info:s3_url': tmp_url,\
                                    'info:featnorm_cu': tmp_feat_b64,\
                                    'info:hash256_cu': tmp_hashcode_b64,\
                                    'info:all_htids': ','.join(all_htids)}))
        #except Exception as inst:
        #    print "[prepare_batch: error] Error for sample {} id {}. {}".format(i,one_id,inst)
    return batch


def check_batch(hb_batch,list_ids,unique_ids,res,dup_ids):
    for i,one_id in enumerate(list_ids):
        tmp_feat = res[0][0][res[0][1].index(i)]
        tmp_hashcode = res[1][0][res[1][1].index(i)]
        tmp_sha1 = unique_ids[i][0]
        tmp_feat_b64_hb = [x[1]['info:featnorm_cu'] for x in hb_batch if x[0]==tmp_sha1][0]
        tmp_hashcode_b64_hb = [x[1]['info:hash256_cu'] for x in hb_batch if x[0]==tmp_sha1][0]
        tmp_feat_hb = np.frombuffer(base64.b64decode(tmp_feat_b64_hb), dtype=np.float32)
        tmp_hashcode_hb = np.frombuffer(base64.b64decode(tmp_hashcode_b64_hb), dtype=np.uint8)
        print "Features difference for id {}: {}".format(one_id,np.sum(np.abs(tmp_feat-tmp_feat_hb)))
        print "Hashcodes difference for id {}: {}".format(one_id,np.sum(np.abs(tmp_hashcode-tmp_hashcode_hb)))

def write_batch(batch,tab_out_name):
    with pool.connection() as connection:
        tab_out = connection.table(tab_out_name)
        batch_write = tab_out.batch()
        print "Pushing batch from sha1 {}.".format(batch[0][0])
        for row in batch:
            batch_write.put(row[0],row[1])
        batch_write.send()

def get_batch_infos(unique_ids):
    sha1_list = [x[0] for x in unique_ids]
    with pool.connection() as connection:
        tab_out = connection.table(tab_out_name)
        batch = tab_out.rows(sha1_list)
        return batch

if __name__=="__main__":
    LI = LocalIndexer('../../conf/global_var_localmysql.json')
    tab_out_name = 'escorts_images_sha1_infos_ext'    
    max_uid = LI.get_max_unique_id()
    #max_uid = 22
    print "We have {} unique images to push.".format(max_uid)

    #start = 0
    start = 34299495
    batch_size = 10
    list_type = ["feats","hashcodes"]
    while start<max_uid:
        list_ids = range(start+1,min(max_uid,start+batch_size)+1)
        start_time = time.time()
        print "[{}] Working on batch from {} to {}.".format(start_time,list_ids[0],list_ids[-1])
        # sha1,htid,url,id
        unique_ids = LI.get_sha1s_htid_url_from_ids(list_ids)
        # [[[feats],[okIds],[[hashcodes],[okIds]]
        res = LI.get_precomp_from_ids(list_ids,list_type)
        # [(htids,sha1)]
        dup_ids = LI.get_all_dup_from_ids(list_ids)
        batch = prepare_batch(list_ids,unique_ids,res,dup_ids)
        write_batch(batch,tab_out_name)
        end_time = time.time()
        print "[{}] Batch from {} to {} done in {}s.".format(end_time,list_ids[0],list_ids[-1],end_time-start_time)
        hb_batch = get_batch_infos(unique_ids)
        check_batch(hb_batch,list_ids,unique_ids,res,dup_ids)
        start = unique_ids[-1][-1]
