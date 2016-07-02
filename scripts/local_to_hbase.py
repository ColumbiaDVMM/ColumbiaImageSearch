import sys
sys.path.append('..')
import base64
import happybase
import numpy as np

from cu_image_search.indexer.local_indexer import LocalIndexer
from cu_image_search.memex_tools.sha1_tools import get_SHA1_from_file

nb_threads = 2
pool = happybase.ConnectionPool(size=nb_threads,host='10.1.94.57')

def prepare_batch(list_ids,unique_ids,res,dup_ids):
    for i,one_id in enumerate(list_ids):
        try:
            tmp_feat = res[0][0][res[0][1].index(i)]
            tmp_feat_b64 = base64.b64encode(tmp_feat)
            tmp_hashcode = res[1][0][res[1][1].index(i)]
            tmp_hashcode_b64 = base64.b64encode(tmp_hashcode)
            tmp_sha1 = unique_ids[i][0]
            tmp_url = unique_ids[i][2]
            all_htids = [x[0] for x in dup_ids if x[1]==tmp_sha1]
            batch.append((tmp_sha1,{'info:s3_url': tmp_url,\
                                    'info:featnorm_cu': tmp_feat_b64,\
                                    'info:hash256_cu': tmp_hashcode_b64,\
                                    'info:all_htids': ','.join(all_htids)}))
        except Exception as inst:
            print "[prepare_batch: error] {}".format(inst)
    return batch

def write_batch(batch,tab_out_name):
    with pool.connection() as connection:
        tab_out = connection.table(tab_out_name)
        batch_write = tab_out.batch()
        print "Pushing batch from sha1 {}.".format(batch[0][0])
        for row in batch:
            batch_write.put(row[0],row[1])
        batch_write.send()


if __name__=="__main__":
    LI = LocalIndexer('../../conf/global_var_localmysql.json')
    tab_out_name = 'escorts_images_sha1_infos_ext'    
    #max_uid = LI.get_max_unique_id()
    max_uid = 22
    print "We have {} unique images to push.".format(max_uid)

    start = 0
    batch_size = 10
    list_type = ["feats","hashcodes"]
    while start<max_uid:
        list_ids = [start+1:min(max_uid,start+batch_size)]
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
        start = unique_ids[-1][-1]
