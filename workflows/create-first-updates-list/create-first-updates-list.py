import json
import time
import numpy as np
from pyspark import SparkContext, SparkConf

max_ts = 9999999999999

def get_list_value(json_x,field_tuple):
    return [x["value"] for x in json_x if x["columnFamily"]==field_tuple[0] and x["qualifier"]==field_tuple[1]]


def get_images_to_be_indexed(data):
    key = data[0]
    json_x = [json.loads(x) for x in data[1].split("\n")]
    try:
        s3url_list = get_list_value(json_x,("info","s3_url"))
        image_discarded_list = get_list_value(json_x,("info","image_discarded"))
        cu_feat_id_list = get_list_value(json_x,("info","cu_feat_id"))
        if s3url_list and s3url_list[0].rstrip().startswith("https://s3") and (not cu_feat_id_list and not image_discarded_list):
            return True
    except Exception as inst:
        print "[Error in get_images_to_be_indexed] key was {}. {}".format(key, inst)
    return False


def build_batch_rdd(batch_udpate):
    # batch_rdd should be created to be stored in hbase table update_infos
    update_id = "index_update_"+str(max_ts-int(time.time()*1000))+'_'+str(np.int32(np.random.random()*(10e6)))
    list_key = []
    for x in batch_udpate:
        list_key.append(x)
    batch_out = [(update_id, [update_id, "info", "list_sha1s", ','.join(list_key)])]
    return sc.parallelize(batch_out)


def create_first_updates(hbase_man_in, hbase_man_out, batch_update_size=10000):
    in_rdd = hbase_man_in.read_hbase_table()
    out_rdd = in_rdd.filter(get_images_to_be_indexed).keys()
    # can foreachPartition be called with a function and a paramater?
    #out_rdd.foreachPartition(save_batch_update,hbase_man_out)
    iterator = out_rdd.toLocalIterator()
    batch_udpate = []
    for x in iterator:
        batch_udpate.append(x)
        if len(batch_udpate)==batch_update_size:
            batch_rdd = build_batch_rdd(batch_udpate)
            hbase_man_out.rdd2hbase(batch_rdd)
            batch_udpate = []
    # last batch
    if batch_udpate:
        batch_rdd = build_batch_rdd(batch_udpate)
        hbase_man_out.rdd2hbase(batch_rdd)
            

if __name__ == '__main__':
    from hbase_manager import HbaseManager
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    tab_sha1_infos_name = job_conf["tab_sha1_infos"]
    tab_updates_name = job_conf["tab_updates"]
    hbase_host = job_conf["hbase_host"]
    sc = SparkContext(appName='create_first_updates_from_'+tab_sha1_infos_name+'_pushed_to_'+tab_updates_name)
    sc.setLogLevel("ERROR")
    conf = SparkConf()
    #columns = ["info:hash256_cu", "info:s3_url", "info:featnorm_cu", "info:image_discarded", "info:cu_feat_id"]
    # anyway features have been computed but not indexed?
    columns = ["info:s3_url", "info:image_discarded", "info:cu_feat_id"]
    hbase_man_in = HbaseManager(sc, conf, hbase_host, tab_sha1_infos_name, columns_list=columns)
    hbase_man_out = HbaseManager(sc, conf, hbase_host, tab_updates_name)
    create_first_updates(hbase_man_in, hbase_man_out)
