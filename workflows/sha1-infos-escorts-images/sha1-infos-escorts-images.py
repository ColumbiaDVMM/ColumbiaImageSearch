import json
import os
from pyspark import SparkContext, SparkConf


def get_list_value(json_x,field_tuple):
    return [x["value"] for x in json_x if x["columnFamily"]==field_tuple[0] and x["qualifier"]==field_tuple[1]]


def to_sha1_key(data):
    cdr_id = data[0]
    json_x = [json.loads(x) for x in data[1].split("\n")]
    sha1 = None
    obj_stored_url = None
    obj_parent = None
    try:
        sha1 = get_list_value(json_x,("info","sha1"))[0].strip()
        obj_stored_url = get_list_value(json_x,("info","obj_stored_url"))[0].strip()
        obj_parent = get_list_value(json_x,("info","obj_parent"))[0].strip()
    except Exception as inst2:
        pass
        #print "[Error] could not get SHA1, obj_stored_url or obj_parent for row {}. {}".format(cdr_id,inst2)
    if cdr_id and sha1 and obj_stored_url and obj_parent:
        return [(sha1, {"info:all_cdr_ids": [cdr_id], "info:s3_url": [obj_stored_url], "info:all_parent_ids": [obj_parent]})]
    return []


def reduce_sha1_infos_discarding(a,b):
    c = dict()
    if "info:image_discarded" in a or "info:image_discarded" in b:
        c["info:all_cdr_ids"] = []
        c["info:all_parent_ids"] = []
        c["info:image_discarded"] = 'discarded because has more than {} cdr_ids'.format(max_images)
    else:
        c["info:all_cdr_ids"] = a["info:all_cdr_ids"]+b["info:all_cdr_ids"]
        c["info:all_parent_ids"] = a["info:all_parent_ids"]+b["info:all_parent_ids"]
    if a["info:s3_url"] and a["info:s3_url"][0] and a["info:s3_url"][0]!=u'None':
        c["info:s3_url"] = a["info:s3_url"]
    else:
        c["info:s3_url"] = b["info:s3_url"]
    if len(c["info:all_cdr_ids"])>max_images or len(c["info:all_parent_ids"])>max_images:
        print("Discarding image with URL: {}".format(c["info:s3_url"][0]))
        c["info:all_cdr_ids"] = []
        c["info:all_parent_ids"] = []
        c["info:image_discarded"] = 'discarded because has more than {} cdr_ids'.format(max_images)
    return c


def split_sha1_kv_filter_max_images_discarded(x):
    fields_list = [("info","all_cdr_ids"), ("info","s3_url"), ("info","all_parent_ids")]
    out = []
    if "info:image_discarded" in x[1]:
        out.append((x[0], [x[0], "info", "image_discarded", x[1]["info:image_discarded"]]))
        out.append((x[0], [x[0], "info", "s3_url", str(x[1]["info:s3_url"][0])]))
        out.append((x[0], [x[0], "info", "all_cdr_ids", x[1]["info:image_discarded"]]))
        out.append((x[0], [x[0], "info", "all_parent_ids", x[1]["info:image_discarded"]]))
    else:
        for field in fields_list:
            out.append((x[0], [x[0], field[0], field[1], ','.join(x[1][field[0]+":"+field[1]])]))
    return out


def fill_sha1_infos(sc, hbase_man_in, hbase_man_out, nb_partitions):
    in_rdd = hbase_man_in.read_hbase_table()
    tmp_rdd = in_rdd.partitionBy(nb_partitions).flatMap(lambda x: to_sha1_key(x)).reduceByKey(reduce_sha1_infos_discarding)
    out_rdd = tmp_rdd.flatMap(lambda x: split_sha1_kv_filter_max_images_discarded(x))
    hbase_man_out.rdd2hbase(out_rdd)


if __name__ == '__main__':
    from hbase_manager import HbaseManager
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    tab_cdrid_name = job_conf["tab_cdrid_name"]
    hbase_host = job_conf["hbase_host"]
    tab_sha1_infos_name = job_conf["tab_sha1_infos_name"]
    max_images = job_conf["max_images"]
    nb_partitions = job_conf["nb_partitions"]
    sc = SparkContext(appName='sha1_infos_from_'+tab_cdrid_name+'_in_'+tab_sha1_infos_name+'_filter_gt_'+str(max_images))
    sc.setLogLevel("ERROR")
    conf = SparkConf()
    # Get only these column from table tab_cdrid_name
    #"info:sha1"
    #"info:obj_stored_url"
    #"info:obj_parent"
    in_columns_list = ["info:sha1", "info:obj_stored_url", "info:obj_parent"]
    hbase_man_in = HbaseManager(sc, conf, hbase_host, tab_cdrid_name, columns_list=in_columns_list)
    hbase_man_out = HbaseManager(sc, conf, hbase_host, tab_sha1_infos_name)
    fill_sha1_infos(sc, hbase_man_in, hbase_man_out, nb_partitions)
