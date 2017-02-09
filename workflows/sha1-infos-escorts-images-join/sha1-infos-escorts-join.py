import json
import os
from pyspark import SparkContext, SparkConf

fields_list = [("info","all_cdr_ids"), ("info","s3_url"), ("info","all_parent_ids")]


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
        #print key,URL_S3,type(URL_S3)
    except Exception as inst2:
        pass
        #print "[Error] could not get SHA1, obj_stored_url or obj_parent for row {}. {}".format(cdr_id,inst2)
    if cdr_id and sha1 and obj_stored_url and obj_parent:
        return [(sha1, {"info:all_cdr_ids": [cdr_id], "info:s3_url": [obj_stored_url], "info:all_parent_ids": [obj_parent]})]
    return []


def sha1_key_json(data):
    sha1 = data[0]
    json_x = [json.loads(x) for x in data[1].split("\n")]
    v = dict()
    for field in fields_list:
        v[':'.join(field)] = get_list_value(json_x,field)[0].strip()
    return [(sha1, v)]


def reduce_sha1_infos(a,b):
    c = dict()
    c["info:all_cdr_ids"] = a["info:all_cdr_ids"]+b["info:all_cdr_ids"]
    c["info:all_parent_ids"] = a["info:all_parent_ids"]+b["info:all_parent_ids"]
    if a["info:s3_url"] and a["info:s3_url"]!=u'None':
        c["info:s3_url"] = a["info:s3_url"]
    else:
        c["info:s3_url"] = b["info:s3_url"]
    return c


def reduce_sha1_infos_unique(a,b):
    # to be used with cogroup, deals with emtpy and potentially redundant dictionnary
    c = dict()
    if b: # we only care about update images
        if a: # merge case
            c["info:all_cdr_ids"] = list(set(a[0]["info:all_cdr_ids"]+b[0]["info:all_cdr_ids"]))
            c["info:all_parent_ids"] = list(set(a[0]["info:all_parent_ids"]+b[0]["info:all_parent_ids"]))
            if a[0]["info:s3_url"] and a[0]["info:s3_url"]!=u'None':
                c["info:s3_url"] = a[0]["info:s3_url"]
            else:
                c["info:s3_url"] = b[0]["info:s3_url"]
        else: # new image case
            c["info:all_cdr_ids"] = b[0]["info:all_cdr_ids"]
            c["info:all_parent_ids"] = b[0]["info:all_parent_ids"]
            c["info:s3_url"] = b[0]["info:s3_url"]
    return c


def split_sha1_kv_filter_max_images(x):
    # split based on max_images value
    out = []
    for field in fields_list:
        field_images = len(x[1][field[0]+":"+field[1]])
        if field_images>max_images:
            # just discard in this case
            return []
        else:
            out.append((x[0], [x[0], field[0], field[1], ','.join(x[1][field[0]+":"+field[1]])]))
    #out = [(x[0], [x[0], field[0], field[1], ','.join(x[1][field[0]+":"+field[1]])]) for field in fields_list]
    return out


def flatten_cogroup(x):
    out = []
    # at this point value is a tuple of two lists with a single or empty dictionary
    c = reduce_sha1_infos_unique(x[1][0],x[1][1])
    # check that result is a new or updated image
    if len(c.keys()) == len(fields_list):
        out.append((x[0], c))
    return out


def update_sha1_infos(sc, hbase_man_in, hbase_man_out):
    in_rdd = hbase_man_in.read_hbase_table()
    update_rdd = in_rdd.flatMap(lambda x: to_sha1_key(x)).reduceByKey(reduce_sha1_infos)
    join_rdd = hbase_man_out.read_hbase_table().flatMap(lambda x: sha1_key_json(x))
    cogroup_rdd = join_rdd.cogroup(update_rdd)
    tmp_rdd = cogroup_rdd.flatMap(lambda x: flatten_cogroup(x))
    out_rdd = tmp_rdd.flatMap(lambda x: split_sha1_kv_filter_max_images(x))
    hbase_man_out.rdd2hbase(out_rdd)


if __name__ == '__main__':
    from hbase_manager import HbaseManager
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    tab_cdrid_name = job_conf["tab_cdrid_name"]
    hbase_host = job_conf["hbase_host"]
    tab_sha1_infos_name = job_conf["tab_sha1_infos_name"]
    max_images = job_conf["max_images"]
    sc = SparkContext(appName='sha1_infos_join_from_'+tab_cdrid_name+'_in_'+tab_sha1_infos_name+'_filter_gt_'+str(max_images))
    sc.setLogLevel("ERROR")
    conf = SparkConf()
    # Get only these column from table tab_cdrid_name
    #"info:sha1"
    #"info:obj_stored_url"
    #"info:obj_parent"
    in_columns_list = ["info:sha1", "info:obj_stored_url", "info:obj_parent"]
    join_columns_list = [':'.join(x) for x in fields_list]
    hbase_man_in = HbaseManager(sc, conf, hbase_host, tab_cdrid_name, columns_list=in_columns_list)
    hbase_man_out = HbaseManager(sc, conf, hbase_host, tab_sha1_infos_name, columns_list=join_columns_list)
    update_sha1_infos(sc, hbase_man_in, hbase_man_out)
