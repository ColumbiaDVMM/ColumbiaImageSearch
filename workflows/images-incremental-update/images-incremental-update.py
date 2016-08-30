import json
import os
from pyspark import SparkContext, SparkConf
from elastic_manager import ES
from hbase_manager import HbaseManager

# default settings
fields_cdr = ["obj_stored_url", "obj_parent", "obj_original_url", "timestamp", "crawl_data.image_id", "crawl_data.memex_ht_id"]
# use key as str(max_ts-ts)+"_"+doc_id so that first rows are the newest
max_ts = 9999999999999
fields_list = [("info","all_cdr_ids"), ("info","s3_url"), ("info","all_parent_ids")]
infos_columns_list = ["info:sha1", "info:obj_stored_url", "info:obj_parent"]


def get_SHA1_from_URL(URL):
    import image_dl
    sha1hash = image_dl.get_SHA1_from_URL_StringIO(URL,1) # 1 is verbose level
    return sha1hash


def get_row_sha1(URL_S3,verbose=False):
    row_sha1 = None
    #print type(URL_S3),URL_S3
    if type(URL_S3) == unicode and URL_S3 != u'None' and URL_S3.startswith('https://s3'):
        row_sha1 = get_SHA1_from_URL(URL_S3)
    if row_sha1 and verbose:
        print "Got new SHA1 {} from_url {}.".format(row_sha1,URL_S3)
    return row_sha1


def check_get_sha1(data):
    json_x = [json.loads(x) for x in data[1].split("\n")]
    # First check if sha1 is not already there...
    try:
        row_sha1 = get_list_value(json_x,("info","sha1"))[0].strip()
        # Check for None here just to be safe
        if row_sha1 is None or row_sha1 == u'None':
            raise ValueError('sha1 is None.')
    except Exception as inst2: 
        # sha1 column does not exist
        URL_S3 = None
        key = data[0]
        try:
            URL_S3 = get_list_value(json_x,("info","obj_stored_url"))[0].strip()
            #print key,URL_S3,type(URL_S3)
        except Exception as inst2:
            print "[Error] for row {}. {}".format(key,inst2)
            return []
        row_sha1 = get_row_sha1(URL_S3,1)
        if row_sha1:
            return [(key, [key, "info", "sha1", row_sha1.upper()])]
    return []


def create_images_tuple(data):
    #print data
    doc_id = data[0]
    json_x = json.loads(data[1])
    #print json_x
    #print json_x["_metadata"]
    # this timestamp is the insertion timestamp
    ts =  json_x["_metadata"]["_timestamp"]
    key = str(max_ts-ts)+"_"+doc_id
    #print key
    #tup_list=[]
    tup_list=[ (key, [key, "info", "doc_id", doc_id])]
    for field in fields_cdr:
        try:
            field_value = json_x[field][0]
            #print field,field_value
            tup_list.append( (key, [key, "info", field, str(field_value)]) )
        except Exception as inst:
            print "[Error] Could not get field {} value for document {}. {}".format(field,doc_id,inst)
    return tup_list


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

def ts_to_cdr_id(data):
    ts_doc_id = data[0]
    list_ts_doc_id = ts_doc_id.split("_")
    ts = list_ts_doc_id[0]
    doc_id = list_ts_doc_id[1]
    #print ts_doc_id,ts,doc_id
    json_x = [json.loads(x) for x in data[1].split("\n")]
    tup_list = [ (doc_id, [doc_id, "info", "insert_ts", str(max_ts-int(ts))]) ]
    # do we want to keep info:doc_id ?
    for x in json_x:
        tup_list.append( (doc_id, [doc_id, x["columnFamily"], x["qualifier"], x["value"]]) )
    return tup_list


def incremental_update(es_man, es_ts_start, hbase_man_ts, hbase_man_cdrinfos, hbase_man_sha1infos, nb_partitions):
    #query = "{\"fields\": [\""+"\", \"".join(fields_cdr)+"\"], \"query\": {\"filtered\": {\"query\": {\"match\": {\"content_type\": \"image/jpeg\"}}, \"filter\": {\"range\" : {\"_timestamp\" : {\"gte\" : "+str(es_ts_start)+"}}}}}}"
    query = "{\"fields\": [\""+"\", \"".join(fields_cdr)+"\"], \"query\": {\"filtered\": {\"query\": {\"match\": {\"content_type\": \"image/jpeg\"}}, \"filter\": {\"range\" : {\"_timestamp\" : {\"gte\" : "+str(es_ts_start)+"}}}}}, \"sort\": [ \"_timestamp\": { \"order\": \"asc\" ] }"
    print query
    es_rdd = es_man.es2rdd(query).partitionBy(nb_partitions)
    images_hb_rdd = es_rdd.flatMap(lambda x: create_images_tuple(x))
    hbase_man_ts.rdd2hbase(images_hb_rdd)
    cdr_ids_infos_rdd = images_hb_rdd.flatMap(lambda x: ts_to_cdr_id(x))
    hbase_man_cdrinfos.rdd2hbase(cdr_ids_infos_rdd)
    # Anyway to get sha1 here without downloading images? 
    # If same obj_original_url that another cdr_id?
    cdr_ids_infos_rdd_with_sha1 = cdr_ids_infos_rdd.flatMap(lambda x: check_get_sha1(x))
    update_rdd = cdr_ids_infos_rdd_with_sha1.flatMap(lambda x: to_sha1_key(x)).reduceByKey(reduce_sha1_infos)
    join_rdd = hbase_man_sha1infos.read_hbase_table().flatMap(lambda x: sha1_key_json(x))
    cogroup_rdd = join_rdd.cogroup(update_rdd)
    tmp_rdd = cogroup_rdd.flatMap(lambda x: flatten_cogroup(x))
    out_rdd = tmp_rdd.flatMap(lambda x: split_sha1_kv_filter_max_images(x))
    hbase_man_sha1infos.rdd2hbase(out_rdd)


if __name__ == '__main__':
    # Read job_conf
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    # Set parameters job_conf
    nb_partitions = job_conf["nb_partitions"]
    # HBase Conf
    hbase_host = job_conf["hbase_host"]
    tab_ts_name = job_conf["tab_ts_name"]
    hbase_man_ts = HbaseManager(sc, conf, hbase_host, tab_ts_name)
    tab_cdrid_infos_name = job_conf["tab_cdrid_infos_name"]
    tab_sha1_infos_name = job_conf["tab_sha1_infos_name"]
    max_images = job_conf["max_images"]
    # ES conf
    es_index = job_conf["es_index"]
    es_domain = job_conf["es_domain"]
    es_host = job_conf["es_host"] 
    es_port = job_conf["es_port"]
    es_user = job_conf["es_user"]
    es_pass = job_conf["es_pass"]
    es_ts_start = job_conf["query_timestamp_start"]
    # query for first row of `tab_ts_name`
    ts_rdd = hbase_man_ts.read_hbase_table()
    hbase_table_fr = ts_rdd.first()
    if es_ts_start==0 and hbase_table_fr!=0:
        es_ts_start = max_ts-hbase_table_fr
        print "Setting start timestamp to: {}".format(es_ts_start)
    # Start job
    sc = SparkContext(appName='images_incremental_update')
    conf = SparkConf()
    es_man = ES(sc, conf, es_index, es_domain, es_host, es_port, es_user, es_pass)
    es_man.set_output_json()
    es_man.set_read_metadata()
    
    join_columns_list = [':'.join(x) for x in fields_list]
    hbase_man_cdrinfos = HbaseManager(sc, conf, hbase_host, tab_cdrid_infos_name, columns_list=in_columns_list)
    hbase_man_sha1infos = HbaseManager(sc, conf, hbase_host, tab_sha1_infos_name, columns_list=join_columns_list)
    incremental_update(es_man, es_ts_start, hbase_man_ts, hbase_man_cdrinfos, hbase_man_sha1infos, nb_partitions)

