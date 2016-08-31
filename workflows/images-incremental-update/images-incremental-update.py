import json
import os
from pyspark import SparkContext, SparkConf
from elastic_manager import ES
from hbase_manager import HbaseManager

# debugging
debug = True
ts_gap = 100

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
    #print("[check_get_sha1] {}".format(data))
    #json_x = [json.loads(x) for x in data[1].split("\n")]
    json_x = data[1]
    # First check if sha1 is not already there...
    try:
        row_sha1 = json_x["info:sha1"].strip()
        # Check for None here just to be safe
        if row_sha1 is None or row_sha1 == u'None':
            raise ValueError('sha1 is None.')
    except Exception as inst2: 
        # sha1 column does not exist
        URL_S3 = None
        key = data[0]
        try:
            URL_S3 = json_x["info:obj_stored_url"].strip()
            #print key,URL_S3,type(URL_S3)
        except Exception as inst2:
            print "[Error] for row {}. {}".format(key,inst2)
            return []
        row_sha1 = get_row_sha1(unicode(URL_S3),1)
        #print("[check_get_sha1.row_sha1] {}".format(row_sha1))
        if row_sha1:
            json_x["info:sha1"] = row_sha1
            return [(key, json_x)]
    return []


def expand_cdr_info(data):
    json_x = data[1]
    key = data[0]
    out = []
    for field in json_x:
        fs = field.split(':')
        out.append((key, [key, fs[0], fs[1], json_x[field]]))
    #print("[expand_cdr_info] {}, {}".format(data, out))
    return out


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
            pass
            #print "[Error] Could not get field {} value for document {}. {}".format(field,doc_id,inst)
    return tup_list


def get_list_value(json_x,field_tuple):
    return [x["value"] for x in json_x if x["columnFamily"]==field_tuple[0] and x["qualifier"]==field_tuple[1]]


def to_sha1_key(data):
    cdr_id = data[0]
    json_x = data[1]
    sha1 = None
    obj_stored_url = None
    obj_parent = None
    try:
        sha1 = json_x["info:sha1"].strip()
        obj_stored_url = json_x["info:obj_stored_url"].strip()
        obj_parent = json_x["info:obj_parent"].strip()
        #print key,URL_S3,type(URL_S3)
    except Exception as inst2:
        pass
        #print "[Error] could not get SHA1, obj_stored_url or obj_parent for row {}. {}".format(cdr_id,inst2)
    #print("[to_sha1_key] {}, {}, {}, {}, {}".format(data, cdr_id, sha1, obj_stored_url, obj_parent))
    if cdr_id and sha1 and obj_stored_url and obj_parent:
        return [(sha1, {"info:all_cdr_ids": [cdr_id], "info:s3_url": [obj_stored_url], "info:all_parent_ids": [obj_parent]})]
    return []


def sha1_key_json(data):
    sha1 = data[0]
    json_x = [json.loads(x) for x in data[1].split("\n")]
    v = dict()
    for field in fields_list:
        v[':'.join(field)] = get_list_value(json_x,field)[0].strip()
    print("[sha1_key_json] {}, {}, {}".format(data, sha1, v))
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


def copy_dict(d_in,d_out):
    for k in d_in:
        d_out[k] = d_in[k]
    return d_out

def reduce_cdrid_infos(a,b):
    c = dict()
    if type(a)==dict:
        c = copy_dict(a,c)
    else:
        c[a[1]+':'+a[2]] = a[3]
    if type(b)==dict:
        c = copy_dict(b,c)
    else:
        c[b[1]+':'+b[2]] = b[3]
    #print("[reduce_cdrid_infos] {}".format(c))
    return c


def reduce_sha1_infos_unique_list(a,b):
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


def reduce_sha1_infos_unique(a, b):
    # to be used with right join, deals with emtpy and potentially redundant dictionnary
    c = dict()
    if b: # we only care about update images
        if a: # merge case
            c["info:all_cdr_ids"] = list(set(a["info:all_cdr_ids"]+b["info:all_cdr_ids"]))
            c["info:all_parent_ids"] = list(set(a["info:all_parent_ids"]+b["info:all_parent_ids"]))
            if a["info:s3_url"] and a["info:s3_url"]!=u'None':
                c["info:s3_url"] = a["info:s3_url"]
            else:
                c["info:s3_url"] = b["info:s3_url"]
        else: # new image case
            c["info:all_cdr_ids"] = b["info:all_cdr_ids"]
            c["info:all_parent_ids"] = b["info:all_parent_ids"]
            c["info:s3_url"] = b["info:s3_url"]
    print("[reduce_sha1_infos_unique] {}, {}, {}".format(a, b, c))
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
    #print("[split_sha1_kv_filter_max_images] {}, {}".format(x, out))
    return out


def flatten_cogroup(x):
    out = []
    # at this point value is a tuple of two lists with a single or empty dictionary
    c = reduce_sha1_infos_unique_list(x[1][0],x[1][1])
    # check that result is a new or updated image
    if len(c.keys()) == len(fields_list):
        out.append((x[0], c))
    return out


def flatten_rightjoin(x):
    out = []
    # at this point value is a tuple of two lists with a single or empty dictionary
    c = reduce_sha1_infos_unique(x[1][0],x[1][1])
    # check that result is a new or updated image
    if len(c.keys()) == len(fields_list):
        out.append((x[0], c))
    return out

def flatten_leftjoin(x):
    out = []
    # at this point value is a tuple of two lists with a single or empty dictionary
    c = reduce_sha1_infos_unique(x[1][1],x[1][0])
    # check that result is a new or updated image
    if len(c.keys()) == len(fields_list):
        out.append((x[0], c))
    print("[flatten_leftjoin] {}, {}".format(x, out))
    return out

def ts_to_cdr_id(data):
    ts_doc_id = data[0]
    list_ts_doc_id = ts_doc_id.split("_")
    ts = list_ts_doc_id[0]
    doc_id = list_ts_doc_id[1]
    #print ts_doc_id,ts,doc_id,len(data),data[1]
    if data[1][2]=='doc_id': # create insert_ts only once
        tup_list = [ (doc_id, [doc_id, "info", "insert_ts", str(max_ts-int(ts))]) ]
    else:
        tup_list = []
    tup_list.append( (doc_id, [doc_id, data[1][1], data[1][2], data[1][3]]) )
    return tup_list


def incremental_update(es_man, es_ts_start, hbase_man_ts, hbase_man_cdrinfos, hbase_man_sha1infos_join, hbase_man_sha1infos_out, nb_partitions):
    #query = "{\"fields\": [\""+"\", \"".join(fields_cdr)+"\"], \"query\": {\"filtered\": {\"query\": {\"match\": {\"content_type\": \"image/jpeg\"}}, \"filter\": {\"range\" : {\"_timestamp\" : {\"gte\" : "+str(es_ts_start)+"}}}}}}"
    #query = "{\"fields\": [\""+"\", \"".join(fields_cdr)+"\"], \"query\": {\"filtered\": {\"query\": {\"match\": {\"content_type\": \"image/jpeg\"}}, \"filter\": {\"range\" : {\"_timestamp\" : {\"gte\" : "+str(es_ts_start)+"}}}}}, \"sort\": [ { \"_timestamp\": { \"order\": \"asc\" } } ] }"
    if debug:
        query = "{\"fields\": [\""+"\", \"".join(fields_cdr)+"\"], \"query\": {\"filtered\": {\"query\": {\"match\": {\"content_type\": \"image/jpeg\"}}, \"filter\": {\"range\" : {\"_timestamp\" : {\"gte\" : "+str(es_ts_start)+", \"lt\": "+str(es_ts_start+ts_gap)+"}}}}}, \"sort\": [ { \"_timestamp\": { \"order\": \"asc\" } } ] }"
    else:
        query = "{\"fields\": [\""+"\", \"".join(fields_cdr)+"\"], \"query\": {\"filtered\": {\"query\": {\"match\": {\"content_type\": \"image/jpeg\"}}, \"filter\": {\"range\" : {\"_timestamp\" : {\"gte\" : "+str(es_ts_start)+"}}}}}, \"sort\": [ { \"_timestamp\": { \"order\": \"asc\" } } ] }"
    print query
    es_rdd = es_man.es2rdd(query)
    images_hb_rdd = es_rdd.partitionBy(nb_partitions).flatMap(lambda x: create_images_tuple(x))
    hbase_man_ts.rdd2hbase(images_hb_rdd)
    cdr_ids_infos_rdd = images_hb_rdd.flatMap(lambda x: ts_to_cdr_id(x))
    hbase_man_cdrinfos.rdd2hbase(cdr_ids_infos_rdd)
    # Anyway to get sha1 here without downloading images? 
    # If same obj_original_url that another cdr_id?
    cdr_ids_infos_rdd_with_sha1 = cdr_ids_infos_rdd.reduceByKey(reduce_cdrid_infos).flatMap(lambda x: check_get_sha1(x))
    hbase_man_cdrinfos.rdd2hbase(cdr_ids_infos_rdd_with_sha1.flatMap(lambda x: expand_cdr_info(x)))
    update_rdd = cdr_ids_infos_rdd_with_sha1.flatMap(lambda x: to_sha1_key(x)).reduceByKey(reduce_sha1_infos_unique)
    sha1_infos_rdd = hbase_man_sha1infos_join.read_hbase_table()
    if not sha1_infos_rdd.isEmpty(): # we need to merge the 'all_cdr_ids' and 'all_parent_ids'
        # spark job seems to get stuck here...
        sha1_infos_rdd_json = sha1_infos_rdd.flatMap(lambda x: sha1_key_json(x))
        join_rdd = update_rdd.leftOuterJoin(sha1_infos_rdd_json).flatMap(lambda x: flatten_leftjoin(x))
        out_rdd = join_rdd.flatMap(lambda x: split_sha1_kv_filter_max_images(x))
    else: # first update
        out_rdd = update_rdd.flatMap(lambda x: split_sha1_kv_filter_max_images(x))
    hbase_man_sha1infos_out.rdd2hbase(out_rdd)


if __name__ == '__main__':
    # Read job_conf
    job_conf = json.load(open("job_conf_notcommited.json","rt"))
    print job_conf
    sc = SparkContext(appName='images_incremental_update')
    conf = SparkConf()
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
    #log4j.LogManager.getRootLogger().setLevel(log4j.Level.ALL)
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
    hbase_table_fr = 0
    try:
        hbase_table_fr_row = ts_rdd.first()
        hbase_table_fr = long(hbase_table_fr_row[0].split('_')[0])
        print("hbase_table_fr = {}".format(hbase_table_fr)) 
    except: # table empty
        pass
    if es_ts_start==0 and hbase_table_fr!=0:
        es_ts_start = max_ts-hbase_table_fr
        print "Setting start timestamp to: {}".format(es_ts_start)
    # Start job
    es_man = ES(sc, conf, es_index, es_domain, es_host, es_port, es_user, es_pass)
    es_man.set_output_json()
    es_man.set_read_metadata()
    join_columns_list = [':'.join(x) for x in fields_list]
    hbase_man_cdrinfos = HbaseManager(sc, conf, hbase_host, tab_cdrid_infos_name, columns_list=infos_columns_list)
    hbase_man_sha1infos_join = HbaseManager(sc, conf, hbase_host, tab_sha1_infos_name, columns_list=join_columns_list)
    hbase_man_sha1infos_out = HbaseManager(sc, conf, hbase_host, tab_sha1_infos_name)
    incremental_update(es_man, es_ts_start, hbase_man_ts, hbase_man_cdrinfos, hbase_man_sha1infos_join, hbase_man_sha1infos_out, nb_partitions)

