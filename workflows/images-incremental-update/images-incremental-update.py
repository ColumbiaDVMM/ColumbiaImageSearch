import os
import json
import time
import datetime

from optparse import OptionParser
from pyspark import SparkContext, SparkConf, StorageLevel
from elastic_manager import ES
from hbase_manager import HbaseManager

# debugging
debug = True
ts_gap = 86400000 # One day

# default settings
fields_cdr = ["obj_stored_url", "obj_parent", "obj_original_url", "timestamp", "crawl_data.image_id", "crawl_data.memex_ht_id"]
# use key as str(max_ts-ts)+"_"+doc_id so that first rows are the newest
max_ts = 9999999999999
# how to deal with info:image_discarded?
fields_list = [("info","all_cdr_ids"), ("info","s3_url"), ("info","all_parent_ids"), ("info","image_discarded")]
infos_columns_list = ["info:sha1", "info:obj_stored_url", "info:obj_parent"]
#compression = "org.apache.hadoop.io.compress.GzipCodec"
# this seems to trigger a java.io.IOException: FAILED_TO_UNCOMPRESS(5)
#  seems recommended to use org.apache.spark.io.LZFCompressionCodec: http://search-hadoop.com/m/q3RTtoCw3T14p5MD


def get_list_value(json_x,field_tuple):
    return [x["value"] for x in json_x if x["columnFamily"]==field_tuple[0] and x["qualifier"]==field_tuple[1]]


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
            URL_S3 = unicode(json_x["info:obj_stored_url"].strip())
            #print key,URL_S3,type(URL_S3)
        except Exception as inst2:
            print "[Error] for row {}. {}".format(key,inst2)
            return []
        row_sha1 = get_row_sha1(unicode(URL_S3),0)
        if row_sha1:
            #print("[check_get_sha1.row_sha1] sha1 for crd_id {}: {} (from URL: {})".format(key, row_sha1, URL_S3))
            json_x["info:sha1"] = row_sha1
            return [(key, json_x)]
    return []


def expand_info(data):
    key = data[0]
    json_x = data[1]
    out = []
    for field in json_x:
        fs = field.split(':')
        out.append((key, [key, fs[0], fs[1], json_x[field]]))
    #print("[expand_info] {}, {}".format(data, out))
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
            str_field_value = unicode(field_value)
            tup_list.append( (key, [key, "info", field, str_field_value]) )
        except Exception as inst:
            pass
            #print "[Error] Could not get field {} value for document {}. {}".format(field,doc_id,inst)
    return tup_list


def to_sha1_key(data):
    cdr_id = data[0]
    json_x = data[1]
    sha1 = None
    obj_stored_url = None
    obj_parent = None
    try:
        sha1 = json_x["info:sha1"].strip()
        obj_stored_url = unicode(json_x["info:obj_stored_url"].strip())
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
        try:
            if field[1]!='s3_url':
                v[':'.join(field)] = list(set([x for x in get_list_value(json_x,field)[0].strip().split(',')]))
            else:
                v[':'.join(field)] = [unicode(get_list_value(json_x,field)[0].strip())]
        except: # field not in row
            pass
    #print("[sha1_key_json] {}, {}, {}".format(data, sha1, v))
    return [(sha1, v)]


def reduce_cdrid_infos(a,b):
    ''' If we have two samples with the same cdr_id we want to keep the newest
    that may be a correction of the older one.
    '''
    c = dict()
    if a["info:insert_ts"] > b["info:insert_ts"]:
        c = a
    else:
        c = b
    print("[reduce_cdrid_infos] {}".format(c))
    return c


def safe_reduce_infos(a, b, c, field):
    try:
        c[field] = list(set(a[field]+b[field]))
    except Exception as inst:
        try:
            c[field] = a[field]
            print("[safe_reduce_infos: error] key error for '{}' for b".format(field))
        except Exception as inst2:
            try:
                c[field] = b[field]
                print("[safe_reduce_infos: error] key error for '{}' for a".format(field))
            except Exception as inst3:
                c[field] = []
                print("[safe_reduce_infos: error] key error for '{}' for both a and b".format(field))
    return c


def safe_assign(a, c, field, fallback):
    if field in a:
        c[field] = a[field]
    else:
        print("[safe_assign: error] we have no {}.".format(field))
        c[field] = fallback
    return c


def reduce_sha1_infos_discarding(a,b):
    c = dict()
    if b:  # sha1 already existed
        if "info:image_discarded" in a or "info:image_discarded" in b:
            c["info:all_cdr_ids"] = []
            c["info:all_parent_ids"] = []
            c["info:image_discarded"] = 'discarded because has more than {} cdr_ids'.format(max_images)
        else:
            # KeyError: 'info:all_cdr_ids'. How could an image not have this field?
            c = safe_reduce_infos(a, b, c, "info:all_cdr_ids")
            c = safe_reduce_infos(a, b, c, "info:all_parent_ids")
        if "info:s3_url" in a and a["info:s3_url"] and a["info:s3_url"][0] and a["info:s3_url"][0]!=u'None':
            c["info:s3_url"] = a["info:s3_url"]
        else:
            if "info:s3_url" in b:
                c["info:s3_url"] = b["info:s3_url"]
            else:
                print("[reduce_sha1_infos_discarding: error] both a and b have no s3 url.")
                c["info:s3_url"] = [None]
    else: # brand new image
        c = safe_assign(a, c, "info:s3_url", [None])
        c = safe_assign(a, c, "info:all_cdr_ids", [])
        c = safe_assign(a, c, "info:all_parent_ids", [])
    if len(c["info:all_cdr_ids"]) > max_images or len(c["info:all_parent_ids"]) > max_images:
        print("Discarding image with URL: {}".format(c["info:s3_url"][0]))
        c["info:all_cdr_ids"] = []
        c["info:all_parent_ids"] = []
        c["info:image_discarded"] = 'discarded because has more than {} cdr_ids'.format(max_images)
    return c


def split_sha1_kv_filter_max_images_discarded(x):
    tmp_fields_list = [("info","all_cdr_ids"), ("info","s3_url"), ("info","all_parent_ids")]
    out = []
    if "info:image_discarded" in x[1]:
        out.append((x[0], [x[0], "info", "image_discarded", x[1]["info:image_discarded"]]))
        str_s3url_value = None
        s3url_value = x[1]["info:s3_url"][0]
        str_s3url_value = unicode(s3url_value)
        out.append((x[0], [x[0], "info", "s3_url", str_s3url_value]))
        # # str(field_value) could fail for unicode strings...
        # except Exception as inst:
        #     print("[split_sha1_kv_filter_max_images_discarded: error] {}. Assuming it is an encoding issue.".format(inst))
        #     try:
        #         str_s3url_value = s3url_value.encode('utf-8')
        #     except:
        #         pass
        # if str_s3url_value:
        #     out.append((x[0], [x[0], "info", "s3_url", str_s3url_value]))
        out.append((x[0], [x[0], "info", "all_cdr_ids", x[1]["info:image_discarded"]]))
        out.append((x[0], [x[0], "info", "all_parent_ids", x[1]["info:image_discarded"]]))
    else:
        for field in tmp_fields_list:
            if field[1]=="s3_url":
                out.append((x[0], [x[0], field[0], field[1], unicode(x[1][field[0]+":"+field[1]][0])]))
            else:
                out.append((x[0], [x[0], field[0], field[1], ','.join(x[1][field[0]+":"+field[1]])]))
    return out


def out_to_dict_str(x):
    key = ":".join([x[1][1],x[1][2]])
    out_dict = dict()
    out_dict[key] = x[1][3]
    return (x[0], json.dumps(out_dict))


def out_to_amandeep_dict_str(x):
    sha1 = x[0]
    # keys should be: "image_sha1", "all_parent_ids", "s3_url"
    out_dict = dict()
    out_dict["image_sha1"] = sha1
    for field in ["all_parent_ids", "s3_url"]:
        out_dict[field] = x[1]["info:"+field]
    return (sha1, json.dumps(out_dict))


def out_from_dict_str(x):
    out_dict = json.loads(x[1])
    key = out_dict.keys()[0]
    cf, cq = key.split(':')
    return (x[0], [x[0], cf, cq, out_dict[key]])


def get_new_s3url_sha1(x):
    value = x[1]
    out = []
    if value[2] == "s3_url":
        sha1 = x[0].strip()
        s3_url = value[3].strip()
        out.append((s3_url, [s3_url, "info", "sha1", sha1]))
    return out


def flatten_leftjoin(x):
    out = []
    # at this point value is a tuple of two lists with a single or empty dictionary
    c = reduce_sha1_infos_discarding(x[1][0],x[1][1])
    out.append((x[0], c))
    #print("[flatten_leftjoin] {}, {}".format(x, out))
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


def to_cdr_id_dict(data):
    print("[to_cdr_id_dict] data: {}".format(data))
    doc_id = data[0]
    v = dict()
    json_x = json.loads(data[1])
    insert_ts = str(json_x["_metadata"]["_timestamp"])
    v["info:insert_ts"] = insert_ts
    v["info:doc_id"] = doc_id
    del json_x["_metadata"]
    for field in json_x:
        try:
            v["info:"+field] = str(json_x[field][0])
        except Exception as inst:
            print("[to_cdr_id_dict: error] {} for doc: {}. Assuming it is an encoding issue.".format(inst, doc_id))
            try:
                v["info:"+field] = json_x[field][0].encode('utf-8')
            except Exception as inst2:
                print("[to_cdr_id_dict: error] failed again ({}) for doc: {}.".format(inst2, doc_id))
                pass
    tup_list = [(doc_id, v)]
    print("[to_cdr_id_dict] {}".format(tup_list))
    return tup_list


def to_s3_url_key(data):
    print("[to_s3_url_key] data: {}".format(data))
    doc_id = data[0]
    v = data[1]
    tup_list = []
    if "info:obj_stored_url" in v:
        s3url = v["info:obj_stored_url"]
        if s3url.startswith('https://s3'):
            v["info:doc_id"] = doc_id
            tup_list = [(s3url, v)]
    print("[to_s3_url_key] {}".format(tup_list))
    return tup_list


def s3url_to_cdr_id_wsha1(data):
    print("[s3url_to_cdr_id_wsha1] data: {}".format(data))
    if len(data[1]) != 2 or data[1][1] is None or data[1][1] == 'None' or data[1][1] == u'None':
        print("[s3url_to_cdr_id_wsha1] incorrect data: {}".format(data))
        return []
    s3_url = data[0]
    v = data[1][0]
    sha1 = data[1][1]
    doc_id = v["info:doc_id"]
    v["info:sha1"] = sha1
    tup_list = [(doc_id, v)]
    print("[s3url_to_cdr_id_wsha1] {}".format(tup_list))
    return tup_list


def s3url_to_cdr_id_nosha1(data):
    print("[s3url_to_cdr_id_wsha1] data: {}".format(data))
    if len(data[1]) == 2 and data[1][1] is not None and data[1][1] != 'None' and data[1][1] != u'None':
        print("[s3url_to_cdr_id_nosha1] beware: incorrect data, s3 url has a sha1: {}".format(data))
    s3_url = data[0]
    v = data[1][0]
    doc_id = v["info:doc_id"]
    tup_list = [(doc_id, v)]
    print("[s3url_to_cdr_id_nosha1] {}".format(tup_list))
    return tup_list


def get_existing_joined_sha1(data):
    print("[get_existing_joined_sha1] data: {}".format(data))
    if len(data[1]) == 2 and data[1][1] is not None and data[1][1] != 'None' and data[1][1] != u'None':
        return True
    return False


def clean_up_s3url_sha1(data):
    #print("[clean_up_s3url_sha1] data: {}".format(data))
    s3url = data[0].strip()
    json_x = [json.loads(x) for x in data[1].split("\n")]
    sha1 = get_list_value(json_x,("info","sha1"))[0].strip()
    print("[clean_up_s3url_sha1] out: {}".format((s3url,sha1)))
    return (s3url,sha1)


def get_s3url_sha1(data):
    sha1 = data[0]
    json_x = data[1]
    try:
        s3url_list = get_list_value(json_x,("info","obj_stored_url"))
        sha1_list = get_list_value(json_x,("info","sha1"))
        if s3url_list and sha1_list:
            s3url = s3url_list[0].strip()
            sha1 = sha1_list[0].strip()
            if not s3url.startswith('https://s3'):
                raise ValueError('s3url is not stored in S3.')
        else:
            if not sha1_list:
                raise ValueError('sha1 is not computed.')
            if not s3url_list:
                raise ValueError('s3url is absent.')
    except Exception as inst:
        print "[Error] Could not get sha1 or s3url for row {}. {}".format(key, inst)
        return []
    if sha1 and s3url:
        #print("[get_s3url_sha1] s3_url {} sha1 is {}".format(s3url, sha1))
        return [(s3url, [s3url, "info", "sha1", sha1.upper()])]
    return []


def save_info_incremental_update(hbase_man_update_out, incr_update_id, info_value, info_name):
    print("[incremental_update] {}: {}".format(info_name, info_value))
    incr_update_infos_list = []
    incr_update_infos_list.append((incr_update_id, [incr_update_id, "info", info_name, str(info_value)]))
    incr_update_infos_rdd = sc.parallelize(incr_update_infos_list)
    hbase_man_update_out.rdd2hbase(incr_update_infos_rdd)


def build_batch_rdd(batch_udpate):
    import numpy as np
    # batch_rdd should be created to be stored in hbase table update_infos
    update_id = "index_update_"+str(max_ts-int(time.time()*1000))+'_'+str(np.int32(np.random.random()*(10e6)))
    list_key = []
    for x in batch_udpate:
        list_key.append(x)
    batch_out = [(update_id, [update_id, "info", "list_sha1s", ','.join(list_key)])]
    return sc.parallelize(batch_out)


def save_new_sha1s_for_index_update(new_sha1s_rdd, hbase_man_update_out, batch_update_size):
    iterator = new_sha1s_rdd.toLocalIterator()
    batch_udpate = []
    batch_id = 0
    for x in iterator:
        batch_udpate.append(x)
        if len(batch_udpate)==batch_update_size:
            # we should use the incremental update id and batch id as row key
            batch_rdd = build_batch_rdd(batch_udpate)
            hbase_man_update_out.rdd2hbase(batch_rdd)
            batch_udpate = []
            batch_id += 1
    # last batch
    if batch_udpate:
        batch_rdd = build_batch_rdd(batch_udpate)
        hbase_man_update_out.rdd2hbase(batch_rdd)


def incremental_update(es_man, es_ts_start, hbase_man_ts, hbase_man_cdrinfos_out, hbase_man_sha1infos_join, hbase_man_sha1infos_out, hbase_man_s3url_sha1_in, hbase_man_s3url_sha1_out, hbase_man_update_in, hbase_man_update_out, nb_partitions, c_options):

    restart = c_options.restart
    identifier = c_options.identifier
    day_to_process = c_options.day_to_process
    save_inter_rdd = c_options.save_inter_rdd
    batch_update_size = c_options.batch_update_size


    start_time = time.time()
    
    # if we restart we should actually look for the most advanced saved rdd and restart from there.
    # we could read the corresponding update row in table_updates to understand where we need to restart from.
    if restart:
        incr_update_id = identifier
    else:
        #incr_update_id = 'incremental_update_'+str(max_ts-int(start_time*1000))
        incr_update_id = datetime.date.fromtimestamp((es_ts_start)/1000).isoformat()
    # need to use pydoop.hdfs to create the directory?
    basepath_save = '/user/skaraman/data/images_incremental_update/'+incr_update_id
    
    ##-- get cdr_ids_infos_rdd
    cdr_ids_infos_rdd_not_loaded = True
    cdr_ids_infos_rdd_path = basepath_save + "/cdr_ids_infos_rdd"
    if restart:
        try:
            cdr_ids_infos_rdd = sc.sequenceFile(cdr_ids_infos_rdd_path).mapValues(json.loads)
            cdr_ids_infos_rdd_not_loaded = False
        except Exception as inst:
            print("Could not load rdd at {}. Error was {}.".format(cdr_ids_infos_rdd_path, inst))

    if cdr_ids_infos_rdd_not_loaded:
        #query = "{\"fields\": [\""+"\", \"".join(fields_cdr)+"\"], \"query\": {\"filtered\": {\"query\": {\"match\": {\"content_type\": \"image/jpeg\"}}, \"filter\": {\"range\" : {\"_timestamp\" : {\"gte\" : "+str(es_ts_start)+"}}}}}}"
        #query = "{\"fields\": [\""+"\", \"".join(fields_cdr)+"\"], \"query\": {\"filtered\": {\"query\": {\"match\": {\"content_type\": \"image/jpeg\"}}, \"filter\": {\"range\" : {\"_timestamp\" : {\"gte\" : "+str(es_ts_start)+"}}}}}, \"sort\": [ { \"_timestamp\": { \"order\": \"asc\" } } ] }"
        if debug:
            query = "{\"fields\": [\""+"\", \"".join(fields_cdr)+"\"], \"query\": {\"filtered\": {\"query\": {\"match\": {\"content_type\": \"image/jpeg\"}}, \"filter\": {\"range\" : {\"_timestamp\" : {\"gte\" : "+str(es_ts_start)+", \"lt\": "+str(es_ts_start+ts_gap)+"}}}}}, \"sort\": [ { \"_timestamp\": { \"order\": \"asc\" } } ] }"
        else:
            query = "{\"fields\": [\""+"\", \"".join(fields_cdr)+"\"], \"query\": {\"filtered\": {\"query\": {\"match\": {\"content_type\": \"image/jpeg\"}}, \"filter\": {\"range\" : {\"_timestamp\" : {\"gte\" : "+str(es_ts_start)+"}}}}}, \"sort\": [ { \"_timestamp\": { \"order\": \"asc\" } } ] }"
        print query
        
        ## get incremental update
        es_rdd = es_man.es2rdd(query)
        if es_rdd.isEmpty():
            print("[incremental_update] empty incremental update when querying from timestamp {}".format(es_ts_start))
            return
        es_rdd_count = es_rdd.count()
        ## save incremental update infos
        incr_update_infos_list = []
        incr_update_infos_list.append((incr_update_id, [incr_update_id, "info", "start_time", str(start_time)]))
        incr_update_infos_list.append((incr_update_id, [incr_update_id, "info", "es_rdd_count", str(es_rdd_count)]))
        incr_update_infos_rdd = sc.parallelize(incr_update_infos_list)
        hbase_man_update_out.rdd2hbase(incr_update_infos_rdd)
        # partition for efficiency
        images_hb_rdd = es_rdd.partitionBy(nb_partitions).persist(StorageLevel.MEMORY_AND_DISK)
        # save incremental update data infos
        images_ts_cdrid_rdd = images_hb_rdd.flatMap(lambda x: create_images_tuple(x))
        hbase_man_ts.rdd2hbase(images_ts_cdrid_rdd)
        min_ts_cdrid = images_ts_cdrid_rdd.min()[0].strip()
        max_ts_cdrid = images_ts_cdrid_rdd.max()[0].strip()
        incr_update_infos_list = []
        incr_update_infos_list.append((incr_update_id, [incr_update_id, "info", "min_ts_cdrid", min_ts_cdrid]))
        incr_update_infos_list.append((incr_update_id, [incr_update_id, "info", "max_ts_cdrid", max_ts_cdrid]))
        incr_update_infos_rdd = sc.parallelize(incr_update_infos_list)
        print("[incremental_update] saving incremental update infos: id {}, min_ts_cdrid {}, max_ts_cdrid {}".format(incr_update_id, min_ts_cdrid, max_ts_cdrid))
        hbase_man_update_out.rdd2hbase(incr_update_infos_rdd)

        ## start processing incremental update
        cdr_ids_infos_rdd = images_hb_rdd.flatMap(lambda x: to_cdr_id_dict(x)).persist(StorageLevel.MEMORY_AND_DISK)
        # save rdd
        if save_inter_rdd:
            #cdr_ids_infos_rdd.mapValues(json.dumps).saveAsSequenceFile(cdr_ids_infos_rdd_path, compressionCodecClass=compression)
            cdr_ids_infos_rdd.mapValues(json.dumps).saveAsSequenceFile(cdr_ids_infos_rdd_path)
            save_info_incremental_update(hbase_man_update_out, incr_update_id, cdr_ids_infos_rdd_path, "cdr_ids_infos_rdd_path")
            # to be loaded with:
            # cdr_ids_infos_rdd = sc.sequenceFile(cdr_ids_infos_rdd_path).mapValues(json.loads)
        images_hb_rdd.unpersist()
    ##-- end get cdr_ids_infos_rdd

    ##-- get s3url_infos_rdd_join
    s3url_infos_rdd_join_not_loaded = True
    # s3url_infos_rdd_join_path = basepath_save + "/s3url_infos_rdd_join"
    # if restart:
    #     try:
    #         #s3url_infos_rdd_join = sc.sequenceFile(s3url_infos_rdd_join_path)
    #         s3url_infos_rdd_join = sc.textFile(s3url_infos_rdd_join_path)
    #         s3url_infos_rdd_join_not_loaded = False
    #     except Exception as inst:
    #         print("Could not load rdd at {}. Error was {}.".format(s3url_infos_rdd_join_path, inst))
        
    if s3url_infos_rdd_join_not_loaded:
        # there could be duplicates cdr_id near indices boundary or corrections might have been applied...
        cdr_ids_infos_rdd_red = cdr_ids_infos_rdd.reduceByKey(reduce_cdrid_infos).persist(StorageLevel.MEMORY_AND_DISK)
        cdr_ids_infos_rdd.unpersist()
        # invert cdr_ids_infos_rdd (k,v) into s3url_infos_rdd (v[s3_url],[v,v['cdr_id']=k])
        s3url_infos_rdd = cdr_ids_infos_rdd_red.flatMap(lambda x: to_s3_url_key(x)).persist(StorageLevel.MEMORY_AND_DISK)
        # read s3url_sha1 table into s3url_sha1 to get sha1 here without downloading images
        s3url_sha1_rdd = hbase_man_s3url_sha1_in.read_hbase_table().map(clean_up_s3url_sha1).persist(StorageLevel.MEMORY_AND_DISK)
        # do a s3url_infos_rdd.leftOuterJoin(s3url_sha1) s3url_sha1_rdd
        s3url_infos_rdd_join = s3url_infos_rdd.leftOuterJoin(s3url_sha1_rdd).persist(StorageLevel.MEMORY_AND_DISK)
        # save rdd
        # if save_inter_rdd:
        #     try:
        #         # check if file exists, delete before trying to write? fails with ArrayWritable error...
        #         #s3url_infos_rdd_join.saveAsSequenceFile(s3url_infos_rdd_join_path, compressionCodecClass=compression)
        #         # this create some issues due to the compression?
        #         s3url_infos_rdd_join.saveAsTextFile(s3url_infos_rdd_join_path, compressionCodecClass=compression)
        #         # to be loaded with
        #         # s3url_infos_rdd_join = sc.sequenceFile(s3url_infos_rdd_join_path)
        #         save_info_incremental_update(hbase_man_update_out, incr_update_id, s3url_infos_rdd_join_path, "s3url_infos_rdd_join_path")
        #     except Exception as inst:
        #         print("Could not save rdd at {}, error was {}.".format(s3url_infos_rdd_join_path, inst))
        s3url_sha1_rdd.unpersist()
        s3url_infos_rdd.unpersist()
    ##-- end get s3url_infos_rdd_join

    ##-- get cdr_ids_infos_rdd_join_sha1
    cdr_ids_infos_rdd_join_sha1_path = basepath_save + "/cdr_ids_infos_rdd_join_sha1"
    cdr_ids_infos_rdd_join_sha1_not_loaded = True
    if restart:
        try:
            cdr_ids_infos_rdd_join_sha1 = sc.sequenceFile(cdr_ids_infos_rdd_join_sha1_path)
            cdr_ids_infos_rdd_join_sha1_not_loaded = False
        except Exception as inst:
            print("Could not load rdd at {}. Error was {}.".format(cdr_ids_infos_rdd_join_sha1_path, inst))

    ## invert s3url_infos_rdd_join (s3_url, (v,sha1)) into cdr_ids_infos_rdd_join_sha1 (k, v) adding info:sha1 in v
    s3url_infos_rdd_with_sha1 = s3url_infos_rdd_join.filter(get_existing_joined_sha1).persist(StorageLevel.MEMORY_AND_DISK)

    if cdr_ids_infos_rdd_join_sha1_not_loaded:
        cdr_ids_infos_rdd_join_sha1 = s3url_infos_rdd_with_sha1.flatMap(lambda x: s3url_to_cdr_id_wsha1(x)).persist(StorageLevel.MEMORY_AND_DISK)
        cdr_ids_infos_rdd_join_sha1_count = cdr_ids_infos_rdd_join_sha1.count()
        save_info_incremental_update(hbase_man_update_out, incr_update_id, cdr_ids_infos_rdd_join_sha1_count, "cdr_ids_infos_rdd_join_sha1_count")
        # save it to hbase
        hbase_man_cdrinfos_out.rdd2hbase(cdr_ids_infos_rdd_join_sha1.flatMap(lambda x: expand_info(x)))
        # save rdd
        if save_inter_rdd:
            try:
                # check if file exists, delete before trying to write? 
                #cdr_ids_infos_rdd_join_sha1.mapValues(json.dumps).saveAsSequenceFile(cdr_ids_infos_rdd_join_sha1_path, compressionCodecClass=compression)
                cdr_ids_infos_rdd_join_sha1.mapValues(json.dumps).saveAsSequenceFile(cdr_ids_infos_rdd_join_sha1_path)
                save_info_incremental_update(hbase_man_update_out, incr_update_id, cdr_ids_infos_rdd_join_sha1_path, "cdr_ids_infos_rdd_join_sha1_path")
            except Exception as inst:
                print("Could not save rdd at {}, error was {}.".format(cdr_ids_infos_rdd_join_sha1_path, inst))
    ##-- end get cdr_ids_infos_rdd_join_sha1

    ##-- build out_join_rdd
    # to sha1 key and save number of joined by s3 url images
    update_join_rdd = cdr_ids_infos_rdd_join_sha1.flatMap(lambda x: to_sha1_key(x)).reduceByKey(reduce_sha1_infos_discarding).persist(StorageLevel.MEMORY_AND_DISK)
    cdr_ids_infos_rdd_join_sha1.unpersist()
    # 0 when loading but 3320468 originally?...
    update_join_rdd_count = update_join_rdd.count()
    save_info_incremental_update(hbase_man_update_out, incr_update_id, update_join_rdd_count, "update_join_rdd_count")
    ## update cdr_ids, and parents cdr_ids for these existing sha1s
    sha1_infos_rdd = hbase_man_sha1infos_join.read_hbase_table().persist(StorageLevel.MEMORY_AND_DISK)
    # we may need to merge some 'all_cdr_ids' and 'all_parent_ids'
    if not sha1_infos_rdd.isEmpty(): 
        sha1_infos_rdd_json = sha1_infos_rdd.partitionBy(nb_partitions).flatMap(lambda x: sha1_key_json(x)).persist(StorageLevel.MEMORY_AND_DISK)
        # check for info:image_discarded in flatten_leftjoin
        update_join_sha1_rdd = update_join_rdd.leftOuterJoin(sha1_infos_rdd_json).flatMap(lambda x: flatten_leftjoin(x)).persist(StorageLevel.MEMORY_AND_DISK)
        sha1_infos_rdd_json.unpersist()
        out_join_rdd_amandeep = update_join_sha1_rdd
    else: # first update
        out_join_rdd_amandeep = update_join_rdd
    # save rdd
    if save_inter_rdd: 
        if out_join_rdd_amandeep.isEmpty(): 
            save_info_incremental_update(hbase_man_update_out, incr_update_id, "EMPTY", "out_join_rdd_path")
        else:
            out_join_rdd_path = basepath_save + "/out_join_rdd"
            try:
                out_join_rdd_amandeep.map(out_to_amandeep_dict_str).saveAsSequenceFile(out_join_rdd_path)
                save_info_incremental_update(hbase_man_update_out, incr_update_id, out_join_rdd_path, "out_join_rdd_path")
            except Exception as inst:
                print("Could not save rdd at {}, error was {}.".format(out_join_rdd_path, inst))
    # save sha1 infos for these joined images
    out_join_rdd = out_join_rdd_amandeep.flatMap(lambda x: split_sha1_kv_filter_max_images_discarded(x)).persist(StorageLevel.MEMORY_AND_DISK)
    hbase_man_sha1infos_out.rdd2hbase(out_join_rdd)
    
    out_join_rdd.unpersist()
    sha1_infos_rdd.unpersist()
    ##-- end build out_join_rdd

    ## for not matching s3url i.e. missing sha1
    # filter on second value member being empty in s3url_infos_rdd_join, and get sha1
    #cdr_ids_infos_rdd_new_sha1 = s3url_infos_rdd_join.filter(lambda x: not get_existing_joined_sha1(x)).flatMap(lambda x: check_get_sha1(x))
    cdr_ids_infos_rdd_new_sha1 = s3url_infos_rdd_join.subtractByKey(s3url_infos_rdd_with_sha1).flatMap(lambda x: s3url_to_cdr_id_nosha1(x)).flatMap(lambda x: check_get_sha1(x)).persist(StorageLevel.MEMORY_AND_DISK)
    # save rdd
    if save_inter_rdd:
        cdr_ids_infos_rdd_new_sha1_path = basepath_save + "/cdr_ids_infos_rdd_new_sha1"
        try:
            # check if file exists, delete before trying to write? 
            #cdr_ids_infos_rdd_new_sha1.mapValues(json.dumps).saveAsSequenceFile(cdr_ids_infos_rdd_new_sha1_path, compressionCodecClass=compression)
            cdr_ids_infos_rdd_new_sha1.mapValues(json.dumps).saveAsSequenceFile(cdr_ids_infos_rdd_new_sha1_path)
            save_info_incremental_update(hbase_man_update_out, incr_update_id, cdr_ids_infos_rdd_new_sha1_path, "cdr_ids_infos_rdd_new_sha1_path")
            # to be loaded with
            #cdr_ids_infos_rdd_new_sha1 = sc.sequenceFile(cdr_ids_infos_rdd_new_sha1_path).mapValues(json.loads)
        except Exception as inst:
            print("Could not save rdd at {}, error was {}.".format(cdr_ids_infos_rdd_new_sha1_path, inst))
    s3url_infos_rdd_join.unpersist()
    cdr_ids_infos_rdd_new_sha1_count = cdr_ids_infos_rdd_new_sha1.count()
    save_info_incremental_update(hbase_man_update_out, incr_update_id, cdr_ids_infos_rdd_new_sha1_count, "cdr_ids_infos_rdd_new_sha1_count")
    # here new sha1s means we did not see the corresponding s3url before, but the sha1 may still be in the sha1_infos table
    # so we still need to merge potentially
    update_rdd = cdr_ids_infos_rdd_new_sha1.flatMap(lambda x: to_sha1_key(x)).reduceByKey(reduce_sha1_infos_discarding).persist(StorageLevel.MEMORY_AND_DISK)
    update_rdd_count = update_rdd.count()
    save_info_incremental_update(hbase_man_update_out, incr_update_id, update_rdd_count, "update_rdd_count")
    hbase_man_cdrinfos_out.rdd2hbase(cdr_ids_infos_rdd_new_sha1.flatMap(lambda x: expand_info(x)))
    cdr_ids_infos_rdd_new_sha1.unpersist()
    ## update cdr_ids, and parents cdr_ids for these new sha1s
    sha1_infos_rdd = hbase_man_sha1infos_join.read_hbase_table().persist(StorageLevel.MEMORY_AND_DISK)
    # we may need to merge some 'all_cdr_ids' and 'all_parent_ids'
    if not sha1_infos_rdd.isEmpty(): 
        sha1_infos_rdd_json = sha1_infos_rdd.partitionBy(nb_partitions).flatMap(lambda x: sha1_key_json(x)).persist(StorageLevel.MEMORY_AND_DISK)
        sha1_infos_rdd.unpersist()
        # check for info:image_discarded in flatten_leftjoin
        join_rdd = update_rdd.leftOuterJoin(sha1_infos_rdd_json).flatMap(lambda x: flatten_leftjoin(x)).persist(StorageLevel.MEMORY_AND_DISK)
        sha1_infos_rdd_json.unpersist()
        out_rdd_amandeep = join_rdd
    else: # first update
        out_rdd_amandeep = update_rdd
    # save rdd
    if save_inter_rdd:
        out_rdd_path = basepath_save + "/out_rdd"
        try:
            out_rdd_amandeep.map(out_to_amandeep_dict_str).saveAsSequenceFile(out_rdd_path)
            save_info_incremental_update(hbase_man_update_out, incr_update_id, out_rdd_path, "out_rdd_path")
        except Exception as inst:
            print("Could not save rdd at {}, error was {}.".format(out_rdd_path, inst))
    ## write out rdd of new images 
    out_rdd = out_rdd_amandeep.flatMap(lambda x: split_sha1_kv_filter_max_images_discarded(x)).persist(StorageLevel.MEMORY_AND_DISK)
    hbase_man_sha1infos_out.rdd2hbase(out_rdd)
    sha1_infos_rdd.unpersist()

    ## save out newly computed sha1
    new_s3url_sha1_rdd = out_rdd.flatMap(lambda x: get_new_s3url_sha1(x))
    hbase_man_s3url_sha1_out.rdd2hbase(new_s3url_sha1_rdd)
    
    ## save out_rdd by batch of 10000 to be indexed?
    new_sha1s_rdd = out_rdd.keys()
    save_new_sha1s_for_index_update(new_sha1s_rdd, hbase_man_update_out, batch_update_size)

    ## save new images update infos
    new_s3url_sha1_rdd_count = new_s3url_sha1_rdd.count()
    print("[incremental_update] new_s3url_sha1_rdd_count count: {}".format(new_s3url_sha1_rdd_count))
    incr_update_infos_list = []
    update_elapsed_time = time.time() - start_time 
    incr_update_infos_list.append((incr_update_id, [incr_update_id, "info", "new_s3url_sha1_rdd_count", str(new_s3url_sha1_rdd_count)]))
    incr_update_infos_list.append((incr_update_id, [incr_update_id, "info", "update_elapsed_time", str(update_elapsed_time)]))
    incr_update_infos_rdd = sc.parallelize(incr_update_infos_list)
    hbase_man_update_out.rdd2hbase(incr_update_infos_rdd)

    out_rdd.unpersist()


if __name__ == '__main__':
    start_time = time.time()
    # parse options
    parser = OptionParser()
    parser.add_option("-r", "--restart", dest="restart", default=False, action="store_true")
    parser.add_option("-i", "--identifier", dest="identifier")
    parser.add_option("-d", "--day_to_process", dest="day_to_process")
    # should define the es_ts_start from day_to_process
    parser.add_option("-s", "--save", dest="save_inter_rdd", default=False, action="store_true")
    parser.add_option("-b", "--batch_update_size", dest="batch_update_size", default=10000)
    (c_options, args) = parser.parse_args()
    print "Got options:", c_options
    # Read job_conf
    job_conf = json.load(open("job_conf_notcommited_release.json","rt"))
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
    tab_s3url_sha1_name = job_conf["tab_s3url_sha1_name"]
    tab_update_name = job_conf["tab_update_name"]
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
    hbase_man_sha1infos_join = HbaseManager(sc, conf, hbase_host, tab_sha1_infos_name, columns_list=join_columns_list)
    hbase_man_s3url_sha1_in = HbaseManager(sc, conf, hbase_host, tab_s3url_sha1_name)
    hbase_man_sha1infos_out = HbaseManager(sc, conf, hbase_host, tab_sha1_infos_name)
    hbase_man_cdrinfos_out = HbaseManager(sc, conf, hbase_host, tab_cdrid_infos_name)
    hbase_man_s3url_sha1_out = HbaseManager(sc, conf, hbase_host, tab_s3url_sha1_name)
    hbase_man_update_in = HbaseManager(sc, conf, hbase_host, tab_update_name)
    hbase_man_update_out = HbaseManager(sc, conf, hbase_host, tab_update_name)
    incremental_update(es_man, es_ts_start, hbase_man_ts, hbase_man_cdrinfos_out, hbase_man_sha1infos_join, hbase_man_sha1infos_out, hbase_man_s3url_sha1_in, hbase_man_s3url_sha1_out, hbase_man_update_in, hbase_man_update_out, nb_partitions, c_options)
    print("[DONE] Update from ts {} done in {}s.".format(es_ts_start, time.time() - start_time))
