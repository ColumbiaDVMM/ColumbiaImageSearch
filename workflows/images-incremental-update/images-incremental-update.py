import os
import json
import time
import calendar
import datetime
import dateutil.parser

from optparse import OptionParser
from pyspark import SparkContext, SparkConf, StorageLevel
from elastic_manager import ES
from hbase_manager import HbaseManager

# debugging
debug = True
day_gap = 86400000 # One day
ts_gap = 10000000
#ts_gap = 10000

# default settings
#fields_cdr = ["obj_stored_url", "obj_parent", "obj_original_url", "timestamp", "crawl_data.image_id", "crawl_data.memex_ht_id"]
fields_cdr = ["obj_stored_url", "obj_parent"]
max_ts = 9999999999999
fields_list = [("info","all_cdr_ids"), ("info","s3_url"), ("info","all_parent_ids"), ("info","image_discarded"), ("info","cu_feat_id")]


def get_list_value(json_x,field_tuple):
    return [x["value"] for x in json_x if x["columnFamily"]==field_tuple[0] and x["qualifier"]==field_tuple[1]]


def get_SHA1_from_URL(URL):
    import image_dl
    sha1hash = image_dl.get_SHA1_from_URL_StringIO(URL,1) # 1 is verbose level
    return sha1hash


def get_row_sha1(URL_S3,verbose=False):
    row_sha1 = None
    if type(URL_S3) == unicode and URL_S3 != u'None' and URL_S3.startswith('https://s3'):
        row_sha1 = get_SHA1_from_URL(URL_S3)
    if row_sha1 and verbose:
        print "Got new SHA1 {} from_url {}.".format(row_sha1,URL_S3)
    return row_sha1


def check_get_sha1(data):
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
        except Exception as inst2:
            print "[Error] for row {}. {}".format(key,inst2)
            return []
        row_sha1 = get_row_sha1(unicode(URL_S3),0)
        if row_sha1:
            json_x["info:sha1"] = row_sha1
            return [(key, json_x)]
    return []


def check_get_sha1_s3url(data):
    URL_S3 = data[0]
    row_sha1 = get_row_sha1(unicode(URL_S3),0)
    if row_sha1:
        return [(URL_S3, (list(data[1][0]), row_sha1))]
    return []


def expand_info(data):
    key = data[0]
    json_x = data[1]
    out = []
    for field in json_x:
        fs = field.split(':')
        out.append((key, [key, fs[0], fs[1], json_x[field]]))
    return out


def create_images_tuple(data):
    doc_id = data[0]
    json_x = json.loads(data[1])
    ts =  json_x["_metadata"]["_timestamp"]
    key = str(max_ts-ts)+"_"+doc_id
    tup_list=[ (key, [key, "info", "doc_id", doc_id])]
    for field in fields_cdr:
        try:
            field_value = json_x[field][0]
            if field.endswith("url"):
                str_field_value = unicode(field_value)
            else:
                str_field_value = str(field_value)
            tup_list.append( (key, [key, "info", field, str_field_value]) )
        except Exception as inst:
            pass
    return tup_list


def cdrid_key_to_sha1_key(data):
    cdr_id = data[0]
    json_x = data[1]
    sha1 = None
    obj_stored_url = None
    obj_parent = None
    try:
        sha1_val = json_x["info:sha1"]
        if type(sha1_val)==list and len(sha1_val)==1:
            sha1 = sha1_val[0].strip()
        else:
            sha1 = sha1_val.strip()
        obj_stored_url = unicode(json_x["info:obj_stored_url"].strip())
        obj_parent = json_x["info:obj_parent"].strip()
    except Exception as inst2:
        print("[cdrid_key_to_sha1_key] could not read sha1, obj_stored_url or obj_parent for cdr_id {}".format(cdr_id))
        pass
    if cdr_id and sha1 and obj_stored_url and obj_parent:
        return [(sha1, {"info:all_cdr_ids": [cdr_id], "info:s3_url": [obj_stored_url], "info:all_parent_ids": [obj_parent]})]
    return []


def cdrid_key_to_s3url_key_sha1_val(data):
    json_x = data[1]
    sha1 = None
    obj_stored_url = None
    try:
        sha1_val = json_x["info:sha1"]
        if type(sha1_val)==list and len(sha1_val)==1:
            sha1 = sha1_val[0].strip()
        else:
            sha1 = sha1_val.strip()
        obj_stored_url = unicode(json_x["info:obj_stored_url"].strip())
    except Exception as inst2:
        pass
    if obj_stored_url and sha1:
        return [(obj_stored_url, sha1)]
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
    return c


def reduce_s3url_infos(a,b):
    a.extend(b)
    return a


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
        # need to keep info:cu_feat_id if it exists
        if "info:cu_feat_id" in b:
            c["info:cu_feat_id"] = b["info:cu_feat_id"]
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


def reduce_s3_keep_one_sha1(a,b):
    if a != b:
        raise ValueError("[reduce_s3_keep_one_sha1: error] one s3url has two differnet sha1 values {} and {}.".format(a, b))
    return a


def split_sha1_kv_filter_max_images_discarded(x):
    tmp_fields_list = [("info","all_cdr_ids"), ("info","s3_url"), ("info","all_parent_ids")]
    out = []
    if "info:image_discarded" in x[1]:
        out.append((x[0], [x[0], "info", "image_discarded", x[1]["info:image_discarded"]]))
        str_s3url_value = None
        s3url_value = x[1]["info:s3_url"][0]
        str_s3url_value = unicode(s3url_value)
        out.append((x[0], [x[0], "info", "s3_url", str_s3url_value]))
        out.append((x[0], [x[0], "info", "all_cdr_ids", x[1]["info:image_discarded"]]))
        out.append((x[0], [x[0], "info", "all_parent_ids", x[1]["info:image_discarded"]]))
    else:
        for field in tmp_fields_list:
            if field[1]=="s3_url":
                out.append((x[0], [x[0], field[0], field[1], unicode(x[1][field[0]+":"+field[1]][0])]))
            else:
                out.append((x[0], [x[0], field[0], field[1], ','.join(x[1][field[0]+":"+field[1]])]))
    return out


def out_to_amandeep_dict_str(x):
    # this is called with map()
    sha1 = x[0]
    # keys should be: "image_sha1", "all_parent_ids", "s3_url", "all_cdr_ids"
    # keep "cu_feat_id" to be able to push images to be indexed
    out_dict = dict()
    out_dict["image_sha1"] = sha1
    for field in ["all_parent_ids", "s3_url", "all_cdr_ids", "cu_feat_id"]:
        if "info:"+field in x[1]:
            out_dict[field] = x[1]["info:"+field]
    return (sha1, json.dumps(out_dict))


def amandeep_dict_str_to_out(x):
    # this is called with mapValues()
    # keys should be: "image_sha1", "all_parent_ids", "s3_url", "all_cdr_ids"
    # keep "cu_feat_id" to be able to push images to be indexed
    tmp_dict = json.loads(x)
    out_dict = dict()
    #sha1 = tmp_dict["image_sha1"]
    for field in ["all_parent_ids", "s3_url", "all_cdr_ids", "cu_feat_id"]:
        if field in tmp_dict:
            out_dict["info:"+field] = tmp_dict[field]
    return out_dict


def flatten_leftjoin(x):
    out = []
    # at this point value is a tuple of two lists with a single or empty dictionary
    c = reduce_sha1_infos_discarding(x[1][0],x[1][1])
    out.append((x[0], c))
    return out


def to_cdr_id_dict(data):
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
    #print("[to_cdr_id_dict] {}".format(tup_list))
    return tup_list


def to_s3_url_key(data):
    doc_id = data[0]
    v = data[1]
    tup_list = []
    if "info:obj_stored_url" in v:
        s3url = v["info:obj_stored_url"]
        if s3url.startswith('https://s3'):
            v["info:doc_id"] = doc_id
            tup_list = [(s3url, v)]
    return tup_list


def to_s3_url_key_dict_list(data):
    doc_id = data[0]
    v = data[1]
    tup_list = []
    if "info:obj_stored_url" in v:
        s3url = v["info:obj_stored_url"]
        if s3url.startswith('https://s3'):
            v["info:doc_id"] = doc_id
            tup_list = [(s3url, [v])]
    return tup_list


def s3url_dict_list_to_cdr_id_wsha1(data):
    if len(data[1]) != 2 or data[1][1] is None or data[1][1] == 'None' or data[1][1] == u'None':
        print("[s3url_dict_list_to_cdr_id_wsha1] incorrect data: {}".format(data))
        return []
    s3_url = data[0]
    list_v = data[1][0]
    sha1 = data[1][1]
    tup_list = []
    for v in list_v:
        if sha1:
            doc_id = v["info:doc_id"]
            if type(sha1) == list and len(sha1)==1:
                v["info:sha1"] = sha1[0]
            else:
                v["info:sha1"] = sha1
            tup_list.append((doc_id, v))
    return tup_list


def get_existing_joined_sha1(data):
    if len(data[1]) == 2 and data[1][1] and data[1][1] is not None and data[1][1] != 'None' and data[1][1] != u'None':
        return True
    return False


def clean_up_s3url_sha1(data):
    try:
        s3url = unicode(data[0]).strip()
        json_x = [json.loads(x) for x in data[1].split("\n")]
        sha1 = get_list_value(json_x,("info","sha1"))[0].strip()
        return [(s3url, sha1)]
    except:
        print("[clean_up_s3url_sha1] failed, data was: {}".format(data))
        return []


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
        print "[get_s3url_sha1: error] Could not get sha1 or s3url for row {}. {}".format(key, inst)
        return []
    if sha1 and s3url:
        return [(s3url, [s3url, "info", "sha1", sha1.upper()])]
    return []


def hbase_out_s3url_sha1(data):
    s3_url = data[0]
    sha1 = data[1]
    if sha1 and s3_url:
        return [(s3_url, [s3_url, "info", "sha1", sha1.upper()])]
    return []


def save_info_incremental_update(hbase_man_update_out, incr_update_id, info_value, info_name):
    print("[save_info_incremental_update] saving update info {}: {}".format(info_name, info_value))
    incr_update_infos_list = []
    incr_update_infos_list.append((incr_update_id, [incr_update_id, "info", info_name, str(info_value)]))
    incr_update_infos_rdd = sc.parallelize(incr_update_infos_list)
    hbase_man_update_out.rdd2hbase(incr_update_infos_rdd)


def build_batch_rdd(batch_udpate, incr_update_id, batch_id):
    import numpy as np
    # batch_rdd should be created to be stored in hbase table update_infos
    #update_id = "index_update_"+str(max_ts-int(time.time()*1000))+'_'+str(np.int32(np.random.random()*(10e6)))
    update_id = "index_update_"+incr_update_id+'_'+str(batch_id)
    list_key = []
    for x in batch_udpate:
        list_key.append(x)
    batch_out = [(update_id, [update_id, "info", "list_sha1s", ','.join(list_key)])]
    return sc.parallelize(batch_out)


def save_new_sha1s_for_index_update(new_sha1s_rdd, hbase_man_update_out, batch_update_size, incr_update_id, total_batches):
    start_save_time = time.time()
    iterator = new_sha1s_rdd.toLocalIterator()
    batch_udpate = []
    batch_id = 0
    for x in iterator:
        batch_udpate.append(x)
        if len(batch_udpate)==batch_update_size:
            try:
                print("[save_new_sha1s_for_index_update] will save batch {}/{} starting with: {}".format(batch_id+1, total_batches, batch_udpate[:10]))
                batch_rdd = build_batch_rdd(batch_udpate, incr_update_id, batch_id)
                batch_id += 1
                print("[save_new_sha1s_for_index_update] saving batch {}/{} of {} new images to HBase.".format(batch_id, total_batches, batch_update_size))
                hbase_man_update_out.rdd2hbase(batch_rdd)
                batch_rdd.unpersist()
            except Exception as inst:
                print("[save_new_sha1s_for_index_update] Could not create/save batch {}. Error was: {}".format(batch_id, inst))
            batch_udpate = []
    # last batch
    if batch_udpate:
        try:    
            print("[save_new_sha1s_for_index_update] will save batch {}/{} starting with: {}".format(batch_id+1, total_batches, batch_udpate[:10]))
            batch_rdd = build_batch_rdd(batch_udpate, incr_update_id, batch_id)
            batch_id += 1
            print("[save_new_sha1s_for_index_update] saving batch {}/{} of {} new images to HBase.".format(batch_id, total_batches, len(batch_udpate)))
            hbase_man_update_out.rdd2hbase(batch_rdd)
            batch_rdd.unpersist()
        except Exception as inst:
            print("[save_new_sha1s_for_index_update] Could not create/save batch {}. Error was: {}".format(batch_id, inst))
    print("[save_new_sha1s_for_index_update] DONE in {}s".format(time.time() - start_save_time))


def load_rdd_json(basepath_save, rdd_name):
    rdd_path = basepath_save + "/" + rdd_name
    rdd = None
    print("[load_rdd_json] trying to load rdd from {}.".format(rdd_path))
    try:
        rdd = sc.sequenceFile(rdd_path).mapValues(json.loads)
    except Exception as inst:
        print("[load_rdd_json: caught error] could not load rdd from {}. Error was {}.".format(rdd_path, inst))
    return rdd


def save_rdd_json(basepath_save, rdd_name, rdd, incr_update_id, hbase_man_update_out):
    rdd_path = basepath_save + "/" + rdd_name
    print("[save_rdd_json] saving rdd to {}.".format(rdd_path))
    if not rdd.isEmpty():
        try:
            rdd.mapValues(json.dumps).saveAsSequenceFile(rdd_path)
            save_info_incremental_update(hbase_man_update_out, incr_update_id, rdd_path, rdd_name+"_path")
        except Exception as inst:
            print("[save_rdd_json: caught error] could not save rdd at {}, error was {}.".format(rdd_path, inst))
    else:
        save_info_incremental_update(hbase_man_update_out, incr_update_id, "EMPTY", rdd_name+"_path")


def get_cdr_ids_infos_rdd(basepath_save, es_man, es_ts_start, hbase_man_update_out, incr_update_id, nb_partitions, restart, save_inter_rdd, start_time):
    rdd_name = "cdr_ids_infos_rdd"
    # always try to load from disk
    cdr_ids_infos_rdd = load_rdd_json(basepath_save, rdd_name)
    if cdr_ids_infos_rdd is not None:
        print("[get_cdr_ids_infos_rdd: info] cdr_ids_infos_rdd loaded rdd from {}.".format(basepath_save + "/" + rdd_name))
        return cdr_ids_infos_rdd

    if debug:
        query = "{\"fields\": [\""+"\", \"".join(fields_cdr)+"\"], \"query\": {\"filtered\": {\"query\": {\"match\": {\"content_type\": \"image/jpeg\"}}, \"filter\": {\"range\" : {\"_timestamp\" : {\"gte\" : "+str(es_ts_start)+", \"lt\": "+str(es_ts_start+ts_gap)+"}}}}}, \"sort\": [ { \"_timestamp\": { \"order\": \"asc\" } } ] }"
    else:
        query = "{\"fields\": [\""+"\", \"".join(fields_cdr)+"\"], \"query\": {\"filtered\": {\"query\": {\"match\": {\"content_type\": \"image/jpeg\"}}, \"filter\": {\"range\" : {\"_timestamp\" : {\"gte\" : "+str(es_ts_start)+"}}}}}, \"sort\": [ { \"_timestamp\": { \"order\": \"asc\" } } ] }"
    print("[get_cdr_ids_infos_rdd] query CDR with: {}".format(query))
    
    # get incremental update
    es_rdd_nopart = es_man.es2rdd(query)
    if es_rdd_nopart.isEmpty():
        print("[get_cdr_ids_infos_rdd] empty incremental update when querying from timestamp {}".format(es_ts_start))
        return None

    # es_rdd_nopart is likely to be underpartitioned
    es_rdd = es_rdd_nopart.partitionBy(nb_partitions)

    # save incremental update infos
    incr_update_infos_list = []
    es_rdd_count = es_rdd.count()
    incr_update_infos_list.append((incr_update_id, [incr_update_id, "info", "start_time", str(start_time)]))
    incr_update_infos_list.append((incr_update_id, [incr_update_id, "info", "es_rdd_count", str(es_rdd_count)]))
    incr_update_infos_rdd = sc.parallelize(incr_update_infos_list)
    hbase_man_update_out.rdd2hbase(incr_update_infos_rdd)

    # save to hbase
    images_ts_cdrid_rdd = es_rdd.flatMap(create_images_tuple)
    print("[get_cdr_ids_infos_rdd] saving images_ts_cdrid_rdd to HBase.")
    hbase_man_ts.rdd2hbase(images_ts_cdrid_rdd)

    min_ts_cdrid = images_ts_cdrid_rdd.min()[0].strip()
    max_ts_cdrid = images_ts_cdrid_rdd.max()[0].strip()

    # save incremental update infos
    incr_update_infos_list = []
    incr_update_infos_list.append((incr_update_id, [incr_update_id, "info", "min_ts_cdrid", min_ts_cdrid]))
    incr_update_infos_list.append((incr_update_id, [incr_update_id, "info", "max_ts_cdrid", max_ts_cdrid]))
    incr_update_infos_rdd = sc.parallelize(incr_update_infos_list)
    print("[get_cdr_ids_infos_rdd] saving incremental update infos: id {}, min_ts_cdrid {}, max_ts_cdrid {}".format(incr_update_id, min_ts_cdrid, max_ts_cdrid))
    hbase_man_update_out.rdd2hbase(incr_update_infos_rdd)

    cdr_ids_infos_rdd = es_rdd.flatMap(to_cdr_id_dict)
    # save rdd
    if save_inter_rdd:
        save_rdd_json(basepath_save, rdd_name, cdr_ids_infos_rdd, incr_update_id, hbase_man_update_out)
    return cdr_ids_infos_rdd
        

def dump_s3url_info(x):
    v = dict()
    v["left"] = x[0]
    v["right"] = x[1]
    return json.dumps(v)


def load_s3url_info(x):
    v = json.loads(x)
    x0 = json.loads(v["left"])
    x1 = v["right"]
    return (x0, x1)


def dump_s3url_info_list_dict(x):
    v = dict()
    v["left"] = dict()
    i = 0
    #for w in list(x[0]):
    for w in x[0]:
        if w:
            v["left"][str(i)] = json.dumps(w)
            i += 1
    if x[1]:
        v["right"] = x[1]
    return json.dumps(v)


def load_s3url_info_list_dict(x):
    v = json.loads(x)
    x0 = []
    x1 = []
    for w in v["left"]:
        x0.append(json.loads(v["left"][w]))
        #x0.append(json.loads(w))
    if "right" in v:
        x1 = v["right"]
    return (x0, x1)


def load_s3url_infos_rdd_join(s3url_infos_rdd_join_path):
    s3url_infos_rdd_join = None
    try:
        #s3url_infos_rdd_join = sc.sequenceFile(s3url_infos_rdd_join_path).mapValues(load_s3url_info)
        s3url_infos_rdd_join = sc.sequenceFile(s3url_infos_rdd_join_path).mapValues(load_s3url_info_list_dict)
        print("[load_s3url_infos_rdd_join: info] first samples of s3url_infos_rdd_join looks like: {}".format(s3url_infos_rdd_join.take(5)))
    except Exception as inst:
        print("[load_s3url_infos_rdd_join: caught error] Could not load rdd at {}. Error was {}.".format(s3url_infos_rdd_join_path, inst))
    return s3url_infos_rdd_join


def save_s3url_infos_rdd_join(s3url_infos_rdd_join, hbase_man_update_out, incr_update_id, s3url_infos_rdd_join_path, s3url_infos_rdd_join_path_str):
    try:
        print("[save_s3url_infos_rdd_join: info] saving 's3url_infos_rdd_join' to {}.".format(s3url_infos_rdd_join_path))
        #s3url_infos_rdd_join.mapValues(dump_s3url_info).saveAsSequenceFile(s3url_infos_rdd_join_path)
        s3url_infos_rdd_join_tosave = s3url_infos_rdd_join.mapValues(dump_s3url_info_list_dict)
        print("[save_s3url_infos_rdd_join: info] first samples of s3url_infos_rdd_join_tosave looks like: {}".format(s3url_infos_rdd_join_tosave.take(5)))
        s3url_infos_rdd_join_tosave.saveAsSequenceFile(s3url_infos_rdd_join_path)
        save_info_incremental_update(hbase_man_update_out, incr_update_id, s3url_infos_rdd_join_path, s3url_infos_rdd_join_path_str)
    except Exception as inst:
        print("[save_s3url_infos_rdd_join: caught error] could not save rdd at {}, error was {}.".format(s3url_infos_rdd_join_path, inst))


def get_s3url_infos_rdd_join(basepath_save, es_man, es_ts_start, hbase_man_update_out, incr_update_id, nb_partitions, restart, save_inter_rdd, start_time):
    rdd_name = "s3url_infos_rdd_join"
    s3url_infos_rdd_join_path = basepath_save + "/" + rdd_name
    # always try to load from disk
    s3url_infos_rdd_join = load_s3url_infos_rdd_join(s3url_infos_rdd_join_path)
    if s3url_infos_rdd_join is not None:
        print("[get_s3url_infos_rdd_join: info] loaded rdd from {}.".format(s3url_infos_rdd_join_path))
        return s3url_infos_rdd_join

    # get dependency cdr_ids_infos_rdd
    cdr_ids_infos_rdd = get_cdr_ids_infos_rdd(basepath_save, es_man, es_ts_start, hbase_man_update_out, incr_update_id, nb_partitions, restart, save_inter_rdd, start_time)
    
    if cdr_ids_infos_rdd is None:
        print("[get_s3url_infos_rdd_join] cdr_ids_infos_rdd is empty.")
        save_info_incremental_update(hbase_man_update_out, incr_update_id, 0, rdd_name+"_count")
        save_info_incremental_update(hbase_man_update_out, incr_update_id, "EMPTY", rdd_name+"_path")
        return None

    print("[get_s3url_infos_rdd_join: info] computing rdd s3url_infos_rdd_join.")
    # there could be duplicates cdr_id near indices boundary or corrections might have been applied...
    cdr_ids_infos_rdd_red = cdr_ids_infos_rdd.reduceByKey(reduce_cdrid_infos)
    # invert cdr_ids_infos_rdd (k,v) into s3url_infos_rdd (v[s3_url],[v,v['cdr_id']=k])
    s3url_infos_rdd = cdr_ids_infos_rdd_red.flatMap(to_s3_url_key_dict_list)
    s3url_infos_rdd_red = s3url_infos_rdd.reduceByKey(reduce_s3url_infos)
    # get some stats
    s3url_infos_rdd_count = s3url_infos_rdd.count()
    print("[get_s3url_infos_rdd_join: info] s3url_infos_rdd_count is: {}".format(s3url_infos_rdd_count))
    s3url_infos_rdd_red_count = s3url_infos_rdd_red.count()
    print("[get_s3url_infos_rdd_join: info] s3url_infos_rdd_red_count is: {}".format(s3url_infos_rdd_red_count))
    print("[get_s3url_infos_rdd_join: info] s3url_infos_rdd_red first samples looks like: {}".format(s3url_infos_rdd_red.take(10)))
    try:
        # try to reload from disk
        s3url_sha1_rdd = sc.sequenceFile(basepath_save + "/s3url_sha1_rdd")
    except Exception as inst:
        # read s3url_sha1 table into s3url_sha1 to get sha1 here without downloading images
        print("[get_s3url_infos_rdd_join] starting to read from s3url_sha1 HBase table.")
        s3url_sha1_rdd = hbase_man_s3url_sha1_in.read_hbase_table().flatMap(clean_up_s3url_sha1)
        try:
            s3url_sha1_rdd.saveAsSequenceFile(basepath_save + "/s3url_sha1_rdd")
        except Exception as inst:
            pass
    s3url_sha1_rdd_count = s3url_sha1_rdd.count()
    print("[get_s3url_infos_rdd_join: info] s3url_sha1_rdd_count is: {}".format(s3url_sha1_rdd_count))
    s3url_sha1_rdd_partitioned = s3url_sha1_rdd.partitionBy(nb_partitions).persist(StorageLevel.MEMORY_AND_DISK)
    s3url_infos_rdd_red_partitioned = s3url_infos_rdd_red.partitionBy(nb_partitions).persist(StorageLevel.MEMORY_AND_DISK)  
    #print("[get_s3url_infos_rdd_join] start running 's3url_infos_rdd_red.cogroup(s3url_sha1_rdd)'.")
    #s3url_infos_rdd_join = s3url_infos_rdd_red_partitioned.cogroup(s3url_sha1_rdd_partitioned)
    print("[get_s3url_infos_rdd_join] start running 's3url_infos_rdd_red.leftOuterJoin(s3url_sha1_rdd)'.")
    s3url_infos_rdd_join = s3url_infos_rdd_red_partitioned.leftOuterJoin(s3url_sha1_rdd_partitioned)
    s3url_infos_rdd_join_count = s3url_infos_rdd_join.count()
    print("[get_s3url_infos_rdd_join: info] s3url_infos_rdd_join_count is: {}".format(s3url_infos_rdd_join_count))
    
    # save rdd.
    if save_inter_rdd:
        save_s3url_infos_rdd_join(s3url_infos_rdd_join, hbase_man_update_out, incr_update_id, s3url_infos_rdd_join_path, "s3url_infos_rdd_join_path")
        
    # unpersist here, s3url_infos_rdd_join has been saved to hdfs
    s3url_sha1_rdd_partitioned.unpersist()
    s3url_infos_rdd_red_partitioned.unpersist()

    return s3url_infos_rdd_join


def get_cdr_ids_infos_rdd_join_sha1(basepath_save, es_man, es_ts_start, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, restart, save_inter_rdd):
    rdd_name = "cdr_ids_infos_rdd_join_sha1"
    # always try to load from disk
    cdr_ids_infos_rdd_join_sha1 = load_rdd_json(basepath_save, rdd_name)
    if cdr_ids_infos_rdd_join_sha1 is not None:
        print("[get_cdr_ids_infos_rdd_join_sha1: info] cdr_ids_infos_rdd_join_sha1 loaded rdd from {}.".format(basepath_save + "/" + rdd_name))
        return cdr_ids_infos_rdd_join_sha1

    # get dependency s3url_infos_rdd_join
    s3url_infos_rdd_join = get_s3url_infos_rdd_join(basepath_save, es_man, es_ts_start, hbase_man_update_out, incr_update_id, nb_partitions, restart, save_inter_rdd, start_time)
    if s3url_infos_rdd_join is None:
        print("[get_cdr_ids_infos_rdd_join_sha1] s3url_infos_rdd_join is empty.")
        save_info_incremental_update(hbase_man_update_out, incr_update_id, 0, rdd_name+"_count")
        save_info_incremental_update(hbase_man_update_out, incr_update_id, "EMPTY", rdd_name+"_path")
        return None

    # invert s3url_infos_rdd_join (s3_url, ([v], sha1)) into cdr_ids_infos_rdd_join_sha1 (k, [v]) adding info:sha1 in each v dict
    s3url_infos_rdd_with_sha1 = s3url_infos_rdd_join.filter(get_existing_joined_sha1)
    cdr_ids_infos_rdd_join_sha1 = s3url_infos_rdd_with_sha1.flatMap(s3url_dict_list_to_cdr_id_wsha1)
    print("[get_cdr_ids_infos_rdd_join_sha1: info] cdr_ids_infos_rdd_join_sha1 first samples look like: {}".format(cdr_ids_infos_rdd_join_sha1.take(5)))

    # save infos to hbase update table
    cdr_ids_infos_rdd_join_sha1_count = cdr_ids_infos_rdd_join_sha1.count()
    save_info_incremental_update(hbase_man_update_out, incr_update_id, cdr_ids_infos_rdd_join_sha1_count, rdd_name+"_count")
    
    # save rdd content to hbase
    print("[get_cdr_ids_infos_rdd_join_sha1] saving cdr_ids_infos_rdd_join_sha1 to HBase.")
    hbase_man_cdrinfos_out.rdd2hbase(cdr_ids_infos_rdd_join_sha1.flatMap(expand_info))

    # save rdd to disk
    if save_inter_rdd:
        save_rdd_json(basepath_save, rdd_name, cdr_ids_infos_rdd_join_sha1, incr_update_id, hbase_man_update_out)
    return cdr_ids_infos_rdd_join_sha1


def get_update_join_rdd(basepath_save, es_man, es_ts_start, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, restart, save_inter_rdd):
    rdd_name = "update_join_rdd"
    # always try to load from disk
    update_join_rdd = load_rdd_json(basepath_save, rdd_name)
    if update_join_rdd is not None:
        print("[get_update_join_rdd: info] update_join_rdd loaded rdd from {}.".format(basepath_save + "/" + rdd_name))
        return update_join_rdd

    # we need cdr_ids_infos_rdd_join_sha1
    cdr_ids_infos_rdd_join_sha1 = get_cdr_ids_infos_rdd_join_sha1(basepath_save, es_man, es_ts_start, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, restart, save_inter_rdd)
    if cdr_ids_infos_rdd_join_sha1 is None:
        print("[get_update_join_rdd] cdr_ids_infos_rdd_join_sha1 is empty.")
        save_info_incremental_update(hbase_man_update_out, incr_update_id, 0, rdd_name+"_count")
        save_info_incremental_update(hbase_man_update_out, incr_update_id, "EMPTY", rdd_name+"_path")
        return None

    # transform cdr_id rdd into sha1 rdd
    print("[get_update_join_rdd] cdr_ids_infos_rdd_join_sha1 first samples are: {}".format(cdr_ids_infos_rdd_join_sha1.take(5)))
    sha1_infos_rdd_from_join = cdr_ids_infos_rdd_join_sha1.flatMap(cdrid_key_to_sha1_key)
    update_join_rdd = sha1_infos_rdd_from_join.reduceByKey(reduce_sha1_infos_discarding)

    # save rdd infos
    update_join_rdd_count = update_join_rdd.count()
    save_info_incremental_update(hbase_man_update_out, incr_update_id, update_join_rdd_count, rdd_name+"_count")

    # save to disk
    if save_inter_rdd:
        save_rdd_json(basepath_save, rdd_name, update_join_rdd, incr_update_id, hbase_man_update_out)
    return update_join_rdd


def compute_out_join_rdd(basepath_save, es_man, es_ts_start, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, restart, save_inter_rdd, start_time):
    ## check if we not already have computed this join step of this update
    out_join_rdd_path = basepath_save + "/out_join_rdd"
    out_join_rdd_amandeep = None
    update_join_rdd_partitioned = None
    sha1_infos_rdd_json = None

    if restart:
        try:
            out_join_rdd_amandeep = sc.sequenceFile(out_join_rdd_path).mapValues(amandeep_dict_str_to_out)
        except Exception as inst:
            pass
        if out_join_rdd_amandeep is not None:
            # consider already processed
            print("[compute_out_join_rdd] out_join_rdd already computed for update {}.".format(incr_update_id))
            return out_join_rdd_amandeep

    ## try to reload rdds that could have already been computed, compute chain of dependencies if needed 
    update_join_rdd = get_update_join_rdd(basepath_save, es_man, es_ts_start, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, restart, save_inter_rdd)
    if update_join_rdd is None:
        print("[compute_out_join_rdd] update_join_rdd is empty.")
    else:
        ## update cdr_ids, and parents cdr_ids for the existing sha1s (if any)
        print("[compute_out_join_rdd] Reading from sha1_infos HBase table.")
        sha1_infos_rdd = hbase_man_sha1infos_join.read_hbase_table()
        if not sha1_infos_rdd.isEmpty(): 
            update_join_rdd_partitioned = update_join_rdd.partitionBy(nb_partitions).persist(StorageLevel.MEMORY_AND_DISK)
            sha1_infos_rdd_json = sha1_infos_rdd.flatMap(sha1_key_json).partitionBy(nb_partitions).persist(StorageLevel.MEMORY_AND_DISK)
            update_join_sha1_rdd = update_join_rdd_partitioned.leftOuterJoin(sha1_infos_rdd_json).flatMap(flatten_leftjoin)
            out_join_rdd_amandeep = update_join_sha1_rdd
        else: # first update
            out_join_rdd_amandeep = update_join_rdd

    ## save rdd
    if save_inter_rdd: 
        if out_join_rdd_amandeep is None or out_join_rdd_amandeep.isEmpty(): 
            save_info_incremental_update(hbase_man_update_out, incr_update_id, "EMPTY", "out_join_rdd_path")
        else:
            try:
                out_join_rdd_amandeep.filter(lambda x: "info:image_discarded" not in x[1]).map(out_to_amandeep_dict_str).saveAsSequenceFile(out_join_rdd_path)
                save_info_incremental_update(hbase_man_update_out, incr_update_id, out_join_rdd_path, "out_join_rdd_path")
            except Exception as inst:
                print("[compute_out_join_rdd] could not save rdd at {}, error was {}.".format(out_join_rdd_path, inst))
   
    if out_join_rdd_amandeep is not None:
        ## save sha1 infos for these joined images in HBase
        out_join_rdd = out_join_rdd_amandeep.flatMap(split_sha1_kv_filter_max_images_discarded)
        print("[compute_out_join_rdd] saving 'out_join_rdd' to sha1_infos HBase table.")
        hbase_man_sha1infos_out.rdd2hbase(out_join_rdd)

    # unpersist here, out_join_rdd_amandeep has been saved to hdfs and HBase
    if update_join_rdd_partitioned is not None:
        update_join_rdd_partitioned.unpersist()
    if sha1_infos_rdd_json is not None:
        sha1_infos_rdd_json.unpersist()

    return out_join_rdd_amandeep


def get_cdr_ids_infos_rdd_new_sha1(basepath_save, es_man, es_ts_start, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, restart, save_inter_rdd):
    ## for not matching s3url i.e. missing sha1
    rdd_name = "cdr_ids_infos_rdd_new_sha1"
    # always try to load from disk
    cdr_ids_infos_rdd_new_sha1 = load_rdd_json(basepath_save, rdd_name)
    if cdr_ids_infos_rdd_new_sha1 is not None:
        print("[get_cdr_ids_infos_rdd_new_sha1: info] cdr_ids_infos_rdd_new_sha1 loaded rdd from {}.".format(basepath_save + "/" + rdd_name))
        print("[get_cdr_ids_infos_rdd_new_sha1: info] cdr_ids_infos_rdd_new_sha1 first samples look like: {}".format(cdr_ids_infos_rdd_new_sha1.take(5)))
        return cdr_ids_infos_rdd_new_sha1
    
    # issue with reading from s3url_sha1. actually issue with leftJoin
    s3url_infos_rdd_join = get_s3url_infos_rdd_join(basepath_save, es_man, es_ts_start, hbase_man_update_out, incr_update_id, nb_partitions, restart, save_inter_rdd, start_time)
    if s3url_infos_rdd_join is not None:
        s3url_infos_rdd_with_sha1 = s3url_infos_rdd_join.filter(get_existing_joined_sha1)
        s3url_infos_rdd_no_sha1 = s3url_infos_rdd_join.subtractByKey(s3url_infos_rdd_with_sha1).partitionBy(nb_partitions)
        s3url_infos_rdd_no_sha1_count = s3url_infos_rdd_no_sha1.count()
        print("[get_cdr_ids_infos_rdd_new_sha1: info] starting to download images to get new sha1s for {} URLs.".format(s3url_infos_rdd_no_sha1_count))
        s3url_infos_rdd_new_sha1 = s3url_infos_rdd_no_sha1.flatMap(check_get_sha1_s3url)
        cdr_ids_infos_rdd_new_sha1 = s3url_infos_rdd_new_sha1.flatMap(s3url_dict_list_to_cdr_id_wsha1)
    else:
        cdr_ids_infos_rdd_new_sha1 = None
    
    if cdr_ids_infos_rdd_new_sha1 is None or cdr_ids_infos_rdd_new_sha1.isEmpty():
        print("[get_cdr_ids_infos_rdd_new_sha1] cdr_ids_infos_rdd_new_sha1 is empty.")
        save_info_incremental_update(hbase_man_update_out, incr_update_id, 0, rdd_name+"_count")
        save_info_incremental_update(hbase_man_update_out, incr_update_id, "EMPTY", rdd_name+"_path")
        return None
    else:
        # save rdd
        print("[get_cdr_ids_infos_rdd_new_sha1: info] cdr_ids_infos_rdd_new_sha1 first samples look like: {}".format(cdr_ids_infos_rdd_new_sha1.take(5)))
        if save_inter_rdd:
            save_rdd_json(basepath_save, "cdr_ids_infos_rdd_new_sha1", cdr_ids_infos_rdd_new_sha1, incr_update_id, hbase_man_update_out)
        # save infos
        cdr_ids_infos_rdd_new_sha1_count = cdr_ids_infos_rdd_new_sha1.count()
        save_info_incremental_update(hbase_man_update_out, incr_update_id, cdr_ids_infos_rdd_new_sha1_count, "cdr_ids_infos_rdd_new_sha1_count")
        print("[get_cdr_ids_infos_rdd_new_sha1] saving 'cdr_ids_infos_rdd_new_sha1' to HBase.")
        hbase_man_cdrinfos_out.rdd2hbase(cdr_ids_infos_rdd_new_sha1.flatMap(expand_info))
    return cdr_ids_infos_rdd_new_sha1


def get_update_rdd(basepath_save, es_man, es_ts_start, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, restart, save_inter_rdd):
    rdd_name = "update_rdd"
    # always try to load from disk
    update_rdd = load_rdd_json(basepath_save, rdd_name)
    if update_rdd is not None:
        print("[get_update_rdd: info] update_rdd loaded rdd from {}.".format(basepath_save + "/" + rdd_name))
        return update_rdd

    cdr_ids_infos_rdd_new_sha1 = get_cdr_ids_infos_rdd_new_sha1(basepath_save, es_man, es_ts_start, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, restart, save_inter_rdd)
    if cdr_ids_infos_rdd_new_sha1 is None:
        print("[get_update_rdd] cdr_ids_infos_rdd_new_sha1 is empty.")
        save_info_incremental_update(hbase_man_update_out, incr_update_id, 0, rdd_name+"_count")
        save_info_incremental_update(hbase_man_update_out, incr_update_id, "EMPTY", rdd_name+"_path")
        return None
        
    # here new sha1s means we did not see the corresponding s3url before, but the sha1 may still be in the sha1_infos table
    # so we still need to merge potentially
    update_rdd = cdr_ids_infos_rdd_new_sha1.flatMap(cdrid_key_to_sha1_key).reduceByKey(reduce_sha1_infos_discarding)
    update_rdd_count = update_rdd.count()
    save_info_incremental_update(hbase_man_update_out, incr_update_id, update_rdd_count, "update_rdd_count")

    # save to disk
    if save_inter_rdd:
        save_rdd_json(basepath_save, rdd_name, update_rdd, incr_update_id, hbase_man_update_out)
    return update_rdd


def compute_out_rdd(basepath_save, es_man, es_ts_start, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, restart, save_inter_rdd, start_time):
    ## check if we not already have computed this join step of this update
    rdd_name = "out_rdd"
    out_rdd_path = basepath_save + "/" + rdd_name
    out_rdd_amandeep = None
    update_rdd_partitioned = None
    sha1_infos_rdd_json = None

    if restart:
        try:
            out_rdd_amandeep = sc.sequenceFile(out_rdd_path).mapValues(amandeep_dict_str_to_out)
        except Exception as inst:
            pass
        if out_rdd_amandeep is not None:
            # consider already processed
            print("[compute_out_rdd] out_rdd already computed for update {}.".format(incr_update_id))
            return out_rdd_amandeep

    ## try to reload rdds that could have already been computed, compute chain of dependencies if needed 
    update_rdd = get_update_rdd(basepath_save, es_man, es_ts_start, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, restart, save_inter_rdd)
    if update_rdd is None:
        print("[compute_out_rdd] update_rdd is empty.")
        save_info_incremental_update(hbase_man_update_out, incr_update_id, 0, rdd_name+"_count")
        save_info_incremental_update(hbase_man_update_out, incr_update_id, "EMPTY", rdd_name+"_path")
        return None

    ## update cdr_ids, and parents cdr_ids for these new sha1s
    sha1_infos_rdd = hbase_man_sha1infos_join.read_hbase_table()
    # we may need to merge some 'all_cdr_ids' and 'all_parent_ids'
    if not sha1_infos_rdd.isEmpty(): 
        update_rdd_partitioned = update_rdd.partitionBy(nb_partitions).persist(StorageLevel.MEMORY_AND_DISK)
        sha1_infos_rdd_json = sha1_infos_rdd.flatMap(sha1_key_json).partitionBy(nb_partitions).persist(StorageLevel.MEMORY_AND_DISK)
        join_rdd = update_rdd_partitioned.leftOuterJoin(sha1_infos_rdd_json).flatMap(flatten_leftjoin)
        out_rdd_amandeep = join_rdd
    else: # first update
        out_rdd_amandeep = update_rdd
    # save rdd
    if save_inter_rdd:
        try:
            out_rdd_save = out_rdd_amandeep.filter(lambda x: "info:image_discarded" not in x[1]).map(out_to_amandeep_dict_str)
            if not out_rdd_save.isEmpty():
                out_rdd_save.saveAsSequenceFile(out_rdd_path)
                save_info_incremental_update(hbase_man_update_out, incr_update_id, out_rdd_path, rdd_name+"_path")
            else:
                print("[compute_out_rdd] 'out_rdd_save' is empty.")
                save_info_incremental_update(hbase_man_update_out, incr_update_id, "EMPTY", rdd_name+"_path")
                #return None
        # org.apache.hadoop.mapred.FileAlreadyExistsException
        except Exception as inst:
            print("[compute_out_rdd] could not save rdd at {}, error was {}.".format(out_rdd_path, inst))
    ## write out rdd of new images 
    out_rdd = out_rdd_amandeep.flatMap(split_sha1_kv_filter_max_images_discarded).persist(StorageLevel.MEMORY_AND_DISK)
    if not out_rdd.isEmpty():
        print("[compute_out_rdd] saving 'out_rdd' to sha1_infos HBase table.")
        hbase_man_sha1infos_out.rdd2hbase(out_rdd)
    else:
        print("[compute_out_rdd] 'out_rdd' is empty.")

    # unpersist here, out_rdd has been save to HDFS and HBase
    if update_rdd_partitioned is not None:
        update_rdd_partitioned.unpersist()
    if sha1_infos_rdd_json is not None:
        sha1_infos_rdd_json.unpersist()

    return out_rdd_amandeep


def save_new_images_for_index(basepath_save, out_rdd,  hbase_man_update_out, incr_update_id, batch_update_size, save_inter_rdd, new_images_to_index_str):
    # save images without cu_feat_id that have not been discarded for indexing
    new_images_to_index = out_rdd.filter(lambda x: "info:image_discarded" not in x[1] and "info:cu_feat_id" not in x[1])
    
    new_images_to_index_count = new_images_to_index.count()
    print("[save_new_images_for_index] {}_count count: {}".format(new_images_to_index_str, new_images_to_index_count))
    save_info_incremental_update(hbase_man_update_out, incr_update_id, new_images_to_index_count, new_images_to_index_str+"_count")

    import numpy as np
    total_batches = int(np.ceil(np.float32(new_images_to_index_count)/batch_update_size))
    # partition to the number of batches?
    # 'save_new_sha1s_for_index_update' uses toLocalIterator()
    new_images_to_index_partitioned = new_images_to_index.partitionBy(total_batches)

    # save to HDFS too
    try:
        new_images_to_index_out_path = basepath_save + "/" + new_images_to_index_str
        new_images_to_index_partitioned.keys().saveAsTextFile(new_images_to_index_out_path)
        save_info_incremental_update(hbase_man_update_out, incr_update_id, new_images_to_index_out_path, new_images_to_index_str+"_path")
    except Exception as inst:
        print("[save_new_images_for_index] could not save rdd 'new_images_to_index' at {}, error was {}.".format(new_images_to_index_out_path, inst))

    # save by batch in HBase to let the API know it needs to index these images    
    print("[save_new_images_for_index] start saving by batches of {} new images.".format(batch_update_size))
    # crashes in 'save_new_sha1s_for_index_update'?
    save_new_sha1s_for_index_update(new_images_to_index_partitioned.keys(), hbase_man_update_out, batch_update_size, incr_update_id, total_batches)
    #save_new_sha1s_for_index_update(new_images_to_index, hbase_man_update_out, batch_update_size, incr_update_id, total_batches)

        

def save_new_s3_url(basepath_save, es_man, es_ts_start, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, restart, save_inter_rdd):
    ## save out newly computed sha1
    cdr_ids_infos_rdd_new_sha1 = get_cdr_ids_infos_rdd_new_sha1(basepath_save, es_man, es_ts_start, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, restart, save_inter_rdd)
    if cdr_ids_infos_rdd_new_sha1 is None:
        print("[save_new_s3_url] cdr_ids_infos_rdd_new_sha1 is empty.")
        save_info_incremental_update(hbase_man_update_out, incr_update_id, 0, "new_s3url_sha1_rdd_count")
        return

    # invert cdr_ids_infos_rdd_new_sha1 to (s3url, sha1) and apply reduceByKey() selecting any sha1
    new_s3url_sha1_rdd = cdr_ids_infos_rdd_new_sha1.flatMap(cdrid_key_to_s3url_key_sha1_val)
    out_new_s3url_sha1_rdd = new_s3url_sha1_rdd.reduceByKey(reduce_s3_keep_one_sha1).flatMap(hbase_out_s3url_sha1)
    print("[save_new_s3_url] saving 'out_new_s3url_sha1_rdd' to HBase.")
    hbase_man_s3url_sha1_out.rdd2hbase(out_new_s3url_sha1_rdd)
    
    ## save new images update infos
    new_s3url_sha1_rdd_count = out_new_s3url_sha1_rdd.count()
    print("[save_new_s3_url] new_s3url_sha1_rdd_count count: {}".format(new_s3url_sha1_rdd_count))
    save_info_incremental_update(hbase_man_update_out, incr_update_id, new_s3url_sha1_rdd_count, "new_s3url_sha1_rdd_count")


def incremental_update(es_man, es_ts_start, hbase_man_ts, hbase_man_cdrinfos_out, hbase_man_sha1infos_join, hbase_man_sha1infos_out, hbase_man_s3url_sha1_in, hbase_man_s3url_sha1_out, hbase_man_update_in, hbase_man_update_out, nb_partitions, c_options):
    # read options
    restart = c_options.restart
    identifier = c_options.identifier
    day_to_process = c_options.day_to_process
    save_inter_rdd = c_options.save_inter_rdd
    batch_update_size = c_options.batch_update_size

    # Often gets error like
    # java.net.SocketException: Broken pipe
    # java.lang.OutOfMemoryError: GC overhead limit exceeded
    # java.lang.OutOfMemoryError: Java heap space
    # org.apache.spark.SparkException: EOF reached before Python server acknowledged
    # Seems to be related to memory configuration of the job...
    # Never managed to finish incremental_update_8524822279848, restart workflow with -r -i incremental_update_8524822279848
    # Managed to finish incremental_update_8524802151126
    # container is running beyond physical memory limits. Current usage: 8.0 GB of 8 GB physical memory used; 41.6 GB of 16.8 GB virtual memory used. Killing container.
    # How to make it really stable? Force a save to disk?
    # https://www.mail-archive.com/search?l=issues@spark.apache.org&q=subject:%22%5Bjira%5D+%5BComment+Edited%5D+(SPARK-5395)+Large+number+of+Python+workers+causing+resource+depletion%22&o=newest&f=1

    start_time = time.time()
    
    # if we restart we should actually look for the most advanced saved rdd and restart from there.
    # we could read the corresponding update row in table_updates to understand where we need to restart from.
    if restart:
        if not identifier:
            raise ValueError('[incremental_update: error] Trying to restart without specifying update identifier.')
        incr_update_id = identifier
    else:
        if day_to_process:
            incr_update_id = datetime.date.fromtimestamp((es_ts_start)/1000).isoformat()
        else:
            incr_update_id = 'incremental_update_'+str(max_ts-int(start_time*1000))

    
    basepath_save = '/user/skaraman/data/images_incremental_update/'+incr_update_id
    
    ## compute update for s3 urls we already now
    out_join_rdd = compute_out_join_rdd(basepath_save, es_man, es_ts_start, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, restart, save_inter_rdd, start_time) 
    ## save potential new images in out_join_rdd by batch of 10000 to be indexed? 
    # They should have been indexed the first time they have been seen... But download could have failed etc.
    # Might be good to retry image without cu_feat_id here when indexing has catched up
    #save_new_images_for_index(out_join_rdd,  hbase_man_update_out, incr_update_id, batch_update_size, "new_images_to_index_join")
    
    ## compute update for new s3 urls
    out_rdd = compute_out_rdd(basepath_save, es_man, es_ts_start, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, restart, save_inter_rdd, start_time) 
    
    ## Unpersist?

    if out_rdd is not None and not out_rdd.isEmpty():
        save_new_images_for_index(basepath_save, out_rdd,  hbase_man_update_out, incr_update_id, batch_update_size, save_inter_rdd, "new_images_to_index")

    save_new_s3_url(basepath_save, es_man, es_ts_start, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, restart, save_inter_rdd)

    update_elapsed_time = time.time() - start_time 
    save_info_incremental_update(hbase_man_update_out, incr_update_id, str(update_elapsed_time), "update_elapsed_time")
    


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
    sc = SparkContext(appName='images_incremental_update_release')
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
    if c_options.day_to_process:
        print("Input date was {}".format(c_options.day_to_process))
        start_date = dateutil.parser.parse(c_options.day_to_process)
        print("Will process full day before {}".format(start_date))
        ts_gap = day_gap
        # ES timestamp in milliseconds
        es_ts_end = calendar.timegm(start_date.utctimetuple())*1000
        # We should query to get all data from LAST day
        es_ts_start = es_ts_end-day_gap
        print("Will query CDR from {} to {}".format(es_ts_start, es_ts_end))

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
