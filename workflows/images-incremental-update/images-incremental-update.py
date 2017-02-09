import os
import json
import time
import calendar
import datetime
import dateutil.parser

import sys
print(sys.version)
import subprocess

dev = True

if dev:
    dev_release_suffix = "_dev"
    base_incremental_path = '/user/skaraman/data/images_incremental_update_dev/'
else:
    dev_release_suffix = "_release"
    base_incremental_path = '/user/worker/dig2/incremental/'


from optparse import OptionParser
from pyspark import SparkContext, SparkConf, StorageLevel
from elastic_manager import ES
from hbase_manager import HbaseManager

# deprecated, now uptonow option
#query_ts_minmax = True # Otherwise get everything after es_ts_start
day_gap = 86400000 # One day
ts_gap = day_gap
time_sleep_update_out = 10
#ts_gap = 10000000
#ts_gap = 10000

# default settings
#fields_cdr = ["obj_stored_url", "obj_parent", "obj_original_url", "timestamp", "crawl_data.image_id", "crawl_data.memex_ht_id"]
max_ts = 9999999999999
fields_cdr = ["obj_stored_url", "obj_parent"]
fields_list = [("info","all_cdr_ids"), ("info","s3_url"), ("info","all_parent_ids"), ("info","image_discarded"), ("info","cu_feat_id")]


##-- General RDD I/O
##------------------
def get_list_value(json_x,field_tuple):
    return [x["value"] for x in json_x if x["columnFamily"]==field_tuple[0] and x["qualifier"]==field_tuple[1]]


def check_hdfs_file(hdfs_file_path):
    proc = subprocess.Popen(["hdfs", "dfs", "-ls", hdfs_file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if "Filesystem closed" in err:
        print("[check_hdfs_file: WARNING] Beware got error '{}' when checking for file: {}.".format(err, hdfs_file_path))
        sys.stdout.flush()
    return out, err


def hdfs_file_exist(hdfs_file_path):
    out, err = check_hdfs_file(hdfs_file_path)
    hdfs_file_exist = "_SUCCESS" in out and not "_temporary" in out and not err
    return hdfs_file_exist


def hdfs_file_failed(hdfs_file_path):
    out, err = check_hdfs_file(hdfs_file_path)
    hdfs_file_failed = "_temporary" in out
    return hdfs_file_failed


def load_rdd_json(basepath_save, rdd_name):
    rdd_path = basepath_save + "/" + rdd_name
    rdd = None
    try:
        if hdfs_file_exist(rdd_path):
            print("[load_rdd_json] trying to load rdd from {}.".format(rdd_path))
            rdd = sc.sequenceFile(rdd_path).mapValues(json.loads)
    except Exception as inst:
        print("[load_rdd_json: caught error] could not load rdd from {}. Error was {}.".format(rdd_path, inst))
    return rdd


def save_rdd_json(basepath_save, rdd_name, rdd, incr_update_id, hbase_man_update_out):
    rdd_path = basepath_save + "/" + rdd_name
    if not rdd.isEmpty():
        try:
            if not hdfs_file_exist(rdd_path):
                print("[save_rdd_json] saving rdd to {}.".format(rdd_path))
                rdd.mapValues(json.dumps).saveAsSequenceFile(rdd_path)
            else:
                print("[save_rdd_json] skipped saving rdd to {}. File already exists.".format(rdd_path))
            save_info_incremental_update(hbase_man_update_out, incr_update_id, rdd_path, rdd_name+"_path")
        except Exception as inst:
            print("[save_rdd_json: caught error] could not save rdd at {}, error was {}.".format(rdd_path, inst))
    else:
        save_info_incremental_update(hbase_man_update_out, incr_update_id, "EMPTY", rdd_name+"_path")


# is this inducing respawn when called twice within short timespan?
# should we reinstantiate a different hbase_man_update_out every time?
def save_info_incremental_update(hbase_man_update_out, incr_update_id, info_value, info_name):
    print("[save_info_incremental_update] saving update info {}: {}".format(info_name, info_value))
    incr_update_infos_list = []
    incr_update_infos_list.append((incr_update_id, [incr_update_id, "info", info_name, str(info_value)]))
    incr_update_infos_rdd = sc.parallelize(incr_update_infos_list)
    hbase_man_update_out.rdd2hbase(incr_update_infos_rdd)
##------------------
##-- END General RDD I/O


##-- S3 URL functions
##-------------------
def clean_up_s3url_sha1(data):
    try:
        s3url = unicode(data[0]).strip()
        json_x = [json.loads(x) for x in data[1].split("\n")]
        sha1 = get_list_value(json_x,("info","sha1"))[0].strip()
        return [(s3url, sha1)]
    except:
        print("[clean_up_s3url_sha1] failed, data was: {}".format(data))
        return []


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


def check_get_sha1_s3url(data):
    URL_S3 = data[0]
    row_sha1 = get_row_sha1(unicode(URL_S3),0)
    if row_sha1:
        return [(URL_S3, (list(data[1][0]), row_sha1))]
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


def reduce_s3url_infos(a,b):
    a.extend(b)
    return a


def reduce_s3_keep_one_sha1(a,b):
    if a != b:
        raise ValueError("[reduce_s3_keep_one_sha1: error] one s3url has two differnet sha1 values {} and {}.".format(a, b))
    return a


def hbase_out_s3url_sha1(data):
    s3_url = data[0]
    sha1 = data[1]
    if sha1 and s3_url:
        return [(s3_url, [s3_url, "info", "sha1", sha1.upper()])]
    return []


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
        if hdfs_file_exist(s3url_infos_rdd_join_path):
            s3url_infos_rdd_join = sc.sequenceFile(s3url_infos_rdd_join_path).mapValues(load_s3url_info_list_dict)
            print("[load_s3url_infos_rdd_join: info] first samples of s3url_infos_rdd_join looks like: {}".format(s3url_infos_rdd_join.take(5)))
    except Exception as inst:
        print("[load_s3url_infos_rdd_join: caught error] Could not load rdd at {}. Error was {}.".format(s3url_infos_rdd_join_path, inst))
    return s3url_infos_rdd_join


def save_s3url_infos_rdd_join(s3url_infos_rdd_join, hbase_man_update_out, incr_update_id, s3url_infos_rdd_join_path, s3url_infos_rdd_join_path_str):
    try:
        if not hdfs_file_exist(s3url_infos_rdd_join_path):
            print("[save_s3url_infos_rdd_join: info] saving 's3url_infos_rdd_join' to {}.".format(s3url_infos_rdd_join_path))
            s3url_infos_rdd_join_tosave = s3url_infos_rdd_join.mapValues(dump_s3url_info_list_dict)
            print("[save_s3url_infos_rdd_join: info] first samples of s3url_infos_rdd_join_tosave looks like: {}".format(s3url_infos_rdd_join_tosave.take(5)))
            s3url_infos_rdd_join_tosave.saveAsSequenceFile(s3url_infos_rdd_join_path)
        else:
            print("[save_s3url_infos_rdd_join] skipped saving rdd to {}. File already exists.".format(s3url_infos_rdd_join_path))
        save_info_incremental_update(hbase_man_update_out, incr_update_id, s3url_infos_rdd_join_path, s3url_infos_rdd_join_path_str)
    except Exception as inst:
        print("[save_s3url_infos_rdd_join: caught error] could not save rdd at {}, error was {}.".format(s3url_infos_rdd_join_path, inst))


def get_s3url_infos_rdd_join(basepath_save, es_man, es_ts_start, es_ts_end, hbase_man_update_out, incr_update_id, nb_partitions, c_options, start_time):
    rdd_name = "s3url_infos_rdd_join"
    s3url_infos_rdd_join_path = basepath_save + "/" + rdd_name
    # always try to load from disk
    s3url_infos_rdd_join = load_s3url_infos_rdd_join(s3url_infos_rdd_join_path)
    if s3url_infos_rdd_join is not None:
        print("[get_s3url_infos_rdd_join: info] loaded rdd from {}.".format(s3url_infos_rdd_join_path))
        return s3url_infos_rdd_join

    # get dependency cdr_ids_infos_rdd
    cdr_ids_infos_rdd = get_cdr_ids_infos_rdd(basepath_save, es_man, es_ts_start, es_ts_end, hbase_man_update_out, incr_update_id, nb_partitions, c_options, start_time)
    
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
    
    if c_options.join_s3url:
        try:
            # try to reload from disk
            s3url_sha1_rdd = sc.sequenceFile(basepath_save + "/s3url_sha1_rdd")
        except Exception as inst:
            # read s3url_sha1 table into s3url_sha1 to get sha1 here without downloading images
            print("[get_s3url_infos_rdd_join] starting to read from s3url_sha1 HBase table.")
            s3url_sha1_rdd = hbase_man_s3url_sha1_in.read_hbase_table().flatMap(clean_up_s3url_sha1)
            # never save that anymore, too big.
            # try:
            #     s3url_sha1_rdd.saveAsSequenceFile(basepath_save + "/s3url_sha1_rdd")
            # except Exception as inst:
            #     pass
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
    else:
        print("[get_s3url_infos_rdd_join: info] skipping join with s3url_sha1 table as requested from options.")
        # Fake a join so everything after run the same way. 
        # The real would have a SHA1 has right side value for already existing s3 URLs
        s3url_infos_rdd_join = s3url_infos_rdd_red.mapValues(lambda x: (x, None))
    
    # Save rdd
    if c_options.save_inter_rdd:
        save_s3url_infos_rdd_join(s3url_infos_rdd_join, hbase_man_update_out, incr_update_id, s3url_infos_rdd_join_path, "s3url_infos_rdd_join_path")
        
    return s3url_infos_rdd_join

def save_new_s3_url(basepath_save, es_man, es_ts_start, es_ts_end, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, c_options):
    ## save out newly computed s3url
    cdr_ids_infos_rdd_new_sha1 = get_cdr_ids_infos_rdd_new_sha1(basepath_save, es_man, es_ts_start, es_ts_end, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, c_options)
    if cdr_ids_infos_rdd_new_sha1 is None:
        print("[save_new_s3_url] cdr_ids_infos_rdd_new_sha1 is empty.")
        save_info_incremental_update(hbase_man_update_out, incr_update_id, 0, "new_s3url_sha1_rdd_count")
        return

    # invert cdr_ids_infos_rdd_new_sha1 to (s3url, sha1) and apply reduceByKey() selecting any sha1
    new_s3url_sha1_rdd = cdr_ids_infos_rdd_new_sha1.flatMap(cdrid_key_to_s3url_key_sha1_val)
    out_new_s3url_sha1_rdd = new_s3url_sha1_rdd.reduceByKey(reduce_s3_keep_one_sha1).flatMap(hbase_out_s3url_sha1)
    print("[save_new_s3_url: info] out_new_s3url_sha1_rdd first samples look like: {}".format(out_new_s3url_sha1_rdd.take(5)))
    print("[save_new_s3_url] saving 'out_new_s3url_sha1_rdd' to HBase.")
    hbase_man_s3url_sha1_out.rdd2hbase(out_new_s3url_sha1_rdd)
    
    ## save new images update infos
    new_s3url_sha1_rdd_count = out_new_s3url_sha1_rdd.count()
    print("[save_new_s3_url] new_s3url_sha1_rdd_count count: {}".format(new_s3url_sha1_rdd_count))
    save_info_incremental_update(hbase_man_update_out, incr_update_id, new_s3url_sha1_rdd_count, "new_s3url_sha1_rdd_count")
##-------------------
##-- END S3 URL functions


## SHA1 and CDR ids related functions
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


def test_info_s3_url(dict_img):
    return "info:s3_url" in dict_img and dict_img["info:s3_url"] and dict_img["info:s3_url"][0]!=u'None' and dict_img["info:s3_url"][0].startswith('https://s3') 


def reduce_sha1_infos_discarding(a,b):
    c = dict()
    if b:  # sha1 already existed
        if "info:image_discarded" in a or "info:image_discarded" in b:
            c["info:all_cdr_ids"] = []
            c["info:all_parent_ids"] = []
            c["info:image_discarded"] = 'discarded because has more than {} cdr_ids'.format(max_images_reduce)
        else:
            # KeyError: 'info:all_cdr_ids'. How could an image not have this field?
            c = safe_reduce_infos(a, b, c, "info:all_cdr_ids")
            c = safe_reduce_infos(a, b, c, "info:all_parent_ids")
        #if "info:s3_url" in a and a["info:s3_url"] and a["info:s3_url"].startswith('https://s3') and a["info:s3_url"][0]!=u'None':
        if test_info_s3_url(a):
            c["info:s3_url"] = a["info:s3_url"]
        else:
            if test_info_s3_url(b):
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
    # should discard if bigger than max(max_images_hbase, max_images_dig)...
    if len(c["info:all_cdr_ids"]) > max_images_reduce or len(c["info:all_parent_ids"]) > max_images_reduce:
        print("Discarding image with URL: {}".format(c["info:s3_url"][0]))
        c["info:all_cdr_ids"] = []
        c["info:all_parent_ids"] = []
        c["info:image_discarded"] = 'discarded because has more than {} cdr_ids'.format(max_images_reduce)
    return c


def split_sha1_kv_images_discarded(x):
    # this prepares data to be saved in HBase
    tmp_fields_list = [("info","all_cdr_ids"), ("info","s3_url"), ("info","all_parent_ids")]
    out = []
    if "info:image_discarded" in x[1] or len(x[1]["info:all_cdr_ids"]) > max_images_hbase or len(x[1]["info:all_parent_ids"]) > max_images_hbase:
        if "info:image_discarded" not in x[1]:
            x[1]["info:image_discarded"] = 'discarded because has more than {} cdr_ids'.format(max_images_hbase)
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


def get_existing_joined_sha1(data):
    if len(data[1]) == 2 and data[1][1] and data[1][1] is not None and data[1][1] != 'None' and data[1][1] != u'None':
        return True
    return False


##-- New images for features computation functions
##---------------
def build_batch_out(batch_update, incr_update_id, batch_id):
    update_id = "index_update_"+incr_update_id+'_'+str(batch_id)
    list_key = []
    for x in batch_update:
        list_key.append(x)
    return [(update_id, [update_id, "info", "list_sha1s", ','.join(list_key)])]


def save_new_sha1s_for_index_update_batchwrite(new_sha1s_rdd, hbase_man_update_out, batch_update_size, incr_update_id, total_batches, nb_batchwrite=32):
    start_save_time = time.time()
    # use toLocalIterator if new_sha1s_rdd would be really big and won't fit in the driver's memory
    #iterator = new_sha1s_rdd.toLocalIterator()
    iterator = new_sha1s_rdd.collect()
    batch_update = []
    batch_out = []
    batch_id = 0
    push_batches = False
    for x in iterator:
        batch_update.append(x)
        if len(batch_update)==batch_update_size:
            if batch_id > 0 and batch_id % nb_batchwrite == 0:
                push_batches = True
            try:
                print("[save_new_sha1s_for_index_update_batchwrite] preparing batch {}/{} starting with: {}".format(batch_id+1, total_batches, batch_update[:10]))
                batch_out.extend(build_batch_out(batch_update, incr_update_id, batch_id))
                batch_id += 1
            except Exception as inst:
                print("[save_new_sha1s_for_index_update_batchwrite] Could not create/save batch {}. Error was: {}".format(batch_id, inst))
            batch_update = []
            if push_batches:
                batch_out_rdd = sc.parallelize(batch_out)
                print("[save_new_sha1s_for_index_update_batchwrite] saving {} batches of {} new images to HBase.".format(len(batch_out), batch_update_size))
                hbase_man_update_out.rdd2hbase(batch_out_rdd)
                batch_out = []
                push_batches = False

    # last batch
    if batch_update:
        try:    
            print("[save_new_sha1s_for_index_update_batchwrite] will prepare and save last batch {}/{} starting with: {}".format(batch_id+1, total_batches, batch_update[:10]))
            batch_out.extend(build_batch_out(batch_update, incr_update_id, batch_id))
            batch_out_rdd = sc.parallelize(batch_out)
            print("[save_new_sha1s_for_index_update_batchwrite] saving {} batches of {} new images to HBase.".format(len(batch_out), batch_update_size))
            hbase_man_update_out.rdd2hbase(batch_out_rdd)
            #batch_rdd.unpersist()
        except Exception as inst:
            print("[save_new_sha1s_for_index_update_batchwrite] Could not create/save batch {}. Error was: {}".format(batch_id, inst))
    print("[save_new_sha1s_for_index_update_batchwrite] DONE in {}s".format(time.time() - start_save_time))


def save_new_images_for_index(basepath_save, out_rdd,  hbase_man_update_out, incr_update_id, batch_update_size, c_options, new_images_to_index_str):
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
    if c_options.save_inter_rdd:
        try:
            new_images_to_index_out_path = basepath_save + "/" + new_images_to_index_str
            if not hdfs_file_exist(new_images_to_index_out_path):
                print("[save_new_images_for_index] saving rdd to {}.".format(new_images_to_index_out_path))
                new_images_to_index_partitioned.keys().saveAsTextFile(new_images_to_index_out_path)
            else:
                print("[save_new_images_for_index] skipped saving rdd to {}. File already exists.".format(new_images_to_index_out_path))
            save_info_incremental_update(hbase_man_update_out, incr_update_id, new_images_to_index_out_path, new_images_to_index_str+"_path")
        except Exception as inst:
            print("[save_new_images_for_index] could not save rdd 'new_images_to_index' at {}, error was {}.".format(new_images_to_index_out_path, inst))

    # save by batch in HBase to let the API know it needs to index these images    
    print("[save_new_images_for_index] start saving by batches of {} new images.".format(batch_update_size))
    # crashes in 'save_new_sha1s_for_index_update'?
    #save_new_sha1s_for_index_update(new_images_to_index_partitioned.keys(), hbase_man_update_out, batch_update_size, incr_update_id, total_batches)
    save_new_sha1s_for_index_update_batchwrite(new_images_to_index_partitioned.keys(), hbase_man_update_out, batch_update_size, incr_update_id, total_batches)
    

##---------------
##-- END New images for features computation functions


##-- Amandeep RDDs I/O
##---------------
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


def filter_out_rdd(x):
    return "info:image_discarded" not in x[1] and len(x[1]["info:all_cdr_ids"]) <= max_images_dig and len(x[1]["info:all_parent_ids"]) <= max_images_dig


##-- END Amandeep RDDs I/O
##---------------


##-- Incremental update get RDDs main functions
##---------------
def get_cdr_ids_infos_rdd(basepath_save, es_man, es_ts_start, es_ts_end, hbase_man_update_out, incr_update_id, nb_partitions, c_options, start_time):
    rdd_name = "cdr_ids_infos_rdd"
    # always try to load from disk
    cdr_ids_infos_rdd = load_rdd_json(basepath_save, rdd_name)
    if cdr_ids_infos_rdd is not None:
        print("[get_cdr_ids_infos_rdd: info] cdr_ids_infos_rdd loaded rdd from {}.".format(basepath_save + "/" + rdd_name))
        return cdr_ids_infos_rdd

    if not c_options.uptonow and es_ts_end is not None:
        query = "{\"fields\": [\""+"\", \"".join(fields_cdr)+"\"], \"query\": {\"filtered\": {\"query\": {\"match\": {\"content_type\": \"image/jpeg\"}}, \"filter\": {\"range\" : {\"_timestamp\" : {\"gte\" : "+str(es_ts_start)+", \"lt\": "+str(es_ts_end)+"}}}}}, \"sort\": [ { \"_timestamp\": { \"order\": \"asc\" } } ] }"
        print("[get_cdr_ids_infos_rdd] query CDR for one day with: {}".format(query))
    else:
        query = "{\"fields\": [\""+"\", \"".join(fields_cdr)+"\"], \"query\": {\"filtered\": {\"query\": {\"match\": {\"content_type\": \"image/jpeg\"}}, \"filter\": {\"range\" : {\"_timestamp\" : {\"gte\" : "+str(es_ts_start)+"}}}}}, \"sort\": [ { \"_timestamp\": { \"order\": \"asc\" } } ] }"
        print("[get_cdr_ids_infos_rdd] query CDR UP TO NOW with: {}".format(query))
    
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
    print("[get_cdr_ids_infos_rdd: info] images_ts_cdrid_rdd first samples look like: {}".format(images_ts_cdrid_rdd.take(5)))
    print("[get_cdr_ids_infos_rdd] saving 'images_ts_cdrid_rdd' to HBase.")
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
    if c_options.save_inter_rdd:
        save_rdd_json(basepath_save, rdd_name, cdr_ids_infos_rdd, incr_update_id, hbase_man_update_out)
    return cdr_ids_infos_rdd


def get_cdr_ids_infos_rdd_join_sha1(basepath_save, es_man, es_ts_start, es_ts_end, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, c_options):
    rdd_name = "cdr_ids_infos_rdd_join_sha1"
    # always try to load from disk
    cdr_ids_infos_rdd_join_sha1 = load_rdd_json(basepath_save, rdd_name)
    if cdr_ids_infos_rdd_join_sha1 is not None:
        print("[get_cdr_ids_infos_rdd_join_sha1: info] cdr_ids_infos_rdd_join_sha1 loaded rdd from {}.".format(basepath_save + "/" + rdd_name))
        return cdr_ids_infos_rdd_join_sha1

    # get dependency s3url_infos_rdd_join
    s3url_infos_rdd_join = get_s3url_infos_rdd_join(basepath_save, es_man, es_ts_start, es_ts_end, hbase_man_update_out, incr_update_id, nb_partitions, c_options, start_time)
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
    print("[get_cdr_ids_infos_rdd_join_sha1: info] cdr_ids_infos_rdd_join_sha1 first samples look like: {}".format(cdr_ids_infos_rdd_join_sha1.take(5)))
    print("[get_cdr_ids_infos_rdd_join_sha1] saving 'cdr_ids_infos_rdd_join_sha1' to HBase.")
    hbase_man_cdrinfos_out.rdd2hbase(cdr_ids_infos_rdd_join_sha1.flatMap(expand_info))

    # save rdd to disk
    if c_options.save_inter_rdd:
        save_rdd_json(basepath_save, rdd_name, cdr_ids_infos_rdd_join_sha1, incr_update_id, hbase_man_update_out)
    return cdr_ids_infos_rdd_join_sha1


def get_update_join_rdd(basepath_save, es_man, es_ts_start, es_ts_end, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, c_options):
    rdd_name = "update_join_rdd"
    # always try to load from disk
    update_join_rdd = load_rdd_json(basepath_save, rdd_name)
    if update_join_rdd is not None:
        print("[get_update_join_rdd: info] update_join_rdd loaded rdd from {}.".format(basepath_save + "/" + rdd_name))
        return update_join_rdd

    # we need cdr_ids_infos_rdd_join_sha1
    cdr_ids_infos_rdd_join_sha1 = get_cdr_ids_infos_rdd_join_sha1(basepath_save, es_man, es_ts_start, es_ts_end, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, c_options)
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
    if c_options.save_inter_rdd:
        save_rdd_json(basepath_save, rdd_name, update_join_rdd, incr_update_id, hbase_man_update_out)
    return update_join_rdd


def compute_out_join_rdd(basepath_save, es_man, es_ts_start, es_ts_end, hbase_man_cdrinfos_out, hbase_man_update_out, hbase_man_sha1infos_out, incr_update_id, nb_partitions, c_options, start_time):
    ## check if we not already have computed this join step of this update
    out_join_rdd_path = basepath_save + "/out_join_rdd"
    out_join_rdd_amandeep = None
    update_join_rdd_partitioned = None
    sha1_infos_rdd_json = None

    if c_options.restart:
        try:
            if hdfs_file_exist(out_join_rdd_path):
                out_join_rdd_amandeep = sc.sequenceFile(out_join_rdd_path).mapValues(amandeep_dict_str_to_out)
        except Exception as inst:
            pass
        if out_join_rdd_amandeep is not None:
            # consider already processed
            print("[compute_out_join_rdd] out_join_rdd already computed for update {}.".format(incr_update_id))
            # if we are re-running this, it might mean we did not manage to save to HBase. Retrying
            save_out_rdd_to_hbase(out_join_rdd_amandeep, hbase_man_sha1infos_out)
            return out_join_rdd_amandeep

    ## try to reload rdds that could have already been computed, compute chain of dependencies if needed 
    # propagate down es_ts_end
    update_join_rdd = get_update_join_rdd(basepath_save, es_man, es_ts_start, es_ts_end, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, c_options)
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
    if c_options.save_inter_rdd: 
        if out_join_rdd_amandeep is None or out_join_rdd_amandeep.isEmpty(): 
            save_info_incremental_update(hbase_man_update_out, incr_update_id, "EMPTY", "out_join_rdd_path")
        else:
            try:
                if not hdfs_file_exist(out_join_rdd_path):
                    out_join_rdd_amandeep.filter(filter_out_rdd).map(out_to_amandeep_dict_str).saveAsSequenceFile(out_join_rdd_path)
                else:
                    print("[compute_out_join_rdd] Skipped saving out_join_rdd. File already exists at {}.".format(out_join_rdd_path))
                save_info_incremental_update(hbase_man_update_out, incr_update_id, out_join_rdd_path, "out_join_rdd_path")
            except Exception as inst:
                print("[compute_out_join_rdd] could not save rdd at {}, error was {}.".format(out_join_rdd_path, inst))
   
    save_out_rdd_to_hbase(out_join_rdd_amandeep, hbase_man_sha1infos_out)
    # if out_join_rdd_amandeep is not None:
    #     ## save sha1 infos for these joined images in HBase
    #     out_join_rdd = out_join_rdd_amandeep.flatMap(split_sha1_kv_images_discarded)
    #     print("[compute_out_join_rdd] saving 'out_join_rdd' to sha1_infos HBase table.")
    #     hbase_man_sha1infos_out.rdd2hbase(out_join_rdd)

    return out_join_rdd_amandeep


def get_cdr_ids_infos_rdd_new_sha1(basepath_save, es_man, es_ts_start, es_ts_end, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, c_options):
    ## for not matching s3url i.e. missing sha1
    rdd_name = "cdr_ids_infos_rdd_new_sha1"
    # always try to load from disk
    cdr_ids_infos_rdd_new_sha1 = load_rdd_json(basepath_save, rdd_name)
    if cdr_ids_infos_rdd_new_sha1 is not None:
        print("[get_cdr_ids_infos_rdd_new_sha1: info] cdr_ids_infos_rdd_new_sha1 loaded rdd from {}.".format(basepath_save + "/" + rdd_name))
        print("[get_cdr_ids_infos_rdd_new_sha1: info] cdr_ids_infos_rdd_new_sha1 first samples look like: {}".format(cdr_ids_infos_rdd_new_sha1.take(5)))
        return cdr_ids_infos_rdd_new_sha1
    
    # get joined (actually all s3 urls of current update if not c_options.join_s3url), subtract, download images
    s3url_infos_rdd_join = get_s3url_infos_rdd_join(basepath_save, es_man, es_ts_start, es_ts_end, hbase_man_update_out, incr_update_id, nb_partitions, c_options, start_time)
    if s3url_infos_rdd_join is not None:
        s3url_infos_rdd_with_sha1 = s3url_infos_rdd_join.filter(get_existing_joined_sha1)
        if not s3url_infos_rdd_with_sha1.isEmpty(): 
            s3url_infos_rdd_no_sha1 = s3url_infos_rdd_join.subtractByKey(s3url_infos_rdd_with_sha1)
        else: # when all new s3 urls or not c_options.join_s3url
            s3url_infos_rdd_no_sha1 = s3url_infos_rdd_join
        s3url_infos_rdd_no_sha1_count = s3url_infos_rdd_no_sha1.count()
        print("[get_cdr_ids_infos_rdd_new_sha1: info] starting to download images to get new sha1s for {} URLs.".format(s3url_infos_rdd_no_sha1_count))
        s3url_infos_rdd_new_sha1 = s3url_infos_rdd_no_sha1.partitionBy(nb_partitions).flatMap(check_get_sha1_s3url)
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
        if c_options.save_inter_rdd:
            save_rdd_json(basepath_save, "cdr_ids_infos_rdd_new_sha1", cdr_ids_infos_rdd_new_sha1, incr_update_id, hbase_man_update_out)
        # save infos
        cdr_ids_infos_rdd_new_sha1_count = cdr_ids_infos_rdd_new_sha1.count()
        save_info_incremental_update(hbase_man_update_out, incr_update_id, cdr_ids_infos_rdd_new_sha1_count, "cdr_ids_infos_rdd_new_sha1_count")
        print("[get_cdr_ids_infos_rdd_new_sha1: info] cdr_ids_infos_rdd_new_sha1 first samples look like: {}".format(cdr_ids_infos_rdd_new_sha1.take(5)))
        print("[get_cdr_ids_infos_rdd_new_sha1] saving 'cdr_ids_infos_rdd_new_sha1' to HBase.")
        hbase_man_cdrinfos_out.rdd2hbase(cdr_ids_infos_rdd_new_sha1.flatMap(expand_info))
    return cdr_ids_infos_rdd_new_sha1


def get_update_rdd(basepath_save, es_man, es_ts_start, es_ts_end, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, c_options):
    rdd_name = "update_rdd"
    # always try to load from disk
    update_rdd = load_rdd_json(basepath_save, rdd_name)
    if update_rdd is not None:
        print("[get_update_rdd: info] update_rdd loaded rdd from {}.".format(basepath_save + "/" + rdd_name))
        return update_rdd

    cdr_ids_infos_rdd_new_sha1 = get_cdr_ids_infos_rdd_new_sha1(basepath_save, es_man, es_ts_start, es_ts_end, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, c_options)
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
    if c_options.save_inter_rdd:
        save_rdd_json(basepath_save, rdd_name, update_rdd, incr_update_id, hbase_man_update_out)

    # also return update_rdd_count to allows dynamic partitioning?
    return update_rdd


def compute_out_rdd(basepath_save, es_man, es_ts_start, es_ts_end, hbase_man_cdrinfos_out, hbase_man_update_out, hbase_man_sha1infos_out, incr_update_id, nb_partitions, c_options, start_time):
    ## check if we not already have computed this join step of this update
    rdd_name = "out_rdd"
    out_rdd_path = basepath_save + "/" + rdd_name
    out_rdd_amandeep = None
    update_rdd_partitioned = None
    sha1_infos_rdd_json = None

    if c_options.restart:
        try:
            if hdfs_file_exist(out_rdd_path):
                out_rdd_amandeep = sc.sequenceFile(out_rdd_path).mapValues(amandeep_dict_str_to_out)
        except Exception as inst:
            # would mean file existed but corrupted?
            pass
        if out_rdd_amandeep is not None:
            # consider already processed
            print("[compute_out_rdd] out_rdd already computed for update {}.".format(incr_update_id))
            # we should try to check if saving to hbase_man_sha1infos_out has completed
            save_out_rdd_to_hbase(out_rdd_amandeep, hbase_man_sha1infos_out)
            return out_rdd_amandeep

    ## try to reload rdds that could have already been computed, compute chain of dependencies if needed 
    # propagate down es_ts_end
    update_rdd = get_update_rdd(basepath_save, es_man, es_ts_start, es_ts_end, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, c_options)
    if update_rdd is None:
        print("[compute_out_rdd] update_rdd is empty.")
        save_info_incremental_update(hbase_man_update_out, incr_update_id, 0, rdd_name+"_count")
        save_info_incremental_update(hbase_man_update_out, incr_update_id, "EMPTY", rdd_name+"_path")
        return None

    ## update cdr_ids, and parents cdr_ids for these new sha1s
    print("[compute_out_rdd] reading from hbase_man_sha1infos_join to get sha1_infos_rdd.")
    sha1_infos_rdd = hbase_man_sha1infos_join.read_hbase_table()
    # we may need to merge some 'all_cdr_ids' and 'all_parent_ids'
    if not sha1_infos_rdd.isEmpty(): 
        print("[compute_out_rdd] partitioning update_rdd.")
        update_rdd_partitioned = update_rdd.partitionBy(nb_partitions)
        print("[compute_out_rdd] partitioning sha1_infos_rdd.")
        sha1_infos_rdd_json = sha1_infos_rdd.flatMap(sha1_key_json).partitionBy(nb_partitions)
        print("[compute_out_rdd] joining sha1_infos_rdd and update_rdd.")
        join_rdd = update_rdd_partitioned.leftOuterJoin(sha1_infos_rdd_json).flatMap(flatten_leftjoin)
        out_rdd_amandeep = join_rdd
    else: # first update
        out_rdd_amandeep = update_rdd
    # save rdd
    if c_options.save_inter_rdd:
        try:
            if not hdfs_file_exist(out_rdd_path):
                # we should discard based on c_options.max_images_dig here actually
                out_rdd_save = out_rdd_amandeep.filter(filter_out_rdd).map(out_to_amandeep_dict_str)
                if not out_rdd_save.isEmpty():
                    out_rdd_save.saveAsSequenceFile(out_rdd_path)
                    save_info_incremental_update(hbase_man_update_out, incr_update_id, out_rdd_path, rdd_name+"_path")
                else:
                    print("[compute_out_rdd] 'out_rdd_save' is empty.")
                    save_info_incremental_update(hbase_man_update_out, incr_update_id, "EMPTY", rdd_name+"_path")
            else:
                print("[compute_out_rdd] Skipped saving out_rdd. File already exists at {}.".format(out_rdd_path))
                #return None
        # org.apache.hadoop.mapred.FileAlreadyExistsException
        except Exception as inst:
            print("[compute_out_rdd] could not save rdd at {}, error was {}.".format(out_rdd_path, inst))

    # save to HBase
    save_out_rdd_to_hbase(out_rdd_amandeep, hbase_man_sha1infos_out)
    # ## write out rdd of new images 
    # out_rdd = out_rdd_amandeep.flatMap(split_sha1_kv_images_discarded)
    # if not out_rdd.isEmpty():
    #     print("[compute_out_rdd] saving 'out_rdd' to sha1_infos HBase table.")
    #     hbase_man_sha1infos_out.rdd2hbase(out_rdd)
    #     # how to be sure this as completed?
    # else:
    #     print("[compute_out_rdd] 'out_rdd' is empty.")

    return out_rdd_amandeep


def save_out_rdd_to_hbase(out_rdd_amandeep, hbase_man_sha1infos_out):
    if out_rdd_amandeep is not None:
        # write out rdd of new images 
        out_rdd = out_rdd_amandeep.flatMap(split_sha1_kv_images_discarded)
        if not out_rdd.isEmpty():
            print("[save_out_rdd_to_hbase] saving 'out_rdd' to sha1_infos HBase table.")
            hbase_man_sha1infos_out.rdd2hbase(out_rdd)
            # how to be sure this as completed?
        else:
            print("[save_out_rdd_to_hbase] 'out_rdd' is empty.")
    else:
        print("[save_out_rdd_to_hbase] 'out_rdd_amandeep' is None.")

##-------------



def incremental_update(es_man, hbase_man_ts, hbase_man_cdrinfos_out, hbase_man_sha1infos_join, hbase_man_sha1infos_out, hbase_man_s3url_sha1_in, hbase_man_s3url_sha1_out, hbase_man_update_out, nb_partitions, c_options):
    
    # We should query to get all data from LAST day
    print("Will process full day before {}".format(c_options.day_to_process))
    start_date = dateutil.parser.parse(c_options.day_to_process)
    # es_ts_end could be set to None if uptonow was set to True
    # ES timestamp in milliseconds
    es_ts_end = calendar.timegm(start_date.utctimetuple())*1000
    es_ts_start = es_ts_end - day_gap
    print("Will query CDR from {} to {}".format(es_ts_start, es_ts_end))
    # We should propagate down es_ts_start AND es_ts_end

    restart = c_options.restart
    save_inter_rdd = c_options.save_inter_rdd
    identifier = c_options.identifier
    day_to_process = c_options.day_to_process
    batch_update_size = c_options.batch_update_size
    
    
    start_time = time.time()
    
    ## set incr_update_id
    if c_options.restart:
        if not c_options.identifier:
            raise ValueError('[incremental_update: error] Trying to restart without specifying update identifier.')
        incr_update_id = c_options.identifier
    else:
        if c_options.day_to_process:
            incr_update_id = datetime.date.fromtimestamp((es_ts_start)/1000).isoformat()
        else:
            incr_update_id = 'incremental_update_'+str(max_ts-int(start_time*1000))

    
    #basepath_save = '/user/skaraman/data/images_incremental_update/'+incr_update_id
    basepath_save = base_incremental_path+incr_update_id+'/images/info'
    
    if c_options.join_s3url:
        ## compute update for s3 urls we already now
        out_join_rdd = compute_out_join_rdd(basepath_save, es_man, es_ts_start, es_ts_end, hbase_man_cdrinfos_out, hbase_man_update_out, hbase_man_sha1infos_out, incr_update_id, nb_partitions, c_options, start_time) 
        ## save potential new images in out_join_rdd by batch of 10000 to be indexed? 
        # They should have been indexed the first time they have been seen... But download could have failed etc.
        # Might be good to retry image without cu_feat_id here when indexing has catched up
        #save_new_images_for_index(out_join_rdd,  hbase_man_update_out, incr_update_id, batch_update_size, "new_images_to_index_join")
    
    ## compute update for new s3 urls
    out_rdd = compute_out_rdd(basepath_save, es_man, es_ts_start, es_ts_end, hbase_man_cdrinfos_out, hbase_man_update_out, hbase_man_sha1infos_out, incr_update_id, nb_partitions, c_options, start_time) 

    if out_rdd is not None and not out_rdd.isEmpty():
        save_new_images_for_index(basepath_save, out_rdd, hbase_man_update_out, incr_update_id, batch_update_size, c_options, "new_images_to_index")

    save_new_s3_url(basepath_save, es_man, es_ts_start, es_ts_end, hbase_man_cdrinfos_out, hbase_man_update_out, incr_update_id, nb_partitions, c_options)

    update_elapsed_time = time.time() - start_time 
    save_info_incremental_update(hbase_man_update_out, incr_update_id, str(update_elapsed_time), "update_elapsed_time")
    

## MAIN
if __name__ == '__main__':
    start_time = time.time()
    

    # Parse options
    parser = OptionParser()
    parser.add_option("-r", "--restart", dest="restart", default=False, action="store_true")
    parser.add_option("-i", "--identifier", dest="identifier") # redudant with day to process now...
    parser.add_option("-d", "--day_to_process", dest="day_to_process", default=datetime.date.today().isoformat())
    parser.add_option("-s", "--save", dest="save_inter_rdd", default=False, action="store_true")
    parser.add_option("-j", "--join_s3url", dest="join_s3url", default=False, action="store_true")
    parser.add_option("-u", "--uptonow", dest="uptonow", default=False, action="store_true")
    parser.add_option("-b", "--batch_update_size", dest="batch_update_size", default=10000)
    # expose max_images_dig so Amandeep can change that on the fly if needed
    parser.add_option("-m", "--max_images_dig", dest="max_images_dig", default=50000)
    # we could add options for uptonow, auto join based on number of s3_urls to download
    (c_options, args) = parser.parse_args()
    print "Got options:", c_options


    # Read job_conf
    job_conf = json.load(open("job_conf_notcommited"+dev_release_suffix+".json","rt"))
    print job_conf
    sc = SparkContext(appName="images_incremental_update"+dev_release_suffix)
    conf = SparkConf()
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
    # Set parameters job_conf
    # should this be estimated from RDD counts actually?
    nb_partitions = job_conf["nb_partitions"]


    # HBase Conf
    hbase_host = job_conf["hbase_host"]
    tab_ts_name = job_conf["tab_ts_name"]
    hbase_man_ts = HbaseManager(sc, conf, hbase_host, tab_ts_name)
    tab_cdrid_infos_name = job_conf["tab_cdrid_infos_name"]
    tab_sha1_infos_name = job_conf["tab_sha1_infos_name"]
    tab_s3url_sha1_name = job_conf["tab_s3url_sha1_name"]
    tab_update_name = job_conf["tab_update_name"]
    # this is the maximum number of cdr_ids for an image to be saved to HBase
    max_images_hbase = job_conf["max_images"]
    # this is the maximum number of cdr_ids for an image to be saved to HDFS for dig
    max_images_dig = c_options.max_images_dig
    max_images_reduce = max(max_images_hbase, max_images_dig)
    # Setup HBase managers
    join_columns_list = [':'.join(x) for x in fields_list]
    hbase_man_sha1infos_join = HbaseManager(sc, conf, hbase_host, tab_sha1_infos_name, columns_list=join_columns_list)
    hbase_man_sha1infos_out = HbaseManager(sc, conf, hbase_host, tab_sha1_infos_name)
    hbase_man_cdrinfos_out = HbaseManager(sc, conf, hbase_host, tab_cdrid_infos_name)
    hbase_man_update_out = HbaseManager(sc, conf, hbase_host, tab_update_name, time_sleep=time_sleep_update_out)
    # actually  only needed if join_s3url is True
    hbase_man_s3url_sha1_in = HbaseManager(sc, conf, hbase_host, tab_s3url_sha1_name)
    hbase_man_s3url_sha1_out = HbaseManager(sc, conf, hbase_host, tab_s3url_sha1_name)
    
    
    # ES conf
    es_index = job_conf["es_index"]
    es_domain = job_conf["es_domain"]
    es_host = job_conf["es_host"] 
    es_port = job_conf["es_port"]
    es_user = job_conf["es_user"]
    es_pass = job_conf["es_pass"]
    # deprecated
    #es_ts_start = job_conf["query_timestamp_start"]
    # Setup ES manager
    es_man = ES(sc, conf, es_index, es_domain, es_host, es_port, es_user, es_pass)
    es_man.set_output_json()
    es_man.set_read_metadata()


    # Run update
    incremental_update(es_man, hbase_man_ts, hbase_man_cdrinfos_out, hbase_man_sha1infos_join, hbase_man_sha1infos_out, hbase_man_s3url_sha1_in, hbase_man_s3url_sha1_out, hbase_man_update_out, nb_partitions, c_options)
    print("[DONE] Update for day {} done in {}s.".format(c_options.day_to_process, time.time() - start_time))
