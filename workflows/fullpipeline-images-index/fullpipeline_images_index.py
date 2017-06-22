import sys
print(sys.version)

import os
import json
import time
import base64
import datetime
import happybase
import subprocess
import numpy as np
import cPickle as pkl

from operator import add
from elastic_manager import ES
from argparse import ArgumentParser
from hbase_manager import HbaseManager
from tempfile import NamedTemporaryFile, mkdtemp
from pyspark import SparkContext, SparkConf, StorageLevel

# The LOPQ parts of this code are subject to Apache License, Version 2.0
# initial code from https://github.com/yahoo/lopq
# Copyright 2015, Yahoo Inc.
# Licensed under the terms of the Apache License, Version 2.0. See the LICENSE file associated with the project for terms.
from pyspark.mllib.clustering import KMeans, KMeansModel
from lopq.model import LOPQModel, LOPQModelPCA, compute_rotations_from_accumulators

STEP_COARSE = 0
STEP_ROTATION = 1
STEP_SUBQUANT = 2

qpr = True

# Some parameters
default_identifier = None
default_batch_update_size = 10000
max_ts = 9999999999999
max_ads_image_dig = 20000
max_ads_image_hbase = 20000
max_ads_image = 20000
max_samples_per_partition = 10000
default_partitions_nb = 240
day_gap = 86400000 # One day
valid_url_start = 'https://s3' 

fields_cdr = ["obj_stored_url", "obj_parent", "content_type"]
fields_list = [("info","s3_url"), ("info","all_parent_ids"), ("info","image_discarded"), ("info","cu_feat_id"), ("info","img_info")]
in_fields_list = ["s3_url", "img_info", "image_discarded", "cu_feat_id"]
feat_column_name = "featnorm_tf"
base_path_import = "hdfs://memex/user/skaraman/fullpipeline-images-index"

# the base_hdfs_path could be set with a parameter too
if qpr:
    job_suffix = "_qpr"
    base_hdfs_path = '/user/skaraman/data/images_summerqpr2017/'
    #base_hdfs_path = "/Users/svebor/Documents/Workspace/CodeColumbia/MEMEX/tmpdata/"
else:
    job_suffix = "_release"
    base_hdfs_path = '/user/worker/dig2/incremental/'

##-- Hbase (happybase)

def get_create_table(table_name, options, families={'info': dict()}):
    try:
        from happybase.connection import Connection
        conn = Connection(options.hbase_ip)
        try:
            table = conn.table(table_name)
            # this would fail if table does not exist
            fam = table.families()
            return table
        # what exception would be raised if table does not exist, actually none.
        # need to try to access families to get error
        except Exception as inst:
            print "[get_create_table: info] table {} does not exist (yet)".format(table_name)
            conn.create_table(table_name, families)
            table = conn.table(table_name)
            print "[get_create_table: info] created table {}".format(table_name)
            return table
    except Exception as inst:
        print inst

##-- General RDD I/O
##------------------

def get_partitions_nb(options, rdd_count=0):
    """ Calculate number of partitions for a RDD.
    """
    # if options.nb_partitions is set use that
    if options.nb_partitions > 0:
        partitions_nb = options.nb_partitions
    elif rdd_count > 0: # if options.nb_partitions is -1 (default)
        #estimate from rdd_count and options.max_samples_per_partition
        import numpy as np
        partitions_nb = int(np.ceil(float(rdd_count)/options.max_samples_per_partition))
    else: # fall back to default partitions nb
        partitions_nb = default_partitions_nb
    print "[get_partitions_nb: log] partitions_nb: {}".format(partitions_nb)
    return partitions_nb


def get_list_value(json_x,field_tuple):
    return [x["value"] for x in json_x if x["columnFamily"]==field_tuple[0] and x["qualifier"]==field_tuple[1]]


def check_hdfs_file(hdfs_file_path):
    proc = subprocess.Popen(["hdfs", "dfs", "-ls", hdfs_file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if "Filesystem closed" in err:
        print("[check_hdfs_file: WARNING] Beware got error '{}' when checking for file: {}.".format(err, hdfs_file_path))
        sys.stdout.flush()
    print "[check_hdfs_file] out: {}, err: {}".format(out, err)
    return out, err


def hdfs_file_exist(hdfs_file_path):
    out, err = check_hdfs_file(hdfs_file_path)
    # too restrictive as even log4j error would be interpreted as non existing file
    #hdfs_file_exist = "_SUCCESS" in out and not "_temporary" in out and not err
    hdfs_file_exist = "_SUCCESS" in out
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

#- only if we use joins
def clean_up_s3url_sha1(data):
    try:
        s3url = unicode(data[0]).strip()
        json_x = [json.loads(x) for x in data[1].split("\n")]
        sha1 = get_list_value(json_x,("info","sha1"))[0].strip()
        return [(s3url, sha1)]
    except:
        print("[clean_up_s3url_sha1] failed, data was: {}".format(data))
        return []
#-

def get_SHA1_imginfo_from_URL(URL,verbose=0):
    import image_dl
    import json
    sha1hash,img_info = image_dl.get_SHA1_imginfo_from_URL_StringIO(URL, verbose) # 1 is verbose level
    return sha1hash, json.dumps(img_info)


def check_get_sha1_imginfo_s3url(data):
    URL_S3 = data[0]
    row_sha1, img_info = get_SHA1_imginfo_from_URL(URL_S3, 1)
    if row_sha1:
        out = [(URL_S3, (list([data[1][0]]), row_sha1, img_info))]
        #print out
        return out
    return []


def reduce_s3url_listadid(a, b):
    """ Reduce to get unique s3url with list of corresponding ad ids.
    """
    a.extend(b)
    return a


def s3url_listadid_sha1_imginfo_to_sha1_alldict(data):
    """ Transforms data expected to be in format (s3_url, ([ad_id], sha1, imginfo)) into a list 
    of tuples (sha1, v) where v contains the "info:s3_url", "info:all_parent_ids" and "info:img_info".
    """
    if len(data[1]) != 3 or data[1][1] is None or data[1][1] == 'None' or data[1][1] == u'None':
        print("[s3url_listadid_imginfo_to_sha1_alldict] incorrect data: {}".format(data))
        return []
    s3_url = data[0]
    listadid = list(data[1][0])
    sha1 = data[1][1]
    img_info = data[1][2]
    all_parent_ids = []
    # if we have a valid sha1
    if sha1:
        # add each ad_id containing this s3_url to all_parent_ids
        for ad_id in listadid: # could this split an ad_id into charachters?
            if len(ad_id)>1:
                all_parent_ids.append(ad_id)
    if sha1 and s3_url and all_parent_ids and img_info:
        out = [(sha1, {"info:s3_url": [s3_url], "info:all_parent_ids": all_parent_ids, "info:img_info": [img_info]})]
        #print out
        return out
    return []



##-------------------
##-- END S3 URL functions

###-------------
### Transformers

# function naming convention is input_to_output
# input/output can indicate key_value if relevant

def CDRv3_to_s3url_adid(data):
    """ Create tuples (s3_url, ad_id) for documents in CDRv3 format.

    :param data: CDR v3 ad document in JSON format
    """
    tup_list = []
    ad_id = data[0]
    # parse JSON
    json_x = json.loads(data[1])
    # look for images in objects field
    for obj in json_x["objects"]:
        # check that content_type corresponds to an image
        if obj["content_type"][0].startswith("image/"):
            # get url, some url may need unicode characters
            s3_url = unicode(obj["obj_stored_url"])
            if s3_url.startswith('https://s3'):
                tup_list.append( (s3_url, ad_id) )
    return tup_list

def CDRv2_to_s3url_adid(data):
    """ Create tuples (s3_url, ad_id) for documents in CDRv2 format.

    :param data: CDR v2 image document in JSON format
    """
    tup_list = []
    # parse JSON
    json_x = json.loads(data[1])
    #print json_x
    if json_x["content_type"][0].startswith("image/"):
        # get url, some url may need unicode characters
        s3_url = unicode(json_x["obj_stored_url"][0])
        ad_id = str(json_x["obj_parent"][0])
        if s3_url.startswith('https://s3'):
            tup_list.append( (s3_url, ad_id) )
    else:
        print "[CDRv2_to_s3url_adid: warning] {} not an image document!".format(data[0])
    return tup_list


def sha1_key_json_values(data):
    # when data was read from HBase and called with flatMapValues
    json_x = [json.loads(x) for x in data.split("\n")]
    v = dict()
    for field in fields_list:
        try:
            # if field is a list of ids
            if field[1]!='s3_url' and field[1]!='img_info': 
                v[':'.join(field)] = list(set([x for x in get_list_value(json_x,field)[0].strip().split(',')]))
            else: # s3url or img_info
                v[':'.join(field)] = [unicode(get_list_value(json_x,field)[0].strip())]
        except: # field not in row
            pass
    return [v]


def safe_reduce_infos(a, b, c, field):
    try:
        c[field] = list(set(a[field]+b[field]))
    except Exception as inst:
        try:
            c[field] = a[field]
            #print("[safe_reduce_infos: error] key error for '{}' for a".format(field))
        except Exception as inst2:
            try:
                c[field] = b[field]
                #print("[safe_reduce_infos: error] key error for '{}' for b".format(field))
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


def reduce_sha1_infos_discarding_wimginfo(a, b):
    c = dict()
    if b:  # sha1 already existed
        if "info:image_discarded" in a or "info:image_discarded" in b:
            c["info:all_parent_ids"] = []
            c["info:image_discarded"] = 'discarded because has more than {} cdr_ids'.format(max_ads_image)
        else:
            c = safe_reduce_infos(a, b, c, "info:img_info")
            c = safe_reduce_infos(a, b, c, "info:all_parent_ids")
        if test_info_s3_url(a):
            c["info:s3_url"] = a["info:s3_url"]
        else:
            if test_info_s3_url(b):
                c["info:s3_url"] = b["info:s3_url"]
            else:
                print("[reduce_sha1_infos_discarding_wimginfo: error] both a and b have no s3 url.")
                c["info:s3_url"] = [None]
        # need to keep info:cu_feat_id if it exists
        if "info:cu_feat_id" in b:
            c["info:cu_feat_id"] = b["info:cu_feat_id"]
    else: # brand new image
        c = safe_assign(a, c, "info:s3_url", [None])
        c = safe_assign(a, c, "info:all_parent_ids", [])
        c = safe_assign(a, c, "info:img_info", [])
    # should discard if bigger than max_ads_image...
    if len(c["info:all_parent_ids"]) > max_ads_image:
        print("[reduce_sha1_infos_discarding_wimginfo: log] Discarding image with URL: {}".format(c["info:s3_url"][0]))
        c["info:all_parent_ids"] = []
        c["info:image_discarded"] = 'discarded because has more than {} cdr_ids'.format(max_ads_image)
    return c


def split_sha1_kv_images_discarded_wimginfo(x):
    # this prepares data to be saved in HBase
    tmp_fields_list = [("info","s3_url"), ("info","all_parent_ids"), ("info","img_info")]
    out = []
    if "info:image_discarded" in x[1] or len(x[1]["info:all_parent_ids"]) > max_ads_image_hbase:
        if "info:image_discarded" not in x[1]:
            x[1]["info:image_discarded"] = 'discarded because has more than {} cdr_ids'.format(max_ads_image_hbase)
        out.append((x[0], [x[0], "info", "image_discarded", x[1]["info:image_discarded"]]))
        str_s3url_value = None
        s3url_value = x[1]["info:s3_url"][0]
        str_s3url_value = unicode(s3url_value)
        out.append((x[0], [x[0], "info", "s3_url", str_s3url_value]))
        out.append((x[0], [x[0], "info", "all_parent_ids", x[1]["info:image_discarded"]]))
    else:
        for field in tmp_fields_list:
            if field[1]=="s3_url":
                out.append((x[0], [x[0], field[0], field[1], unicode(x[1][field[0]+":"+field[1]][0])]))
            elif field[1]=="img_info": 
                # deal with an older update that does not have this field.
                try:
                    out.append((x[0], [x[0], field[0], field[1], x[1][field[0]+":"+field[1]][0]]))
                except Exception:
                    pass
            else:
                out.append((x[0], [x[0], field[0], field[1], ','.join(x[1][field[0]+":"+field[1]])]))
    return out


def flatten_leftjoin(x):
    out = []
    # at this point value is a tuple of two lists with a single or empty dictionary
    c = reduce_sha1_infos_discarding_wimginfo(x[1][0],x[1][1])
    out.append((x[0], c))
    return out


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
            print("[save_new_sha1s_for_index_update_batchwrite] saving {} batches of {} new images to HBase.".format(len(batch_out), len(batch_update)))
            hbase_man_update_out.rdd2hbase(batch_out_rdd)
            #batch_rdd.unpersist()
        except Exception as inst:
            print("[save_new_sha1s_for_index_update_batchwrite] Could not create/save batch {}. Error was: {}".format(batch_id, inst))
    print("[save_new_sha1s_for_index_update_batchwrite] DONE in {}s".format(time.time() - start_save_time))


def save_new_images_for_index(basepath_save, out_rdd,  hbase_man_update_out, incr_update_id, args, new_images_to_index_str):
    batch_update_size = args.batch_update_size

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
    if args.save_inter_rdd:
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

def out_to_amandeep_dict_str_wimginfo(x):
    # this is called with map()
    sha1 = x[0]
    # keys should be: "image_sha1", "all_parent_ids", "s3_url", "img_info"
    # keep "cu_feat_id" to be able to push images to be indexed
    out_dict = dict()
    out_dict["image_sha1"] = sha1
    # use "for field in fields_list:" instead? and ':'.join(field)
    for field in ["all_parent_ids", "s3_url", "cu_feat_id", "img_info"]:
        if "info:"+field in x[1]:
            out_dict[field] = x[1]["info:"+field]
    return (sha1, json.dumps(out_dict))


def amandeep_dict_str_to_out_wimginfo(x):
    # this is called with mapValues()
    # keys should be: "image_sha1", "all_parent_ids", "s3_url", "all_cdr_ids", "img_info"
    # keep "cu_feat_id" to be able to push images to be indexed
    tmp_dict = json.loads(x)
    out_dict = dict()
    #sha1 = tmp_dict["image_sha1"]
    for field in ["all_parent_ids", "s3_url", "cu_feat_id", "img_info"]:
        if field in tmp_dict:
            out_dict["info:"+field] = tmp_dict[field]
    return out_dict


def filter_out_rdd(x):
    return "info:image_discarded" not in x[1] and len(x[1]["info:all_parent_ids"]) <= max_ads_image_dig

# Functions for extractions
def amandeep_jsonstr_to_dict(x):
    '''Read each item json string in the out RDD and get needed fields'''
    in_dict = json.loads(x)
    out_dict = dict()
    
    for field in in_fields_list:
        if field in in_dict:
            out_dict[field] = in_dict[field]

    return out_dict


def get_out_rdd(basepath_save):
    '''Read the out_rdd if it exists.'''
    rdd_name = "out_rdd"
    out_rdd_path = basepath_save + "/" + rdd_name
    if hdfs_file_exist(out_rdd_path):
        out_rdd = sc.sequenceFile(out_rdd_path).mapValues(amandeep_jsonstr_to_dict)
    else:
        err = "[get_out_rdd: error] Could not find out_rdd at: {}".format(out_rdd_path)
        raise ValueError(err)
    return out_rdd

##-- END Amandeep RDDs I/O
##---------------


##-- Incremental update get RDDs main functions
##---------------
def build_query_CDR(es_ts_start, es_ts_end, args):
    print("Will query CDR from {} to {}".format(es_ts_start, es_ts_end))

    if es_ts_start is not None:
        gte_range = "\"gte\" : "+str(es_ts_start)
    else:
        gte_range = "\"gte\" : "+str(0)
    if es_ts_end is not None:
        lt_range = "\"lt\": "+str(es_ts_end)
    else:
        # max_ts or ts of now?
        lt_range = "\"lt\": "+str(max_ts)
    # build range ts
    range_timestamp = "{\"range\" : {\"_timestamp\" : {"+",".join([gte_range, lt_range])+"}}}"
    # build query
    query = None
    # will depend on args.cdr_format too
    if args.cdr_format == 'v2':
        query = "{\"fields\": [\""+"\", \"".join(fields_cdr)+"\"], \"query\": {\"filtered\": {\"query\": {\"match\": {\"content_type\": \"image/jpeg\"}}, \"filter\": "+range_timestamp+"}}, \"sort\": [ { \"_timestamp\": { \"order\": \"asc\" } } ] }"
    elif args.cdr_format == 'v3':
        print "[build_query_CDR: ERROR] CDR format v3 not yet supported."
        # need to get fields objects.fields_cdr?
        # and match: {"objects.content_type": "image/jpeg"?
    else:
        print "[build_query_CDR: ERROR] Unkown CDR format: {}".format(options.cdr_format)
    return query


def get_s3url_adid_rdd(basepath_save, es_man, es_ts_start, es_ts_end, hbase_man_update_out, ingestion_id, options, start_time):
    rdd_name = "s3url_adid_rdd"
    prefnout = "get_s3url_adid_rdd: "

    # Try to load from disk (always? or only if args.restart is true?)
    if args.restart:
        s3url_adid_rdd = load_rdd_json(basepath_save, rdd_name)
        if s3url_adid_rdd is not None:
            print("[{}log] {} loaded rdd from {}.".format(prefnout, rdd_name, basepath_save + "/" + rdd_name))
            return s3url_adid_rdd

    # Format query to ES to get images
    query = build_query_CDR(es_ts_start, es_ts_end, args)
    if query is None:
        print("[{}log] empty query...".format(prefnout))
        return None
    print("[{}log] query CDR: {}".format(prefnout, query))
    
    # Actually get images
    es_rdd_nopart = es_man.es2rdd(query)
    if es_rdd_nopart.isEmpty():
        print("[{}log] empty ingestion...".format(prefnout))
        return None

    # es_rdd_nopart is likely to be underpartitioned
    es_rdd_count = es_rdd_nopart.count()
    # should we partition based on count and max_samples_per_partition?
    es_rdd = es_rdd_nopart.partitionBy(get_partitions_nb(options, es_rdd_count))

    # save ingestion infos
    ingestion_infos_list = []
    ingestion_infos_list.append((ingestion_id, [ingestion_id, "info", "start_time", str(start_time)]))
    ingestion_infos_list.append((ingestion_id, [ingestion_id, "info", "es_rdd_count", str(es_rdd_count)]))
    ingestion_infos_rdd = sc.parallelize(ingestion_infos_list)
    hbase_man_update_out.rdd2hbase(ingestion_infos_rdd)

    # transform to (s3_url, adid) format
    s3url_adid_rdd = None
    if args.cdr_format == 'v2':
        s3url_adid_rdd = es_rdd.flatMap(CDRv2_to_s3url_adid)
    elif args.cdr_format == 'v3':
        s3url_adid_rdd = es_rdd.flatMap(CDRv3_to_s3url_adid)
    else:
        print "[get_s3url_adid_rdd: ERROR] Unkown CDR format: {}".format(options.cdr_format)

    if args.save_inter_rdd:
        save_rdd_json(basepath_save, rdd_name, s3url_adid_rdd, ingestion_id, hbase_man_update_out)
    return s3url_adid_rdd


def save_out_rdd_to_hdfs(basepath_save, out_rdd, hbase_man_update_out, ingestion_id, rdd_name):
    out_rdd_path = basepath_save + "/" + rdd_name
    try:
        if not hdfs_file_exist(out_rdd_path):
            out_rdd_save = out_rdd.filter(filter_out_rdd).map(out_to_amandeep_dict_str_wimginfo)
            if not out_rdd_save.isEmpty():
                # how to force overwrite here?
                out_rdd_save.saveAsSequenceFile(out_rdd_path)
                save_info_incremental_update(hbase_man_update_out, ingestion_id, out_rdd_path, rdd_name+"_path")
            else:
                print("[save_out_rdd_to_hdfs] 'out_rdd_save' is empty.")
                save_info_incremental_update(hbase_man_update_out, ingestion_id, "EMPTY", rdd_name+"_path")
        else:
            print "[save_out_rdd_to_hdfs] Skipped saving out_rdd. File already exists at {}.".format(out_rdd_path)
    except Exception as inst:
        print "[save_out_rdd_to_hdfs: error] Error when trying to save out_rdd to {}. {}".format(out_rdd_path, inst)


def save_out_rdd_to_hbase(out_rdd, hbase_man_sha1infos_out):
    if out_rdd is not None:
        # write out rdd of new images 
        out_rdd_hbase = out_rdd.flatMap(split_sha1_kv_images_discarded_wimginfo)
        if not out_rdd_hbase.isEmpty():
            print("[save_out_rdd_to_hbase] saving 'out_rdd' to sha1_infos HBase table.")
            hbase_man_sha1infos_out.rdd2hbase(out_rdd_hbase)
            # how to be sure this as completed?
        else:
            print("[save_out_rdd_to_hbase] 'out_rdd' is empty.")
    else:
        print("[save_out_rdd_to_hbase] 'out_rdd' is None.")

##-------------

def join_ingestion(hbase_man_sha1infos_join, ingest_rdd, options, ingest_rdd_count):
    # update parents cdr_ids for existing sha1s
    print("[join_ingestion] reading from hbase_man_sha1infos_join to get sha1_infos_rdd.")
    sha1_infos_rdd = hbase_man_sha1infos_join.read_hbase_table()
    # we may need to merge some 'all_parent_ids'
    if not sha1_infos_rdd.isEmpty(): 
        sha1_infos_rdd_count = sha1_infos_rdd.count()
        # use get_partitions_nb(options, rdd_count)
        nb_partitions_ingest = get_partitions_nb(options, ingest_rdd_count)
        nb_partitions_sha1_infos = get_partitions_nb(options, sha1_infos_rdd_count)
        # partitioned rdd in the same number of partitions to minmize shuffle in the leftOuterJoin
        nb_partitions = max(nb_partitions_sha1_infos, nb_partitions_ingest)
        #sha1_infos_rdd_json = sha1_infos_rdd.flatMap(sha1_key_json).partitionBy(nb_partitions)
        sha1_infos_rdd_json = sha1_infos_rdd.partitionBy(nb_partitions).flatMapValues(sha1_key_json_values)
        ingest_rdd_partitioned = ingest_rdd.partitionBy(nb_partitions)
        join_rdd = ingest_rdd_partitioned.leftOuterJoin(sha1_infos_rdd_json).flatMap(flatten_leftjoin)
        out_rdd = join_rdd
    else: # first update
        out_rdd = ingest_rdd
    return out_rdd


def run_ingestion(es_man, hbase_man_sha1infos_join, hbase_man_sha1infos_out, hbase_man_update_out, es_ts_start, es_ts_end, args):
    
    print max_ads_image
    ingestion_id = args.ingestion_id

    restart = args.restart
    batch_update_size = args.batch_update_size

    start_time = time.time()
    basepath_save = args.base_hdfs_path+ingestion_id+'/images/info'
    
    # get images from CDR, output format should be (s3_url, ad_id)
    # NB: later on we will load from disk from another job
    s3url_adid_rdd = get_s3url_adid_rdd(basepath_save, es_man, es_ts_start, es_ts_end, hbase_man_update_out, ingestion_id, args, start_time)

    if s3url_adid_rdd is None:
        print "No data retrieved!"
        return

    # reduce by key to download each image once
    s3url_adid_rdd_red = s3url_adid_rdd.flatMapValues(lambda x: [[x]]).reduceByKey(reduce_s3url_listadid)
    s3url_adid_rdd_red_count = s3url_adid_rdd_red.count()
    save_info_incremental_update(hbase_man_update_out, args.ingestion_id, s3url_adid_rdd_red_count, "s3url_adid_rdd_red_count")

    # process (compute SHA1, and reduce by SHA1)
    # repartition first based on s3url_adid_rdd_red_count?
    # this could be done as a flatMapValues
    # s3url_adid_rdd_red.partitionBy(get_partitions_nb(args, s3url_adid_rdd_red_count)).flatMapValues(...)
    s3url_infos_rdd = s3url_adid_rdd_red.flatMap(check_get_sha1_imginfo_s3url)
    
    print '[s3url_infos_rdd: first] {}'.format(s3url_infos_rdd.first())
    # transform to (SHA1, imginfo)
    sha1_infos_rdd = s3url_infos_rdd.flatMap(s3url_listadid_sha1_imginfo_to_sha1_alldict)
    print '[sha1_infos_rdd: first] {}'.format(sha1_infos_rdd.first())
    ingest_rdd = sha1_infos_rdd.reduceByKey(reduce_sha1_infos_discarding_wimginfo)
    print '[ingest_rdd: first] {}'.format(ingest_rdd.first())
    ingest_rdd_count = ingest_rdd.count()
    save_info_incremental_update(hbase_man_update_out, args.ingestion_id, ingest_rdd_count, "ingest_rdd_count")

    # save to disk
    if args.save_inter_rdd:
        save_rdd_json(basepath_save, "ingest_rdd", ingest_rdd, ingestion_id, hbase_man_update_out)

    # join with existing sha1 (not needed for qpr...)
    out_rdd = join_ingestion(hbase_man_sha1infos_join, ingest_rdd, args, ingest_rdd_count)
    print '[out_rdd: first] {}'.format(out_rdd.first())
    save_out_rdd_to_hdfs(basepath_save, out_rdd, hbase_man_update_out, ingestion_id, "out_rdd")
    save_out_rdd_to_hbase(out_rdd, hbase_man_sha1infos_out)

    #if out_rdd is not None and not out_rdd.isEmpty():
    #    save_new_images_for_index(basepath_save, out_rdd, hbase_man_update_out, ingestion_id, args, "new_images_to_index")

    ingest_elapsed_time = time.time() - start_time 
    save_info_incremental_update(hbase_man_update_out, ingestion_id, str(ingest_elapsed_time), "ingest_elapsed_time")
    return ingest_rdd_count


def extract(val):    
    global DSE
    try:
        _ = DSE
        # good to go
    except:
        import socket
        hostname = socket.gethostname()
        # need to initialize DSE, but do it just once for each worker
        import tfdeepsentibank
        modelpath = 'tfdeepsentibank.npy'
        imgmeanpath = 'imagenet_mean.npy'
        DSE = tfdeepsentibank.DeepSentibankExtractor(modelpath, imgmeanpath)
        print "Initialized deep sentibank model on {}".format(hostname)
    # extract
    import numpy as np
    import base64
    try:
        if type(val["s3_url"]) is list:
            input_img_url = val["s3_url"][0]
        else:
            input_img_url = val["s3_url"]
    except Exception as inst:
        print "Could not get img url from {}".format(val)
        return
    output = DSE.get_features_from_URL(input_img_url)
    if output is not None:
        # normalize feature (could be optional)
        feat_norm_tf = output/np.linalg.norm(output)
        # should we add infos about the extractor, features dimensions?
        # encode in base64
        val[feat_column_name] = base64.b64encode(feat_norm_tf)
        # should be read as feat_norm = np.frombuffer(base64.b64decode(featnorm_tf), dtype=np.float32)
        return val


def run_extraction(hbase_man_sha1infos_out, hbase_man_update_out, ingestion_id, args):
    start_time = time.time()
    basepath_save = args.base_hdfs_path+ingestion_id+'/images/info'

    out_rdd = get_out_rdd(basepath_save)
    # should it be x[1] is not None?
    #out_rdd_wfeat = out_rdd.mapValues(extract).filter(lambda x: x is not None)
    out_rdd_wfeat = out_rdd.mapValues(extract).filter(lambda x: x[1] is not None)
    
    # save to disk
    save_rdd_json(basepath_save, "out_rdd_wfeat", out_rdd_wfeat, ingestion_id, hbase_man_update_out)
    # save to hbase
    save_out_rdd_wfeat_to_hbase(out_rdd_wfeat, hbase_man_sha1infos_out)

    extraction_elapsed_time = time.time() - start_time 
    save_info_incremental_update(hbase_man_update_out, ingestion_id, str(extraction_elapsed_time), "extraction_elapsed_time")

# lopq helpers functions
def save_hdfs_pickle(m, pkl_path):
    """
    Given a python object and a path on hdfs, save the object as a pickle file locally and copy the file
    to the hdfs path.
    """
    print 'Saving pickle to temp file...'
    f = NamedTemporaryFile(delete=False)
    pkl.dump(m, f, -1)
    f.close()

    print 'Copying pickle file to hdfs...'
    copy_to_hdfs(f, pkl_path)
    os.remove(f.name)


def copy_from_hdfs(hdfs_path):
    tmp_dir = mkdtemp()
    subprocess.call(['hadoop', 'fs', '-copyToLocal', hdfs_path, tmp_dir])
    return os.path.join(tmp_dir, hdfs_path.split('/')[-1])


def copy_to_hdfs(f, hdfs_path):
    subprocess.call(['hadoop', 'fs', '-copyFromLocal', f.name, hdfs_path])


def default_data_loading(sc, data_path, sampling_ratio, seed):
    """
    This function loads data from a text file, sampling it by the provided
    ratio and random seed, and interprets each line as a tab-separated (id, data) pair
    where 'data' is assumed to be a base64-encoded pickled numpy array.
    The data is returned as an RDD of (id, numpy array) tuples.
    """
    # Load and sample down the dataset
    d = sc.textFile(data_path).sample(False, sampling_ratio, seed)

    # The data is (id, vector) tab-delimited pairs where each vector is
    # a base64-encoded pickled numpy array
    d = d.map(lambda x: x.split('\t')).map(lambda x: (x[0], pkl.loads(base64.decodestring(x[1]))))

    return d


# pca related functions
def compute_pca(sc, args, data_load_fn=default_data_loading):

    def seqOp(a, b):
        a += np.outer(b, b)
        return a

    def combOp(a, b):
        a += b
        return a

    # Load data
    d = data_load_fn(sc, args.pca_data, args.sampling_ratio_pca, args.seed)
    d.cache()

    # Determine the data dimension
    D = len(d.first())
    print "d.first: {}, D: {}".format(d.first(),D)

    # Count data points
    count = d.count()
    mu = d.aggregate(np.zeros(D), add, add)
    mu = mu / float(count)

    # Compute covariance estimator
    summed_covar = d.treeAggregate(np.zeros((D, D)), seqOp, combOp, depth=args.agg_depth)

    A = summed_covar / (count - 1) - np.outer(mu, mu)
    E, P = np.linalg.eigh(A)

    params = {
        'mu': mu,   # mean
        'P': P,     # PCA matrix
        'E': E,     # eigenvalues
        'A': A,     # covariance matrix
        'c': count  # sample size
    }

    save_hdfs_pickle(params, args.pca_full_output)


def reduce_pca(args):

    filename = copy_from_hdfs(args.pca_full_output)
    print 'Loading PCA Model locally from {} copied from {}'.format(filename, args.pca_full_output)
    params = pkl.load(open(filename))
    os.remove(filename)
    P = params['P']
    E = params['E']
    mu = params['mu']

    # Reduce dimension - eigenvalues assumed in ascending order
    E = E[-args.pca_D:]
    P = P[:,-args.pca_D:]

    # Balance variance across halves
    permuted_inds = eigenvalue_allocation(2, E)
    P = P[:, permuted_inds]

    # Save new params
    f = NamedTemporaryFile(delete=False)
    pkl.dump({'P': P, 'mu': mu }, open(f.name, 'w'))
    f.close()
    copy_to_hdfs(f, args.pca_reduce_output)
    os.remove(f.name)


def apply_PCA(x, mu, P):
    """
    Example of applying PCA.
    """
    return np.dot(x - mu, P)

# train index functions
def load_data(sc, args, data_load_fn=default_data_loading):
    """
    Load training data as an RDD.
    """
    # Load data
    vecs = data_load_fn(sc, args.data, args.sampling_ratio, args.seed)
    print 'Sample is: {}'.format(vecs.first())
    
    # Apply PCA if needed
    if args.pca_model is not None:
        # Check if we should get PCA model
        print 'Loading PCA model from {}'.format(args.pca_model)
        filename = copy_from_hdfs(args.pca_model)
        params = pkl.load(open(filename))
        # TODO: we should also remove tmp dir
        os.remove(filename)
        P = params['P']
        mu = params['mu']
        print 'Applying PCA from model {}'.format(args.pca_model)
        vecs = vecs.map(lambda x: apply_PCA(x, mu, P))

    print 'Sample is: {}'.format(vecs.first())

    # Split the vectors
    split_vecs = vecs.map(lambda x: np.split(x, 2))
    print 'Sample is: {}'.format(split_vecs.first())

    return split_vecs


def train_coarse(sc, split_vecs, V, seed=None):
    """
    Perform KMeans on each split of the data with V clusters each.
    """

    # Cluster first split
    first = split_vecs.map(lambda x: x[0])
    first.cache()
    print 'Total training set size: %d' % first.count()
    print 'Starting training coarse quantizer...'
    C0 = KMeans.train(first, V, initializationMode='random', maxIterations=10, seed=seed)
    print '... done training coarse quantizer.'
    first.unpersist()

    # Cluster second split
    second = split_vecs.map(lambda x: x[1])
    second.cache()
    print 'Starting training coarse quantizer...'
    C1 = KMeans.train(second, V, initializationMode='random', maxIterations=10, seed=seed)
    print '... done training coarse quantizer.'
    second.unpersist()

    return np.vstack(C0.clusterCenters), np.vstack(C1.clusterCenters)


def train_rotations(sc, split_vecs, M, Cs):
    """
    For compute rotations for each split of the data using given coarse quantizers.
    """

    Rs = []
    mus = []
    counts = []
    for split in xrange(2):

        print 'Starting rotation fitting for split %d' % split

        # Get the data for this split
        data = split_vecs.map(lambda x: x[split])

        # Get kmeans model
        model = KMeansModel(Cs[split])

        R, mu, count = compute_local_rotations(sc, data, model, M / 2)
        Rs.append(R)
        mus.append(mu)
        counts.append(count)

    return Rs, mus, counts


def accumulate_covariance_estimators(sc, data, model):
    """
    Analogous function to function of the same name in lopq.model.

    :param SparkContext sc:
        a SparkContext
    :param RDD data:
        an RDD of numpy arrays
    :param KMeansModel model:
        a KMeansModel instance for which to fit local rotations
    """

    def get_residual(x):
        cluster = model.predict(x)
        centroid = model.clusterCenters[cluster]
        residual = x - centroid
        return (cluster, residual)

    def seq_op(acc, x):
        acc += np.outer(x, x)
        return acc

    # Compute (assignment, residual) k/v pairs
    residuals = data.map(get_residual)
    residuals.cache()

    # Collect counts and mean residuals
    count = residuals.countByKey()
    mu = residuals.reduceByKey(add).collectAsMap()

    # Extract the dimension of the data
    D = len(mu.values()[0])

    # Collect accumulated outer products
    A = residuals.aggregateByKey(np.zeros((D, D)), seq_op, add).collectAsMap()

    residuals.unpersist()

    return A, mu, count


def dict_to_ndarray(d, N):
    """
    Helper for collating a dict with int keys into an ndarray. The value for a key
    becomes the value at the corresponding index in the ndarray and indices missing
    from the dict become zero ndarrays of the same dimension.

    :param dict d:
        a dict of (int, ndarray) or (int, number) key/values
    :param int N:
        the size of the first dimension of the new ndarray (the rest of the dimensions
        are determined by the shape of elements in d)
    """

    el = d.values()[0]
    if type(el) == np.ndarray:
        value_shape = el.shape
        arr = np.zeros((N,) + value_shape)
    else:
        arr = np.zeros(N)

    for i in d:
        arr[i] = d[i]
    return arr


def compute_local_rotations(sc, data, model, num_buckets):
    """
    Analogous to the function of the same name in lopq.model.

    :param SparkContext sc:
        a SparkContext
    :param RDD data:
        an RDD of numpy arrays
    :param KMeansModel model:
        a KMeansModel instance for which to fit local rotations
    :param int num_buckets:
        the number of subvectors over which to balance residual variance
    """
    # Get estimators
    A, mu, count = accumulate_covariance_estimators(sc, data, model)

    # Format as ndarrays
    V = len(model.centers)
    A = dict_to_ndarray(A, V)
    mu = dict_to_ndarray(mu, V)
    count = dict_to_ndarray(count, V)

    # Compute params
    R, mu = compute_rotations_from_accumulators(A, mu, count, num_buckets)

    return R, mu, count


def train_subquantizers(sc, split_vecs, M, subquantizer_clusters, model, seed=None):
    """
    Project each data point into it's local space and compute subquantizers by clustering
    each fine split of the locally projected data.
    """
    b = sc.broadcast(model)

    def project_local(x):
        x = np.concatenate(x)
        coarse = b.value.predict_coarse(x)
        return b.value.project(x, coarse)

    projected = split_vecs.map(project_local)

    # Split the vectors into the subvectors
    split_vecs = projected.map(lambda x: np.split(x, M))
    split_vecs.cache()

    subquantizers = []
    for split in xrange(M):
        data = split_vecs.map(lambda x: x[split])
        data.cache()
        sub = KMeans.train(data, subquantizer_clusters, initializationMode='random', maxIterations=10, seed=seed)
        data.unpersist()
        subquantizers.append(np.vstack(sub.clusterCenters))

    return (subquantizers[:len(subquantizers) / 2], subquantizers[len(subquantizers) / 2:])

def validate_arguments(args, model):
    """
    Check provided command line arguments to ensure they are coherent. Provide feedback for potential errors.
    """

    # Parse steps
    args.steps = set(map(int, args.steps.split(',')))

    # Check that the steps make sense
    if STEP_ROTATION not in args.steps and len(args.steps) == 2:
        print 'Training steps invalid'
        sys.exit(1)

    # Find parameters and warn of possibly unintentional discrepancies
    if args.V is None:
        if model is not None:
            args.V = model.V
            print 'Parameter V not specified: using V=%d from provided model.' % model.V
        else:
            print 'Parameter V not specified and no existing model provided. Exiting.'
            sys.exit(1)
    else:
        if model is not None and model.V != args.V:
            if STEP_COARSE in args.steps:
                print 'Parameter V differs between command line argument and provided model: ' + \
                      'coarse quantizers will be trained with V=%d' % args.V
            else:
                print 'Parameter V differs between command line argument and provided model: ' + \
                      'coarse quantizers must be retrained or this discrepancy corrected. Exiting.'
                sys.exit(1)

    if STEP_ROTATION in args.steps or STEP_SUBQUANT in args.steps:
        if args.M is None:
            if model is not None:
                args.M = model.M
                print 'Parameter M not specified: using M=%d from provided model.' % model.M
            else:
                print 'Parameter M not specified and no existing model provided. Exiting.'
                sys.exit(1)
        else:
            if model is not None and model.M != args.M:
                if STEP_ROTATION in args.steps:
                    print 'Parameter M differs between command line argument and provided model: ' + \
                          'model will be trained with M=%d' % args.M
                else:
                    print 'Parameter M differs between command line argument and provided model: ' + \
                          'rotations must be retrained or this discrepancy corrected. Exiting.'
                    sys.exit(1)

    if STEP_ROTATION in args.steps:
        if STEP_COARSE not in args.steps and (model is None or model.Cs is None):
            print 'Cannot train rotations without coarse quantizers. Either train coarse quantizers or provide an existing model. Exiting.'
            sys.exit(1)

    if STEP_SUBQUANT in args.steps:
        if STEP_COARSE not in args.steps and (model is None or model.Cs is None):
            print 'Cannot train subquantizers without coarse quantizers. Either train coarse quantizers or provide an existing model. Exiting.'
            sys.exit(1)
        if STEP_ROTATION not in args.steps and (model is None or model.Rs is None or model.mus is None):
            print 'Cannot train subquantizers without rotations. Either train rotations or provide an existing model. Exiting.'
            sys.exit(1)

    return args

def compute_codes(sc, args, data_load_fn=default_data_loading):

    # Load model
    model = None
    if args.model_pkl:
        filename = copy_from_hdfs(args.model_pkl)
        model = pkl.load(open(filename))
        os.remove(filename)
    elif args.model_proto:
        filename = copy_from_hdfs(args.model_proto)
        model = LOPQModel.load_proto(args.model_proto)
        os.remove(filename)

    print 'LOPQModel is of type: {}'.format(type(model))

    # Load data
    d = data_load_fn(sc, args.compute_data, args.sampling_ratio, args.seed)

    # Distribute model instance
    m = sc.broadcast(model)

    # Compute codes and convert to string
    codes = d.map(lambda x: (x[0], m.value.predict(x[1]))).map(lambda x: '%s\t%s' % (x[0], json.dumps(x[1])))

    codes.saveAsTextFile(args.output)


def parse_ingestion_id(ingestion_id):
    # split ingestion id
    domain, es_ts_start, es_ts_end  = ingestion_id.split('-')
    # return infos
    return domain, es_ts_start, es_ts_end


def set_missing_parameters(args):
    # all pca_data, model_data, compute_data should be set to out_rdd_wfeat
    rdd_feat_path = args.base_hdfs_path+args.ingestion_id+'/images/info/out_rdd_wfeat'
    if args.pca_data is None:
        print 'Setting args.pca_data to {}'.format(rdd_feat_path)
        args.pca_data = rdd_feat_path
    if args.model_data is None:
        print 'Setting args.model_data to {}'.format(rdd_feat_path)
        args.model_data = rdd_feat_path
    if args.compute_data is None:
        print 'Setting args.compute_data to {}'.format(rdd_feat_path)
        args.compute_data = rdd_feat_path
    # pca_full_output, pca_reduce_output does not really matter, but should not conflict between domains
    # output that should be loaded in image search service
    if args.model_pkl is None:
        model_pkl = args.base_hdfs_path+args.ingestion_id+'/images/index/lopq_model'
        print 'Setting args.model_pkl to {}'.format(model_pkl)
        args.model_pkl = model_pkl
    if args.codes_output is None:
        codes_output = args.base_hdfs_path+args.ingestion_id+'/images/index/lopq_codes'
        print 'Setting args.codes_output to {}'.format(codes_output)
        args.codes_output = codes_output
    return args

def adapt_parameters(args, nb_images):
    # TODO: we could adapt the following parameters to optimize speed/quality
    # - V: default 16
    # - M: default 8
    # - subquantizer_clusters: 256
    # - sampling_ratio_pca: default 1.0
    # - sampling_ratio_model: default 1.0
    # - subquantizer_sampling_ratio: default 1.0
    return args

## MAIN
if __name__ == '__main__':
    start_time = time.time()

    # Setup parser for arguments options
    parser = ArgumentParser()

    # Define groups
    job_group = parser.add_argument_group("job", "Job related parameters")
    hbase_group = parser.add_argument_group("hbase", "HBase related parameters")
    es_group = parser.add_argument_group("es", "ElasticSearch related parameters")
    feat_group = parser.add_argument_group("feat", "Features related parameters")
    index_group = parser.add_argument_group("index", "Indexing related parameters")

    # Define HBase related arguments
    hbase_group.add_argument("--hbase_host", dest="hbase_host", required=True)
    hbase_group.add_argument("--hbase_port", dest="hbase_port", default=2181)
    hbase_group.add_argument("--hbase_ip", dest="hbase_ip", default="10.1.94.57")
    # BEWARE: these tables should be already created
    # we could just have a table_prefix
    hbase_group.add_argument("--table_sha1", dest="tab_sha1_infos_name", required=True)
    hbase_group.add_argument("--table_update", dest="tab_update_name", required=True)

    # Define ES related options
    es_group.add_argument("--es_host", dest="es_host", required=True)
    es_group.add_argument("--es_domain", dest="es_domain", required=True)
    es_group.add_argument("--es_user", dest="es_user", required=True)
    es_group.add_argument("--es_pass", dest="es_pass", required=True)
    es_group.add_argument("--es_port", dest="es_port", default=9200)
    es_group.add_argument("--es_index", dest="es_index", default='memex-domains')
    # deprecated should be detected from ingestion id.
    #es_group.add_argument("--es_ts_start", dest="es_ts_start", help="start timestamp in ms", default=None)
    #es_group.add_argument("--es_ts_end", dest="es_ts_end", help="end timestamp in ms", default=None)
    es_group.add_argument("--cdr_format", dest="cdr_format", choices=['v2', 'v3'], default='v2')
    
    # Define features reulated options
    feat_group.add_argument("--feat_column_name", dest="feat_column_name", default=feat_column_name, help="column where features will be saved in HBase")

    # Define job related options
    job_group.add_argument("-i", "--ingestion_id", dest="ingestion_id", required=True)
    job_group.add_argument("-s", "--save", dest="save_inter_rdd", default=False, action="store_true")
    job_group.add_argument("-b", "--batch_update_size", dest="batch_update_size", type=int, default=default_batch_update_size)
    job_group.add_argument("--max_ads_image_dig", dest="max_ads_image_dig", type=int, default=max_ads_image_dig)
    job_group.add_argument("--max_ads_image_hbase", dest="max_ads_image_hbase", type=int, default=max_ads_image_hbase)
    # should this be estimated from RDD counts actually?
    #job_group.add_argument("-p", "--nb_partitions", dest="nb_partitions", type=int, default=480)
    job_group.add_argument("-p", "--nb_partitions", dest="nb_partitions", type=int, default=-1)
    job_group.add_argument("-d", "--day_to_process", dest="day_to_process", help="using format YYYY-MM-DD", default=None)
    job_group.add_argument("--max_samples_per_partition", dest="max_samples_per_partition", type=int, default=max_samples_per_partition)
    job_group.add_argument("--base_hdfs_path", dest="base_hdfs_path", default=base_hdfs_path)
    # should we still allow the input of day to process and estimate ts start and end from it?

    # Define index related options
    index_group.add_argument('--seed', dest='seed', type=int, default=None, help='optional random seed')
    index_group.add_argument('--agg_depth', dest='agg_depth', type=int, default=4, help='depth of tree aggregation to compute covariance estimator')
    index_group.add_argument('--pca_D', dest='pca_D', type=int, default=256, help='number of dimensions to keep for PCA (default: 256)')
    index_group.add_argument('--pca_data_udf', dest='pca_data_udf', type=str, default="deepsentibanktf_udf", help='module name from which to load a data loading UDF')
    index_group.add_argument('--model_data_udf', dest='data_udf', type=str, default="deepsentibanktf_udf", help='module name from which to load a data loading UDF')
    # we need to maintain the id (sha1) for the codes computation
    index_group.add_argument('--codes_data_udf', dest='data_udf', type=str, default="deepsentibanktf_udf_wid", help='module name from which to load a data loading UDF')
    # Model hyperparameters
    # TODO: estimate good parameters given an amount of data to index?
    index_group.add_argument('--V', dest='V', type=int, default=16, help='number of coarse clusters')
    index_group.add_argument('--M', dest='M', type=int, default=8, help='total number of subquantizers')
    index_group.add_argument('--subquantizer_clusters', dest='subquantizer_clusters', type=int, default=256, help='number of subquantizer clusters')
    # Ratios could also be determined based on number of samples
    index_group.add_argument('--sampling_ratio_pca', dest='sampling_ratio_pca', type=float, default=1.0, help='proportion of data to sample for pca training')
    index_group.add_argument('--sampling_ratio_model', dest='sampling_ratio_model', type=float, default=1.0, help='proportion of data to sample for training')
    index_group.add_argument('--subquantizer_sampling_ratio', dest='subquantizer_sampling_ratio', type=float, default=1.0, help='proportion of data to subsample for subquantizer training')
    # Training and output directives
    index_group.add_argument('--steps', dest='steps', type=str, default='0,1,2', help='comma-separated list of integers indicating which steps of training to perform')
    # TODO: All these should be build from ingestion_id
    index_group.add_argument('--pca_data', dest='pca_data', type=str, default=None, help='hdfs path to pca input data')
    index_group.add_argument('--model_data', dest='model_data', type=str, default=None, help='hdfs path to model input data')
    index_group.add_argument('--compute_data', dest='compute_data', type=str, default=None, help='hdfs path to codes input data')
    index_group.add_argument('--pca_full_output', dest='pca_full_output', type=str, default=None, help='hdfs path to output pca pickle file of parameters')
    index_group.add_argument('--pca_reduce_output', dest='pca_reduce_output', type=str, default=None, help='hdfs path to output reduced pca pickle file of parameters')
    index_group.add_argument('--model_pkl', dest='model_pkl', type=str, default=None, help='hdfs path to save pickle file of resulting model parameters')
    index_group.add_argument('--codes_output', dest='codes_output', type=str, default=None, required=True, help='hdfs path to codes output data')

    # Parse
    try:
        args = parser.parse_args()
        print "Got options:", args
        # Are these still relevant?
        max_ads_image_dig = args.max_ads_image_dig
        max_ads_image_hbase = args.max_ads_image_hbase
        max_ads_image = max(max_ads_image_dig, max_ads_image_hbase)
    except Exception as inst:
        print inst
        parser.print_help()
    ingestion_id = args.ingestion_id
    domain, es_ts_start, es_ts_end = parse_ingestion_id(ingestion_id)

    # Set missing parameters
    args = set_missing_parameters(args)
    
    # Setup SparkContext    
    sc = SparkContext(appName="build_images_index_"+ingestion_id+job_suffix)
    conf = SparkConf()
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
    
    # Setup HBase managers
    join_columns_list = [':'.join(x) for x in fields_list]
    get_create_table(args.tab_sha1_infos_name, args)
    hbase_fullhost = args.hbase_host+':'+str(args.hbase_port)
    # only if we assume we can run updates...
    hbase_man_sha1infos_join = HbaseManager(sc, conf, hbase_fullhost, args.tab_sha1_infos_name, columns_list=join_columns_list)
    hbase_man_sha1infos_out = HbaseManager(sc, conf, hbase_fullhost, args.tab_sha1_infos_name)
    get_create_table(args.tab_update_name, args)
    hbase_man_update_out = HbaseManager(sc, conf, hbase_fullhost, args.tab_update_name)
    
    # Setup ES manager
    es_man = ES(sc, conf, args.es_index, args.es_domain, args.es_host, args.es_port, args.es_user, args.es_pass)
    es_man.set_output_json()
    es_man.set_read_metadata()

    ## Run
    print "[START] Starting building index for ingestion id: {}".format(ingestion_id)
    # save any needed info with calls like save_info_incremental_update(hbase_man_update_out, args.ingestion_id, ingest_rdd_count, "ingest_rdd_count")

    # delete what is no longer needed after each step?
    # step 1: get images
    print "[STEP #1] Starting ingesting images for ingestion id: {}".format(ingestion_id)
    start_step = time.time()
    sc.addPyFile(base_path_import+'/image_dl.py')
    # TODO: get number of unique images to tune parameters of indexing
    nb_images = run_ingestion(es_man, hbase_man_sha1infos_join, hbase_man_sha1infos_out, hbase_man_update_out, es_ts_start, es_ts_end, args)
    print "[STEP #1] Done in {:.2f}s. We have {} images.".format(time.time() - start_step, nb_images)

    # step 2: get features
    start_step = time.time()
    print "[STEP #2] Starting features extraction for ingestion_id: {}".format(ingestion_id)
    sc.addPyFile(base_path_import+'/features/network.py')
    sc.addPyFile(base_path_import+'/features/tfdeepsentibank.py')
    sc.addFile(base_path_import+'/features/imagenet_mean.npy')
    sc.addFile(base_path_import+'/features/tfdeepsentibank.npy')
    run_extraction(hbase_man_sha1infos_out, hbase_man_update_out, ingestion_id, args)
    print "[STEP #2] Done in {:.2f}s".format(time.time() - start_step)
    
    # step 3: build index
    args = adapt_parameters(args, nb_images)
    start_step = time.time()
    print "[STEP #3] Starting building index for ingestion_id: {}".format(ingestion_id)
    sc.addPyFile(base_path_import+'/index/memex_udf.py')
    sc.addPyFile(base_path_import+'/index/deepsentibanktf_udf.py')
    sc.addPyFile(base_path_import+'/index/deepsentibanktf_udf_wid.py')
    # 3.1: compute pca
    if args.pca_data_udf:
        udf_module = __import__(args.pca_data_udf, fromlist=['udf'])
        load_udf = udf_module.udf
        compute_pca(sc, args, data_load_fn=load_udf)
    else:
        compute_pca(sc, args)
    # 3.2: reduce pca
    reduce_pca(args)
    # 3.3: build model
    # Initialize and validate
    model = None
    args = validate_arguments(args, model)

    # Build descriptive app name
    get_step_name = lambda x: {STEP_COARSE: 'coarse', STEP_ROTATION: 'rotations', STEP_SUBQUANT: 'subquantizers'}.get(x, None)
    steps_str = ', '.join(filter(lambda x: x is not None, map(get_step_name, sorted(args.steps))))
    APP_NAME = 'LOPQ{V=%d,M=%d}; training %s' % (args.V, args.M, steps_str)

    # Load UDF module if provided and load training data RDD
    if args.model_data_udf:
        udf_module = __import__(args.model_data_udf, fromlist=['udf'])
        load_udf = udf_module.udf
        # NB: load data method splits vectors into 2 parts, after applying pca if model is provided
        data = load_data(sc, args, data_load_fn=load_udf)
    else:
        # NB: load data method splits vectors into 2 parts, after applying pca if model is provided
        data = load_data(sc, args)

    # Initialize parameters
    Cs = Rs = mus = subs = None

    # Get coarse quantizers
    if STEP_COARSE in args.steps:
        Cs = train_coarse(sc, data, args.V, seed=args.seed)
    else:
        Cs = model.Cs

    # Get rotations
    if STEP_ROTATION in args.steps:
        Rs, mus, counts = train_rotations(sc, data, args.M, Cs)
    else:
        Rs = model.Rs
        mus = model.mus

    # Get subquantizers
    if STEP_SUBQUANT in args.steps:
        model = LOPQModel(V=args.V, M=args.M, subquantizer_clusters=args.subquantizer_clusters, parameters=(Cs, Rs, mus, None))

        if args.subquantizer_sampling_ratio != 1.0:
            data = data.sample(False, args.subquantizer_sampling_ratio, args.seed)

        subs = train_subquantizers(sc, data, args.M, args.subquantizer_clusters, model, seed=args.seed)

    # Final output model
    if args.pca_reduce_output is not None:
        # Check if we should get PCA model
        print 'Loading PCA model from {}'.format(args.pca_reduce_output)
        filename = copy_from_hdfs(args.pca_reduce_output)
        params = pkl.load(open(filename))
        # TODO: we should also remove tmp dir
        os.remove(filename)
        P = params['P']
        mu = params['mu']
        model = LOPQModelPCA(V=args.V, M=args.M, subquantizer_clusters=args.subquantizer_clusters, parameters=(Cs, Rs, mus, subs, P, mu))
    else:
        model = LOPQModel(V=args.V, M=args.M, subquantizer_clusters=args.subquantizer_clusters, parameters=(Cs, Rs, mus, subs))

    # Should we add the PCA Model to the LOPQModel?
    if args.model_pkl:
        print 'Saving model as pickle to {}'.format(args.model_pkl)
        save_hdfs_pickle(model, args.model_pkl)
    save_info_incremental_update(hbase_man_update_out, args.ingestion_id, args.model_pkl, "lopq_model_pkl")
    print "[STEP #3] Done in {:.2f}s".format(time.time() - start_step)

    # step 4: compute codes
    start_step = time.time()
    if args.codes_data_udf:
        udf_module = __import__(args.codes_data_udf, fromlist=['udf'])
        load_udf = udf_module.udf
        compute_codes(sc, args, data_load_fn=load_udf)
    else:
        compute_codes(sc, args)
    save_info_incremental_update(hbase_man_update_out, args.ingestion_id, args.codes_output, "lopq_codes_path")
    print "[STEP #4] Done in {:.2f}s".format(time.time() - start_step)

    print "[DONE] Built index for ingestion {} in {}s.".format(ingestion_id, time.time() - start_time)

