import sys
print(sys.version)

import os
import json
import time
import datetime
import happybase
import subprocess

from argparse import ArgumentParser
from pyspark import SparkContext, SparkConf, StorageLevel
from hbase_manager import HbaseManager

qpr = True

# Some parameters
default_identifier = None
default_batch_update_size = 10000
max_ts = 9999999999999
max_samples_per_partition = 10000
day_gap = 86400000 # One day
valid_url_start = 'https://s3' 

# features related field could be define by parameters?
in_fields_list = ["s3_url", "img_info", "image_discarded", "cu_feat_id"]
feat_column_name = "featnorm_tf"

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

##-- Out RDDs I/O
##---------------

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


##-- END Out RDDs I/O
##---------------


##-- Incremental update get RDDs main functions
##---------------

def save_out_rdd_wfeat_to_hbase(out_rdd, hbase_man_sha1infos_out):
    if out_rdd is not None:
        # write out rdd with features
        out_rdd_hbase = out_rdd.flatMap(build_output_hbase)
        if not out_rdd_hbase.isEmpty():
            print "[save_out_rdd_wfeat_to_hbase: log] saving 'out_rdd_wfeat' to sha1_infos HBase table."
            hbase_man_sha1infos_out.rdd2hbase(out_rdd_hbase)
        else:
            print "[save_out_rdd_wfeat_to_hbase: log] 'out_rdd_wfeat' is empty."
    else:
        print "[save_out_rdd_wfeat_to_hbase: log] 'out_rdd_wfeat' is None."


def build_output_hbase(x):
    # this prepares data to be saved in HBase
    tmp_fields_list = [("info",feat_column_name)]
    out = []
    for field in tmp_fields_list:
        if x[1] is not None and field[1] in x[1]:
            out.append((x[0], [x[0], field[0], field[1], x[1][field[1]]]))
    return out

##-------------

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


def run_extraction(hbase_man_sha1infos_out, hbase_man_update_out, ingestion_id, c_options):
    start_time = time.time()
    basepath_save = c_options.base_hdfs_path+ingestion_id+'/images/info'

    out_rdd = get_out_rdd(basepath_save)
    # should it be x[1] is not None?
    out_rdd_wfeat = out_rdd.mapValues(extract).filter(lambda x: x is not None)
    
    # save to disk
    save_rdd_json(basepath_save, "out_rdd_wfeat", out_rdd_wfeat, ingestion_id, hbase_man_update_out)
    # save to hbase
    save_out_rdd_wfeat_to_hbase(out_rdd_wfeat, hbase_man_sha1infos_out)

    extraction_elapsed_time = time.time() - start_time 
    save_info_incremental_update(hbase_man_update_out, ingestion_id, str(extraction_elapsed_time), "extraction_elapsed_time")


def get_ingestion_start_end_id(c_options):
    # Get es_ts_start and es_ts_end
    es_ts_start = None
    es_ts_end = None
    if c_options.es_ts_start is not None:
        es_ts_start = c_options.es_ts_start
    if c_options.es_ts_end is not None:
        es_ts_end = c_options.es_ts_end
    if c_options.es_ts_start is None and c_options.es_ts_end is None and c_options.day_to_process is not None:
        # Compute for day to process
        import calendar
        import dateutil.parser
        try:
            start_date = dateutil.parser.parse(c_options.day_to_process)
            es_ts_end = calendar.timegm(start_date.utctimetuple())*1000
            es_ts_start = es_ts_end - day_gap
        except Exception as inst:
            print "[get_ingestion_start_end_id: log] Could not parse 'day_to_process'."
    # Otherwise we want ALL images
    if es_ts_start is None:
        es_ts_start = 0
    if es_ts_end is None:
        es_ts_end = max_ts
    # form ingestion id
    ingestion_id = '-'.join([c_options.es_domain, str(es_ts_start), str(es_ts_end)])

    return es_ts_start, es_ts_end, ingestion_id


## MAIN
if __name__ == '__main__':
    start_time = time.time()

    # Setup parser for arguments options
    parser = ArgumentParser()

    # Define groups
    job_group = parser.add_argument_group("job", "Job related parameters")
    hbase_group = parser.add_argument_group("hbase", "HBase related parameters")
    es_group = parser.add_argument_group("es", "ElasticSearch related parameters")
    # we could define parameters for the features extraction...
    # feature column name
    # if we normalize the feature
    # or even features type if we define an extractor that implements get_features_from_URL
    feat_group = parser.add_argument_group("feat", "Features related parameters")

    # Define HBase related arguments
    hbase_group.add_argument("--hbase_host", dest="hbase_host", required=True)
    hbase_group.add_argument("--hbase_port", dest="hbase_port", default=2181)
    hbase_group.add_argument("--hbase_ip", dest="hbase_ip", default="10.1.94.57")
    # BEWARE: these tables should be already created
    # we could just have a table_prefix
    hbase_group.add_argument("--table_sha1", dest="tab_sha1_infos_name", required=True)
    hbase_group.add_argument("--table_update", dest="tab_update_name", required=True)

    # Define ES related options, just to know ingestion id...
    es_group.add_argument("--es_domain", dest="es_domain", required=True)
    es_group.add_argument("--es_ts_start", dest="es_ts_start", help="start timestamp in ms", default=None)
    es_group.add_argument("--es_ts_end", dest="es_ts_end", help="end timestamp in ms", default=None)
    
    # Define job related options
    job_group.add_argument("-i", "--identifier", dest="identifier")
    job_group.add_argument("-b", "--batch_update_size", dest="batch_update_size", type=int, default=default_batch_update_size)
    # should this be estimated from RDD counts actually?
    job_group.add_argument("-p", "--nb_partitions", dest="nb_partitions", type=int, default=-1)
    job_group.add_argument("--max_samples_per_partition", dest="max_samples_per_partition", type=int, default=max_samples_per_partition)
    job_group.add_argument("--base_hdfs_path", dest="base_hdfs_path", default=base_hdfs_path)
    # should we still allow the input of day to process and estimate ts start and end from it?
    
    # Parse
    
    try:
        c_options = parser.parse_args()
        print "Got options:", c_options
    except Exception as inst:
        print inst
        parser.print_help()
    
    es_ts_start, es_ts_end, ingestion_id = get_ingestion_start_end_id(c_options)


    # Setup SparkContext    
    sc = SparkContext(appName="extract-features-"+ingestion_id+job_suffix)
    sc.addPyFile('hdfs://memex/user/skaraman/extract-features/network.py')
    sc.addPyFile('hdfs://memex/user/skaraman/extract-features/tfdeepsentibank.py')
    sc.addFile('hdfs://memex/user/skaraman/extract-features/imagenet_mean.npy')
    sc.addFile('hdfs://memex/user/skaraman/extract-features/tfdeepsentibank.npy')
    conf = SparkConf()
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
    
    # Setup HBase managers
    # just to be sure we will be able to write out to the table
    get_create_table(c_options.tab_sha1_infos_name, c_options)
    get_create_table(c_options.tab_update_name, c_options)
    # hbase managers
    hbase_fullhost = c_options.hbase_host+':'+str(c_options.hbase_port)
    hbase_man_sha1infos_out = HbaseManager(sc, conf, hbase_fullhost, c_options.tab_sha1_infos_name)
    hbase_man_update_out = HbaseManager(sc, conf, hbase_fullhost, c_options.tab_update_name)
    
    # Run extraction
    print "[START] Starting extracting features for ingestion {}".format(ingestion_id)
    run_extraction(hbase_man_sha1infos_out, hbase_man_update_out, ingestion_id, c_options)
    print "[DONE] Extracted features for ingestion {} in {}s.".format(ingestion_id, time.time() - start_time)

