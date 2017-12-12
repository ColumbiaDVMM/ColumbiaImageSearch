import json
import time
import datetime
import numpy as np
from pyspark import SparkContext, SparkConf

max_ts = 9999999999999
valid_extr_type = ["sbcmdline_feat_full_image", "dlib_feat_dlib_face"]
processed_suffix = "_processed"
updateid_suffix = "_updateid"
extr_column_family = "ext"


def get_list_value(json_x, field_tuple):
    return [x["value"] for x in json_x if x["columnFamily"] == field_tuple[0] and x["qualifier"] == field_tuple[1]]


def get_tuple_list_value_start(json_x, field_tuple):
    return [(x["qualifier"], x["value"]) for x in json_x if x["columnFamily"] == field_tuple[0] and x["qualifier"].startswith(field_tuple[1])]


def check_extr_performed(extr_tuple_list, extr_type):
    # Check that there is at least one feature
    for tup in extr_tuple_list:
        if tup[0] != extr_type + updateid_suffix and tup[0] != extr_type + processed_suffix:
            return True
    return False


def check_extr_claimed(extr_tuple_list, extr_type):
    # Check that image has been claimed by an update
    for tup in extr_tuple_list:
        if tup[0] == extr_type + updateid_suffix or tup[0] == extr_type + processed_suffix:
            return True
    return False


def get_unprocessed_images(data):
    # Get images not processed for that extr_type and not yet claimed
    key = data[0]
    json_x = [json.loads(x) for x in data[1].split("\n")]
    try:
        extr_tuple_list = get_tuple_list_value_start(json_x, (extr_column_family, extr_type))
        # Check that extraction was not performed on the image and that it was not claimed by a previous update
        if not check_extr_performed(extr_tuple_list, extr_type) and not check_extr_claimed(extr_tuple_list, extr_type):
            return True
    except Exception as inst:
        print "[Error in get_images_to_be_indexed] key was {}. {}".format(key, inst)
    return False


def get_processed_images(data, extr_type):
    # Get images processed for that extr_type and not yet claimed
    key = data[0]
    json_x = [json.loads(x) for x in data[1].split("\n")]
    try:
        extr_tuple_list = get_tuple_list_value_start(json_x, (extr_column_family, extr_type))
        # Check that extraction was performed on the image but it was not claimed by a previous update
        if check_extr_performed(extr_tuple_list, extr_type) and not check_extr_claimed(extr_tuple_list, extr_type):
            return True
    except Exception as inst:
        print "[Error in get_images_processed] key was {}. {}".format(key, inst)
    return False


def build_batch_rdd(batch_udpate, extr_type, processed=True):
    # Create a batch_rdd to be stored in hbase table update_infos, and one batch to save the update_id for each image
    # Build update_id, should contain extraction type e.g. index_update_dlib_feat_dlib_face_2017-09-29_53-ec27
    spark_suffix = "_spark"+str(max_ts-int(time.time()*1000))+'_'+str(np.int32(np.random.random()*(10e6)))
    update_id = "index_update_"+extr_type+"_"+datetime.date.today().isoformat()+spark_suffix
    # Build batch of images
    list_key = []
    batch_images_out = []
    for x in batch_udpate:
        list_key.append(x)
        if processed:
            batch_images_out.append((x, [x, extr_column_family, extr_type + processed_suffix, 1]))
        batch_images_out.append((x, [x, extr_column_family, extr_type + updateid_suffix, update_id]))
    batch_update_out = [(update_id, [update_id, "info", "list_sha1s", ','.join(list_key)]),
                        (update_id, [update_id, "info", "created", datetime.now().strftime('%Y-%m-%d:%H.%M.%S')])]
    if processed:
        batch_update_out.extend([(update_id, [update_id, "info", "started", datetime.now().strftime('%Y-%m-%d:%H.%M.%S')]),
                                 (update_id, [update_id, "info", "processed", datetime.now().strftime('%Y-%m-%d:%H.%M.%S')])])
    return sc.parallelize(batch_update_out), sc.parallelize(batch_images_out)


def create_processed_updates(hbase_man_in, hbase_man_updates_out, hbase_man_sha1infos_out, extr_type,
                           batch_update_size=4096):
    # Read all images and keep those that have been processed (i.e. have features stored in HBase) but not claimed by an update
    in_rdd = hbase_man_in.read_hbase_table()
    out_rdd = in_rdd.filter(lambda row: get_processed_images(row, extr_type)).keys()
    # Build batches
    iterator = out_rdd.toLocalIterator()
    batch_udpate = []
    for x in iterator:
        batch_udpate.append(x)
        if len(batch_udpate)==batch_update_size:
            batch_update_rdd, batch_images_out_rdd = build_batch_rdd(batch_udpate, extr_type)
            hbase_man_updates_out.rdd2hbase(batch_update_rdd)
            hbase_man_sha1infos_out.rdd2hbase(batch_images_out_rdd)
            batch_udpate = []
    # Last batch
    if batch_udpate:
        batch_update_rdd, batch_images_out_rdd = build_batch_rdd(batch_udpate, extr_type)
        hbase_man_updates_out.rdd2hbase(batch_update_rdd)
        hbase_man_sha1infos_out.rdd2hbase(batch_images_out_rdd)


def create_unprocessed_updates(hbase_man_in, hbase_man_updates_out, hbase_man_sha1infos_out, extr_type,
                             batch_update_size=4096):
    # Read all images and keep those that have not been processed (i.e. have no features stored in HBase) and not claimed by an update
    in_rdd = hbase_man_in.read_hbase_table()
    out_rdd = in_rdd.filter(lambda row: get_unprocessed_images(row, extr_type)).keys()
    # Build batches
    iterator = out_rdd.toLocalIterator()
    batch_udpate = []
    for x in iterator:
        batch_udpate.append(x)
        if len(batch_udpate) == batch_update_size:
            batch_update_rdd, batch_images_out_rdd = build_batch_rdd(batch_udpate, extr_type, processed=False)
            hbase_man_updates_out.rdd2hbase(batch_update_rdd)
            hbase_man_sha1infos_out.rdd2hbase(batch_images_out_rdd)
            batch_udpate = []
    # Last batch
    if batch_udpate:
        batch_update_rdd, batch_images_out_rdd = build_batch_rdd(batch_udpate, extr_type, processed=False)
        hbase_man_updates_out.rdd2hbase(batch_update_rdd)
        hbase_man_sha1infos_out.rdd2hbase(batch_images_out_rdd)
            

if __name__ == '__main__':
    from hbase_manager import HbaseManager
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    tab_sha1_infos_name = job_conf["tab_sha1_infos"]
    tab_updates_name = job_conf["tab_updates"]
    hbase_host = job_conf["hbase_host"]
    # Should be either "sbcmdline_feat_full_image" or "dlib_feat_dlib_face"
    extr_type = job_conf["extr_type"]
    if extr_type not in valid_extr_type:
        raise ValueError("Unexpected extr_type: {}".format(extr_type))
    # TODO: have a switch for processed/unprocessed?
    sc = SparkContext(appName='create_'+extr_type+'_legacy_updates_from_'+tab_sha1_infos_name+'_pushed_to_'+tab_updates_name)
    sc.setLogLevel("ERROR")
    conf = SparkConf()
    columns = ["ext"]
    hbase_man_in = HbaseManager(sc, conf, hbase_host, tab_sha1_infos_name, columns_list=columns)
    hbase_man_updates_out = HbaseManager(sc, conf, hbase_host, tab_updates_name)
    hbase_man_sha1infos_out = HbaseManager(sc, conf, hbase_host, tab_sha1_infos_name)
    create_processed_updates(hbase_man_in, hbase_man_updates_out, hbase_man_sha1infos_out)
    create_unprocessed_updates(hbase_man_in, hbase_man_updates_out, hbase_man_sha1infos_out)
