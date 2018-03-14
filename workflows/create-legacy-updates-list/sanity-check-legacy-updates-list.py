from __future__ import print_function

import json
from pyspark import SparkContext, SparkConf

max_ts = 9999999999999
valid_extr_type = ["sbcmdline_feat_full_image", "dlib_feat_dlib_face"]
processed_suffix = "_processed"
processed_update_column = "processed"
updateid_suffix = "_updateid"
extr_column_family = "ext"
info_column_family = "info"


def get_list_value(json_x, field_tuple):
    return [x["value"] for x in json_x if x["columnFamily"] == field_tuple[0] and x["qualifier"] == field_tuple[1]]

def get_tuple_list_value_start(json_x, field_tuple):
    return [(x["qualifier"], x["value"]) for x in json_x if x["columnFamily"] == field_tuple[0] and
            x["qualifier"].startswith(field_tuple[1])]

def get_tuple_list_colum_family(json_x, column_family):
    return [(x["qualifier"], x["value"]) for x in json_x if x["columnFamily"] == column_family]


def check_processed(update_tuple_list):
    # Check that there is the "processed_update_column"
    for tup in update_tuple_list:
        if tup[0] == processed_update_column:
            return True
    return False


def get_unprocessed_update(data, extr_type):
    key = data[0]
    # Check updates of that extr_type
    if extr_type in key:
        json_x = [json.loads(x) for x in data[1].split("\n")]
        try:
            update_tuple_list = get_tuple_list_colum_family(json_x, info_column_family)
            # Check that this update does NOT have the "processed" column
            if not check_processed(update_tuple_list):
                print("[get_unprocessed_update: log] Found unprocessed update: {}".format(key))
                return True
        except Exception as inst:
            print("[get_unprocessed_update: error] key was {}. {}".format(key, inst))
    else:
        log_msg = "[get_unprocessed_update: log] skipping update of another type than {}. Key was {}"
        print(log_msg.format(extr_type, key))
    return False


def get_processed_update(data, extr_type):
    key = data[0]
    # Check updates of that extr_type
    if extr_type in key:
        json_x = [json.loads(x) for x in data[1].split("\n")]
        try:
            update_tuple_list = get_tuple_list_colum_family(json_x, info_column_family)
            # Check that this update has the "processed" column
            if check_processed(update_tuple_list):
                print("[get_processed_update: log] Found processed update: {}".format(key))
                return True
        except Exception as inst:
            print("[get_processed_update: error] key was {}. {}".format(key, inst))
    else:
        log_msg = "[get_processed_update: log] skipping update of another type than {}. Key was {}"
        print(log_msg.format(extr_type, key))
    return False


def mark_completeness(x, batch_size):
    import json
    list_sha1s_field = ("info", "list_sha1s")
    json_x = [json.loads(y) for y in x.split("\n")]
    list_sha1s_col = get_list_value(json_x, list_sha1s_field)[0].split(',')
    if len(list_sha1s_col) == batch_size:
        return (True, batch_size)
    else:
        return (False, len(list_sha1s_col))


def check_updates_completeness(updates_rdd, batch_size):
    marked_updates_rdd = updates_rdd.mapValues(lambda x: mark_completeness(x, batch_size))
    nb_complete_updates = marked_updates_rdd.filter(lambda x: x[1][0]).count()
    uncomplete_updates_rdd = marked_updates_rdd.filter(lambda x: not x[1][0])
    iterator = uncomplete_updates_rdd.toLocalIterator()
    uncomplete_updates_list = []
    for x in iterator:
        uncomplete_updates_list.append((x[0], x[1][1]))
    return nb_complete_updates, uncomplete_updates_list



def check_processed_updates(hbase_man_updates, extr_type, batch_size=8192):
    # Read all updates and count those that have been marked as processed
    in_rdd = hbase_man_updates.read_hbase_table()
    updates_rdd = in_rdd.filter(lambda row: get_processed_update(row, extr_type))

    # Check completeness
    nb_complete_updates, uncomplete_updates_list = check_updates_completeness(updates_rdd,
                                                                              batch_size)

    complete_msg = "Found {} processed batches of {} images for extraction: {}"
    print(complete_msg.format(nb_complete_updates, batch_size, extr_type))
    uncomplete_msg = "Found {} processed batches of less than {} images for extraction: {}."
    print(uncomplete_msg.format(len(uncomplete_updates_list), batch_size, extr_type))


def check_unprocessed_updates(hbase_man_updates, extr_type, batch_size=2048):
    # Read all udpates and count those that have been not marked has processed (should we check "created"
    in_rdd = hbase_man_updates.read_hbase_table()
    updates_rdd = in_rdd.filter(lambda row: get_unprocessed_update(row, extr_type))

    # Check completeness
    nb_complete_updates, uncomplete_updates_list = check_updates_completeness(updates_rdd,
                                                                              batch_size)

    complete_msg = "Found {} unprocessed batches of {} images for extraction: {}"
    print(complete_msg.format(nb_complete_updates, batch_size, extr_type))
    uncomplete_msg = "Found {} unprocessed batches of less than {} images for extraction: {}."
    print(uncomplete_msg.format(len(uncomplete_updates_list), batch_size, extr_type))

if __name__ == '__main__':
    from hbase_manager import HbaseManager
    JOB_CONF = json.load(open("job_conf.json", "rt"))
    print(JOB_CONF)
    #tab_sha1_infos_name = job_conf["tab_sha1_infos"]
    tab_updates_name = JOB_CONF["tab_updates"]
    hbase_host = JOB_CONF["hbase_host"]
    # Should be either "sbcmdline_feat_full_image" or "dlib_feat_dlib_face"
    extr_type = JOB_CONF["extr_type"]
    # We can have a bigger update size for images that have already been processed as no extraction would be needed
    batch_update_processed_size = JOB_CONF["batch_update_processed_size"]
    batch_update_unprocessed_size = JOB_CONF["batch_update_unprocessed_size"]
    if extr_type not in valid_extr_type:
        raise ValueError("Unexpected extr_type: {}".format(extr_type))
    SC = SparkContext(appName='check_'+extr_type+'_legacy_updates_in_'+tab_updates_name)
    SC.setLogLevel("ERROR")
    conf = SparkConf()
    columns = ["ext"]
    hbase_man_updates = HbaseManager(SC, conf, hbase_host, tab_updates_name)
    check_processed_updates(hbase_man_updates, extr_type, batch_size=batch_update_processed_size)
    check_unprocessed_updates(hbase_man_updates, extr_type, batch_size=batch_update_unprocessed_size)
