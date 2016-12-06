import json
from pyspark import SparkContext, SparkConf


def get_list_value(json_x,field_tuple):
    return [x["value"] for x in json_x if x["columnFamily"]==field_tuple[0] and x["qualifier"]==field_tuple[1]]


def update_not_processed(data):
    json_x = [json.loads(x) for x in data[1].split("\n")]
    try:
        indexed_update = get_list_value(json_x,("info","indexed"))[0]
        # Check for None here just to be safe
        if indexed_update is None:
            raise ValueError("[update_not_processed] update {} has an invalid indexed value.".format(data[0]))
        return False
    except: # Update has not beed processed
        print("[update_not_processed] update {} has not been indexed.".format(data[0]))
        return True


def check_updates(hbase_man_in):
    in_rdd = hbase_man_in.read_hbase_table()
    nb_updates = in_rdd.count()
    updates_notprocessed_rdd = in_rdd.filter(update_not_processed)
    nb_updates_notprocessed = updates_notprocessed_rdd.count()
    print('We have {} updates not processed out of {}.'.format(nb_updates_notprocessed, nb_updates))
    print("[check_updates] DONE.")


if __name__ == '__main__':
    from hbase_manager import HbaseManager
    job_conf = json.load(open("job_conf_dev.json","rt"))
    print job_conf
    tab_updates_name = job_conf["tab_updates_name"]
    hbase_host = job_conf["hbase_host"]
    sc = SparkContext(appName='check_updates_'+tab_updates_name)
    sc.setLogLevel("ERROR")
    conf = SparkConf()
    # read rows starting from 'index_update_' in 'tab_updates_name'
    hbase_man_in = HbaseManager(sc, conf, hbase_host, tab_updates_name, row_start='index_update_', row_end='index_update_~')
    check_updates(hbase_man_in)
