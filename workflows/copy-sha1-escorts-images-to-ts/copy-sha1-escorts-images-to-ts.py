import json
from pyspark import SparkContext, SparkConf

def get_list_value(json_x,field_tuple):
    return [x["value"] for x in json_x if x["columnFamily"]==field_tuple[0] and x["qualifier"]==field_tuple[1]]

def get_sha1(data):
    key = data[0]
    sha1 = None
    json_x = [json.loads(x) for x in data[1].split("\n")]
    try:
        sha1_list = get_list_value(json_x,("info","sha1"))
        if sha1_list:
            sha1 = sha1_list[0].strip()
    except Exception as inst:
        print "[Error] could not get sha1 for row {}. {}".format(key,inst)
    if sha1:
        return [(key, [key, "info", "sha1", sha1.upper()])]
    return []

def copy_sha1(hbase_man_in, hbase_man_out):
    in_rdd = hbase_man_in.read_hbase_table()
    out_rdd = in_rdd.flatMap(lambda x: get_sha1(x))
    hbase_man_out.rdd2hbase(out_rdd)

if __name__ == '__main__':
    from hbase_manager import HbaseManager
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    tab_name_in = job_conf["tab_name_in"]
    tab_name_out = job_conf["tab_name_out"]
    hbase_host = job_conf["hbase_host"]
    sc = SparkContext(appName='copy_sha1_from_'+tab_name_in+'_to_'+tab_name_out)
    sc.setLogLevel("ERROR")
    conf = SparkConf()
    hbase_man_in = HbaseManager(sc, conf, hbase_host, tab_name_in)
    hbase_man_out = HbaseManager(sc, conf, hbase_host, tab_name_out)
    copy_sha1(hbase_man_in, hbase_man_out)
