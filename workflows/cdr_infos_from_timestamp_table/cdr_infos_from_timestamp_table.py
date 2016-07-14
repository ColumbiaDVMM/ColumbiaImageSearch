import json
from hbase_manager import HbaseManager
from pyspark import SparkContext, SparkConf

max_ts = 9999999999999

#def get_list_value(json_x,field_tuple):
#    return [x["value"] for x in json_x if x["columnFamily"]==field_tuple[0] and x["qualifier"]==field_tuple[1]]

def ts_to_cdr_id(data):
    ts_doc_id = data[0]
    list_ts_doc_id = ts_doc_id.split("_")
    ts = list_ts_doc_id[0]
    doc_id = list_ts_doc_id[1]
    #print ts_doc_id,ts,doc_id
    json_x = [json.loads(x) for x in data[1].split("\n")]
    tup_list = [ (doc_id, [doc_id, "info", "insert_ts", str(max_ts-int(ts))]) ]
    # do we want to keep info:doc_id ?
    for x in json_x:
        tup_list.append( (doc_id, [doc_id, x["columnFamily"], x["qualifier"], x["value"]]) )
    return tup_list

def fill_cdr_ids_infos(hbase_man_in, hbase_man_out):
    in_rdd = hbase_man_in.read_hbase_table()
    cdr_ids_infos_rdd = in_rdd.flatMap(lambda x: ts_to_cdr_id(x))
    hbase_man_out.rdd2hbase(cdr_ids_infos_rdd)
    print "[fill_cdr_ids_infos] Done."

if __name__ == '__main__':
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    tab_name_in = job_conf["tab_name_in"]
    tab_name_out = job_conf["tab_name_out"]
    hbase_host = job_conf["hbase_host"]
    sc = SparkContext(appName=tab_name_in+'_to_'+tab_name_out)
    sc.setLogLevel("ERROR")
    conf = SparkConf()
    #in_columns_list = ["meta:sha1", "meta:columbia_near_dups_sha1", "meta:columbia_near_dups_sha1_dist"]
    #hbase_man_in = HbaseManager(sc, conf, hbase_host, tab_name_in, columns_list=in_columns_list)
    hbase_man_in = HbaseManager(sc, conf, hbase_host, tab_name_in)
    hbase_man_out = HbaseManager(sc, conf, hbase_host, tab_name_out)
    fill_cdr_ids_infos(hbase_man_in, hbase_man_out)
