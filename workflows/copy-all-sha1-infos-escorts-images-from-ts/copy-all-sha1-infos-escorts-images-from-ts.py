import json
from pyspark import SparkContext, SparkConf

fields = [("info","all_cdr_ids"), ("info","s3_url"), ("info","all_parent_ids")]

def get_list_value(json_x,field_tuple):
    return [x["value"] for x in json_x if x["columnFamily"]==field_tuple[0] and x["qualifier"]==field_tuple[1]]

def get_infos(data):
    key = data[0]
    try:
        fields_values = {}
        json_x = [json.loads(x) for x in data[1].split("\n")]
        for field in fields:
            field_value = get_list_value(json_x,field)[0]
            if field_value:
                fields_values[field] = field_value.strip()
            else:
                #print "[get_infos: log] no value found for key {} for field {}".format(key,field)
                return []
        out = [(key, [key, field[0], field[1], fields_values[field]]) for field in fields]
        #print "[get_infos: log] return value for key {} is: {}".format(key,out)
        return out 
    except Exception as inst:
        print "[Error] could not get sha1 infos for row {}. {}".format(key,inst)
    return []

def copy_sha1_infos(hbase_man_in, hbase_man_out):
    in_rdd = hbase_man_in.read_hbase_table()
    out_rdd = in_rdd.partitionBy(80).flatMap(lambda x: get_infos(x))
    hbase_man_out.rdd2hbase(out_rdd)

if __name__ == '__main__':
    from hbase_manager import HbaseManager
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    tab_name_in = job_conf["tab_name_in"]
    tab_name_out = job_conf["tab_name_out"]
    hbase_host = job_conf["hbase_host"]
    sc = SparkContext(appName='copy_sha1_infos_from_'+tab_name_in+'_to_'+tab_name_out)
    #sc.setLogLevel("ERROR")
    conf = SparkConf()
    hbase_man_in = HbaseManager(sc, conf, hbase_host, tab_name_in)
    hbase_man_out = HbaseManager(sc, conf, hbase_host, tab_name_out)
    copy_sha1_infos(hbase_man_in, hbase_man_out)
