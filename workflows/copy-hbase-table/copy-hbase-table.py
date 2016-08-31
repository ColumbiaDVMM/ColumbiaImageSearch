import json
from pyspark import SparkContext, SparkConf


def get_list_value(json_x,field_tuple):
    return [x["value"] for x in json_x if x["columnFamily"]==field_tuple[0] and x["qualifier"]==field_tuple[1]]


def build_output(data):
    key = data[0]
    try:
        fields_values = dict()
        json_x = [json.loads(x) for x in data[1].split("\n")]
        fields = [(x["columnFamily"], x["qualifier"]) for x in json_x]
        #print("[build_output] fields: {}".format(fields))
        for field in fields:
            field_value = get_list_value(json_x,field)[0]
            if field_value:
                fields_values[field] = field_value.strip()
            else:
                return []
        out = [(key, [key, field[0], field[1], fields_values[field]]) for field in fields]
        #print("[build_output] out: {}".format(out))
        return out 
    except Exception as inst:
        print "[Error] could not build output for row {}. {}".format(key,inst)
    return []


def copy_cdrinfos(hbase_man_in, hbase_man_out, nb_partitions):
    in_rdd = hbase_man_in.read_hbase_table().partitionBy(nb_partitions).flatMap(lambda x: build_output(x))
    hbase_man_out.rdd2hbase(in_rdd)


if __name__ == '__main__':
    from hbase_manager import HbaseManager
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    tab_name_in = job_conf["tab_name_in"]
    tab_name_out = job_conf["tab_name_out"]
    hbase_host = job_conf["hbase_host"]
    nb_partitions = job_conf["nb_partitions"]
    sc = SparkContext(appName='copy_from_'+tab_name_in+'_to_'+tab_name_out)
    sc.setLogLevel("ERROR")
    conf = SparkConf()
    hbase_man_in = HbaseManager(sc, conf, hbase_host, tab_name_in)
    hbase_man_out = HbaseManager(sc, conf, hbase_host, tab_name_out)
    copy_cdrinfos(hbase_man_in, hbase_man_out, nb_partitions)
