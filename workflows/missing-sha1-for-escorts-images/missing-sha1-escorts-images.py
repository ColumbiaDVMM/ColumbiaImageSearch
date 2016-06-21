import json
import os
from pyspark import SparkContext, SparkConf

def get_list_value(json_x,field_tuple):
    return [x["value"] for x in json_x if x["columnFamily"]==field_tuple[0] and x["qualifier"]==field_tuple[1]]

def has_sha1(data):
    key = data[0]
    image_id = None
    URL_S3 = None
    json_x = [json.loads(x) for x in data[1].split("\n")]
    out = []
    try:
        sha1 = get_list_value(json_x,("info","sha1"))[0]
    except IndexError:
        print "No sha1 for row {}".format(key)
        URL_S3 = None
        image_id = None
        try:
            URL_S3 = get_list_value(json_x,("info","obj_stored_url"))[0]
        except Exception as inst:
            print "[Error] for row {}. {}".format(key,inst)
            pass
        try:
            image_id = get_list_value(json_x,("info","crawl_data.image_id"))[0]
        except Exception as inst:
            print "[Error] for row {}. {}".format(key,inst)
            pass
        print "Missing sha1 for image with cdr_id {}.".format(key)
        if type(URL_S3) == unicode and URL_S3 is not None and URL_S3 != u'None' and URL_S3.startswith('https://s3'):
            out.append((key, [key, "info", "s3_url", URL_S3]))
        else:
            out.append((key, [key, "info", "s3_url|corrupted", "corrupted "+str(URL_S3)]))
        if image_id is not None:
            out.append((key, [key, "info", "image_id", image_id]))
        else:
            out.append((key, [key, "info", "image_id|missing", "missing"]))
    except Exception as inst:
        print "[Error] for row {}. {}".format(key,inst)
        pass
    return out

def fill_missing_sha1(sc, hbase_man, hbase_man_missing):
    in_rdd = hbase_man.read_hbase_table()
    out_rdd = in_rdd.flatMap(lambda x: has_sha1(x))
    hbase_man_missing.rdd2hbase(out_rdd)

if __name__ == '__main__':
    from hbase_manager import HbaseManager
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    tab_name = job_conf["tab_name"]
    tab_missing_name = job_conf["tab_missing_name"]
    hbase_host = job_conf["hbase_host"]
    in_columns_list = ["info:obj_stored_url","info:crawl_data.image_id","info:sha1"]
    sc = SparkContext(appName='missing-sha1_'+tab_name+'_to_'+tab_missing_name)
    sc.setLogLevel("ERROR")
    conf = SparkConf()
    hbase_man = HbaseManager(sc, conf, hbase_host, tab_name, columns_list=in_columns_list)
    hbase_man_missing = HbaseManager(sc, conf, hbase_host, tab_missing_name)
    fill_missing_sha1(sc, hbase_man, hbase_man_missing)
