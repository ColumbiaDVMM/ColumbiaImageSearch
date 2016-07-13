import json
from elastic_manager import ES
from hbase_manager import HbaseManager
from pyspark import SparkContext, SparkConf

fields_dig = ["_id"]

def split_dig_id_to_cdr_id(data):
    print data
    cdr_id = data[0].strip().split("/")[-1]
    tup_list = [(cdr_id, [cdr_id, "info", "missing_image", str(1)])]
    return tup_list

def move_data(es_man, hbase_man):
    #query = "{\"fields\": [\""+"\", \"".join(fields_cdr)+"\"], \"query\": {\"filtered\": {\"query\": {\"match\": {\"content_type\": \"image/jpeg\"}}, \"filter\": {\"range\" : {\"_timestamp\" : {\"gte\" : "+str(es_ts_start)+"}}}}}}"
    #query = "{ \"fields\": [\"_id\"], \"query\": { \"filtered\": { \"query\": { \"match_all\": {}}, \"filter\": {\"missing\": { \"field\": \"hasImagePart\" }}}}}"
    query = "{ \"fields\": [\"_id\"], \"query\": { \"filtered\": { \"query\": { \"prefix\": { \"_id\": \"http://dig.isi.edu/ht/data/webpage/AA00\" }}, \"filter\": {\"missing\": { \"field\": \"hasImagePart\" }}}}}"
    print query
    es_rdd = es_man.es2rdd(query)
    images_hb_rdd = es_rdd.flatMap(lambda x: split_dig_id_to_cdr_id(x))
    hbase_man.rdd2hbase(images_hb_rdd)

if __name__ == '__main__':
    # Read job_conf
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    
    # Set parameters job_conf
    tab_name = job_conf["tab_name"]
    hbase_host = job_conf["hbase_host"]
    es_index = job_conf["es_index"]
    es_domain = job_conf["es_domain"]
    es_host = job_conf["es_host"] 
    es_port = job_conf["es_port"]
    es_user = job_conf["es_user"]
    es_pass = job_conf["es_pass"]
    
    # Start job
    sc = SparkContext(appName=tab_name)
    conf = SparkConf()
    es_man = ES(sc, conf, es_index, es_domain, es_host, es_port, es_user, es_pass)
    #es_man.set_output_json()
    #es_man.set_read_metadata()
    hbase_man = HbaseManager(sc, conf, hbase_host, tab_name)
    move_data(es_man, hbase_man)
