import json
import time
from elastic_manager import ES
from hbase_manager import HbaseManager
from pyspark import SparkContext, SparkConf

# default settings
new_crawler = False
fields_cdr = ["obj_stored_url", "crawl_data.image_id", "crawl_data.memex_ht_id", "timestamp", "obj_original_url", "obj_parent"]
filter_crawldata_image_id = "exists"

def set_fields_filter(new_crawler):
    global fields_cdr,filter_crawldata_image_id
    if new_crawler:
        print "new_crawler is True."
        fields_cdr = ["obj_stored_url", "timestamp", "obj_original_url", "obj_parent"]
        filter_crawldata_image_id = "missing"
    else:
        print "new_crawler is False."
        fields_cdr = ["obj_stored_url", "crawl_data.image_id", "crawl_data.memex_ht_id", "timestamp", "obj_original_url", "obj_parent"]
        filter_crawldata_image_id = "exists"

def create_images_tuple(data):
    print data
    doc_id = data[0]
    json_x = data[1]
    tup_list=[]
    for field in fields_cdr:
        try:
            field_value = json_x[field][0]
            #print field,field_value
            tup_list.append( (doc_id, [doc_id, "info", field, str(field_value)]) )
        except Exception as inst:
            print "[Error] Could not get field {} value for document {}. {}".format(field,doc_id,inst)
            time.sleep(5)
    # How to access the sort value here?
    #tup_list.append( (doc_id, [doc_id, "info", "ts", json_x["sort"]]) )
    return tup_list

def move_data(es_man, hbase_man):
    #query = "{\"query\": {\"filtered\": {\"query\": {\"match\": {\"content_type\": \"image/jpeg\"}},\"filter\": [{\"range\": {\"timestamp\": {\"gt\": 1451606400000}}}]}}}"
    #query = "{ \"fields\": [\""+"\", \"".join(fields_cdr)+"\"], \"query\": { \"filtered\": { \"query\": { \"match\": { \"content_type\": \"image/jpeg\" }}, \"filter\" : { \"exists\": { \"field\": \"crawl_data.image_id\" } } } }, \"sort\": [ \"_timestamp\" ] }"
    # new crawler do not produce a "crawl_data.image_id"
    query = "{ \"fields\": [\""+"\", \"".join(fields_cdr)+"\"], \"query\": { \"filtered\": { \"query\": { \"match\": { \"content_type\": \"image/jpeg\" }}, \"filter\" : { \""+filter_crawldata_image_id+"\": { \"field\": \"crawl_data.image_id\" } } } }, \"sort\": [ \"_timestamp\" ] }"
    print query
    es_rdd = es_man.es2rdd(query)
    images_hb_rdd = es_rdd.flatMap(lambda x: create_images_tuple(x))
    hbase_man.rdd2hbase(images_hb_rdd)

if __name__ == '__main__':
    # Read job_conf
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    # Set parameters job_conf
    tab_name = job_conf["tab_name"]
    hbase_host = job_conf["hbase_host"]
    new_crawler = job_conf["new_crawler"]
    es_index = job_conf["es_index"]
    es_domain = job_conf["es_domain"]
    es_host = job_conf["es_host"] 
    es_port = job_conf["es_port"]
    es_user = job_conf["es_user"]
    es_pass = job_conf["es_pass"]
    # Update fields based on new_crawler value
    set_fields_filter(new_crawler)
    # Start job
    sc = SparkContext(appName=tab_name)
    sc.setLogLevel("ERROR")
    conf = SparkConf()
    es_man = ES(sc, conf, es_index, es_domain, es_host, es_port, es_user, es_pass)
    hbase_man = HbaseManager(sc, conf, hbase_host, tab_name)
    move_data(es_man, hbase_man)
