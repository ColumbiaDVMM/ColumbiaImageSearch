import json
from hbase_manager import HbaseManager
from pyspark import SparkContext, SparkConf

fields_dig = ["_id"]

def split_ts_cdrid_rowkey(data):
    #print data
    cdr_id = data[0].strip().split("_")[-1]
    kv = (cdr_id, 1)
    #print kv
    return kv

def count_dups(hbase_man_timestamp, outfilename):
    in_rdd = hbase_man_timestamp.read_hbase_table()
    count_rdd = in_rdd.map(lambda x: split_ts_cdrid_rowkey(x))
    summary_rdd = count_rdd.reduceByKey(lambda x,y: x + y).map(lambda (x,y): (y, x)).sortByKey(0,1).map(lambda (x,y): (y,x)).map(lambda x: x[0] + ":" + str(x[1]))
    print summary_rdd.count()
    summary_rdd.saveAsTextFile(outfilename)

if __name__ == '__main__':
    # Read job_conf
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    
    # Set parameters job_conf
    tab_timestamp_name = job_conf["tab_timestamp_name"]
    hbase_host = job_conf["hbase_host"]
    outfilename = job_conf["outfilename"]
    
    # Start job
    sc = SparkContext(appName=tab_timestamp_name+"_count_dups")
    conf = SparkConf()
    hbase_man_timestamp = HbaseManager(sc, conf, hbase_host, tab_timestamp_name)
    count_dups(hbase_man_timestamp, outfilename)
