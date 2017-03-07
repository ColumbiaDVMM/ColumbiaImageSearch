import sys
print(sys.version)

import json
from pyspark import SparkContext, SparkConf
from hbase_manager import HbaseManager

## MAIN
if __name__ == '__main__':
    
    # Read job_conf
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    sc = SparkContext(appName="test_read_hbase")
    conf = SparkConf()
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

    # HBase Conf
    hbase_host = job_conf["hbase_host"]
    tab_name = job_conf["tab_name"]
    hbase_man = HbaseManager(sc, conf, hbase_host, tab_name)

    # Out conf
    out_file = job_conf["out_file"]

    # Run test
    in_rdd = hbase_man.read_hbase_table()
    in_rdd.saveAsSequenceFile(out_file)
