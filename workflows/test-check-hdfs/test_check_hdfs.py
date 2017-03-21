import sys
print(sys.version)
import subprocess

import json
from pyspark import SparkContext, SparkConf

def check_hdfs_file(hdfs_file_path):
    proc = subprocess.Popen(["hdfs", "dfs", "-ls", hdfs_file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if "Filesystem closed" in err:
        print("[check_hdfs_file: WARNING] Beware got error '{}' when checking for file: {}.".format(err, hdfs_file_path))
        sys.stdout.flush()
    print "[check_hdfs_file] out: {}, err: {}".format(out, err)
    return out, err


def hdfs_file_exist(hdfs_file_path):
    out, err = check_hdfs_file(hdfs_file_path)
    hdfs_file_exist = "_SUCCESS" in out
    return hdfs_file_exist

## MAIN
if __name__ == '__main__':
    
    # Read job_conf
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    sc = SparkContext(appName="test_check_hdfs")
    conf = SparkConf()
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

    # File confs
    file_exist_name = job_conf["file_exist"]
    file_notexist_name = job_conf["file_notexist"]
    # Test existing file
    if hdfs_file_exist(file_exist_name):
        print "File {} exists as expected.".format(file_exist_name)
    else:
        print "[ERROR] Unable to detect existing file {}.".format(file_exist_name)
    # Test non existing file
    if hdfs_file_exist(file_notexist_name):
        print "[ERROR] Wrongly considered file {} as existing.".format(file_exist_name)
    else:
        print "File {} does not exist as expected.".format(file_exist_name)

    
