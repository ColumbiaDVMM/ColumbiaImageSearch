import sys

print(sys.version)

import json
from pyspark import SparkContext, SparkConf
from hbase_manager import HbaseManager

from cufacesearch.featurizer.featsio import parse_feat_line

def build_output(x):
  out = []
  sha1, _, bbox, feat = parse_feat_line(x)
  if feat.any():
    # normalize?
    import numpy as np
    feat_norm = feat / np.linalg.norm(feat)
    # encode in base64
    import base64
    feat_b64 = base64.b64encode(feat_norm)
    # prepare output
    out.append((sha1, [sha1, 'face', 'dlib_feat_dlib_face_'+'_'.join(bbox), feat_b64]))
  return out


## MAIN
if __name__ == '__main__':
  # Read job_conf
  job_conf = json.load(open("job_conf.json", "rt"))
  print job_conf
  sc = SparkContext(appName="push_data_to_hbase")
  conf = SparkConf()
  log4j = sc._jvm.org.apache.log4j
  log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

  # In conf
  in_file = job_conf["in_file"]

  # HBase Conf
  hbase_host = job_conf["hbase_host"]
  tab_name = job_conf["tab_name"]
  hbase_man_out = HbaseManager(sc, conf, hbase_host, tab_name)

  # Read, parse and save
  in_rdd = sc.textFile(in_file).flatMap(build_output)
  hbase_man_out.rdd2hbase(in_rdd)

