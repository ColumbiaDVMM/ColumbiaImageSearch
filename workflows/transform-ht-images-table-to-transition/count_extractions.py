"""It looks like we are missing extractions.

We have 21349452 face features and 62288020 full image features out of 92622061 images in the search
indices.

Check:
- number of images for which we have (at least one) features
- total count of features in HBase
- get list of images for which we have no features and extr_processed is not set
(i.e. sbpycaffe_feat_full_image/dlib_feat_dlib_face/_processed)
For sbpycaffe_feat_full_image, we should always have a feature if processed is 1, and processed cannot be 0.
For dlib_feat_dlib_face, we should always have a feature if processed is 1, no feature if processed is set to 0.

"""

from __future__ import print_function

import json
import happybase
from pyspark import SparkContext, SparkConf

TTransportException = happybase._thriftpy.transport.TTransportException

CF = "columnFamily"
QF = "qualifier"
VA = "value"


def get_list_val(jsx, field_tuple):
  return [x[VA] for x in jsx if x[CF] == field_tuple[0] and x[QF] == field_tuple[1]]


def get_list_qfval(jsx, column_family):
  return [(x[QF], x[VA]) for x in jsx if x[CF] == column_family]


def get_filtered_list_qfval(jsx, col_fam, qf_start):
  return [(x[QF], x[VA]) for x in jsx if x[CF] == col_fam and x[QF].startswith(qf_start)]


# E.g. field_tuple=("info", "s3_url")
def get_value(json_x, key, field_tuple):
  value = None
  try:
    value = get_list_val(json_x, field_tuple)
    if value:
      value = value[0].strip()
  except Exception as inst:
    print("[Error] could not get value of {} for row {}. {}".format(field_tuple, key, inst))
  return value


def check_processed(data):
  key = data[0]
  status = [key, 0, 0]
  try:
    json_x = [json.loads(x) for x in data[1].split("\n")]
    list_qf_va = get_filtered_list_qfval(json_x, EXTR_COL, EXTR_TYPE)
    for entry in list_qf_va:
      if entry[0].endswith("_processed"):
        status[1] = 1
      elif not entry[0].endswith("_updateid") and not entry[0].endswith("_failed"):
        status[2] += 1
  except Exception as inst:
    print("Check failed ({}) for key {}".format(inst, key))
  return status


def check_table():
  in_rdd = HBASE_MAN_IN.read_hbase_table()
  out_rdd = in_rdd.map(check_processed)
  print("out_rdd.first(): {}".format(out_rdd.first()))
  count_rdd = out_rdd.reduce(lambda x, y: (0, x[1] + y[1], x[2] + y[2]))
  print("count_rdd: {}".format(count_rdd))
  missing_rdd = out_rdd.filter(lambda x: x[1]==0)
  missing_rdd.saveAsTextFile(OUT_PATH)
  print("missing_rdd count: {}".format(missing_rdd.count()))


if __name__ == '__main__':
  from hbase_manager import HbaseManager

  # Read conf
  JOB_CONF = json.load(open("job_conf_count_extr.json", "rt"))
  print(JOB_CONF)
  TAB_NAME_CHECK = JOB_CONF["tab_name_check"]
  HBASE_HOST_SPARK = JOB_CONF["hbase_host"]
  EXTR_TYPE = JOB_CONF["extr_type"]
  EXTR_COL = JOB_CONF["extr_column"]
  OUT_PATH = JOB_CONF["out_path"]

  # Setup spark job
  SC = SparkContext(appName='count_extractions_' + EXTR_TYPE + '_in_' + TAB_NAME_CHECK)
  SC.setLogLevel("ERROR")
  CONF = SparkConf()
  HBASE_MAN_IN = HbaseManager(SC, CONF, HBASE_HOST_SPARK, TAB_NAME_CHECK)
  check_table()

  print("Check completed.")
