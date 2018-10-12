from __future__ import print_function

import json
import happybase
from pyspark import SparkContext, SparkConf

TTransportException = happybase._thriftpy.transport.TTransportException

CF = "columnFamily"
QF = "qualifier"
VA = "value"


# E.g. families={'info': dict()}
def get_create_table(table_name, conn, families):
  try:
    # what exception would be raised if table does not exist, actually none.
    # need to try to access families to get error
    table = conn.table(table_name)
    # this would fail if table does not exist
    _ = table.families()
    return table
  except Exception as inst:
    # TODO: act differently based on error type (connection issue or actually table missing)
    #if type(inst) == TTransportException:
    if isinstance(inst, TTransportException):
      raise inst
    else:
      msg = "[get_create_table: info] table {} does not exist (yet): {}{}"
      print(msg.format(table_name, type(inst), inst))
      conn.create_table(table_name, families)
      table = conn.table(table_name)
      print("[get_create_table: info] created table {}".format(table_name))
      return table


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


def transform(data):
  key = data[0]
  fields = []
  try:
    json_x = [json.loads(x) for x in data[1].split("\n")]

    for one_map in MAPPINGS:
      cf_out, qf_out = one_map[1].split(':')
      if cf_out not in TAB_OUT_FAMILIES:
        raise ValueError("Error: column family {} is not in {}".format(cf_out, TAB_OUT_FAMILIES))
      # Filtered mapping
      if one_map[0].endswith("*"):
        # Read mapping
        cf_in, qf_start_in = one_map[0].split(':')
        # Check mapping validity (should we allow renaming of column qualifier?)
        if not qf_out.endswith("*") or qf_start_in != qf_out:
          raise ValueError("Incorrect mapping definition: {}".format(one_map))
        # Get values corresponding to mapping
        list_qf_va = get_filtered_list_qfval(json_x, cf_in, qf_start_in[:-1])
        # Prepare mapping output
        for qf_va in list_qf_va:
          fields.append((key, [key, cf_out, qf_va[0], qf_va[1]]))
      else: # Exact mapping
        if qf_out.endswith("*"):
          raise ValueError("Incorrect mapping definition: {}".format(one_map))
        val = get_value(json_x, key, one_map[0].split(':'))
        fields.append((key, [key, cf_out, qf_out, val]))

  except Exception as inst:
    print("Transformation failed ({}) for key {}: {}".format(inst, key, fields))
  return fields


def transform_table():
  in_rdd = HBASE_MAN_IN.read_hbase_table()
  #out_rdd = in_rdd.flatMap(lambda x: transform(x))
  out_rdd = in_rdd.flatMap(transform)
  HBASE_MAN_OUT.rdd2hbase(out_rdd)


if __name__ == '__main__':
  from hbase_manager import HbaseManager

  # Read conf
  JOB_CONF = json.load(open("job_conf.json", "rt"))
  print(JOB_CONF)
  TAB_NAME_IN = JOB_CONF["tab_name_in"]
  TAB_NAME_OUT = JOB_CONF["tab_name_out"]
  # should have 'data' and 'img'
  TAB_OUT_FAMILIES = JOB_CONF["tab_out_families"]
  HBASE_HOST_SPARK = JOB_CONF["hbase_host"]
  # TODO: we need to specify mapping in columns => out columns
  MAPPINGS = JOB_CONF["mappings"]
  # see mx_ht_images_details_111k
  # escorts_images_sha1_infos_from_ts_subsampled_newformat => ht_images_infos_merged_subsampled
  # discard ad:*. ext:sbcmdline (at least for real transform). just do not put them in mappings
  # mappings should be an array of arrays like:
  # ["ext:dlib*", "data:dlib*"]
  # ["ext:sbpycaffe*", "data:sbpycaffe*"]
  # ["info:s3_url", "data:s3_url"]
  # Could be a parameter in conf
  HAPPYBASE_HOST = '10.108.16.137'

  # TODO: should we also transform update table?

  # Try to create "tab_name_out"
  HBASE_TIMEOUT = None
  NB_THREADS = 1
  POOL = happybase.ConnectionPool(size=NB_THREADS, host=HAPPYBASE_HOST, timeout=HBASE_TIMEOUT)
  with POOL.connection() as CONN:
    get_create_table(TAB_NAME_OUT, CONN, TAB_OUT_FAMILIES)

  # Setup spark job
  SC = SparkContext(appName='transform_' + TAB_NAME_IN + '_to_' + TAB_NAME_OUT)
  SC.setLogLevel("ERROR")
  CONF = SparkConf()
  HBASE_MAN_IN = HbaseManager(SC, CONF, HBASE_HOST_SPARK, TAB_NAME_IN)
  HBASE_MAN_OUT = HbaseManager(SC, CONF, HBASE_HOST_SPARK, TAB_NAME_OUT)
  transform_table()

  print("Transformation completed.")
