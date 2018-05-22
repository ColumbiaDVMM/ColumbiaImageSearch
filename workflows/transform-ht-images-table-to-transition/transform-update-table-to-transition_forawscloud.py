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
  """Get or create HBase table with given column families.

  :param table_name: name of table
  :param conn: happybase connection
  :param families: dictionnary of column families
  :return: happybase table
  """
  try:
    # what exception would be raised if table does not exist, actually none.
    # need to try to access families to get error
    table = conn.table(table_name)
    # this would fail if table does not exist
    _ = table.families()
    return table
  except Exception as inst:
    if isinstance(inst, TTransportException):
      raise inst
    else:
      msg = "[get_create_table: info] table {} does not exist (yet): {}{}"
      print(msg.format(table_name, type(inst), inst))
      conn.create_table(table_name, families)
      table = conn.table(table_name)
      print("[get_create_table: info] created table {}".format(table_name))
      return table


def transform(data):
  """Filter rows that needs to be maintained.

  :param data: row
  :return: list of fields to be saved to new table
  """
  key = data[0]
  keep = False
  fields = []
  for str_filter in FILTERING:
    if key.startswith(str_filter):
      keep = True
      break
  if keep:
    try:
      json_x = [json.loads(x) for x in data[1].split("\n")]
      # Is there any column we would want to discard?...
      for entry in json_x:
        fields.append((key, [key, entry[CF], entry[QF], entry[VA]]))
    except Exception as inst:
      print("Transformation failed ({}) for key {}: {}".format(inst, key, fields))
  return fields


def transform_table():
  """Transform update table.
  """
  in_rdd = HBASE_MAN_IN.read_hbase_table()
  out_rdd = in_rdd.flatMap(transform)
  HBASE_MAN_OUT.rdd2hbase(out_rdd)


if __name__ == '__main__':
  from hbase_manager import HbaseManager

  # Read conf
  JOB_CONF = json.load(open("job_conf_update.json", "rt"))
  print(JOB_CONF)
  TAB_NAME_IN = JOB_CONF["tab_name_in"]
  TAB_NAME_OUT = JOB_CONF["tab_name_out"]
  TAB_OUT_FAMILIES = JOB_CONF["tab_out_families"]
  HBASE_HOST_SPARK = JOB_CONF["hbase_host"]
  FILTERING = JOB_CONF["filtering"]
  # Could be a parameter in conf
  HAPPYBASE_HOST = "10.108.16.137"

  # Try to create "tab_name_out"
  HBASE_TIMEOUT = None
  NB_THREADS = 1
  POOL = happybase.ConnectionPool(size=NB_THREADS, host=HAPPYBASE_HOST, timeout=HBASE_TIMEOUT)
  with POOL.connection() as CONN:
    get_create_table(TAB_NAME_OUT, CONN, TAB_OUT_FAMILIES)

  # Setup spark job
  SC = SparkContext(appName="transform_" + TAB_NAME_IN + "_to_" + TAB_NAME_OUT)
  SC.setLogLevel("ERROR")
  CONF = SparkConf()
  HBASE_MAN_IN = HbaseManager(SC, CONF, HBASE_HOST_SPARK, TAB_NAME_IN)
  HBASE_MAN_OUT = HbaseManager(SC, CONF, HBASE_HOST_SPARK, TAB_NAME_OUT)
  transform_table()

  print("Transformation completed.")
