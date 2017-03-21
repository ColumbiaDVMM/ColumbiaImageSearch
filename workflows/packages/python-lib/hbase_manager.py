__author__ = 'amandeep'

import time

class HbaseManager(object):
    def __init__(self, sc, conf, hbase_hostname, hbase_tablename, **kwargs):
        self.name = "ES2HBase"
        self.sc = sc
        self.conf = conf
        self.hbase_conf = {"hbase.zookeeper.quorum": hbase_hostname}
        self.hbase_table = hbase_tablename
        self.columns_list = None
        self.row_start = None
        self.row_stop = None
        self.key_only = False
        self.time_sleep = 0 # default is to save right away
        if 'columns_list' in kwargs:
            self.columns_list = kwargs['columns_list']
        if 'time_sleep' in kwargs:
            self.time_sleep = int(kwargs['time_sleep'])
        if 'row_start' in kwargs:
            self.row_start = kwargs['row_start']
        if 'row_stop' in kwargs:
            self.row_stop = kwargs['row_stop']
        if 'key_only' in kwargs:
            self.key_only = bool(kwargs['key_only'])
        

    def rdd2hbase(self, data_rdd):
        self.hbase_conf['hbase.mapred.outputtable'] = self.hbase_table
        self.hbase_conf['mapreduce.outputformat.class'] = "org.apache.hadoop.hbase.mapreduce.TableOutputFormat"
        self.hbase_conf['mapreduce.job.output.key.class'] = "org.apache.hadoop.hbase.io.ImmutableBytesWritable"
        self.hbase_conf['mapreduce.job.output.value.class'] = "org.apache.hadoop.io.Writable"

        key_conv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
        value_conv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
        
        # 1) saveAsNewAPIHadoopDataset seems to fail sometime with errors like:
        # - Container [pid=7897,containerID=container_1459636669274_6150_01_000002] is running beyond physical memory limits. Current usage: 8.0 GB of 8 GB physical memory used; 41.7 GB of 16.8 GB virtual memory used. Killing container.
        # - anyway to set some parameters to fix this?
        print("[HbaseManager.rdd2hbase] Will save to HBase in {}s using conf: {}".format(self.time_sleep, self.hbase_conf))
        if self.time_sleep:
            time.sleep(self.time_sleep)

        data_rdd.saveAsNewAPIHadoopDataset(
            conf=self.hbase_conf,
            keyConverter=key_conv,
            valueConverter=value_conv)

    def read_hbase_table(self):
        self.hbase_conf['hbase.mapreduce.inputtable'] = self.hbase_table
        # https://hbase.apache.org/xref/org/apache/hadoop/hbase/mapreduce/TableInputFormat.html
        if self.columns_list:
            print("[HbaseManager.read_hbase_table] Will read only columns: {}".format(','.join(self.columns_list)))
            self.hbase_conf['hbase.mapreduce.scan.columns'] = ' '.join(self.columns_list)
        if self.row_start:
            print("[HbaseManager.read_hbase_table] Will start reading from row: {}".format(self.row_start))
            self.hbase_conf['hbase.mapreduce.scan.row.start'] = str(self.row_start)
        if self.row_stop:
            print("[HbaseManager.read_hbase_table] Will strop reading at row: {}".format(self.row_stop))
            self.hbase_conf['hbase.mapreduce.scan.row.stop'] = str(self.row_stop)
        # # how to integrate org.apache.hadoop.hbase.filter.KeyOnlyFilter to read only row keys?
        # # Actually does not seem possible for now, need to edit 'createScanFromConfiguration' in 'TableInputFormat'
        # # would need to instantiate filter and call setFilter, add methods like addColumn(s) called addFilter(s)?
        # if self.key_only:
        #     print("[HbaseManager.read_hbase_table] Will return only keys.")
        #     self.hbase_conf['org.apache.hadoop.hbase.filter'] = "org.apache.hadoop.hbase.filter.KeyOnlyFilter"
        #     self.hbase_conf['hbase.mapreduce.scan.setFilter'] = "org.apache.hadoop.hbase.filter.KeyOnlyFilter"

        
        key_conv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
        value_conv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
        
        hbase_rdd = self.sc.newAPIHadoopRDD("org.apache.hadoop.hbase.mapreduce.TableInputFormat",
                                             "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
                                             "org.apache.hadoop.hbase.client.Result",
                                             keyConverter=key_conv,
                                             valueConverter=value_conv,
                                             conf=self.hbase_conf)
        # do that outside if needed
        # hbase_rdd = hbase_rdd.flatMapValues(lambda v: v.split("\n")).mapValues(json.loads)
        return hbase_rdd
