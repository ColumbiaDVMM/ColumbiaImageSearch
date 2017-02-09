__author__ = 'amandeep'

"""
USE THESE FOR HBASE
self.hbase_host = 'zk04.xdata.data-tactics-corp.com:2181'
self.hbase_table = 'test_ht_aman'
"""

"""
EXECUTE AS
spark-submit  --master local[*]     --executor-memory=4g     --driver-memory=4g    \
 --jars jars/elasticsearch-hadoop-2.2.0-m1.jar,jars/spark-examples_2.10-2.0.0-SNAPSHOT.jar,jars/random-0.0.1-SNAPSHOT-shaded.jar    \
   es2hbase.py     -hostname  els.istresearch.com -port 19200 \
   -username memex -password <es_password> -indexname <esindex> \
    -doctype <esdoctype> -hbasehostname <hbasehostname> \
    -hbasetablename <hbase_tablename>
"""
import argparse
from pyspark import SparkContext, SparkConf
from hbase_manager import HbaseManager


class ES(object):
    def __init__(self, spark_context, spark_conf, index, doc, es_hostname, es_port, es_username, es_password):
        self.name = "ES2HBase"
        self.sc = spark_context
        self.conf = spark_conf
        self.es_conf = {}
        self.es_conf['es.resource'] = index + "/" + doc
        self.es_conf['es.nodes'] = es_hostname + ":" + str(es_port)
        self.es_conf['es.index.auto.create'] = "no"
        self.es_conf['es.net.http.auth.user'] = es_username
        self.es_conf['es.net.http.auth.pass'] = es_password
        self.es_conf['es.net.ssl'] = "true"
        self.es_conf['es.nodes.discovery'] = "false"
        self.es_conf['es.http.timeout'] = "1m"
        self.es_conf['es.http.retries'] = "1"
        self.es_conf['es.nodes.client.only'] = "false"
        self.es_conf['es.nodes.wan.only'] = "true"

    def set_output_json(self):
        # Does this give the "_timestamp" field?
        self.es_conf['es.output.json'] = "true" 

    def set_read_metadata(self):
        self.es_conf['es.read.metadata'] = "true" 

    def es2rdd(self, query):
        self.es_conf['es.query'] = query
        print self.es_conf
        es_rdd = self.sc.newAPIHadoopRDD(inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
                                         keyClass="org.apache.hadoop.io.NullWritable",
                                         valueClass="org.apache.hadoop.io.Text",
                                         conf=self.es_conf)
        # es_rdd.map(lambda x: json.dumps(ES2HBase.printable_doc(x))).saveAsTextFile('/tmp/cdr_v2_ads')
        return es_rdd


# if __name__ == '__main__':
#     argp = argparse.ArgumentParser()
#     argp.add_argument("-hostname", help="Elastic Search Server hostname, defaults to 'localhost'", default="localhost")
#     argp.add_argument("-port", type=int, help="Elastic Search Server port,defaults to 9200", default=9200)
#     argp.add_argument("-username", help="username for ElasticSearch", default="")
#     argp.add_argument("-password", help="password for ElasticSearch", default="")
#     argp.add_argument("-indexname", help="ElasticSearch index name")
#     argp.add_argument("-doctype", help="ElasticSearch doc type")
#     argp.add_argument("-hbasehostname", help="ElasticSearch doc type")
#     argp.add_argument("-hbasetablename", help="ElasticSearch doc type")
#
#     # ads query
#     query = "{\"query\": {\"filtered\": {\"filter\": {\"exists\": {\"field\": \"extractions\"}},\"query\": " \
#             "{\"match_all\": {}}}}}"
#     # images query
#     """query = "{\"query\": {\"filtered\": {\"filter\": {\"exists\": {\"field\": \"obj_parent\"}},\"query\": {\"match_all\"" \
#             ": {}}}},\"size\": 4000}" """
#
#     arguments = argp.parse_args()
#     es2hbase = ES(arguments.indexname, arguments.doctype, arguments.hostname, arguments.port, arguments.username,
#                         arguments.password)
#     es_rdd = es2hbase.es2rdd(query)
#     hm = HbaseManager(arguments.hbasehostname, arguments.hbasetablename)
#     hm.rdd2hbase(es_rdd)
#     es2hbase.sc.stop()
#     print "Done!"
