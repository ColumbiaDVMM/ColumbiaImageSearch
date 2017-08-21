import time
import json
import datetime
from kafka import KafkaProducer
from elasticsearch import Elasticsearch
from ..common.conf_reader import ConfReader

default_prefix = "ES_"

class CDRIngester(ConfReader):

  def __init__(self, global_conf_filename, prefix=default_prefix):
    super(CDRIngester, self).__init__(global_conf_filename, prefix)
    self.batch_size = 10
    # Producer related attributes
    self.producer = None
    self.out_servers = None
    self.out_topic = None
    # ES related attributes
    self.els_index = None
    self.els_doc_type = None
    self.els_instance = None
    self.els_user = None
    self.els_pass = None
    # Initialize
    self.initialize_source()
    self.initialize_output()
    # Technically should be read from '_meta' of ES instance
    # GET memex-domains/_mapping/domain
    self.obj_stored_prefix = self.get_required_param('obj_stored_prefix')

  def set_pp(self):
    self.pp = "CDRIngester"

  def initialize_source(self):
    """ Use information contained in `self.global_conf` to initialize ElasticSearch source
    """
    self.els_index = self.get_required_param('es_index')
    self.els_doc_type = self.get_required_param('es_doc_type')
    self.els_instance = self.get_required_param('es_instance')
    self.els_user = self.get_required_param('es_user')
    self.els_pass = self.get_required_param('es_pass')
    init_source_msg = "[{}: log] CDRIngester source initialized with values:\n"
    init_source_msg += "- index: {};\n- instance: {};\n- user: {};\n- pass: {}."
    print init_source_msg.format(self.pp, self.els_index, self.els_instance, self.els_user, self.els_pass)

  def initialize_output(self):
    """ Use information contained in `self.global_conf` to initialize Kafka output
    """
    self.out_servers = self.get_param('out_servers')
    self.out_topic = self.get_required_param('out_topic')
    if self.out_servers:
      self.producer = KafkaProducer(bootstrap_servers=self.out_servers)
    else:
      self.producer = KafkaProducer()
    init_out_msg = "[{}: log] CDRIngester output initialized with values:\n- out_servers: {};\n- out_topic: {}."
    print init_out_msg.format(self.pp, self.out_servers, self.out_topic)

  def get_prefix(self):
    return self.obj_stored_prefix

  def get_batch(self, team="HG"):
    """ Should return a batch of CDR document posted by requested crawling team
    """
    query = "{\"query\": {\"filtered\": {\"query\": {\"match\": {\"team\": \""+team+"\"}}}}, \"size\": "+str(self.batch_size)+" }"

    es = Elasticsearch('https://'+self.els_user+':'+self.els_pass+'@'+self.els_instance)
    cdr_infos = []

    while True:
      try:
        if self.verbose > 0:
          print "[{}.get_batch: log] ElasticSearch query at:".format(self.pp, str(datetime.datetime.now()))
        response = es.search(index=self.els_index, doc_type=self.els_doc_type, body=query, search_type="scan", scroll="5m")
        break
      except Exception as inst:
        if self.verbose > 0:
          log_msg = "[{}.get_batch: log] ElasticSearch error when requesting: {} at {}"
          print log_msg.format(self.pp, query, str(datetime.datetime.now()))
          print "[{}.get_batch: log] {}".format(self.pp, inst)
        # Could be just a timeout that will be solved next time we query...
        # Give ES a little rest first.
          time.sleep(10)

    if self.verbose > 0:
      "[CDRIngester: log] Got "+str(response['hits']['total'])+" results in "+str(response['took'])+"ms."

    scrollId = response['_scroll_id']
    response = es.scroll(scroll_id=scrollId, scroll="5m")
    while len(response['hits']['hits']) > 0 and len(cdr_infos) < self.batch_size:
      print "Getting "+str(len(response['hits']['hits']))+" docs."
      cdr_infos.extend(response['hits']['hits'])
      scrollId = response['_scroll_id']
      # Scrolling can fail quite often.
      while True:
        try:
          if self.verbose > 0:
            print "[CDRIngester.get_batch: log] ElasticSearch query at:",str(datetime.datetime.now())
          response = es.scroll(scroll_id=scrollId, scroll="5m")
          break
        except Exception as inst:
          if self.verbose > 0:
            log_msg = "[{}.get_batch: log] ElasticSearch error when requesting: {} at {}"
            print log_msg.format(self.pp, query, str(datetime.datetime.now()))
            print "[{}.get_batch: log] {}".format(self.pp, inst)
          # Could be just a timeout that will be solved next time we query...
          # Give ES a little rest first.
          time.sleep(10)

    # We could trim to get exactly `self.batch_size` samples
    if len(cdr_infos) > self.batch_size:
      return cdr_infos

  def push_batch(self):
    cdr_infos = self.get_batch()
    print "[{}.push_batch: info] Got {} documents.".format(self.pp, len(cdr_infos))
    for doc in cdr_infos:
      self.producer.send(self.out_topic, json.dumps(doc['_source']).encode('utf-8'))
    print "[{}.push_batch: info] Pushed {} documents to topic {}.".format(self.pp, len(cdr_infos), self.out_topic)