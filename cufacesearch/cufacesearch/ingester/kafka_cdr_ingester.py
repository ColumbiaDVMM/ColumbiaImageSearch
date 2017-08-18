import time
import datetime
from elasticsearch import Elasticsearch
from ..common.conf_reader import ConfReader

default_prefix = "ES_"

class CDRIngester(ConfReader):

  def __init__(self, global_conf_filename, prefix=default_prefix):
    super(CDRIngester, self).__init__(global_conf_filename, prefix)
    self.batch_size = 10
    self.initialize_source()

  def set_pp(self):
    self.pp = "CDRIngester"

  def initialize_source(self):
    """ Use information contained in `self.global_conf` to initialize ElasticSearch
    """
    self.els_index = self.get_required_param('es_index')
    self.els_doc_type = self.get_required_param('es_doc_type')
    self.els_instance = self.get_required_param('es_instance')
    self.els_user = self.get_required_param('es_user')
    self.els_pass = self.get_required_param('es_pass')
    init_msg = "[CDRIngester: log] CDRIngester initialized with values:\n- index: {};\n- instance: {};\n- user: {};\n- pass: {}."
    print init_msg.format(self.els_index, self.els_instance, self.els_user, self.els_pass)

  def get_prefix(self):
    # GET memex-domains/_mapping/domain
    self.prefix = self.get_required_param('obj_stored_prefix')

  def get_batch(self, team="HG"):
    """ Should return a batch of CDR document posted by requested crawling team
    """
    query = "{\"query\": {\"filtered\": {\"query\": {\"match\": {\"team\": \""+team+"\"}}}}, \"size\": "+str(self.batch_size)+" }"

    es = Elasticsearch('https://'+self.els_user+':'+self.els_pass+'@'+self.els_instance)
    cdr_infos = []

    while True:
      try:
        if self.verbose>0:
          print "[CDRIngester.get_batch: log] ElasticSearch query at:",str(datetime.datetime.now())
        response = es.search(index=self.els_index, doc_type=self.els_doc_type, body=query, search_type="scan", scroll="5m")
        break
      except Exception as inst:
        if self.verbose>0:
          print "[CDRIngester.get_batch: log] ElasticSearch error when requesting:",query,"... at:",str(datetime.datetime.now())
          print "[CDRIngester.get_batch: log] {}".format(inst)
        # Could be just a timeout that will be solved next time we query...
        # Give ES a little rest first.
          time.sleep(10)

    if self.verbose>0:
      "[CDRIngester: log] Got "+str(response['hits']['total'])+" results in "+str(response['took'])+"ms."

    scrollId = response['_scroll_id']
    response = es.scroll(scroll_id=scrollId, scroll="5m")
    while len(response['hits']['hits'])>0 and len(cdr_infos)<self.batch_size:
      print "Getting "+str(len(response['hits']['hits']))+" docs."
      cdr_infos.extend(response['hits']['hits'])
      scrollId = response['_scroll_id']
      # Scrolling can fail quite often.
      while True:
        try:
          if self.verbose>0:
            print "[CDRIngester.get_batch: log] ElasticSearch query at:",str(datetime.datetime.now())
          response = es.scroll(scroll_id=scrollId, scroll="5m")
          break
        except Exception as inst:
          if self.verbose>0:
            print "[CDRIngester.get_batch: log] ElasticSearch error when requesting:",query,"... at:",str(datetime.datetime.now())
            print "[CDRIngester.get_batch: log] {}".format(inst)
          # Could be just a timeout that will be solved next time we query...
          # Give ES a little rest first.
          time.sleep(10)

    # Trim to get exactly `self.batch_size` samples?
    if len(cdr_infos)>self.batch_size:
      return cdr_infos

    # TODO: Have a producer to push to a Kafka queue