import os
import sys
import time
import datetime
from elasticsearch import Elasticsearch
from generic_ingester import GenericIngester

class CDRIngester(GenericIngester):

    def initialize_source(self):
        """ Use information contained in `self.global_conf` to initialize MySQL config
        """
        self.els_index = self.global_conf['ist_els_index']
        self.els_doc_type = self.global_conf['ist_els_doc_type']
        self.els_instance = self.global_conf['ist_els_instance']
        self.els_user = self.global_conf['ist_els_user']
        self.els_pass = self.global_conf['ist_els_pass']
        self.fields_cdr = ["obj_stored_url", "obj_parent", "obj_original_url", "timestamp", "crawl_data.image_id", "crawl_data.memex_ht_id"]
        print "[CDRIngester: log] CDRIngester initialized with values:\
         \n- index: {};\n- instance: {};\n- user: {};\n- pass: {}.".format(self.els_index,self.els_instance,self.els_user,self.els_pass)

    def get_batch(self):
        """ Should return a list of (id,url,other_data) querying for `batch_size` samples from `self.source` from `start`
        """
        if self.start is None or self.batch_size is None:
            print "[CDRIngester.get_batch: error] Parameters 'start' or 'batch_size' not set."
            return None
        query = "{\"fields\": [\""+"\", \"".join(self.fields_cdr)+"\"], \"query\":\
                 {\"filtered\": {\"query\": {\"match\": {\"content_type\": \"image/jpeg\"}},\
                  \"filter\": {\"range\" : {\"_timestamp\" : {\"gte\" : "+str(self.start)+"}}}}},\
                  \"sort\": [ { \"_timestamp\": { \"order\": \"asc\" }  }], \"size\": "+str(self.batch_size)+" }"
        cdr_infos=[]
        scroll_ids=[]
        es = Elasticsearch('https://'+self.els_user+':'+self.els_pass+'@'+self.els_instance)
        while True:
            try:
                if self.verbose>0:
                    print "[CDRIngester.get_batch: log] ElasticSearch query at:",str(datetime.datetime.now())
                response = es.search(index=self.els_index,doc_type=self.els_doc_type,body=query, search_type="scan", scroll="5m")
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
        # Trim to get exactly `self.batch_size` samples
        if len(cdr_infos)>self.batch_size:
            cdr_infos = cdr_infos[:self.batch_size]
        elif len(cdr_infos)<self.batch_size and self.fail_less_than_batch:
            print "[CDRIngester.get_batch: error] Not enough images ("+str(len(re))+")"
            return None
        return [(cdr_img["_id"],cdr_img["fields"]["obj_stored_url"][0],cdr_img) for cdr_img in cdr_infos]
        
