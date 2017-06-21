import os
import sys
import time
import json
import struct
import numpy as np
from collections import OrderedDict
from ..memex_tools.sha1_tools import get_SHA1_from_file, get_SHA1_from_data

START_HDFS = '/user/'

class DictOutput():
    
    def __init__(self, mode='Old'):
        self.map = dict()
        if mode == 'CamelCase':
            self.fillDictCamelCase()
        else:
            self.fillDictOld()

    def fillDictCamelCase(self):
        self.map['images'] = "Images"
        self.map['query_sha1'] = "QuerySha1"
        self.map['similar_images'] = "SimilarImages"
        self.map['ads_cdr_ids'] = "AdsCDRIds"
        self.map['distance'] = "Distance"
        self.map['number'] = "Number"
        self.map['sha1'] = "Sha1"
        self.map['cached_image_urls'] = "CachedImageURLs"

    def fillDictOld(self):
        self.map['images'] = "images"
        self.map['query_sha1'] = "query_sha1"
        self.map['similar_images'] = "similar_images"
        self.map['ads_cdr_ids'] = "ads_cdr_ids"
        self.map['distance'] = "distance"
        self.map['number'] = "number"
        self.map['sha1'] = "sha1"
        self.map['cached_image_urls'] = "cached_image_urls"


class SearcherLOPQHBase():

    def __init__(self, global_conf_filename):
        self.global_conf_filename = global_conf_filename
        self.global_conf = json.load(open(global_conf_filename,'rt'))
        self.read_conf()
        self.init_lopq()
        self.init_hbaseindexer()
        self.init_feature_extractor()
        self.load_codes()
        self.url_field = 'info:s3_url'
        self.needed_output_columns = [self.url_field]

    def read_conf(self):
    	# these parameters may be overwritten by web call
        self.sim_limit = self.global_conf['SE_sim_limit']
        self.quota = self.sim_limit*10
        self.near_dup = self.global_conf['SE_near_dup']
        self.near_dup_th =  self.global_conf['SE_near_dup_th']
        self.ratio = self.global_conf['SE_ratio']
        self.topfeature = 0
        if "SE_topfeature" in self.global_conf:
            self.topfeature = int(self.global_conf['SE_topfeature'])
        self.out_dir = ""
        if "SE_outdir" in self.global_conf:
            self.out_dir = self.global_conf['SE_outdir']
            from ..memex_tools.image_dl import mkpath
            mkpath(self.out_dir)
        
    def init_lopq(self):
        """ Initialize LOPQ model and searcher from `global_conf` value.
        """
        field = 'SE_lopq'
        if field not in self.global_conf:
            raise ValueError("[Searcher: error] "+field+" is not defined in configuration file.")
        elif self.global_conf[field]=="lopq_pca":
            from lopq.model import LOPQModelPCA
            from lopq.search import LOPQSearcher
            import pickle
            # actually load pickle from disk
            lopq_model_path = self.global_conf['SE_lopqmodel']
            if lopq_model_path.startswith(START_HDFS):
                # deal with HDFS path
                from lopq.utils import copy_from_hdfs
                import shutil
                filename = copy_from_hdfs(lopq_model_path)
                lopq_model = pickle.load(filename)
                try:
                    shutil.rmtree(os.path.dirname(filename))
                except Exception as inst:
                    pass
            else:
                # local path in config
                lopq_model = pickle.load(open(lopq_model_path,"rb"))
            self.searcher_lopq = LOPQSearcher(lopq_model)
        else:
            raise ValueError("[SearcherLOPQHBase: error] unkown 'lopq' type {}.".format(self.global_conf[field]))

    def init_hbaseindexer(self):
        """ Initialize HbBase Indexer from `global_conf` value.
        """
        field = 'SE_indexer'
        if field not in self.global_conf:
            raise ValueError("[SearcherLOPQHBase: error] "+field+" is not defined in configuration file.")
        elif self.global_conf[field]=="hbase_indexer_minimal":
            from ..indexer.hbase_indexer_minimal import HBaseIndexerMinimal
            self.indexer = HBaseIndexerMinimal(self.global_conf_filename)
        else:
            raise ValueError("[SearcherLOPQHBase: error] unkown 'indexer' {}.".format(self.global_conf[field]))

    def init_feature_extractor(self):
        """ Initialize Feature Extractor from `global_conf` value.
        """
        field = 'SE_feature_extractor'
        if field not in self.global_conf:
            raise ValueError("[SearcherLOPQHBase: error] "+field+" is not defined in configuration file.")
        elif self.global_conf[field]=="sentibank_tensorflow":
            from ..feature_extractor.sentibank_tensorflow import SentiBankTensorflow
            self.feature_extractor = SentiBankTensorflow(self.global_conf_filename)
        else:
            raise ValueError("[SearcherLOPQHBase: error] unkown 'feature_extractor' {}.".format(self.global_conf[field]))

    def load_codes(self):
        codes_path = self.global_conf['SE_codes_path']
        if codes_path.startswith(START_HDFS):
            self.searcher_lopq.add_codes_from_hdfs(codes_path)
        else:
            self.searcher_lopq.add_codes_from_local(codes_path)

    def check_ratio(self):
        '''Check if we need to set the ratio based on topfeature.'''
        if self.topfeature > 0:
            self.ratio = self.topfeature*1.0/len(self.indexer.sha1_featid_mapping)
            print "[Searcher.check_ratio: log] Set ratio to {} as we want top {} images out of {} indexed.".format(self.ratio, self.topfeature, len(self.indexer.sha1_featid_mapping))


    def build_output(self, nb_query, corrupted, list_sha1_id, sim, sim_score, options_dict=dict()):
        
        dec = 0
        output = []
        do = DictOutput()
        
        for i in range(0,nb_query):    
            output.append(dict())
            if i in corrupted:
                output[i][do.map['similar_images']] = OrderedDict([[do.map['number'],0],\
                                                           [do.map['sha1'],[]],\
                                                           [do.map['cached_image_urls'],[]],\
                                                           [do.map['distance'],[]]])
                dec += 1
                continue
            ii = i - dec
            output[i][do.map['similar_images']] = OrderedDict([[do.map['number'],len(sim[ii])],\
                                                               [do.map['sha1'],[]],\
                                                               [do.map['cached_image_urls'],[]],\
                                                               [do.map['distance'],[]]])
            output[i][do.map['query_sha1']] = list_sha1_id[ii]
            ok_sims = []
            for jj,simj in enumerate(sim[ii]):
                found_columns = [c in simj[1] for c in self.needed_output_columns]
                if found_columns.count(True) == len(self.needed_output_columns):
                    output[i][do.map['similar_images']][do.map['sha1']].append(simj[0].strip())
                    output[i][do.map['similar_images']][do.map['cached_image_urls']].append(simj[1][self.url_field].strip())
                    ok_sims.append(jj)
            output[i][do.map['similar_images']][do.map['distance']]=[sim_score[ii][jj] for jj in ok_sims]
        outp = OrderedDict([[do.map['number'],nb_query],[do.map['images'],output]])
        return outp


    def build_error_output(self, nb_query, inst):
        errors = dict()
        errors['search'] = "[format_output ERROR] could not prepare output. Error was: {}".format(inst)
        output = []
        do = DictOutput()
        outp = OrderedDict([[do.map['number'],nb_query],[do.map['images'],output],['errors',errors]])
        return outp


    def format_output(self, sim, sim_score, nb_query, corrupted, list_sha1_id, options_dict=dict()):
        # read lopq similarity results and get 'cached_image_urls', 'ads_cdr_ids'
        # and filter out if near_dup is activated
        print "[SearcherLOPQHBase.format_output: log] options are: {}".format(options_dict)
        start_build_output = time.time()
        outp = self.build_output(nb_query, corrupted, list_sha1_id, sim, sim_score, options_dict)
        print "[SearcherLOPQHBase.format_output: log] build_output took: {}".format(time.time() - start_build_output)
        #print "[Searcher.format_output: log] output {}".format(output)
        return outp

    def search_image_filelist(self, image_list, options_dict=dict()):
        # initilization
        search_id = str(time.time())
        # get sha1s and feats for each URL in image_list
        list_sha1_id = []
        feats = []
        tmp_sha1s_feats = self.feature_extractor.compute_sha1_features_fromURLs_nodiskout(image_list)
        for sample in tmp_sha1s_feats:
            list_sha1_id.append(sample[0])
            feats.append(sample[1][0])
        return self.search_from_feats(feats, list_sha1_id, options_dict)


    def search_from_feats(feats, list_sha1_id, options_dict=dict()):
        # format of results is a list of namedtuples as: namedtuple('Result', ['id', 'code', 'dist'])
        # this does not support batch query
        sim = []
        sim_score = []
        # check what is the near duplicate config
        filter_near_dup = False
        if (self.near_dup and "near_dup" not in options_dict) or ("near_dup" in options_dict and options_dict["near_dup"]):
            filter_near_dup = True
            if "near_dup_th" in options_dict:
                near_dup_th = options_dict["near_dup_th"]
            else:
                near_dup_th = self.near_dup_th
        # query for each feature
        for i in range(len(list_sha1_id)):
            results, visited = self.searcher_lopq.search(self.searcher_lopq.model.apply_PCA(feats[i]), quota=self.quota, limit=self.sim_limit, with_dists=True)
            # parse output
            tmp_sim = []
            tmp_sim_score = []
            for res in results:
                if (filter_near_dup and res.dist<=near_dup_th) or not filter_near_dup:
                    tmp_sim.append(res.id)
                    tmp_sim_score.append(res.dist)
            # add maintained results
            # TODO we need to get s3 urls and add as second value of sim tuple as dict with key 'info:s3_url'
            # Use HBaseIndexerMinimal for that self.needed_output_columns
            rows = self.hbase_indexer_minimal.get_columns_from_sha1_rows(tmp_sim, self.needed_output_columns)
            print "[SearcherLOPQHBase.search_from_feats: log] rows: {}".format(rows)
            # tmp_sim_wurl = []
            # for i,row in enumerate(rows):
            #     if tmp_sim[i] != row[0]:
            #         print "Did we loose ordering {} vs. {}?".format(tmp_sim[i], row[0])
            #     url_dict = dict()
            #     url_dict[self.url_field] = row[1][self.url_field]
            #     tmp_sim_wurl.append((row[0], url_dict))
            # sim.append(tmp_sim_wurl)    
            sim.append(rows) # that would work if needed columns contains only the s3_url?
            sim_score.append(tmp_sim_score)
        # format output
        return self.format_output(sim, sim_score, len(list_sha1_id), [], list_sha1_id, options_dict)

