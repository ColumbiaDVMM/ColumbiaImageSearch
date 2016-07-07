import os
import sys
import time
import json
import struct
import MySQLdb
import numpy as np
from collections import OrderedDict 
from ..memex_tools.sha1_tools import get_SHA1_from_file, get_SHA1_from_data

class Searcher():

    def __init__(self,global_conf_filename):
        self.global_conf_filename = global_conf_filename
        self.global_conf = json.load(open(global_conf_filename,'rt'))
        self.read_conf()
        self.init_indexer()
        self.init_ingester() # just for expand metada

    def read_conf(self):
    	# these parameters may be overwritten by web call
        self.features_dim = self.global_conf['FE_features_dim']
        self.sim_limit = self.global_conf['SE_sim_limit']
        self.near_dup = self.global_conf['SE_near_dup']
        self.near_dup_th =  self.global_conf['SE_near_dup_th']
        self.get_dup = self.global_conf['SE_get_dup']
        self.ratio = self.global_conf['SE_ratio']

    def init_ingester(self):
        """ Initialize `SE_ingester` from `global_conf['ingester']` value.

        Currently supported ingester types are:
        - mysql_ingester
        - cdr_ingester
        """
        field = 'SE_ingester'
        if field not in self.global_conf:
            raise ValueError("[Searcher: error] "+field+" is not defined in configuration file.")
        if self.global_conf[field]=="mysql_ingester":
            from ..ingester.mysql_ingester import MySQLIngester
            self.ingester = MySQLIngester(self.global_conf_filename)
        elif self.global_conf[field]=="cdr_ingester":
            from ..ingester.cdr_ingester import CDRIngester
            self.ingester = CDRIngester(self.global_conf_filename)
        else:
            raise ValueError("[Searcher: error] unkown 'ingester' {}.".format(self.global_conf[field]))

    def init_indexer(self):
        """ Initialize `indexer` from `global_conf['SE_indexer']` value.

        Currently supported indexer types are:
        - local_indexer
        - hbase_indexer
        """
        field = 'SE_indexer'
        if field not in self.global_conf:
            raise ValueError("[Searcher: error] "+field+" is not defined in configuration file.")
        if self.global_conf[field]=="local_indexer":
            from ..indexer.local_indexer import LocalIndexer
            self.indexer = LocalIndexer(self.global_conf_filename)
        elif self.global_conf[field]=="hbase_indexer":
            from ..indexer.hbase_indexer import HBaseIndexer
            self.indexer = HBaseIndexer(self.global_conf_filename)
        else:
            raise ValueError("[Searcher: error] unkown 'indexer' {}.".format(self.global_conf[field]))

    def filter_near_dup(self,nums):
        # nums is a list of ids then distances
        # onum is the number of similar images
        onum = len(nums)/2
        temp_nums=[]
        #print "[Searcher.filter_near_dup: log] nums {}".format(nums)
        for one_num in range(0,onum):
            # maintain only near duplicates, i.e. distance less than self.near_dup_th
            if float(nums[onum+one_num])>self.near_dup_th:
                return temp_nums
            # insert id at its right place
            temp_nums.insert(one_num,nums[one_num])
            # insert corresponding distance at the end
            temp_nums.insert(len(temp_nums),nums[onum+one_num])
        #print "[Searcher.filter_near_dup: log] temp_nums {}".format(temp_nums)
        return temp_nums

    def get_dup_infos(self,sim,sim_score):
        new_sim = []
        new_sim_score = []

        for i in range(0,len(sim)):    
            new_sim.append([])
            new_sim_score.append([])
            tmpresult = self.indexer.get_url_infos(sim[i])
            #print "[Searcher.get_dup_infos: log] tmpresult {}".format(tmpresult)
            #print len(tmpresult)
            p = 0
            for k in tmpresult:
                if sim[i][p][4]!=k[1]:
                    p = p+1
                if not self.indexer.demo:
                    new_sim[i].append((sim[i][p][0],sim[i][p][1],sim[i][p][2],sim[i][p][3],k[0],sim[i][p][5]))
                else:
                    new_sim[i].append((k[2],k[3],k[4],k[5],k[0],sim[i][p][5]))
                new_sim_score[i].append(sim_score[i][p])
                    
        return new_sim,new_sim_score


    def expand_metadata(self,sim):
        for i in range(0,len(sim)):    
            if not sim[i]: # empty
                continue
            #print "[Searcher.expand_metadata: log] sim[i] before expansion {}".format(sim[i])
            sim[i] = self.ingester.expand_metadata(sim[i])
            #print "[Searcher.expand_metadata: log] sim[i] after expansion {}".format(sim[i])
        return sim


    def read_sim(self,simname,nb_query):
    	# intialization
        sim = []
        sim_score = []
        
        # read similar images
        count = 0
        f = open(simname);
        for line in f:
            #sim_index.append([])
            nums = line.replace(' \n','').split(' ')
            if self.near_dup: #filter near duplicate here
                nums=self.filter_near_dup(nums)
            #print nums
            onum = len(nums)/2
            n = min(self.sim_limit,onum)
            #print n
            if n==0: # no returned images, e.g. no near duplicate
                sim.append(())
                sim_score.append([])
                continue
            sim.append(self.indexer.get_sim_infos(nums[0:n]))
            sim_score.append(nums[onum:onum+n])
            count = count + 1
            if count == nb_query:
                break
        f.close()
        return sim,sim_score

    def format_output(self, simname, nb_query, outputname):
    	# read hashing similarity results
        sim,sim_score = self.read_sim(simname,nb_query)
        
        # get_duplicates if needed
        if self.get_dup:
            #print "[Searcher.format_output: log] sim before get_dup_infos {}".format(sim)
            sim,sim_score = self.get_dup_infos(sim,sim_score)
            #print "[Searcher.format_output: log] sim after get_dup_infos {}".format(sim)

        # expand metadata
        sim = self.expand_metadata(sim)
        
        # build final output
        output = []
        for i in range(0,nb_query):    
            output.append(dict())
            output[i]['similar_images']= OrderedDict([['number',len(sim[i])],['image_urls',[]],['cached_image_urls',[]],['page_urls',[]],['ht_ads_id',[]],['ht_images_id',[]],['sha1',[]],['distance',[]]])
            for simj in sim[i]:
                output[i]['similar_images']['image_urls'].append(simj[0])
                output[i]['similar_images']['cached_image_urls'].append(simj[1])
                output[i]['similar_images']['page_urls'].append(simj[2])
                output[i]['similar_images']['ht_ads_id'].append(simj[3])
                output[i]['similar_images']['ht_images_id'].append(simj[4])
                output[i]['similar_images']['sha1'].append(simj[5])
            output[i]['similar_images']['distance']=sim_score[i]
        outp = OrderedDict([['number',nb_query],['images',output]])
        json.dump(outp, open(outputname,'w'),indent=4, sort_keys=False)    

    def search_one_imagepath(self,image_path):
    	# initilization
        search_id = str(time.time())
        all_img_filenames = [image_path]
        return self.search_from_image_filenames(all_img_filenames,search_id)
        
    def search_image_list(self,image_list):
        # initilization
        search_id = str(time.time())
        i = 0
        # read all images
        dl_images = []
        batch = []
        all_img_filenames = []
        for line in open(image_list):
            image_line = line.replace('\n','')
            if len(image_line)>2:
                # Check if image or web address
                if image_line[0:4]=="http":
                    # Push image to be downloaded image
                    batch.append((i,image_line,None))
                    dl_images.append(i)
                all_img_filenames.append(image_line)
                i+=1
        # download the images we need
        if batch:
            readable_images = self.indexer.image_downloader.download_images(batch,search_id)
            for i,img_tup in enumerate(readable_images):
                print "[Searcher.search_image_list: log] {} readable image tuple {}.".format(i,img_tup)
                dl_pos = dl_images.index(img_tup[0])
                all_img_filenames[dl_images[dl_pos]]=img_tup[-1]
        return self.search_from_image_filenames(all_img_filenames,search_id)

    def search_from_image_filenames(self,all_img_filenames,search_id):
        # compute all sha1s
        corrupted = []
        list_sha1_id = []
        valid_images = []
        for i,image_name in enumerate(all_img_filenames):
            print i,image_name
            if image_name[0:4]!="http":
                sha1 = get_SHA1_from_file(image_name)
                print i,image_name,sha1
                list_sha1_id.append(sha1)
                valid_images.append((i,sha1,image_name))
            else: # we did not manage to download image
                corrupted.append(i)
        # get indexed images
        list_ids_sha1_found = self.indexer.get_ids_from_sha1s(list_sha1_id)
        list_ids_found = [x[0] for x in list_ids_sha1_found]
        list_sha1_found = [x[1] for x in list_ids_sha1_found]
        # get there features
        feats,ok_ids = self.indexer.hasher.get_precomp_feats(list_ids_found)
        if len(ok_ids)!=len(list_ids_found):
            raise ValueError("[Searcher.search_from_image_filenames: error] We did not get enough precomputed features ({}) from list of {} images.".format(len(ok_ids),len(list_ids_found)))
        # compute new images features
        not_indexed_sha1 = set(list_sha1_id)-set(list_sha1_found)
        #res = self.indexer.get_precomp_from_sha1(list_ids_sha1_found)
        new_files = []
        all_valid_images = []
        precomp_img_filenames=[]
        for i,sha1,image_name in valid_images:
            if sha1 in list_sha1_found: # image is indexed
                precomp_img_filenames.append(image_name)
            else:
                new_files.append(image_name[-1])
            all_valid_images.append(all_img_filenames[i])
        #print "[Searcher.search_from_image_filenames: log] new_files {}".format(new_files)
        features_filename,ins_num = self.indexer.feature_extractor.compute_features(new_files,search_id)
        if ins_num!=len(new_files):
            raise ValueError("[Searcher.search_from_image_filenames: error] We did not get enough features ({}) from list of {} images.".format(ins_num,len(new_files)))
        # merge feats with features_filename
        final_featuresfile = search_id+'.dat'
        read_dim = self.features_dim*4
        read_type = np.float32
        #print "[Searcher.search_from_image_filenames: log] feats {}".format(feats)
        with open(features_filename,'rb') as new_feats, open(final_featuresfile,'wb') as out:
            for image_name in all_valid_images:
                if image_name in precomp_img_filenames:
                    # select precomputed 
                    precomp_pos = precomp_img_filenames.index(image_name)
                    tmp_feat = feats[precomp_pos][:]
                else:
                    # read from new feats
                    tmp_feat = np.frombuffer(new_feats.read(read_dim),dtype=read_type)
                out.write(tmp_feat)
        # query with merged features_filename
        simname = self.indexer.hasher.get_similar_images_from_featuresfile(final_featuresfile,self.ratio)
        outputname = simname[:-4]+".json"
        self.format_output(simname, len(all_valid_images), outputname)
        return outputname
