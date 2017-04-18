import os
import sys
import time
import json
import struct
import numpy as np
from collections import OrderedDict
from ..memex_tools.sha1_tools import get_SHA1_from_file, get_SHA1_from_data
#from ..hasher import _hasher_obj_py as hop


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
        self.map['cdr_ids'] = "CDRIds"
        self.map['distance'] = "Distance"
        self.map['number'] = "Number"
        self.map['sha1'] = "Sha1"
        self.map['cached_image_urls'] = "CachedImageURLs"

    def fillDictOld(self):
        self.map['images'] = "images"
        self.map['query_sha1'] = "query_sha1"
        self.map['similar_images'] = "similar_images"
        self.map['ads_cdr_ids'] = "ads_cdr_ids"
        self.map['cdr_ids'] = "cdr_ids"
        self.map['distance'] = "distance"
        self.map['number'] = "number"
        self.map['sha1'] = "sha1"
        self.map['cached_image_urls'] = "cached_image_urls"


class Searcher():

    def __init__(self, global_conf_filename):
        self.global_conf_filename = global_conf_filename
        self.global_conf = json.load(open(global_conf_filename,'rt'))
        self.read_conf()
        self.init_indexer()
        self.init_ingester() # just for expand metada
        self.needed_output_columns = ['info:s3_url']

    def read_conf(self):
    	# these parameters may be overwritten by web call
        self.features_dim = self.global_conf['FE_features_dim']
        self.sim_limit = self.global_conf['SE_sim_limit']
        self.near_dup = self.global_conf['SE_near_dup']
        self.near_dup_th =  self.global_conf['SE_near_dup_th']
        self.get_dup = self.global_conf['SE_get_dup']
        self.ratio = self.global_conf['SE_ratio']
        self.topfeature = 0
        if "SE_topfeature" in self.global_conf:
            self.topfeature = int(self.global_conf['SE_topfeature'])

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
        elif self.global_conf[field]=="hbase_ingester":
            from ..ingester.hbase_ingester import HBaseIngester
            self.ingester = HBaseIngester(self.global_conf_filename)
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

    def compute_features_listimgfiles(self, listimgfiles, search_id):
        # we could switch between GPU and CPU based on number of images.
        # TEMPORARY USE ONLY CPU as GPU card will be changed soon
        features_filename, ins_num = self.indexer.feature_extractor.compute_features(listimgfiles, search_id, 'CPU')
        if ins_num != len(listimgfiles):
            print_err = "[Searcher.compute_features_listimgfiles: error] We did not get enough features ({}) from list of {} images."
            raise ValueError(print_err.format(ins_num,len(listimgfiles)))
        return features_filename
        

    def filter_near_dup(self, nums, near_dup_th=None):
        # nums is a list of ids then distances
        # onum is the number of similar images
        if not near_dup_th:
            near_dup_th = self.near_dup_th
        #print("[filter_near_dup] near_dup_th: {}".format(near_dup_th))
        onum = len(nums)/2
        temp_nums = []
        #print "[Searcher.filter_near_dup: log] nums {}".format(nums)
        for one_num in range(0,onum):
            # maintain only near duplicates, i.e. distance less than self.near_dup_th
            if float(nums[onum+one_num])>near_dup_th:
                return temp_nums
            # insert id at its right place
            temp_nums.insert(one_num,nums[one_num])
            # insert corresponding distance at the end
            temp_nums.insert(len(temp_nums),nums[onum+one_num])
        #print "[Searcher.filter_near_dup: log] temp_nums {}".format(temp_nums)
        return temp_nums


    def read_sim(self, simname, nb_query, options_dict=dict()):
    	# intialization
        sim = []
        sim_score = []
        
        # read similar images
        count = 0
        f = open(simname);
        for line in f:
            #sim_index.append([])
            nums = line.replace(' \n','').split(' ')
            #filter near duplicate here
            if (self.near_dup and "near_dup" not in options_dict) or ("near_dup" in options_dict and options_dict["near_dup"]):
                if "near_dup_th" in options_dict:
                    near_dup_th = options_dict["near_dup_th"]
                else:
                    near_dup_th = self.near_dup_th
                nums = self.filter_near_dup(nums, near_dup_th)
            #print nums
            onum = len(nums)/2
            n = min(self.sim_limit,onum)
            #print n
            if n==0: # no returned images, e.g. no near duplicate
                sim.append(())
                sim_score.append([])
                continue
            # get the needed_output_columns and sha1s
            sim_infos = self.indexer.get_sim_infos(nums[0:n], columns=self.needed_output_columns)
            # beware, need to make sure sim and sim_score are still aligned
            print("[read_sim] got {} sim_infos from {} samples".format(len(sim_infos), n))
            sim.append(sim_infos)
            sim_score.append(nums[onum:onum+n])
            count = count + 1
            if count == nb_query:
                break
        f.close()
        return sim,sim_score


    def read_sim_nodiskout(self, out_res, nb_query, options_dict=dict()):
        # intialization
        sim = []
        sim_score = []
        
        # read similar images
        count = 0
        for one_res in out_res:
            print("[read_sim_nodiskout: log] type(one_res): {}".format(type(one_res)))
            print("[read_sim_nodiskout: log] one_res: {}".format(one_res))
            # one_res would ned to be a numpy array
            nums = [one_res[1::2], one_res[::2]]
            #filter near duplicate here
            if (self.near_dup and "near_dup" not in options_dict) or ("near_dup" in options_dict and options_dict["near_dup"]):
                if "near_dup_th" in options_dict:
                    near_dup_th = options_dict["near_dup_th"]
                else:
                    near_dup_th = self.near_dup_th
                nums = self.filter_near_dup(nums, near_dup_th)
            #print nums
            onum = len(nums)/2
            n = min(self.sim_limit,onum)
            #print n
            if n==0: # no returned images, e.g. no near duplicate
                sim.append(())
                sim_score.append([])
                continue
            # just get the sha1 at this point
            sim_infos = self.indexer.get_sim_infos(nums[0:n], columns=self.needed_output_columns)
            # beware, need to make sure sim and sim_score are still aligned
            print("[read_sim] got {} sim_infos from {} samples".format(len(sim_infos), n))
            sim.append(sim_infos)
            sim_score.append(nums[onum:onum+n])
            count = count + 1
            if count == nb_query:
                break
        f.close()
        return sim,sim_score


    def read_sim_sha1(self, simname, nb_query, options_dict=dict()):
        # intialization
        sim = []
        sim_score = []
        
        # read similar images
        count = 0
        f = open(simname);
        for line in f:
            #sim_index.append([])
            nums = line.replace('\n','').split(' ')
            #filter near duplicate here
            if (self.near_dup and "near_dup" not in options_dict) or ("near_dup" in options_dict and options_dict["near_dup"]):
                if "near_dup_th" in options_dict:
                    near_dup_th = options_dict["near_dup_th"]
                else:
                    near_dup_th = self.near_dup_th
                nums = self.filter_near_dup(nums, near_dup_th)
            #print nums
            onum = len(nums)/2
            n = min(self.sim_limit,onum)
            #print n
            if n==0: # no returned images, e.g. no near duplicate
                sim.append(())
                sim_score.append([])
                continue
            # just get the sha1 at this point
            # beware, need to make sure sim and sim_score are still aligned
            #sim.append(self.indexer.get_full_sha1_rows(nums[0:n]))
            sim.append(self.indexer.get_columns_from_sha1_rows(nums[0:n], columns=self.needed_output_columns))
            sim_score.append(nums[onum:onum+n])
            count = count + 1
            if count == nb_query:
                break
        f.close()
        return sim,sim_score


    def build_output(self, nb_query, corrupted, list_sha1_id, sim, sim_score, options_dict=dict()):
        # method that takes as input: nb_query, corrupted, list_sha1_id, sim, sim_score, options_dict=dict()
        #print "[Searcher.format_output: log] sim: {}".format(sim)
        # build final output
        # options_dict could be used to request more output infos 'cdr_ids' etc
        dec = 0
        output = []
        do = DictOutput()
        #needed_columns = ['info:s3_url', 'info:all_cdr_ids', 'info:all_parent_ids']
        
        for i in range(0,nb_query):    
            output.append(dict())
            if i in corrupted:
                output[i][do.map['similar_images']] = OrderedDict([[do.map['number'],0],\
                                                           [do.map['sha1'],[]],\
                                                           [do.map['cached_image_urls'],[]],\
                                                           [do.map['cdr_ids'],[]],\
                                                           [do.map['ads_cdr_ids'],[]],\
                                                           [do.map['distance'],[]]])
                dec += 1
                continue
            ii = i - dec
            output[i][do.map['similar_images']] = OrderedDict([[do.map['number'],len(sim[ii])],\
                                                               [do.map['sha1'],[]],\
                                                               [do.map['cached_image_urls'],[]],\
                                                               [do.map['cdr_ids'],[]],\
                                                               [do.map['ads_cdr_ids'],[]],\
                                                               [do.map['distance'],[]]])
            output[i][do.map['query_sha1']] = list_sha1_id[ii]
            ok_sims = []
            for jj,simj in enumerate(sim[ii]):
                found_columns = [c in simj[1] for c in self.needed_output_columns]
                if found_columns.count(True) == len(self.needed_output_columns):
                    output[i][do.map['similar_images']][do.map['sha1']].append(simj[0].strip())
                    output[i][do.map['similar_images']][do.map['cached_image_urls']].append(simj[1]['info:s3_url'].strip())
                    #output[i]['similar_images']['cdr_ids'].append(simj[1]['info:all_cdr_ids'].strip())
                    #output[i]['similar_images']['ads_cdr_ids'].append(simj[1]['info:all_parent_ids'].strip())
                    ok_sims.append(jj)
                #else:
                #    print "[Searcher.format_output: log] Found invalid image: {}. found_columns: {}".format(simj[0],found_columns)
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


    def format_output(self, simname, nb_query, corrupted, list_sha1_id, options_dict=dict()):
        # read hashing similarity results and get 'cached_image_urls', 'cdr_ids', 'ads_cdr_ids'
        print "[Searcher.format_output: log] options are: {}".format(options_dict)
        if 'sha1_sim' in options_dict:
            sha1sim = options_dict['sha1_sim']
        else:
            sha1sim = False
        try:
            if sha1sim:
                sim,sim_score = self.read_sim_sha1(simname, nb_query, options_dict)
            else:
                sim,sim_score = self.read_sim(simname, nb_query, options_dict)
        except Exception as inst:
            print "[Searcher.format_output: error] {}".format(inst)
            return self.build_error_output(nb_query, inst)

        outp = self.build_output(nb_query, corrupted, list_sha1_id, sim, sim_score, options_dict)
        #print "[Searcher.format_output: log] output {}".format(output)
        
        return outp

    def format_output_nodiskout(self, out_res, nb_query, corrupted, list_sha1_id, options_dict=dict()):
        # read similarity results from memory
        print "[Searcher.format_output_nodiskout: log] options are: {}".format(options_dict)
        try:
            sim, sim_score = self.read_sim_nodiskout(out_res, nb_query, options_dict)
        except Exception as inst:
            return self.build_error_output(nb_query, inst)

        outp = self.build_output(nb_query, corrupted, list_sha1_id, sim, sim_score, options_dict)
        return outp

    def search_one_imagepath(self,image_path):
    	# initilization
        search_id = str(time.time())
        all_img_filenames = [image_path]
        return self.search_from_image_filenames(all_img_filenames,search_id)


    def search_image_filelist(self, image_list, options_dict=dict()):
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
        #print "[Searcher.search_image_list: log] all_img_filenames: {}.".format(all_img_filenames)
        # download the images we need
        if batch:
            readable_images = self.indexer.image_downloader.download_images(batch, search_id)
            for i,img_tup in enumerate(readable_images):
                #print "[Searcher.search_image_list: log] {} readable image tuple {}.".format(i,img_tup)
                dl_pos = dl_images.index(img_tup[0])
                all_img_filenames[dl_images[dl_pos]]=img_tup[-1]
        #print "[Searcher.search_image_list: log] all_img_filenames: {}.".format(all_img_filenames)
        print("[search_image_filelist: log] options_dict: {}".format(options_dict))
        if "no_diskout" in options_dict and options_dict["no_diskout"]:
            # print("[search_image_filelist: log] using no_diskout")
            # outp, outputname = self.search_from_image_filenames_nodiskout(all_img_filenames, search_id, options_dict)
            print("[search_image_filelist: log] using no_diskout is not yet fully supported. calling search_from_image_filenames anyway")
            outp, outputname = self.search_from_image_filenames(all_img_filenames, search_id, options_dict)
        else:
            outp, outputname = self.search_from_image_filenames(all_img_filenames, search_id, options_dict)
        return outputname

        
    def search_image_list(self, image_list, options_dict=dict()):
        # initilization
        start_search = time.time()
        search_id = str(start_search)
        i = 0
        # read all images
        dl_images = []
        batch = []
        all_img_filenames = []
        for line in image_list:
            image_line = line.replace('\n','')
            if len(image_line)>2:
                # Check if image or web address
                if image_line[0:4]=="http":
                    # Push image to be downloaded image
                    batch.append((i,image_line,None))
                    dl_images.append(i)
                all_img_filenames.append(image_line)
                i+=1
        #print "[Searcher.search_image_list: log] all_img_filenames: {}.".format(all_img_filenames)
        # download the images we need
        if batch:
            readable_images = self.indexer.image_downloader.download_images(batch, search_id)
            if not readable_images:
                return {'error': 'could not download any image.'}
            for i,img_tup in enumerate(readable_images):
                #print "[Searcher.search_image_list: log] {} readable image tuple {}.".format(i,img_tup)
                dl_pos = dl_images.index(img_tup[0])
                all_img_filenames[dl_images[dl_pos]]=img_tup[-1]
        #print "[Searcher.search_image_list: log] all_img_filenames: {}.".format(all_img_filenames)
        print "[Searcher.search_image_filelist: log] Search prepared in {}s.".format(time.time() - start_search)
        #outp, outputname = self.search_from_image_filenames(all_img_filenames, search_id, options_dict)
        print("[Searcher.search_image_filelist: log] options_dict: {}".format(options_dict))
        if "no_diskout" in options_dict:
            print("[search_image_filelist: log] using no_diskout")
            outp, outputname = self.search_from_image_filenames_nodiskout(all_img_filenames, search_id, options_dict)
        else:
            outp, outputname = self.search_from_image_filenames(all_img_filenames, search_id, options_dict)
        
        return outp

    def search_from_image_filenames_nocache(self, all_img_filenames, search_id, options_dict=dict()):
        corrupted = []
        valid_img_filenames = []
        valid_img = []
        list_sha1_id = []
        outp = []
        for i, image_name in enumerate(all_img_filenames):
            if image_name[0:4]!="http":
                sha1 = get_SHA1_from_file(image_name)
                if sha1:
                    list_sha1_id.append(sha1)
                    valid_img.append((i,sha1,image_name))
                    valid_img_filenames.append(image_name)
                else:
                    corrupted.append(i)
            else: # we did not manage to download image
                # need to deal with that in output formatting too
                corrupted.append(i)
        if valid_img_filenames:
            features_filename = self.compute_features_listimgfiles(valid_img_filenames, search_id)
            #features_filename, ins_num = self.indexer.feature_extractor.compute_features(valid_img_filenames, search_id)
            #if ins_num!=len(valid_img_filenames):
            #    raise ValueError("[Searcher.search_from_image_filenames_nocache: error] We did not get enough features ({}) from list of {} images.".format(ins_num,len(new_files)))
            # query with features_filename
            simname = self.indexer.hasher.get_similar_images_from_featuresfile(features_filename, self.ratio)
            outp = self.format_output(simname, len(all_img_filenames), corrupted, list_sha1_id, options_dict)
            # cleanup
            os.remove(simname)
        return outp


    def search_from_image_filenames(self, all_img_filenames, search_id, options_dict=dict()):
        # compute all sha1s
        start_search = time.time()
        corrupted = []
        list_sha1_id = []
        valid_images = []
        for i,image_name in enumerate(all_img_filenames):
            if image_name[0:4]!="http":
                sha1 = get_SHA1_from_file(image_name)
                if sha1:
                    list_sha1_id.append(sha1)
                    valid_images.append((i,sha1,image_name))
                else:
                    print("[Searcher.search_from_image_filenames: log] image {} is corrupted.".format(image_name))
                    corrupted.append(i)
            else: # we did not manage to download image
                # need to deal with that in output formatting too
                corrupted.append(i)
        #print "[Searcher.search_from_image_filenames: log] valid_images {}".format(valid_images)
        # get indexed images
        list_ids_sha1_found = self.indexer.get_ids_from_sha1s(list_sha1_id)
        print "[Searcher.search_from_image_filenames: log] list_ids_sha1_found {}".format(list_ids_sha1_found)
        tmp_list_ids_found = [x[0] for x in list_ids_sha1_found if x[0] is not None]
        list_sha1_found = [x[1] for x in list_ids_sha1_found if x[0] is not None]
        #print "[Searcher.search_from_image_filenames: log] list_sha1_id {}".format(list_sha1_id)
        #print "[Searcher.search_from_image_filenames: log] list_sha1_found {}".format(list_sha1_found)
        # this is to keep proper ordering
        list_ids_found = [tmp_list_ids_found[list_sha1_found.index(sha1)] for sha1 in list_sha1_id if sha1 in list_sha1_found]
        #print "[Searcher.search_from_image_filenames: log] tmp_list_ids_found {}".format(tmp_list_ids_found)
        print "[Searcher.search_from_image_filenames: log] list_ids_found {}".format(list_ids_found)
        if list_ids_found:
            # get the features, hasher starts to count at 1
            feats,ok_ids = self.indexer.hasher.get_precomp_feats([x+1 for x in list_ids_found])
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
                new_files.append(image_name)
            all_valid_images.append(all_img_filenames[i])
        # check images are jpeg (and convert them here?)
        print "[Searcher.search_from_image_filenames: log] all_valid_images {}".format(all_valid_images)
        print "[Searcher.search_from_image_filenames: log] new_files {}".format(new_files)
        features_filename = self.compute_features_listimgfiles(new_files, search_id)
        #features_filename,ins_num = self.indexer.feature_extractor.compute_features(new_files,search_id)
        #if ins_num!=len(new_files):
        #    raise ValueError("[Searcher.search_from_image_filenames: error] We did not get enough features ({}) from list of {} images.".format(ins_num,len(new_files)))
        # merge feats with features_filename
        final_featuresfile = search_id+'.dat'
        read_dim = self.features_dim*4
        read_type = np.float32
        features_wrote = 0
        #print "[Searcher.search_from_image_filenames: log] feats {}".format(feats)
        with open(features_filename,'rb') as new_feats, open(final_featuresfile,'wb') as out:
            for image_name in all_valid_images:
                #print "[Searcher.search_from_image_filenames: log] saving feature of image {}".format(image_name)
                if image_name in precomp_img_filenames:
                    # select precomputed 
                    precomp_pos = precomp_img_filenames.index(image_name)
                    #print "[Searcher.search_from_image_filenames: log] getting precomputed feature at position {}".format(precomp_pos)
                    tmp_feat = feats[precomp_pos][:]
                else:
                    # read from new feats
                    tmp_feat = np.frombuffer(new_feats.read(read_dim),dtype=read_type)
                print "[Searcher.search_from_image_filenames: log] tmp_feat for image {} has norm {} and is: {}".format(image_name,np.linalg.norm(tmp_feat),tmp_feat)
                out.write(tmp_feat)
                features_wrote += 1
        print "[Searcher.search_from_image_filenames: log] Search prepared in {}s".format(time.time() - start_search)
        if features_wrote:
            # query with merged features_filename
            simname = self.indexer.hasher.get_similar_images_from_featuresfile(final_featuresfile, self.ratio)
        outputname = simname[:-4]+".json"
        start_format = time.time()
        outp = self.format_output(simname, len(all_img_filenames), corrupted, list_sha1_id, options_dict)
        print "[Searcher.search_from_image_filenames: log] Formatting done in {}s".format(time.time() - start_format)
        print "[Searcher.search_from_image_filenames: log] saving output to {}".format(outputname)
        json.dump(outp, open(outputname,'w'), indent=4, sort_keys=False)    
        print "[Searcher.search_from_image_filenames: log] Search done in {}s".format(time.time() - start_search)
        return outp, outputname


    def search_from_listid_get_simname(self, list_ids_sha1, search_id):
        # list_ids_sha1 will be list of tuples (integer_id, sha1)
        # sanity check
        nb_imgs = len(self.indexer.sha1_featid_mapping)
        valid_ids_sha1 = []
        corrupted = []
        for img_id,sha1 in list_ids_sha1:
            if img_id < nb_imgs:
                if sha1 != self.indexer.sha1_featid_mapping[img_id]:
                    print "[Searcher.search_from_listid_get_simname: error] misaligned image {} vs. {} id {}".format(sha1, self.indexer.sha1_featid_mapping[img_id], img_id)
                    try:
                        sha1_pos = self.indexer.sha1_featid_mapping.index(sha1)
                        print "[Searcher.search_from_listid_get_simname: info] image {} can actually be found at {}".format(sha1, sha1_pos)
                    except Exception:
                        print "[Searcher.search_from_listid_get_simname: info] image {} cannot be found in index.".format(sha1)
                    corrupted.append(sha1)
                else:
                    valid_ids_sha1.append((img_id, sha1))
            else:
                print "[Searcher.search_from_listid_get_simname] trying to access image {} when searching image {} while we have only {} images".format(img_id, sha1, nb_imgs)
                corrupted.append(sha1)
        if valid_ids_sha1:
            # get the features, hasher starts to count at 1
            feats, ok_ids = self.indexer.hasher.get_precomp_feats([x[0]+1 for x in valid_ids_sha1])
            if len(ok_ids) != len(valid_ids_sha1):
                raise ValueError("[Searcher.search_from_sha1_list_get_simname: error] We did not get enough precomputed features ({}) from list of {} images.".format(len(ok_ids),len(list_ids_found)))
        final_featuresfile = search_id+'.dat'
        print "[Searcher.search_from_listid_get_simname: log] writing {} features to {}".format(len(valid_ids_sha1), final_featuresfile)
        read_dim = self.features_dim*4
        read_type = np.float32
        features_wrote = 0
        #print "[Searcher.search_from_image_filenames: log] feats {}".format(feats)
        with open(final_featuresfile,'wb') as out:
            for precomp_pos,img_id in enumerate(valid_ids_sha1):
                tmp_feat = feats[precomp_pos][:]
                #print "[Searcher.search_from_sha1_list_get_simname: log] tmp_feat for image {} has norm {} and is: {}".format(img_id, np.linalg.norm(tmp_feat), tmp_feat)
                out.write(tmp_feat)
                features_wrote += 1
        if features_wrote:
            # query with merged features_filename
            print "[Searcher.search_from_listid_get_simname: log] searching for similar images from features file {}".format(final_featuresfile)
            simname = self.indexer.hasher.get_similar_images_from_featuresfile(final_featuresfile, self.ratio)
        else:
            print "[Searcher.search_from_listid_get_simname: log] no features to search for similar images."
            simname = None
        return simname, corrupted


    def search_from_sha1_list_get_simname(self, all_img_sha1s, search_id):
        # get indexed images
        list_ids_sha1_found = self.indexer.get_ids_from_sha1s(all_img_sha1s)
        tmp_list_ids_found = [x[0] for x in list_ids_sha1_found if x[0] is not None]
        list_sha1_found = [x[1] for x in list_ids_sha1_found if x[0] is not None]
        # this is to keep proper ordering
        list_ids_found = [tmp_list_ids_found[list_sha1_found.index(sha1)] for sha1 in all_img_sha1s if sha1 in list_sha1_found]
        #print "[Searcher.search_from_sha1_list: log] tmp_list_ids_found {}".format(tmp_list_ids_found)
        #print "[Searcher.search_from_sha1_list_get_simname: log] list_ids_found {}".format(list_ids_found)
        if list_ids_found:
            # get the features, hasher starts to count at 1
            feats, ok_ids = self.indexer.hasher.get_precomp_feats([x+1 for x in list_ids_found])
            if len(ok_ids) != len(list_ids_found):
                raise ValueError("[Searcher.search_from_sha1_list_get_simname: error] We did not get enough precomputed features ({}) from list of {} images.".format(len(ok_ids),len(list_ids_found)))
        # this should not be empty
        corrupted = list(set(all_img_sha1s)-set(list_sha1_found))
        if corrupted:
            print "[Searcher.search_from_sha1_list_get_simname: log] some sha1s were not found: {}".format(corrupted)
        final_featuresfile = search_id+'.dat'
        read_dim = self.features_dim*4
        read_type = np.float32
        features_wrote = 0
        #print "[Searcher.search_from_image_filenames: log] feats {}".format(feats)
        with open(final_featuresfile,'wb') as out:
            for precomp_pos,img_id in enumerate(list_ids_found):
                tmp_feat = feats[precomp_pos][:]
                #print "[Searcher.search_from_sha1_list_get_simname: log] tmp_feat for image {} has norm {} and is: {}".format(img_id, np.linalg.norm(tmp_feat), tmp_feat)
                out.write(tmp_feat)
                features_wrote += 1
        if features_wrote:
            # query with merged features_filename
            simname = self.indexer.hasher.get_similar_images_from_featuresfile(final_featuresfile, self.ratio)
        else:
            simname = None
        return simname, corrupted

    def search_from_sha1_list(self, all_img_sha1s, search_id, options_dict=dict()):
        # compute all sha1s
        start_search = time.time()
        simname, corrupted = self.search_from_sha1_list_get_simname(all_img_sha1s, search_id)
        print "[Searcher.search_from_sha1_list: log] Search prepared in {}s".format(time.time() - start_search)
        
        if simname is not None:
            outputname = simname[:-4]+".json"
            start_format = time.time()
            outp = self.format_output(simname, len(all_img_sha1s), corrupted, all_img_sha1s, options_dict)
            print "[Searcher.search_from_sha1_list: log] Formatting done in {}s".format(time.time() - start_format)
            print "[Searcher.search_from_sha1_list: log] saving output to {}".format(outputname)
            json.dump(outp, open(outputname,'w'), indent=4, sort_keys=False)    
            print "[Searcher.search_from_sha1_list: log] Search done in {}s".format(time.time() - start_search)
        else:
            # prepare dummy output?
            outp = None
            outputname = None
        return outp, outputname

    # # this is not yet working.
    # def search_from_image_filenames_nodiskout(self, all_img_filenames, search_id, options_dict=dict()):
    #     # compute all sha1s
    #     start_search = time.time()
    #     corrupted = []
    #     list_sha1_id = []
    #     valid_images = []
    #     for i,image_name in enumerate(all_img_filenames):
    #         if image_name[0:4]!="http":
    #             sha1 = get_SHA1_from_file(image_name)
    #             if sha1:
    #                 list_sha1_id.append(sha1)
    #                 valid_images.append((i,sha1,image_name))
    #             else:
    #                 print("[Searcher.search_from_image_filenames_nodiskout: log] image {} is corrupted.".format(image_name))
    #                 corrupted.append(i)
    #         else: # we did not manage to download image
    #             # need to deal with that in output formatting too
    #             corrupted.append(i)
    #     #print "[Searcher.search_from_image_filenames: log] valid_images {}".format(valid_images)
    #     # get indexed images
    #     list_ids_sha1_found = self.indexer.get_ids_from_sha1s(list_sha1_id)
    #     print "[Searcher.search_from_image_filenames_nodiskout: log] list_ids_sha1_found {}".format(list_ids_sha1_found)
    #     tmp_list_ids_found = [x[0] for x in list_ids_sha1_found if x[0] is not None]
    #     list_sha1_found = [x[1] for x in list_ids_sha1_found if x[0] is not None]
    #     #print "[Searcher.search_from_image_filenames: log] list_sha1_id {}".format(list_sha1_id)
    #     #print "[Searcher.search_from_image_filenames: log] list_sha1_found {}".format(list_sha1_found)
    #     # this is to keep proper ordering
    #     list_ids_found = [tmp_list_ids_found[list_sha1_found.index(sha1)] for sha1 in list_sha1_id if sha1 in list_sha1_found]
    #     #print "[Searcher.search_from_image_filenames: log] tmp_list_ids_found {}".format(tmp_list_ids_found)
    #     print "[Searcher.search_from_image_filenames_nodiskout: log] list_ids_found {}".format(list_ids_found)
    #     if list_ids_found:
    #         # get the features, hasher starts to count at 1
    #         feats,ok_ids = self.indexer.hasher.get_precomp_feats([x+1 for x in list_ids_found])
    #         if len(ok_ids)!=len(list_ids_found):
    #             raise ValueError("[Searcher.search_from_image_filenames_nodiskout: error] We did not get enough precomputed features ({}) from list of {} images.".format(len(ok_ids),len(list_ids_found)))
    #     # compute new images features
    #     not_indexed_sha1 = set(list_sha1_id)-set(list_sha1_found)
    #     #res = self.indexer.get_precomp_from_sha1(list_ids_sha1_found)
    #     new_files = []
    #     all_valid_images = []
    #     precomp_img_filenames=[]
    #     for i,sha1,image_name in valid_images:
    #         if sha1 in list_sha1_found: # image is indexed
    #             precomp_img_filenames.append(image_name)
    #         else:
    #             new_files.append(image_name)
    #         all_valid_images.append(all_img_filenames[i])
    #     # check images are jpeg (and convert them here?)
    #     print "[Searcher.search_from_image_filenames_nodiskout: log] all_valid_images {}".format(all_valid_images)
    #     print "[Searcher.search_from_image_filenames_nodiskout: log] new_files {}".format(new_files)
    #     features_filename = self.compute_features_listimgfiles(new_files, search_id)
    #     #features_filename,ins_num = self.indexer.feature_extractor.compute_features(new_files,search_id)
    #     #if ins_num!=len(new_files):
    #     #    raise ValueError("[Searcher.search_from_image_filenames: error] We did not get enough features ({}) from list of {} images.".format(ins_num,len(new_files)))
    #     # merge feats with features_filename
    #     final_featuresfile = search_id+'.dat'
    #     read_dim = self.features_dim*4
    #     read_type = np.float32
    #     features_wrote = 0
    #     #print "[Searcher.search_from_image_filenames: log] feats {}".format(feats)
    #     with open(features_filename,'rb') as new_feats, open(final_featuresfile,'wb') as out:
    #         for image_name in all_valid_images:
    #             #print "[Searcher.search_from_image_filenames: log] saving feature of image {}".format(image_name)
    #             if image_name in precomp_img_filenames:
    #                 # select precomputed 
    #                 precomp_pos = precomp_img_filenames.index(image_name)
    #                 #print "[Searcher.search_from_image_filenames: log] getting precomputed feature at position {}".format(precomp_pos)
    #                 tmp_feat = feats[precomp_pos][:]
    #             else:
    #                 # read from new feats
    #                 tmp_feat = np.frombuffer(new_feats.read(read_dim),dtype=read_type)
    #             print "[Searcher.search_from_image_filenames_nodiskout: log] tmp_feat for image {} has norm {} and is: {}".format(image_name,np.linalg.norm(tmp_feat),tmp_feat)
    #             out.write(tmp_feat)
    #             features_wrote += 1
    #     print "[Searcher.search_from_image_filenames_nodiskout: log] Search prepared in {}s".format(time.time() - start_search)
    #     if features_wrote:
    #         # how to properly interact with out_res?
    #         out_res = hop.ResVector(self.indexer.hasher.get_similar_images_from_featuresfile_nodiskout(final_featuresfile, self.ratio))
    #     start_format = time.time()
    #     outp = self.format_output_nodiskout(out_res, len(all_img_filenames), corrupted, list_sha1_id, options_dict)
    #     print "[Searcher.search_from_image_filenames_nodiskout: log] Formatting done in {}s".format(time.time() - start_format)
    #     outputname = str(search_id)+"-sim.json"
    #     print "[Searcher.search_from_image_filenames_nodiskout: log] Saving output to {}".format(outputname)
    #     json.dump(outp, open(outputname,'w'), indent=4, sort_keys=False)    
    #     print "[Searcher.search_from_image_filenames_nodiskout: log] Search done in {}s".format(time.time() - start_search)
    #     return outp, outputname


