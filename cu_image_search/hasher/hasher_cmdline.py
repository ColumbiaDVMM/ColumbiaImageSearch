import os
import time
import json
import shutil
import numpy as np
from ..memex_tools.image_dl import mkpath
from ..memex_tools.binary_file import read_binary_file

class HasherCmdLine():

    def __init__(self,global_conf_filename):
        self.global_conf = json.load(open(global_conf_filename,'rt'))
        self.base_update_path = os.path.dirname(__file__)
        if 'LI_base_update_path' in self.global_conf:
            self.base_update_path = self.global_conf['LI_base_update_path']
        if 'HA_base_update_path' in self.global_conf:
            self.base_update_path = self.global_conf['HA_base_update_path']
        self.features_dim = self.global_conf['FE_features_dim']
        self.bits_num = self.global_conf['HA_bits_num']
        self.hashing_execpath = os.path.join(os.path.dirname(__file__),'../hashing/')
        self.hashing_outpath = os.path.join(self.base_update_path,'hash_bits/')
        mkpath(self.hashing_outpath)
        # need to be able to set/get master_update file.
        self.master_update_file = "update_list_dev.txt"
        if 'HA_master_update_file' in self.global_conf:
            self.master_update_file = self.global_conf['HA_master_update_file']

    def compute_hashcodes(self,features_filename,ins_num,startid):
        """ Compute ITQ hashcodes for the features in 'features_filename'

        :param features_filename: filepath for the binary file containing the features
        :type features_filename: string
        :param ins_num: number of features in 'features_filename'
        :type ins_num: integer
        :returns hashbits_filepath: filepath for the binary file containing the hashcodes
        """
        feature_filepath = features_filename[:-4]+'_norm'
        # we could be passing additional arguments here
        command = self.hashing_execpath+'hashing_update '+features_filename+' '+str(ins_num)+' '+self.hashing_execpath
        print command
        os.system(command)        
        hashbits_filepath = os.path.join(self.hashing_outpath,str(startid)+'_itq_norm_'+str(self.bits_num))
        itq_output_path = features_filename[:-4] + '_itq_norm_'+str(self.bits_num)
        print "[HasherCmdLine.compute_hashcodes: log] Moving {} to {}.".format(itq_output_path,hashbits_filepath)
        shutil.move(itq_output_path, hashbits_filepath)
        os.remove(features_filename)
        return hashbits_filepath

    def get_max_feat_id(self):
        """ Returns number of images indexed based on the size of hashcodes files.
        """
        with open(os.path.join(self.base_update_path,self.master_update_file),'rt') as master_file:
            total_nb = 0
            # sum up sizes of files in master_file
            for line in master_file:
                statinfo = os.stat(os.path.join(self.hashing_outpath,line.strip()))
                total_nb += statinfo.st_size*8/self.bits_num
        return total_nb

    def compress_feats(self):
        """ Compress the features with zlib.
        """
        mkpath(os.path.join(self.base_update_path,'comp_features'))
        mkpath(os.path.join(self.base_update_path,'comp_idx'))
        # we could be passing additional arguments here
        command = self.hashing_execpath+'compress_feats '+self.base_update_path+' '+self.master_update_file
        # this will work only if all features are present in self.base_update_path/features
        print command
        os.system(command)

    # we would need to be able to compress just one update file and merge with previous update.

    # deprecated, now in memex_tools/binary_file.py
    # def read_binary_file(self,X_fn,str_precomp,list_feats_id,read_dim,read_type):
    #     X = []
    #     ok_ids = []
    #     with open(X_fn,"rb") as f_preout:
    #         for i in range(len(list_feats_id)):
    #             try:
    #                 X.append(np.frombuffer(f_preout.read(read_dim),dtype=read_type))
    #                 ok_ids.append(i)
    #             except Exception as inst:
    #                 print "[HasherCmdLine.read_binary_file: error] Could not read requested {} with id {}. {}".format(str_precomp,list_feats_id[i],inst)
    #     return X,ok_ids

    def get_precomp_X(self,list_feats_id,str_precomp,read_dim,read_type):
        query_time = time.time()
        # save queries id in binary file
        query_precomp_fn = "{}_query_{}".format(str_precomp,query_time)
        X_fn = "{}_{}".format(str_precomp,query_time)
        with open(query_precomp_fn,"wb") as f_prein:
            for feat_id in list_feats_id:
                f_prein.write(struct.pack('i',feat_id))
        # query for features
        command = self.hashing_execpath+"get_precomp_{} {} {} {}".format(str_precomp,query_precomp_fn,X_fn,self.base_update_path)
        print "[HasherCmdLine.get_precomp_X: log] running command: {}".format(command)
        os.system(command)
        # read features/hashcodes
        X, ok_ids = read_binary_file(X_fn,str_precomp,list_feats_id,read_dim,read_type)
        # cleanup
        os.remove(query_precomp_fn)
        os.remove(X_fn)
        return X,ok_ids

    def get_precomp_feats(self,list_feats_id):
        """ Get precomputed features from 'list_feats_id'
        """
        return self.get_precomp_X(list_feats_id,"feats",self.features_dim*4,np.float32)

    def get_precomp_hashcodes(self,list_feats_id):
        """ Get precomputed hashcodes from 'list_feats_id'
        """
        return self.get_precomp_X(list_feats_id,"hashcodes",self.bits_num/8,np.uint8)

    def get_similar_images_from_featuresfile(self,featurefilename,ratio):
        """ Get similar images of the images with features in 'featurefilename'.

        :param featurefilename: features of the query images.
        :type featurefilename: string
        :param ratio: ratio of images retrieved with hashing that will be reranked.
        :type ratio: float
        :returns simname: filename of the simname text file.
        """
        command = self.hashing_execpath+"hashing {} {} {} {} {}".format(featurefilename,self.hashing_execpath,self.base_update_path,self.bits_num,ratio)
        #command = self.hashing_execpath+"hashing "+featurefilename+" "+str(self.bits_num)+" "+str(ratio)
        print "[HasherCmdLine.get_similar_images: log] running command: {}".format(command)
        os.system(command)
        initname = featurefilename[:-4] + '-sim.txt'
        simname = featurefilename[:-4] + '-sim_'+str(ratio)+'.txt'
        print "[HasherCmdLine.get_similar_images: log] try to rename {} to {}".format(initname,simname)
        os.rename(initname,simname)
        return simname

