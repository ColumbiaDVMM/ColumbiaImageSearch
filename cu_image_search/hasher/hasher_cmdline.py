import os
import time
import json
import shutil
import struct
import numpy as np
from ..memex_tools.image_dl import mkpath

class HasherCmdLine():

    def __init__(self,global_conf_filename):
        self.global_conf = json.load(open(global_conf_filename,'rt'))
        self.base_update_path = self.global_conf['LI_base_update_path']
        self.features_dim = self.global_conf['FE_features_dim']
        self.bits_num = self.global_conf['HA_bits_num']
        self.hashing_execpath = os.path.join(os.path.dirname(__file__),'../hashing/')
        self.hashing_outpath = os.path.join(self.base_update_path,'hash_bits')

    def compute_hashcodes(self,features_filename,ins_num,startid):
        feature_filepath = features_filename[:-4]+'_norm'
        # we could be passing additional arguments here
        command = self.hashing_execpath+'hashing_update '+features_filename+' '+str(ins_num)+' '+self.hashing_execpath
        print command
        os.system(command)
        
        hashbits_filepath = os.path.join(self.hashing_outpath,str(startid)+'_itq_norm_'+str(self.bits_num))
        itq_output_path = features_filename[:-4] + '_itq_norm_'+str(self.bits_num)
        shutil.move(itq_output_path, hashbits_filepath)
        os.remove(features_filename)
        return hashbits_filepath

    def compress_feats(self):
        mkpath(os.path.join(self.base_update_path,'comp_features'))
        mkpath(os.path.join(self.base_update_path,'comp_idx'))
        # we could be passing additional arguments here
        command = self.hashing_execpath+'compress_feats '+self.base_update_path
        # this will work only if all features are still present in self.base_update_path/features
        print command
        os.system(command)

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
        X = []
        ok_ids = []
        with open(X_fn,"rb") as f_preout:
            for i in range(len(list_feats_id)):
                try:
                    X.append(np.frombuffer(f_preout.read(read_dim),dtype=read_type))
                    ok_ids.append(i)
                except Exception as inst:
                    print "[HasherCmdLine.get_precomp_X: error] Could not read requested {} with id {}. {}".format(str_precomp,list_feats_id[i],inst)
        # cleanup
        os.remove(query_precomp_fn)
        os.remove(X_fn)
        return X,ok_ids

    def get_precomp_feats(self,list_feats_id):
        return self.get_precomp_X(list_feats_id,"feats",self.features_dim*4,np.float32)

    def get_precomp_hashcodes(self,list_feats_id):
        return self.get_precomp_X(list_feats_id,"hashcodes",self.bits_num/8,np.uint8)


