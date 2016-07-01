import os
import time
import json
import shutil
from ..memex_tools.image_dl import mkpath

class HasherCmdLine():

    def __init__(self,global_conf_filename):
        self.global_conf = json.load(open(global_conf_filename,'rt'))
        self.base_update_path = self.global_conf['LI_base_update_path']
        self.hashing_execpath = os.path.join(os.path.dirname(__file__),'../hashing/')
        self.hashing_outpath = os.path.join(self.base_update_path,'hash_bits')

    def compute_hashcodes(self,features_filename,ins_num,startid):
        feature_filepath = features_filename[:-4]+'_norm'
        command = self.hashing_execpath+'hashing_update '+features_filename+' '+str(ins_num)+' '+self.hashing_execpath
        print command
        os.system(command)
        
        hashbits_filepath = os.path.join(self.hashing_outpath,str(startid)+'_itq_norm_256')
        itq_output_path = features_filename[:-4] + '_itq_norm_256'
        shutil.move(itq_output_path, hashbits_filepath)
        os.remove(features_filename)
        return hashbits_filepath

    def compress_feats(self):
        mkpath(os.path.join(self.base_update_path,'comp_features'))
        mkpath(os.path.join(self.base_update_path,'comp_idx'))
        command = self.hashing_execpath+'compress_feats '+self.base_update_path
        print command
        os.system(command)
