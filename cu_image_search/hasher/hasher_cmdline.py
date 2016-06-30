import os
import time
import json
import shutil
from ..memex_tools.image_dl import mkpath

class HasherCmdLine():

    def __init__(self,global_conf_filename):
        self.global_conf = json.load(open(global_conf_filename,'rt'))
        self.features_path = self.global_conf["FE_features_path"]
        self.hashing_path = self.global_conf["HA_hashing_path"]
        mkpath(self.hashing_path)

    def compute_hashcodes(self,features_filename,ins_num,startid):
        feature_filepath = featurefilename[:-4]+'_norm'
        command = self.hashing_path+'/hashing_update ' + featurefilename + ' ' + str(ins_num)
        print command
        os.system(command)
        
        hashbits_filepath = os.path.join(self.hashing_path,startid+'_itq_norm_256')
        itq_output_path = featurefilename[:-4] + '_itq_norm_256'
        shutil.move(itq_output_path, hashbits_filepath)
        os.remove(featurefilename)
        return hashbits_filepath

    def compress_feats(self):
        command = self.hashing_path+'/compress_feats'
        print command
        os.system(command)
