import os
import time
import json
import shutil
from ..memex_tools.image_dl import mkpath

class HasherCmdLine():

    def __init__(self,global_conf_filename):
        self.global_conf = json.load(open(global_conf_filename,'rt'))
        self.features_path = self.global_conf["FE_features_path"]
        self.hashing_path = os.path.join(os.path.dirname(__file__),'../hashing/')

    def compute_hashcodes(self,features_filename,ins_num,startid):
        feature_filepath = features_filename[:-4]+'_norm'
        command = self.hashing_path+'hashing_update '+features_filename+' '+str(ins_num)+' '+self.hashing_path
        print command
        os.system(command)
        
        hashbits_filepath = os.path.join(self.hashing_path,str(startid)+'_itq_norm_256')
        itq_output_path = features_filename[:-4] + '_itq_norm_256'
        shutil.move(itq_output_path, hashbits_filepath)
        os.remove(featurefilename)
        return hashbits_filepath

    def compress_feats(self):
        command = self.hashing_path+'/compress_feats'
        print command
        os.system(command)
