import os
import pwd
import sys
import time
import json
import shutil
import random
import subprocess
import numpy as np
from .generic_hasher import GenericHasher
from ..memex_tools.image_dl import mkpath
from ..memex_tools.binary_file import read_binary_file
# should me move the _hasher_obj_py.so?
#from ..hashing_new.python import _hasher_obj_py
import _hasher_obj_py as hop

class HasherSwig(GenericHasher):

    def __init__(self,global_conf_filename):
        self.global_conf = json.load(open(global_conf_filename,'rt'))
        self.base_update_path = os.path.dirname(__file__)
        self.base_model_path = os.path.join(os.path.dirname(__file__),'../../data/')
        if 'LI_base_update_path' in self.global_conf:
            self.base_update_path = self.global_conf['LI_base_update_path']
        if 'HA_base_update_path' in self.global_conf:
            self.base_update_path = self.global_conf['HA_base_update_path']
        if 'HA_path' in self.global_conf:
            self.hashing_execpath = os.path.join(os.path.dirname(__file__),self.global_conf['HA_path'])
        else:
            self.hashing_execpath = os.path.join(os.path.dirname(__file__),'../hashing/')
        if 'HA_exec' in self.global_conf:
            self.hashing_execfile = self.global_conf['HA_exec']
        else:
            self.hashing_execfile = 'hashing'
        self.features_dim = self.global_conf['FE_features_dim']
        self.bits_num = self.global_conf['HA_bits_num']

        self.hashing_outpath = os.path.join(self.base_update_path,'hash_bits/')
        mkpath(self.hashing_outpath)
        # need to be able to set/get master_update file in HasherObjectPy too.
        self.master_update_file = "update_list_dev.txt"
        if 'HA_master_update_file' in self.global_conf:
            print("Setting HA_master_update_file is not yet supported for HasherSwig")
            sys.exit(-1)
            self.master_update_file = self.global_conf['HA_master_update_file']

        self.hasher = hop.new_HasherObjectPy()
        hop.HasherObjectPy_set_feature_dim(self.hasher, self.features_dim)
        hop.HasherObjectPy_set_bit_num(self.hasher, self.bits_num)
        hop.HasherObjectPy_set_base_updatepath(self.hasher, str(self.base_update_path))
        #hop.HasherObjectPy_set_base_modelpath(self.hasher, "/home/ubuntu/memex/data/")
        hop.HasherObjectPy_set_base_modelpath(self.hasher, str(self.base_model_path))
        self.init_hasher()


    def __del__(self):
        # clean exit deleting SWIG object
        hop.delete_HasherObjectPy(self.hasher)


    def init_hasher(self):
        status = hop.HasherObjectPy_initialize(self.hasher)
        if status != 0:
            print("Hasher was not able to initialize")
            sys.exit(-1)


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
        proc = subprocess.Popen(command.split(' '), stdout=subprocess.PIPE)
        print "[HasherSwig.compute_hashcodes: log] running command: {}".format(command)
        sys.stdout.flush()
        (out, err) = proc.communicate()
        print "[HasherSwig.compute_hashcodes: log] program output:", out
        print "[HasherSwig.compute_hashcodes: log] program error:", err
        sys.stdout.flush()
        #print command
        #os.system(command)        
        hashbits_filepath = os.path.join(self.hashing_outpath,str(startid)+'_itq_norm_'+str(self.bits_num))
        itq_output_path = features_filename[:-4] + '_itq_norm_'+str(self.bits_num)
        print "[HasherSwig.compute_hashcodes: log] Moving {} to {}.".format(itq_output_path,hashbits_filepath)
        shutil.move(itq_output_path, hashbits_filepath)
        os.remove(features_filename)
        return hashbits_filepath


    def get_max_feat_id(self):
        """ Returns number of images indexed based on the size of hashcodes files.
        """
        total_nb = 0
        try:
            with open(os.path.join(self.base_update_path,self.master_update_file),'rt') as master_file:
                # sum up sizes of files in master_file
                for line in master_file:
                    statinfo = os.stat(os.path.join(self.hashing_outpath,line.strip()+'_itq_norm_'+str(self.bits_num)))
                    total_nb += statinfo.st_size*8/self.bits_num
        except Exception as inst:
            print "[HasherSwig.get_max_feat_id: error] {}".format(inst)
        return total_nb


    def compress_feats(self):
        """ Compress the features with zlib.
        """
        mkpath(os.path.join(self.base_update_path,'comp_features'))
        mkpath(os.path.join(self.base_update_path,'comp_idx'))
        args = [self.base_update_path+'/', str(self.features_dim), '1', self.master_update_file, str(self.bits_num)]
        subprocess_command = [self.hashing_execpath+"compress_feats"] + args
        # this will work only if features to be compressed are present in self.base_update_path/features
        proc = subprocess.Popen(subprocess_command, stdout=subprocess.PIPE)
        print "[HasherSwig.compress_feats: log] running command: {}".format(subprocess_command)
        (out, err) = proc.communicate()
        print "[HasherSwig.compress_feats: log] program output:", out
        print "[HasherSwig.compress_feats: log] program error:", err

    def get_precomp_X(self,list_feats_id,str_precomp,read_dim,read_type):
        import struct
        query_time = time.time()
        # save queries id in binary file
        query_precomp_fn = "{}_query_{}_p{}_{}".format(str_precomp, query_time, os.getpid(), random.random())
        X_fn = "{}_{}".format(str_precomp,query_time)
        with open(query_precomp_fn,"wb") as f_prein:
            for feat_id in list_feats_id:
                f_prein.write(struct.pack('i',feat_id))
        # query for features
        command = self.hashing_execpath+"get_precomp_{} {} {} {}".format(str_precomp,query_precomp_fn,X_fn,self.base_update_path)
        print("[HasherSwig.get_precomp_X: log] running command: {}".format(command))
        sys.stdout.flush()
        os.system(command)
        # read features/hashcodes
        X, ok_ids = read_binary_file(X_fn,str_precomp,list_feats_id,read_dim,read_type)
        #print X,X[0].shape
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


    def get_similar_images_from_featuresfile(self, featurefilename, ratio, near_dup_th=-1.0):
        """ Get similar images of the images with features in 'featurefilename'.

        :param featurefilename: features of the query images.
        :type featurefilename: string
        :param ratio: ratio of images retrieved with hashing that will be reranked.
        :type ratio: float
        :param near_dup_th: near dup threshold, if positive, only images below this distance value will be returned.
        :type near_dup_th: float
        :returns simname: filename of the simname text file.
        """

        hop.HasherObjectPy_set_ratio(self.hasher, ratio)
        # needed?
        sys.stdout = sys.stderr
        hop.HasherObjectPy_set_near_dup_th(self.hasher, near_dup_th)
        hop.HasherObjectPy_set_query_feats_from_disk(self.hasher, featurefilename)
        hop.HasherObjectPy_set_outputfile(self.hasher, featurefilename[:-4])
        hop.HasherObjectPy_find_knn(self.hasher)
        initname = featurefilename[:-4] + '-sim.txt'
        simname = featurefilename[:-4] + '-sim_'+str(ratio)+'.txt'
        print "[HasherSwig.get_similar_images: log] try to rename {} to {}".format(initname,simname)
        # this would raise an error if results have not been computed
        os.rename(initname,simname)
        return simname

    def get_similar_images_from_featuresfile_nodiskout(self, featurefilename, ratio, demote=False):
        """ Get similar images of the images with features in 'featurefilename'.

        :param featurefilename: features of the query images.
        :type featurefilename: string
        :param ratio: ratio of images retrieved with hashing that will be reranked.
        :type ratio: float
        :returns simlist: list of nearest neighbors of each query
        """

        hop.HasherObjectPy_set_ratio(self.hasher, ratio)
        # needed?
        sys.stdout = sys.stderr
        hop.HasherObjectPy_set_query_feats_from_disk(self.hasher, featurefilename)
        hop.HasherObjectPy_set_outputfile(self.hasher, featurefilename[:-4])
        out_res = hop.HasherObjectPy_find_knn_nodiskout(self.hasher)
        print "[HasherSwig.get_similar_images_from_featuresfile_nodiskout: log] out_res: {}".format(out_res)
        return out_res
