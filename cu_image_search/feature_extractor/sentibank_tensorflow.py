import os
import sys
import time
import json
import math
import numpy as np
from scipy import misc
from ..memex_tools.image_dl import mkpath
import tfdeepsentibank


class SentiBankTensorflow():

    def __init__(self, global_conf_filename):
        self.global_conf = json.load(open(global_conf_filename,'rt'))
        self.base_update_path = None
        self.sentibank_path = None
        if "LI_base_update_path" in self.global_conf:
            self.base_update_path = self.global_conf["LI_base_update_path"]
        if "LI_sentibank_path" in self.global_conf:
            # this should contain the tfdeepsentibank.npy and imagenet_mean.npy files
            self.sentibank_path = self.global_conf["LI_sentibank_path"]
        if self.base_update_path is None: # fallback to file folder
            self.base_update_path = os.path.dirname(__file__)
        self.features_path = os.path.join(self.base_update_path,'features/')
        self.features_dim = self.global_conf["FE_features_dim"]
        mkpath(self.features_path)
        print "[SentiBankTensorflow.init: log] sentibank_path is {}.".format(self.sentibank_path)
        print "[SentiBankTensorflow.init: log] features_path is {}.".format(self.features_path)
        self.DSE = tfdeepsentibank.DeepSentibankExtractor(os.path.join(self.sentibank_path,'tfdeepsentibank.npy'), os.path.join(self.sentibank_path,'imagenet_mean.npy'))


    def compute_features(self, new_files, startid):
        # should we do that by batch
        # we could aslo check if len(img_filename)>2
        feats = [self.DSE.get_features_from_img_filename(img_filename)[0] for img_filename in new_files]
        # save to disk to keep the rest of the pipeline equal for now
        # will be read in hasher_swig get_similar_images_from_featuresfile and passed to set_query_feats_from_disk
        featurefilename = os.path.join(self.features_path, str(startid)+'-features_fc7.dat')
        # features should be written in binary format
        with open(featurefilename, 'wb') as outfile:
            for feat in feats:
                feat.tofile(outfile)
        return featurefilename, len(new_files)


    def compute_features_nodiskout(self, new_files):
        return [self.DSE.get_features_from_img_filename(img_filename)[0] for img_filename in new_files]

    def compute_features_fromURLs_nodiskout(self, list_urls):
        return [self.DSE.get_features_from_URL(URL)[0] for URL in list_urls]
