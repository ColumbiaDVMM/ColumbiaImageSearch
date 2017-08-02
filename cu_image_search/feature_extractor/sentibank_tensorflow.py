import os
import sys
import json
from ..memex_tools.image_dl import mkpath
import tfdeepsentibank


class SentiBankTensorflow():


    def dl_file(self, url, outpath):
        import requests
        response = requests.get(url, stream=True)

        # Throw an error for bad status codes
        response.raise_for_status()
        if not os.path.exists(outpath):
            with open(outpath, 'wb') as handle:
                for block in response.iter_content(1024):
                    handle.write(block)


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
        local_tfsb_path = os.path.join(self.sentibank_path,'tfdeepsentibank.npy')
        local_imgnetmean_path = os.path.join(self.sentibank_path,'imagenet_mean.npy')
        if not os.path.exists(self.sentibank_path):
            mkpath(self.sentibank_path)
        # Try to download model and imagenet mean? Where should we put them?
        # They are in Svebor's Dropbox for now...
        # urllib.urlretrieve seems to hang forever sometimes...
        if not os.path.exists(local_tfsb_path):
            from . import tfsentibank_npy_urldlpath, imagenet_mean_npy_urldlpath
            import urllib
            print "Downloading Sentibank model..."
            sys.stdout.flush()
            #urllib.urlretrieve(tfsentibank_npy_urldlpath, local_tfsb_path)
            self.dl_file(tfsentibank_npy_urldlpath, local_tfsb_path)
            print "Done."
            sys.stdout.flush()
        if not os.path.exists(local_imgnetmean_path):
            from . import tfsentibank_npy_urldlpath, imagenet_mean_npy_urldlpath
            import urllib
            # Hangs forever?
            print "Downloading ImageNet mean..."
            sys.stdout.flush()
            #urllib.urlretrieve(imagenet_mean_npy_urldlpath, local_imgnetmean_path)
            self.dl_file(imagenet_mean_npy_urldlpath, local_imgnetmean_path)
            print "Done."
            sys.stdout.flush()
        print "[SentiBankTensorflow.init: log] sentibank_path is {}.".format(self.sentibank_path)
        print "[SentiBankTensorflow.init: log] features_path is {}.".format(self.features_path)
        sys.stdout.flush()
        self.DSE = tfdeepsentibank.DeepSentibankExtractor(local_tfsb_path, local_imgnetmean_path)


    def compute_features(self, new_files, startid):
        # should we do that by batch
        # we could aslo check if len(img_filename)>2
        feats = [self.DSE.get_features_from_img_filename(img_filename)[0] for img_filename in new_files]
        # save to disk to keep the rest of the pipeline equal for now
        # will be read in hasher_swig get_similar_images_from_featuresfile and passed to set_query_feats_from_disk
        #featurefilename = os.path.join(self.features_path, str(startid)+'-features_fc7.dat')
        featurefilename = os.path.join(self.features_path, str(startid) + '.dat')
        # features should be written in binary format
        with open(featurefilename, 'wb') as outfile:
            for feat in feats:
                feat.tofile(outfile)
        return featurefilename, len(new_files)


    def compute_features_nodiskout(self, new_files):
        return [self.DSE.get_features_from_img_filename(img_filename)[0] for img_filename in new_files]

    def compute_features_fromURLs_nodiskout(self, list_urls):
        return [self.DSE.get_features_from_URL(URL)[0] for URL in list_urls]

    def compute_sha1_features_fromURLs_nodiskout(self, list_urls):
        return [self.DSE.get_sha1_features_from_URL(URL) for URL in list_urls]

    def compute_sha1_features_fromB64s_nodiskout(self, list_b64_str):
        return [self.DSE.get_sha1_features_from_B64(b64_str) for b64_str in list_b64_str]
