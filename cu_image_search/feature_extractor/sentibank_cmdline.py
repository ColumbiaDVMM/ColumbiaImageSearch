import os
import sys
import time
import json
import math
from ..memex_tools.image_dl import mkpath

class SentiBankCmdLine():

    def __init__(self, global_conf_filename):
        self.global_conf = json.load(open(global_conf_filename,'rt'))
        self.base_update_path = None
        self.sentibank_path = None
        self.device = 'GPU'
        if "LI_base_update_path" in self.global_conf:
            self.base_update_path = self.global_conf["LI_base_update_path"]
        if "LI_sentibank_path" in self.global_conf:
            self.sentibank_path = self.global_conf["LI_sentibank_path"]
        if "LI_device" in self.global_conf:
            self.device = self.global_conf["LI_device"]
        if self.base_update_path is None: # fallback to file folder
            self.base_update_path = os.path.dirname(__file__)
        self.features_path = os.path.join(self.base_update_path,'features/')
        self.features_dim = self.global_conf["FE_features_dim"]

        mkpath(self.features_path)

    def compute_features(self, new_files, startid):
        import subprocess as sub
        # create file listing images to be processed
        img_filename = os.path.join(self.features_path, str(startid)+'.txt')
        featurename = img_filename[:-4] + '-features'
        featurefilename = featurename + '_fc7.dat'
        if new_files:
            f = open(img_filename,'w')
            #print new_files
            f.writelines([filename+'\n' for filename in new_files])
            f.close()
        else:
            f = open(featurefilename,"wb")
            f.close()
            return featurefilename, 0
        if self.sentibank_path is None: # fallback to file folder
            self.sentibank_path = os.path.join(os.path.dirname(__file__),'sentibank/')
        print "[SentiBankCmdLine.compute_features: log] sentibank_path is {}.".format(self.sentibank_path)
        mkpath(self.features_path)
        print "[SentiBankCmdLine.compute_features: log] features_path is {}.".format(self.features_path)

        # set parameters and filenames for feature extraction process
        feature_num = self.features_dim
        testname = img_filename[:-4] + '-test.txt'
        protoname = img_filename[:-4] + '-test.prototxt'

        # format input images list for caffe, i.e. adding dummy label 0
        f = open(testname,'w')
        ins_num = 0
        for line in open(img_filename):
            imgname = line.replace('\n','')
            if len(imgname)>2:
                ins_num = ins_num + 1
                f.write(imgname+' 0\n')
        f.close()

        batch_size = min(64,ins_num)
        if batch_size == 0: # no images?
            return featurefilename,0
        iteration = int(math.ceil(ins_num/float(batch_size)))
        print 'feature extraction image_number:', ins_num, 'batch_size:', batch_size, 'iteration:', iteration
        f = open(self.sentibank_path+'test.prototxt')
        proto = f.read()
        f.close()
        proto = proto.replace('test.txt',testname).replace('batch_size: 1','batch_size: '+str(batch_size))
        proto = proto.replace('imagenet_mean.binaryproto',self.sentibank_path+'imagenet_mean.binaryproto')
        f = open(protoname,'w');
        f.write(proto)
        f.close()
        command = self.sentibank_path+'extract_nfeatures_gpu '+self.sentibank_path+'caffe_sentibank_train_iter_250000 '+protoname+ ' fc7 '+featurename+'_fc7 '+str(iteration)+' '+self.device;
        print "[SentiBankCmdLine.compute_features: log] command {}.".format(command)
        sys.stdout.flush()
        output, error = sub.Popen(command.split(' '), stdout=sub.PIPE, stderr=sub.PIPE).communicate()
        print "[SentiBankCmdLine.compute_features: log] output {}.".format(output) 
        print "[SentiBankCmdLine.compute_features: log] error {}.".format(error)
        sys.stdout.flush()
        #os.system(command)
        os.remove(protoname)
        os.remove(testname)
        return featurefilename, ins_num
