import os
import time
import json
import math
from ..memex_tools.image_dl import mkpath

class SentiBankCmdLine():

    def __init__(self,global_conf_filename):
        self.global_conf = json.load(open(global_conf_filename,'rt'))
        self.features_path = self.global_conf["FE_features_path"]
        #self.sentibank_path = self.global_conf["FE_sentibank_path"]

    def compute_features(self,new_files,startid):
        self.sentibank_path = os.path.join(os.path.dirname(__file__),'sentibank/')
        print "[SentiBankCmdLine.compute_features: log] Set sentibank_path to {}.".format(self.sentibank_path)
        mkpath(self.features_path)

        # create file listing images to be processed
        img_filename = os.path.join(self.features_path,str(startid)+'.txt')
        f = open(img_filename,'w')
        f.writelines([filename+'\n' for filename in new_files])
        f.close()

        # set parameters and filenames for feature extraction process
        device = 'GPU'
        feature_num = 4096
        testname = img_filename[:-4] + '-test.txt'
        protoname = img_filename[:-4] + '-test.prototxt'
        featurename = img_filename[:-4] + '-features'
        featurefilename = featurename+'_fc7.dat'

        # format input images list for caffe
        f = open(testname,'w')
        ins_num = 0
        for line in open(img_filename):
            imgname = line.replace('\n','')
            if len(imgname)>2:
                ins_num = ins_num + 1
                f.write(imgname+' 0\n')
        f.close()

        batch_size = min(64,ins_num)
        iteration = int(math.ceil(ins_num/float(batch_size)))
        print 'feature extraction image_number:', ins_num, 'batch_size:', batch_size, 'iteration:', iteration
        f = open(self.sentibank_path+'test.prototxt')
        proto = f.read()
        f.close()
        proto = proto.replace('test.txt',testname).replace('batch_size: 1','batch_size: '+str(batch_size))
        f = open(protoname,'w');
        f.write(proto)
        f.close()
        command = self.sentibank_path+'extract_nfeatures_gpu '+self.sentibank_path+'caffe_sentibank_train_iter_250000 '+protoname+ ' fc7 '+featurename+'_fc7 '+str(iteration)+' '+device;
        print command
        os.system(command)
        os.remove(protoname)
        os.remove(testname)
        return featurefilename,ins_num
