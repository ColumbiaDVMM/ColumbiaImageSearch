import os,sys,json
import struct,time
import MySQLdb

from array import *
#sys.path.append('libsvm-3.18/python')
#from svmutil import *
from collections import OrderedDict 
import math
import ntpath
import hashlib
import datetime
import pickle
import urllib

import json
ist_down=False
feature_num=4096

global_var = json.load(open('global_var_all.json'))
isthost=global_var['ist_db_host']
istuser=global_var['ist_db_user']
istpwd=global_var['ist_db_pwd']
istdb=global_var['ist_db_dbname']
localhost=global_var['local_db_host']
localuser=global_var['local_db_user']
localpwd=global_var['local_db_pwd']
localdb=global_var['local_db_dbname']

def get_all_precomp_feats(feats_id):
    precomp_featurename = 'precomp-features'
    precomp_featurefilename = 'precomp-feats.dat'
    now=datetime.datetime.now()
    # fill features indices
    f_pre = open(precomp_featurename,'wb')
    for feat_id in feats_id:
        f_pre.write(struct.pack('i',feat_id))
    f_pre.close()
    # Execute get_precomp_feats
    command = '../get_precomp_feats '+precomp_featurename+' '+precomp_featurefilename
    #print command
    os.system(command)
    # Read results from precomp_featurefilename
    f_pre=open(precomp_featurefilename,'rb')
    feats=None
    for feat_id in feats_id:
        one_feat = f_pre.read(feature_num*4)
        if not feats:
            feats=one_feat
        else:
            #concatenante with np
            feats=np.vstack((feats,one_feat))
    f_pre.close()
    return feats

def get_one_feat(start):
    db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
    c=db.cursor()
    query="select id,location from uniqueIds where htid>\""+start+"\" order by htid ASC LIMIT 1;"
    print query
    c.execute(query)
    remax = c.fetchall()
    print remax
    if len(remax):
        feat_id = int(remax[0][0])
        location = int(remax[0][1])
    else:
        feat_id = 0
        location = None
    db.close()
    return feat_htid

def compute_feats(images,testname,protoname):
    nb_images=len(images)
    featurefilename="feats.dat"
    batch_size = min(64,nb_images)
    iteration = int(math.ceil(nb_images/float(batch_size)))
    print 'image_number:', nb_images, 'batch_size:', batch_size, 'iteration:', iteration
    f = open('test.prototxt')
    proto = f.read()
    f.close()
    proto = proto.replace('test.txt',testname.replace('\\','/')).replace('batch_size: 1','batch_size: '+str(batch_size))
    f = open(protoname,'w');
    f.write(proto)
    f.close()
    command = prefix+'extract_nfeatures caffe_sentibank_train_iter_250000 '+protoname+ ' fc7,prob '+featurefilename.replace('\\','/')+','+fresh_featurefilename.replace('\\','/')+'_prob '+str(iteration)+' '+device;
    print command
    os.system(command)
    print 'sentibank time: ', time.time() - t0
    # Read results from precomp_featurefilename
    f_feat=open(featurefilename,'rb')
    feats=None
    for feat_id in range(nb_images):
        one_feat = f_feat.read(feature_num*4)
        if not feats:
            feats=one_feat
        else:
            #concatenante with np
            feats=np.vstack((feats,one_feat))
    f_feat.close()
    return feats

def fill_test(testname,feats_url):
    image_id=0
    f = open(testname,'w')
    for image_url in feats_url:
        urllib.urlretrieve(image_url, str(image_id)+".jpg")
        f.write(str(image_id)+".jpg"+' 0\n')
        image_id=image_id+1
    f.close()

if __name__ == '__main__':
    feats_id=[]
    feats_url=[]
    testname = 'temp_test.txt'
    protoname = 'temp_test.prototxt'
    device = 'GPU'
    for start_id in range(70000000,110000000,1000000):
        onefeat,oneurl=get_one_feat(start_id)
        feats_id.append(onefeat)
        feats_url.append(oneurl)
    # Pre-computed features
    pre_feats=get_all_precomp_feats(feats_id)
    # Recompute features
    fill_test(testname,feats_url)
    recomp_feats=compute_feats(feats_url,testname,protoname)
    # Compare
    for one_img in range(len(feats_id)):
        dist=np.linalg.norm(pre_feats[one_img,:],recomp_feats[one_img,:])
        print feats_id[one_img],dist
