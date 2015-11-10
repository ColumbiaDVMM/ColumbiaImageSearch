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
import numpy as np

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

prefix='./'

def get_all_precomp_feats(feats_id):
    precomp_featurename = 'temp_precomp-features'
    precomp_featurefilename = 'temp_precomp-feats.dat'
    now=datetime.datetime.now()
    # fill features indices
    f_pre = open(precomp_featurename,'wb')
    for feat_id in feats_id:
        f_pre.write(struct.pack('i',feat_id))
    f_pre.close()
    # Execute get_precomp_feats
    command = prefix+'get_precomp_feats '+precomp_featurename+' '+precomp_featurefilename
    #print command
    os.system(command)
    # Read results from precomp_featurefilename
    f_pre=open(precomp_featurefilename,'rb')
    feats=None
    for feat_id in feats_id:
        one_feat = np.fromfile(f_pre,dtype=np.float32,count=feature_num)
        if feats==None:
            feats=one_feat
        else:
            #concatenante with np
            feats=np.vstack((feats,one_feat))
    f_pre.close()
    return feats

def get_one_feat(start):
    db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
    c=db.cursor()
    #query="select id,location,htid from uniqueIds where htid>\""+str(start)+"\" order by htid ASC LIMIT 1;"
    query="select id,location,htid from uniqueIds where id=\""+str(start)+"\";"
    print query
    c.execute(query)
    remax = c.fetchall()
    print remax
    if len(remax):
        feat_id = int(remax[0][0])
        location = str(remax[0][1])
        htid = int(remax[0][2])
    else:
        feat_id = 0
        location = None
	htid = 0
    db.close()
    return feat_id,location,htid

def compute_feats(images,testname,protoname):
    nb_images=len(images)
    print nb_images
    featurefilename="tmp_feats"
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
    command = prefix+'extract_nfeatures caffe_sentibank_train_iter_250000 '+protoname+ ' fc7,prob '+featurefilename.replace('\\','/')+','+featurefilename.replace('\\','/')+'_prob '+str(iteration)+' '+device;
    print command
    os.system(command)
    # Read results from precomp_featurefilename
    f_feat=open(featurefilename+".dat",'rb')
    feats=None
    for feat_id in range(nb_images):
        one_feat = np.fromfile(f_feat,dtype=np.float32,count=feature_num)
        if feats==None:
            feats=one_feat
        else:
            #concatenante with np
            feats=np.vstack((feats,one_feat))
    f_feat.close()
    return feats

def fill_test(testname,feats_url):
    f = open(testname,'w')
    for image_url in feats_url:
	image_name=image_url.rsplit('/')[-1]
        urllib.urlretrieve(image_url, image_name)
        f.write(image_name+' 0\n')
    f.close()

if __name__ == '__main__':
    feats_id=[]
    feats_url=[]
    feats_htid=[]
    testname = 'temp_test.txt'
    protoname = 'temp_test.prototxt'
    device = 'CPU' #GPU does not work?
    #for start_id in range(70000000,110000000,1000000):
    for start_id in range(1000000,20000000,1000000):
        onefeat,oneurl,one_htid=get_one_feat(start_id)
        feats_id.append(onefeat)
        feats_url.append(oneurl)
	feats_htid.append(one_htid)
    # Pre-computed features
    pre_feats=get_all_precomp_feats(feats_id)
    # Recompute features
    fill_test(testname,feats_url)
    recomp_feats=compute_feats(feats_url,testname,protoname)
    # Compare
    for one_img in range(len(feats_id)):
	#print pre_feats.shape,pre_feats[one_img,:].shape
	#print recomp_feats.shape,recomp_feats[one_img,:].shape
        dist=np.linalg.norm(pre_feats[one_img,:]-recomp_feats[one_img,:])
        print feats_id[one_img],feats_htid[one_img],feats_url[one_img],dist
