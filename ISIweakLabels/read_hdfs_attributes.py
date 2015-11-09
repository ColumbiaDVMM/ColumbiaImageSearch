import os
import os.path as osp
import json
import pickle
import MySQLdb
import random
import datetime
import struct
from sklearn import svm
import numpy as np

global_var = json.load(open('../global_var_all.json'))
isthost=global_var['ist_db_host']
istuser=global_var['ist_db_user']
istpwd=global_var['ist_db_pwd']
istdb=global_var['ist_db_dbname']
localhost=global_var['local_db_host']
localuser=global_var['local_db_user']
localpwd=global_var['local_db_pwd']
localdb=global_var['local_db_dbname']

base_hdfs_path="./trial113"
feature_num = 4096
#base_hdfs_path="hdfs://memex:/user/worker/crf/trial113"

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
    command = './get_precomp_feats '+precomp_featurename+' '+precomp_featurefilename
    print command
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

# Get feature from image htid
# Beware that maybe some images where not processed because their download failed.
def get_precompfeatid_fromhtid(image_htid):
    db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
    c=db.cursor()
    query="select id from uniqueIds where htid=\""+image_htid+"\";"
    print query
    c.execute(query)
    remax = c.fetchall()
    print remax
    if len(remax):
        feat_id = int(remax[0][0])
    else:
        feat_id = 0
    db.close()
    return feat_id

# Get all images from an ad
def getImageHtIdsFromAdId(ad_id):
    db=MySQLdb.connect(host=isthost,user=istuser,passwd=istpwd,db=istdb)
    c=db.cursor()
    sql='select id from images where ads_id='+str(ad_id)
    c.execute(sql)
    re = c.fetchall()
    db.close()
    return [one_img[0] for one_img in re]

# First get all ads, attributes and images ids.

if not osp.isfile('all_attr_data.pkl'):

    all_ads=[]
    all_imgs=[]
    all_vals=[]
    attr_set=set()
    attr_vals={}

    for part in xrange(0, 16):
     filename = base_hdfs_path+"/part-000"+"%02d" % part
     fp = open(filename)
     for line in fp:
        [ads_id,attr,val]=line.split('\t')
        # values for each ads
        all_ads.append(ads_id)
        all_imgs.append(getImageHtIdsFromAdId(ads_id))
        all_vals.append([attr,val.rstrip()])
        # set of attributes and their values
        attr_set.add(attr)
        if attr not in attr_vals.keys():
            attr_vals[attr]=set()
        attr_vals[attr].add(val)
     fp.close()

    print len(all_ads)
    for one_attr in attr_set:
        print one_attr+":",attr_vals[one_attr]

    all_attr_data={}
    all_attr_data['all_ads']=all_ads
    all_attr_data['all_vals']=all_vals
    all_attr_data['all_imgs']=all_imgs
    all_attr_data['attr_set']=attr_set
    all_attr_data['attr_vals']==attr_vals
    pickle.dump(all_attr_data,open('all_attr_data.pkl',"wb"))

else:

    all_attr_data=pickle.load(open('all_attr_data.pkl',"rb"))

# Now we should have all attributes and corresponding images ids.
# Start training SVMs
for one_attr in all_attr_data['attr_set']:
    print("Processing attribute {}".format(one_attr))
    pos={}
    train={}
    test={}
    labels_train=[]
    labels_test=[]
    label_id=0
    # Get all samples annotated with this attribute
    for one_val in all_attr_data['attr_vals'][one_attr]:
        print("Getting positive samples of {}".format(one_val))
        pos[one_val] = [i for i, x in enumerate(all_attr_data['all_vals']) if x[0]==one_attr and x[1]==one_val]
        # Random sample 2/3 of each value as training samples and the last 1/3 at test.
        train[one_val] = [pos[one_val][i] for i in sorted(random.sample(xrange(len(pos['one_val'])), int(len(pos['one_val'])*2./3)))]
        test[one_val] = list(set(pos[one_val])-set(train[one_val]))
        labels_train.extend([label_id]*len(train[one_val]))
        labels_test.extend([label_id]*len(test[one_val]))
        label_id=label_id+1
    train_feats_id=[]
    for one_val in all_attr_data['all_vals'][one_attr]:
        for sample in train[one_val]:
            train_feats_id.append(get_precompfeatid_fromhtid(all_attr_data['all_imgs'][sample]))
    train_feats=get_all_precomp_feats(train_feats_id)
    clf = svm.SVC()
    clf.fit(train_feats, labels_train)
    pickle.dump(clf,open('svmmodel_'+str(one_attr)+'.pkl','wb'))
    data={}
    data['train_ids']=train
    data['test_ids']=test
    data['labels_train']=labels_train
    data['labels_test']=labels_test
    pickle.dump(data,open('data_'+str(one_attr)+'.pkl','wb'))