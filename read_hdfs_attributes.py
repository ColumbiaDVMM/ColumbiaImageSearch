import os,sys
import os.path as osp
import json
import pickle
import MySQLdb
import random
import datetime
import struct
from sklearn import svm
import numpy as np

global_var = json.load(open('./global_var_all.json'))
isthost=global_var['ist_db_host']
istuser=global_var['ist_db_user']
istpwd=global_var['ist_db_pwd']
istdb=global_var['ist_db_dbname']
localhost=global_var['local_db_host']
localuser=global_var['local_db_user']
localpwd=global_var['local_db_pwd']
localdb=global_var['local_db_dbname']

attr_dir="./ISIweakLabels/"
base_hdfs_path=attr_dir+"trial113"
feature_num = 4096
use_svm_weights=True
kernel_type='rbf' # or 'linear'
if use_svm_weights:
    #class_weights_type='balanced'
    class_weights_type='auto'
else:
    class_weights_type=None
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
    #print command
    os.system(command)
    # Read results from precomp_featurefilename
    f_pre=open(precomp_featurefilename,'rb')
    tmp_format=tuple(["f"]*feature_num)
    feat_format=''.join(tmp_format)
    read_struct=struct.Struct(feat_format)
    feats=None
    for pos,feat_id in enumerate(feats_id):
        one_feat = np.asarray(read_struct.unpack(f_pre.read(feature_num*4)),dtype=np.float32)
        if pos==0:
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
    init_query="select id from uniqueIds where htid in (%s);"
    in_p=', '.join(map(lambda x: '%s', image_htid))
    query = init_query % (in_p,)
    #print query
    #print image_htid
    #print query % tuple(image_htid)
    c.execute(query,tuple(image_htid))
    remax = c.fetchall()
    #print remax
    feats_id=[]
    for i in range(len(remax)):
        feats_id.append(int(remax[i][0]))
    db.close()
    return feats_id

# Get all images from an ad
def getImageHtIdsFromAdId(ad_id):
    db=MySQLdb.connect(host=isthost,user=istuser,passwd=istpwd,db=istdb)
    c=db.cursor()
    sql='select id from images where ads_id='+str(ad_id)
    c.execute(sql)
    re = c.fetchall()
    db.close()
    return [one_img[0] for one_img in re]


if __name__ == "__main__":

  # First get all ads, attributes and images ids.
  pickle_file = attr_dir+"all_attr_data.pkl"
  print("Looking for file {}".format(pickle_file))
  if not osp.isfile(pickle_file):
     print("File {} not found, recomputing.".format(pickle_file))

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
     all_attr_data['attr_vals']=attr_vals
     pickle.dump(all_attr_data,open(pickle_file,"wb"))

  else:
     print("File {} found, loading.".format(pickle_file))
     all_attr_data=pickle.load(open(pickle_file,"rb"))

  # Now we should have all attributes and corresponding images ids.
  # Start training SVMs
  for attr in all_attr_data['attr_set']:
   one_attr = attr.rstrip()
   data_attr='data_'+str(one_attr)+'.pkl'
   if class_weights_type:
       svm_model_file='svmmodel_'+str(one_attr)+'_'+kernel_type+'_balanced.pkl'
   else:
       svm_model_file='svmmodel_'+str(one_attr)+'_'+kernel_type+'.pkl'
   if osp.isfile(data_attr) and osp.isfile(svm_model_file):
    data=pickle.load(open(data_attr,'rb'))
    clf=pickle.load(open(svm_model_file,'rb'))
    test=data['test']
    test_feats_id=[]
    test_imgslabels=[]
    labels=[]
    label_id=0
    for ind,val in enumerate(sorted(all_attr_data['attr_vals'][one_attr])):
      one_val = val.rstrip()
      print("Getting test images with value: {}.".format(one_val))
      labels.append(label_id)
      for sample in test[one_val]:
        #print sample
        sys.stdout.flush()
        imgs_id=all_attr_data['all_imgs'][sample]
        if not imgs_id: # there is no image in this ad!
            continue
        sample_feat_ids=get_precompfeatid_fromhtid(imgs_id)
        if not sample_feat_ids:
            continue
        test_feats_id.extend(sample_feat_ids)
        test_imgslabels.extend([int(ind)]*len(sample_feat_ids))
    print("Looking for {} test features.".format(len(test_feats_id)))
    test_feats=get_all_precomp_feats(test_feats_id)
    predictions=clf.predict(test_feats)
    print predictions
    res={}
    res['predictions']=predictions
    res['labels']=test_imgslabels
    if class_weights_type:
        res_file='res_'+str(one_attr)+'_'+kernel_type+'_balanced.pkl'
    else:
        res_file='res_'+str(one_attr)+'_'+kernel_type+'.pkl'
    pickle.dump(res,open(res_file,"wb"))
   else: 
    if not osp.isfile(data_attr):
      print("Processing attribute {}".format(one_attr))
      pos={}
      train={}
      test={}
      # Get all samples annotated with this attribute, samples here are ads.
      for val in all_attr_data['attr_vals'][one_attr]:
        one_val = val.rstrip()
        print("Getting positive samples of {}.".format(one_val))
        pos[one_val] = [i for i, x in enumerate(all_attr_data['all_vals']) if x[0]==one_attr and x[1]==one_val]
        #print pos[one_val]
        # Random sample 2/3 of each value as training samples and the last 1/3 at test.
        train[one_val] = [pos[one_val][i] for i in sorted(random.sample(xrange(len(pos[one_val])), int(len(pos[one_val])*2./3)))]
        test[one_val] = list(set(pos[one_val])-set(train[one_val]))
        print("We have {} training samples.".format(len(train[one_val])))
        #labels_train.extend([label_id]*len(train[one_val]))
        #labels_test.extend([label_id]*len(test[one_val]))
      train_feats_id=[]
      train_imgslabels=[]
      for ind,val in enumerate(sorted(all_attr_data['attr_vals'][one_attr])):
        one_val = val.rstrip()
        print("Getting images with value: {}.".format(one_val))
        for sample in train[one_val]:
          #print sample
          sys.stdout.flush()
          imgs_id=all_attr_data['all_imgs'][sample]
          if not imgs_id: # there is no image in this ad!
            continue
          sample_feat_ids=get_precompfeatid_fromhtid(imgs_id)
          if not sample_feat_ids:
            continue
          train_feats_id.extend(sample_feat_ids)
          train_imgslabels.extend([int(ind)]*len(sample_feat_ids))
      data={}
      data['train_feats_ids']=train_feats_id
      data['train_imgslabels']=train_imgslabels
      data['test']=test
      data['train']=train
      pickle.dump(data,open(data_attr,'wb'))
    else:
      data=pickle.load(open(data_attr,'rb'))
    train_feats_id=data['train_feats_ids']
    train_imgslabels=data['train_imgslabels']
    # Need to convert ads indices to images indices somehow...
    print("Looking for {} features.".format(len(train_feats_id)))
    train_feats=get_all_precomp_feats(train_feats_id)
    print train_imgslabels
    clf = svm.SVC(kernel=kernel_type,class_weight=class_weights_type)
    clf.fit(train_feats, train_imgslabels)
    pickle.dump(clf,open(svm_model_file,'wb'))
