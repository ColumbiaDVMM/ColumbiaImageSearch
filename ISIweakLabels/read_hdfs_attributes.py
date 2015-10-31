import time
import json
import pickle
import MySQLdb

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
#base_hdfs_path="hdfs://memex:/user/worker/crf/trial113"
all_ads=[]
all_attr=[]
all_imgs=[]
all_vals=dict()

def getImageHtIdsFromAdId(ad_id):
	db=MySQLdb.connect(host=isthost,user=istuser,passwd=istpwd,db=istdb)
	c=db.cursor()
	sql='select id from images where ads_id='+str(ad_id)
	c.execute(sql)
	re = c.fetchall()
	db.close()
	return [one_img[0] for one_img in re]


for part in xrange(0, 16):
 filename = base_hdfs_path+"/part-000"+"%02d" % part
 fp = open(filename)
 for line in fp:
	[ads_id,attr,val]=line.split('\t')
	#print val.rstrip()
	all_ads.append(ads_id)
	all_imgs.append(getImageHtIdsFromAdId(ads_id))
	all_attr.append(attr)
	if attr not in all_vals.keys():
		all_vals[attr]=[]
	all_vals[attr].append(val.rstrip())
 fp.close()

print len(all_ads)
for one_attr in set(all_attr):
	print one_attr+":",set(all_vals[one_attr])

all_attr_data={}
all_attr_data['all_ads']=all_ads
all_attr_data['all_attr']=all_attr
all_attr_data['all_vals']=all_vals
all_attr_data['all_imgs']=all_imgs
pickle.dump(all_attr_data,open('all_attr_data.pkl',"wb"))
