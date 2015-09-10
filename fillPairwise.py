import os,sys,json
import struct,time
import MySQLdb
# http://happybase.readthedocs.org/en/latest/user.html
import happybase
connection = happybase.Connection('10.1.94.57')
connection.tables()

from array import *
#sys.path.append('libsvm-3.18/python')
#from svmutil import *
from collections import OrderedDict 
import math
import ntpath
import hashlib
import datetime
import pickle

import json
global_var = json.load(open('global_var_all.json'))
isthost=global_var['ist_db_host']
istuser=global_var['ist_db_user']
istpwd=global_var['ist_db_pwd']
istdb=global_var['ist_db_dbname']
localhost=global_var['local_db_host']
localuser=global_var['local_db_user']
localpwd=global_var['local_db_pwd']
localdb=global_var['local_db_dbname']

def getUpdateInfos():
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
	c=db.cursor()
	query="select * from pairwise_infos order by update_id DESC LIMIT 1;"
	print query
	c.execute(query) 
	remax = c.fetchall()
	print remax
	if len(remax)>0:
		update_id=remax[0][0]
		last_id=remax[0][1]
	else: # First update
		update_id=1
		last_id=0
	print update_id,last_id
	db.close()
	return update_id,last_id

def getBiggestDBId(): # Should be the biggest id currently in the DB for potential later update...
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
	c=db.cursor()
	sql='select id from uniqueIds order by id desc limit 1'
	query_id = []
	start_time = time.time()
	c.execute(sql, query_id)
	re = c.fetchall()
	biggest_dbid = re[0][0]
	db.close()
	print "Biggest ID in LOCAL DB:",biggest_dbid
	return biggest_dbid # Biggest HT ID or unique ID?

def getImagesInfos(last_id,pairwise_batch_size):
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
	c=db.cursor()
	query="select * from uniqueIds where id>\""+last_id+"\" order by id ASC LIMIT \""+pairwise_batch_size+"\";"
	print query
	c.execute(query) #Should we use id or htid here?
	remax = c.fetchall()
	print remax
	db.close()
	return remax

def filter_near_dup(nums,dist_ths):
	onum=len(nums)/2
	temp_nums=[]
	for one_num in range(0,onum):
		if float(nums[onum+one_num])>dist_ths:
			return temp_nums
		temp_nums.insert(one_num,nums[one_num])
		temp_nums.insert(len(temp_nums),nums[onum+one_num])
	return temp_nums		
	

if __name__ == '__main__':
	
	# Loop forever here?

	t0 = time.time()
	if len(sys.argv)>5:
		print  "This program fill the HT HBase with near duplicate links between similar images.\nUsage: python fillPairwise.py [batch_size] [post_ranking_ratio] [get_duplicate=1] [near_dup=1] [near_dup_th=0.15]"
		exit()

	# Get some update_id
	[update_id,last_id]=getUpdateInfos()
	biggest_dbid=getBiggestDBId()

	pairwise_filename = "pairwise"+str(update_id)
	logname = pairwise_filename[:-4]+'.log'
	flog=open(logname, 'w')
	sim_limit = 10000 # Does not really matter, big enough to return all near duplicates.
	pairwise_batch_size = 1000
	global_var = json.load(open('global_var_all.json'))
	print >>flog,pairwise_filename,len(sys.argv)
	if len(sys.argv)>1:
		pairwise_batch_size = int(sys.argv[1])
	ratio = '0.0001'
	if len(sys.argv)>2:
		ratio = sys.argv[2]
	get_dup = 1
	dupstr = '_dup'
	if len(sys.argv)>3:
		get_dup = int(sys.argv[3])
		if get_dup==0:
			dupstr=''
	near_dup = 1
	if len(sys.argv)>4:
		near_dup = int(sys.argv[4])
	near_dup_th = 0.15
	neardupstr=''
	if near_dup:
		neardupstr='_neardup'+str(near_dup_th)
	

	feature_num = 4096
	simname = pairwise_filename[:-4] + '-sim.txt'
	featurename = pairwise_filename[:-4] + '-features'
	featurefilename = featurename+'_fc7.dat'
	now=datetime.datetime.now()

	#outputname = pairwise_filename[:-4] + '-sim_'+ratio+dupstr+neardupstr+'.json'

	ins_num = 0
	always_recompute = 0;
		
	# Get one batch of pairwise_batch_size images
	images_infos = getImagesInfos(last_id,pairwise_batch_size)
	if len(images_infos)<pairwise_batch_size:
		print "Not enough images for this batch: ",str(len(images_infos))
		quit()

	# Get these features
	f_pre = open(precomp_featurename,'wb')
	print images_infos
	ht_ids=[]
	for one_img in images_infos:
		ht_ids.append(one_img[1])
		feat_id=one_img[3]
		f_pre.write(struct.pack('i',feat_id))
	f_pre.close()
	command = prefix+'get_precomp_feats '+featurename+' '+featurefilename;
	print command
	os.system(command)

	# Compare with images in DB
	command = prefix+'hashing '+featurefilename + ' 256 '+ratio;
	print command
	os.system(command)
	os.rename(featurename + '_fc7-sim.txt',simname)

	# Process output
	f = open(simname);
	sim =[]
	sim_score=[]
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
	c=db.cursor()
	sql='SELECT NULL,location,NULL,NULL,htid,sha1 FROM uniqueIds WHERE id in (%s) ORDER BY FIELD(id, %s)' 

	# get similar images
	count = 0
	for line in f:
		#sim_index.append([])
		nums=line.replace(' \n','').split(' ')
		if near_dup: #filter near duplicate here
			nums=filter_near_dup(nums,float(near_dup_th))
		#print nums
		onum = len(nums)/2
		n = min(sim_limit,onum)
		#print n
		if n==0: # no returned images, e.g. no near duplicate
			sim.append(())
			sim_score.append([])
			continue
		query_num = []
		for i in range(0,n):
			query_num.append(int(nums[i])+1)
		in_p=', '.join(map(lambda x: '%s', query_num))
		sqlq = sql % (in_p,in_p)
		#print sqlq
		c.execute(sqlq, query_num*2)
		sim.append(c.fetchall())
		sim_score.append(nums[onum:onum+n])
		count = count + 1
		if count == pairwise_batch_size:
			break
	f.close()
		
	if get_dup:
		new_sim = []
		new_sim_score = []
		if not global_var['demo']:
			sql='SELECT htid,uid FROM fullIds WHERE uid in (%s) ORDER BY FIELD(uid, %s)' 
		else:
			sql='SELECT htid,uid,url,location,ads_url,ads_id FROM fullIds WHERE uid in (%s) ORDER BY FIELD(uid, %s)' 
		for i in range(0,pairwise_batch_size):	
			new_sim.append([])
			new_sim_score.append([])
			if not sim[i]: # empty
				continue
			query_num = [simj[4] for simj in sim[i]]
			in_p=', '.join(map(lambda x: '%s', query_num))
			sqlq = sql % (in_p,in_p)
			c.execute(sqlq, query_num*2)
			tmpresult = c.fetchall()
			#print len(tmpresult)
			p = 0
			for k in tmpresult:
				if sim[i][p][4]!=k[1]:
					p = p+1
				if not global_var['demo']:
					new_sim[i].append((sim[i][p][0],sim[i][p][1],sim[i][p][2],sim[i][p][3],k[0],sim[i][p][5]))
				else:
					new_sim[i].append((k[2],k[3],k[4],k[5],k[0],sim[i][p][5]))
				new_sim_score[i].append(sim_score[i][p])
				
		sim = new_sim
		sim_score = new_sim_score
			
	db.close()


	print sim
	print sim_score
	print ht_ids
	# Fill HBase
	# https://happybase.readthedocs.org/en/latest/user.html#performing-batch-mutations
	# tab = connection.table('aaron_memex_ht-images')
	# # This may hang...
	# # tab.put('1',{'meta:columbia_near_dups' : ''})
	# # tab.put('1',{'meta:columbia_near_dups_simscore' : ''})
	
	# output = []
	# for i in range(0,pairwise_batch_size):	
	# 	output.append(dict())
	# 	output[i]['similar_images']= OrderedDict([['number',len(sim[i])],['image_urls',[]],['cached_image_urls',[]],['page_urls',[]],['ht_ads_id',[]],['ht_images_id',[]],['sha1',[]],['distance',[]]])
	# 	for simj in sim[i]:
	# 		output[i]['similar_images']['image_urls'].append(simj[0])
	# 		output[i]['similar_images']['cached_image_urls'].append(simj[1])
	# 		output[i]['similar_images']['page_urls'].append(simj[2])
	# 		output[i]['similar_images']['ht_ads_id'].append(simj[3])
	# 		output[i]['similar_images']['ht_images_id'].append(simj[4])
	# 		output[i]['similar_images']['sha1'].append(simj[5])
	# 	output[i]['similar_images']['distance']=sim_score[i]
	# outp = OrderedDict([['number',nb_query],['images',output]])
	# json.dump(outp, open(outputname,'w'),indent=4, sort_keys=False)		
 
	#Cleaning	
	#os.remove(testname)
	
	# Mark update as finished.

	print 'query time: ', time.time() - t0
