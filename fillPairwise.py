import os,sys,json
import struct,time
import MySQLdb
# http://happybase.readthedocs.org/en/latest/user.html
import happybase

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

def writeEnded(update_id,last_id,biggest_dbid,worker):
        # State that we have finised process one batch
        db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
        c=db.cursor()
        pid = os.getpid()
        query="update pairwise_infos set ended=TRUE where update_id="+str(update_id)+" AND last_id="+str(last_id)+" AND worker="+str(worker)+" AND biggest_dbid="+str(biggest_dbid)+" AND proc_id="+str(pid)+";"
        #print query
        c.execute(query)
        db.commit()
        db.close()

def writeStart(update_id,last_id,biggest_dbid,worker):
	# State that we will process one batch
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
        c=db.cursor()
        pid = os.getpid()
        query="insert into pairwise_infos (update_id,last_id,biggest_dbid,worker,proc_id) values ("+str(update_id)+","+str(last_id)+","+str(biggest_dbid)+","+str(worker)+","+str(pid)+");"
        #print query
        c.execute(query)
        db.commit()
        db.close()

def cleanError(update_id,last_id,worker,proc_id):
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
        c=db.cursor()
        query="delete from pairwise_infos where update_id="+str(update_id)+" AND last_id="+str(last_id)+" AND worker="+str(worker)+" AND proc_id="+str(proc_id)+";"
        #print query
        c.execute(query)
        db.commit()
        db.close()


def checkWorkerStatus(worker):
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
	c=db.cursor()
	query="select * from pairwise_infos where worker=\""+str(worker)+"\" order by update_id DESC LIMIT 1;"
	#print query
	c.execute(query) 
	remax = c.fetchall()
	#print remax
	if len(remax)>0:
		update_id=remax[0][0]
		last_id=remax[0][1]
		proc_id=remax[0][3]
		ended=remax[0][5]
		#print "Ended",ended
		if not ended:
			print "Error: previous update "+str(update_id)+" of worker "+str(worker)+" not completed (last_id:"+str(last_id)+",proc_id:"+str(proc_id)+")"
			#check if proc_id running
			try:
				os.getpgid(proc_id)
				print "Still running. Leaving."
			except OSError: # not running. delete line
				print "Process is dead. Cleaning...",
				cleanError(update_id,last_id,worker,proc_id)
				print "Leaving."				
			quit()
	return True

def getUpdateInfos():
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
	c=db.cursor()
	query="select * from pairwise_infos order by update_id DESC LIMIT 1;"
	#print query
	c.execute(query) 
	remax = c.fetchall()
	#print remax
	if len(remax)>0:
		update_id=remax[0][0]+1
		last_id=remax[0][1]
	else: 
		update_id=1
		last_id=0
	#print "update_id:",update_id,"last_id:",last_id
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
	#print "Biggest ID in LOCAL DB:",biggest_dbid
	return biggest_dbid # Biggest HT ID or unique ID?

def getImagesInfos(last_id,pairwise_batch_size):
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
	c=db.cursor()
	query="select * from uniqueIds where id>\""+str(last_id)+"\" order by id ASC LIMIT "+str(pairwise_batch_size)+";"
	#print query
	c.execute(query) #Should we use id or htid here?
	remax = c.fetchall()
	#print remax
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
	
	t0 = time.time()
	if len(sys.argv)>5:
		print  "This program fill the HT HBase with near duplicate links between similar images.\nUsage: python fillPairwise.py [worker_id] [batch_size] [post_ranking_ratio] [get_duplicate=1] [near_dup=1] [near_dup_th=0.15]"
		exit()

	# Startup update
	worker_id=1
	if len(sys.argv)>1:
		worker_id = int(sys.argv[1])
	# If this worker is already running we would have quit
	if checkWorkerStatus(worker_id): 
		print "Starting worker",str(worker_id)
	pairwise_batch_size = 4
	if len(sys.argv)>2:
		pairwise_batch_size = int(sys.argv[2])
	[update_id,last_id]=getUpdateInfos()
	biggest_dbid=getBiggestDBId()
	writeStart(update_id,last_id+pairwise_batch_size,biggest_dbid,worker_id)

	pairwise_filename = "update/logs/pairwise"+str(update_id)
	logname = pairwise_filename+'.log'
	flog=open(logname, 'w')
	sim_limit = 10000 # Does not really matter, big enough to return all near duplicates.
	global_var = json.load(open('global_var_all.json'))
	print >>flog,pairwise_filename,"Worker:",str(worker_id),"Update_id:",str(update_id)
	ratio = '0.0001'
	if len(sys.argv)>3:
		ratio = sys.argv[3]
	get_dup = 1
	dupstr = '_dup'
	if len(sys.argv)>4:
		get_dup = int(sys.argv[4])
		if get_dup==0:
			dupstr=''
	near_dup = 1
	if len(sys.argv)>5:
		near_dup = int(sys.argv[5])
	near_dup_th = 0.15
	neardupstr=''
	if near_dup:
		neardupstr='_neardup'+str(near_dup_th)
	

	feature_num = 4096
	simname = pairwise_filename + '-sim.txt'
	featurename = pairwise_filename + '-features'
	featurefilename = featurename+'_fc7.dat'
	now=datetime.datetime.now()

	#outputname = pairwise_filename[:-4] + '-sim_'+ratio+dupstr+neardupstr+'.json'

	ins_num = 0
	always_recompute = 0;
	prefix = './' # should be empty for windows
		
	# Get one batch of pairwise_batch_size images
	images_infos = getImagesInfos(last_id,pairwise_batch_size)
	if len(images_infos)<pairwise_batch_size:
		print >>flog,"Not enough images for this batch: ",str(len(images_infos))
		quit()

	# Get these features
	f_pre = open(featurename,'wb')
	#print images_infos
	ht_ids=[]
	for one_img in images_infos:
		ht_ids.append(one_img[1])
		feat_id=one_img[3]
		f_pre.write(struct.pack('i',feat_id))
	f_pre.close()
	command = prefix+'get_precomp_feats '+featurename+' '+featurefilename+' >> '+logname+' 2>&1';
	print >>flog,command
	flog.close()
	os.system(command)
	flog=open(logname, 'a')

	# Compare with images in DB
	command = prefix+'hashing '+featurefilename + ' 256 '+ratio+' >> '+logname+' 2>&1';
	print >>flog,command
	flog.close()
	os.system(command)
	flog=open(logname, 'a')
	os.rename(featurename + '_fc7-sim.txt',simname)

	# Process output
	f = open(simname);
	sim =[]
	sim_score=[]
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
	c=db.cursor()
	sql='SELECT htid,id FROM uniqueIds WHERE id in (%s) ORDER BY FIELD(id, %s)' 

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
		print >>flog,"Getting duplicates"
		new_sim = []
		new_sim_score = []
		dup_list = []
		sql='SELECT htid,uid FROM fullIds WHERE uid in (%s) ORDER BY FIELD(uid, %s)' 
		for i in range(0,pairwise_batch_size):	
			new_sim.append([])
			new_sim_score.append([])
			dup_list.append([])
			# We have to fill HBase rows for duplicates too.
                        in_p=', '.join(map(lambda x: '%s', [ht_ids[i]]))
                        sqlq = sql % (in_p,in_p)
                        c.execute(sqlq, [ht_ids[i],ht_ids[i]]) 
                        tmpresult = c.fetchall()
			for k in tmpresult:
				if k[0]!=ht_ids[i]:
					dup_list[i].append(k[0])
			if not sim[i]: # empty
				continue
			query_num = [simj[0] for simj in sim[i]]
			in_p=', '.join(map(lambda x: '%s', query_num))
			sqlq = sql % (in_p,in_p)
			c.execute(sqlq, query_num*2) # query_num*2 duplicates the list.
			tmpresult = c.fetchall()
			#print len(tmpresult)
			p = 0
			for k in tmpresult:
				if sim[i][p][0]!=k[1]:
					p = p+1
				new_sim[i].append(k[0])
				new_sim_score[i].append(str(max(0,float(sim_score[i][p]))))		
		sim = new_sim
		sim_score = new_sim_score
			
	db.close()


	print >>flog,len(sim),sim
	print >>flog,len(dup_list),dup_list
	print >>flog,len(sim_score),sim_score
	print >>flog,len(ht_ids),ht_ids
	# Fill HBase
	# https://happybase.readthedocs.org/en/latest/user.html#performing-batch-mutations
	# This may hang?...
	connection = happybase.Connection('10.1.94.57')
	connection.tables()
	tab = connection.table('aaron_memex_ht-images')
	b = tab.batch()
	for i in range(0,pairwise_batch_size):
		sim_str = ','.join(map(str, sim[i]))
		sim_dist = ','.join(map(str, sim_score[i]))
		b.put(''+str(ht_ids[i])+'',{'meta:columbia_near_dups' : ''+sim_str+''})
		b.put(''+str(ht_ids[i])+'',{'meta:columbia_near_dups_dist' : ''+sim_dist+''})
		b.put(''+str(ht_ids[i])+'',{'meta:columbia_near_dups_biggest_dbid' : ''+str(biggest_dbid)+''})
		for dup in dup_list[i]:
			b.put(''+str(dup)+'',{'meta:columbia_near_dups' : ''+sim_str+''})
	                b.put(''+str(dup)+'',{'meta:columbia_near_dups_dist' : ''+sim_dist+''})
        	        b.put(''+str(dup)+'',{'meta:columbia_near_dups_biggest_dbid' : ''+str(biggest_dbid)+''})

	b.send()
	connection.close()
	
	#Cleaning	
	os.remove(simname)
	os.remove(featurename)
	os.remove(featurefilename)
	
	# Mark update as finished.
	writeEnded(update_id,last_id+pairwise_batch_size,biggest_dbid,worker_id)

	print >>flog,'Pairwise compute time: ', time.time() - t0
	flog.close()
