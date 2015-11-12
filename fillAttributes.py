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

def writeEnded(update_id,last_id,worker):
        # State that we have finised process one batch
        db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
        c=db.cursor()
        pid = os.getpid()
        query="update attributes_infos set ended=TRUE where update_id="+str(update_id)+" AND last_id="+str(last_id)+" AND worker="+str(worker)+" AND proc_id="+str(pid)+";"
        #print query
        c.execute(query)
        db.commit()
        db.close()

def writeStart(update_id,last_id,worker):
	# State that we will process one batch
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
        c=db.cursor()
        pid = os.getpid()
        query="insert into attributes_infos (update_id,last_id,worker,proc_id) values ("+str(update_id)+","+str(last_id)+","+str(worker)+","+str(pid)+");"
        #print query
        c.execute(query)
        db.commit()
        db.close()

def cleanError(update_id,last_id,worker,proc_id):
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
        c=db.cursor()
        query="delete from attributes_infos where update_id="+str(update_id)+" AND last_id="+str(last_id)+" AND worker="+str(worker)+" AND proc_id="+str(proc_id)+";"
        #print query
        c.execute(query)
        db.commit()
        db.close()


def checkWorkerStatus(worker,batchsize):
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
	c=db.cursor()
	query="select * from attributes_infos where worker=\""+str(worker)+"\" AND ended=FALSE order by update_id DESC LIMIT 1;"
	#print query
	c.execute(query) 
	remax = c.fetchall()
	#print remax
	[update_idok,last_idok]=getUpdateInfos()
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
				if proc_id==0: # manually reset
					raise OSError
				os.getpgid(proc_id)
				print "Still running. Leaving."
				quit()
			except OSError: # not running. delete line
				print "Process is dead. Cleaning...",
				cleanError(update_id,last_id,worker,proc_id)
				print "Restarting this batch..."
				update_idok=update_id
				last_idok=last_id-batchsize # Won't work if failed process was using another batchsize				
	return update_idok,last_idok

def getUpdateInfos():
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
	c=db.cursor()
	query="select * from attributes_infos order by update_id DESC LIMIT 1;"
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

def getImagesInfos(last_id,batch_size):
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
	c=db.cursor()
	query="select * from uniqueIds where id>\""+str(last_id)+"\" order by id ASC LIMIT "+str(batch_size)+";"
	#print query
	c.execute(query) #Should we use id or htid here?
	remax = c.fetchall()
	#print remax
	db.close()
	return remax

if __name__ == '__main__':
	
	t0 = time.time()
	if len(sys.argv)>5:
		print  "This program fill the HT HBase with attributes of images.\nUsage: python fillAttributes.py [worker_id] [batch_size]"
		exit()

	# Startup update
	worker_id=1
	if len(sys.argv)>1:
		worker_id = int(sys.argv[1])
	# If this worker is already running we would have quit
	batch_size = 100
	if len(sys.argv)>2:
		batch_size = int(sys.argv[2])
	[update_id,last_id] = checkWorkerStatus(worker_id,batch_size)
	print "Starting worker",str(worker_id)
	writeStart(update_id,last_id+batch_size,worker_id)

	att_filename = "update/logs/attributes/logAttributes"+str(update_id)
	logname = att_filename +'.log'
	flog=open(logname, 'w')
	global_var = json.load(open('global_var_all.json'))
	print >>flog,att_filename ,"Worker:",str(worker_id),"Update_id:",str(update_id)

	feature_num = 4096
	simname = att_filename + '-sim.txt'
	featurename = att_filename + '-features'
	featurefilename = featurename +'_fc7.dat'
	now=datetime.datetime.now()

	#outputname = pairwise_filename[:-4] + '-sim_'+ratio+dupstr+neardupstr+'.json'

	ins_num = 0
	always_recompute = 0;
	prefix = './' # should be empty for windows
		
	# Get one batch of pairwise_batch_size images
	images_infos = getImagesInfos(last_id,batch_size)
	if len(images_infos)<batch_size:
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

    # read features
    # precomputed features are l2-normalized...

    # compute attributes

	# push features and attributes to hbase

	print >>flog,len(sim),sim
	print >>flog,len(dup_list),dup_list
	print >>flog,len(sim_score),sim_score
	print >>flog,len(ht_ids),ht_ids
	# Fill HBase
	# https://happybase.readthedocs.org/en/latest/user.html#performing-batch-mutations
	# This may hang?...
	connection = happybase.Connection('10.1.94.57')
	#connection.tables()
	# Connection timeout issue...
	# happybase.hbase.ttypes.IOError: IOError(_message="java.net.SocketTimeoutException: callTimeout=0, callDuration=17367:...
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
