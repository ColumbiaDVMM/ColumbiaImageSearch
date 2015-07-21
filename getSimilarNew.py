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

def exist_img_precompfeat(query_sha1):
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
	c=db.cursor()
	query="select id from uniqueIds where sha1=\""+query_sha1+"\";"
	print query
	c.execute(query) #Should we use id or htid here?
	remax = c.fetchall()
	print remax
	if len(remax):
		feat_id = int(remax[0][0])
	else:
		feat_id = 0
	db.close()
	return feat_id

def get_featid_from_SHA1filename(img_filename):
	img_basename=path_leaf(img_filename)
	return exist_img_precompfeat(img_basename[:-4])

def path_leaf(path):
    	head, tail = ntpath.split(path)
    	return tail or ntpath.basename(head)

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
	#currentDir = os.getcwd()
	#os.chdir('deepsentibank')
	if len(sys.argv)<2:
		print  "This program takes one or multiple images as input, and output similar images.\nUsage: python getSimilar.py image_path/image_path_list.txt [similar_image_number] [post_ranking_ratio] [get_duplicate=1] [near_dup=0] [near_dup_th=0.15] [CPU/GPU] [DEVICE_ID=0]"
		exit()
	img_filename = sys.argv[1]
	sim_limit = 100
	global_var = json.load(open('global_var_all.json'))
	print len(sys.argv)
	if len(sys.argv)>2:
		sim_limit = int(sys.argv[2])
	ratio = '0.0001'
	if len(sys.argv)>3:
		ratio = sys.argv[3]
	get_dup = 1
	dupstr = '_dup'
	if len(sys.argv)>4:
		get_dup = int(sys.argv[4])
		if get_dup==0:
			dupstr=''
	near_dup = 0
	if len(sys.argv)>5:
		near_dup = int(sys.argv[5])
	near_dup_th = 0.15
	device = 'CPU'
	if len(sys.argv)>6:
		near_dup_th = float(sys.argv[6])
	neardupstr=''
	if near_dup:
		neardupstr='_neardup'+str(near_dup_th)
	if len(sys.argv)>7 and sys.argv[7]=='GPU':
		device = 'GPU'
		if len(sys.argv)>8 and sys.argv[8].find('DEVICE_ID=')>-1:
			device = device + ' ' + sys.argv[8]
	
	feature_num = 4096
	testname = img_filename[:-4] + '-test.txt'
	protoname = img_filename[:-4] + '-test.prototxt'
	featurename = img_filename[:-4] + '-features'
	precomp_featurename = img_filename[:-4] + '-precomp-features'
	featurefilename = featurename+'_fc7.dat'
	fresh_featurefilename = featurename+'-fresh_fc7'
	precomp_featurefilename = featurename+'-precomp_fc7.dat'
	now=datetime.datetime.now()
	#outputname = img_filename[:-4] + '-sim_'+str(sim_limit)+'_'+ratio+dupstr+'_'+now.strftime('%Y-%m-%d_%H')+'.json'
	outputname = img_filename[:-4] + '-sim_'+str(sim_limit)+'_'+ratio+dupstr+neardupstr+'.json'
	#print outputname

	ins_num = 0
	always_recompute = 0;
		
	if not os.path.exists(outputname):
		simname = featurename + '_fc7-sim_'+ratio+'.txt'
		#precomp_feats=[]
		# To maintain proper alignment of output
		all_img_filenames=[]
		precomp_img_filenames=[]
		f = open(testname,'w')
		f_pre = open(precomp_featurename,'wb')
		if img_filename[-4:]=='.txt':
			for line in open(img_filename):
				imgname = line.replace('\n','')
				if len(imgname)>2:
					f_img = open(imgname, 'rb') # TODO: check if image or web address, download if web
					sha1=hashlib.sha1(f_img.read()).hexdigest().upper()
					f_img.close()
					feat_id=exist_img_precompfeat(sha1)
					print feat_id
					if feat_id != 0:
						#precomp_feats.append(feat_id)
						f_pre.write(struct.pack('i',feat_id))
						precomp_img_filenames.append(imgname)
					else:
						ins_num = ins_num + 1
						f.write(imgname+' 0\n')
					all_img_filenames.append(imgname)
		else: #Single image query, filename should be sha1 but not guaranteed if not call from php.
			f_img = open(img_filename, 'rb')
			sha1=hashlib.sha1(f_img.read()).hexdigest().upper()
			f_img.close()
			feat_id=exist_img_precompfeat(sha1)
			if feat_id != 0:
				#precomp_feats.append(feat_id)
				f_pre.write(struct.pack('i',feat_id))
				precomp_img_filenames.append(img_filename)
			else:
				f.write(img_filename+' 0\n')
				ins_num = 1
			all_img_filenames.append(img_filename)
		f.close()
		f_pre.close()
		if os.name=='nt':
			prefix = ''
		else:
			prefix = './'
		nb_query=len(all_img_filenames)

#		if not os.path.exists(featurefilename) and ins_num>0:
		if ins_num>0 and (always_recompute or not os.path.exists(featurefilename)):
			batch_size = min(64,ins_num)
			iteration = int(math.ceil(ins_num/float(batch_size)))
			print 'image_number:', ins_num, 'batch_size:', batch_size, 'iteration:', iteration

			f = open('test.prototxt')
			proto = f.read()
			f.close()
			proto = proto.replace('test.txt',testname.replace('\\','/')).replace('batch_size: 1','batch_size: '+str(batch_size))
			f = open(protoname,'w');
			f.write(proto)
			f.close()
			command = prefix+'extract_nfeatures caffe_sentibank_train_iter_250000 '+protoname+ ' fc7,prob '+fresh_featurefilename.replace('\\','/')+','+fresh_featurefilename.replace('\\','/')+'_prob '+str(iteration)+' '+device;
			print command
			os.system(command)
			print 'sentibank time: ', time.time() - t0
			#os.system(prefix+'getBiconcept caffe_sentibank_train_iter_250000 '+protoname+ ' fc7 '+featurename.replace('\\','/')+'_fc7 1 CPU')
			#os.system(prefix+'getBiconcept caffe_sentibank_train_iter_250000 '+protoname+ ' prob '+featurename.replace('\\','/')+'_prob 1 CPU')

			os.remove(protoname)
		# get precomputed features
		nb_precomp=len(precomp_img_filenames)
		out_fresh_featurefilename=fresh_featurefilename.replace('\\','/')+".dat"
		if nb_precomp>0:
			command = prefix+'get_precomp_feats '+precomp_featurename+' '+precomp_featurefilename;
			print command
			os.system(command)
			# merge with freshly computed features
			if ins_num>0:
				#read featurefilename and featurefilename_precomp
				#use list of files to know where to read feature from to build final features file
				f_pre=open(precomp_featurefilename,'rb')
				f_fresh=open(out_fresh_featurefilename,'rb')
				f_final=open(featurefilename,'wb')
				print featurefilename,"should contain",str(nb_query),"features."
				# How to read and write properly features vectors?
				# Use numpy? numpy.fromfile, numpy.ndarray.tofile
				for img in all_img_filenames:
					if img in precomp_img_filenames: # read from pre
						one_feat = f_pre.read(feature_num*4)
					else:
						one_feat = f_fresh.read(feature_num*4)
					#print len(one_feat)
					#print one_feat
					f_final.write(one_feat)
				f_pre.close()
				f_fresh.close()
				f_final.close()
			else: #only precomp features here
				command = 'mv '+precomp_featurefilename+' '+featurefilename;
				print command
				os.system(command)
		else: # only fresh features here
			command = 'mv '+out_fresh_featurefilename+' '+featurefilename;
			print command
			os.system(command)

		os.remove(testname)
		if not os.path.exists(simname) or always_recompute:
			command = prefix+'hashing '+featurefilename + ' 256 '+ratio;
			print command
			os.system(command)
			os.rename(featurename + '_fc7-sim.txt',simname)
		
		#os.remove(probfilename.dat)
		#os.remove(featurefilename)
		#print prob,feature
		#os.system('cd ..')
		#os.chdir(currentDir)
		#print simname
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
			if count == nb_query:
				break
		f.close()
		
		#print "sim_score",sim_score		

		#print sim_score
		#print sim
		# get_duplicate
		if get_dup:
			new_sim = []
			new_sim_score = []
			if not global_var['demo']:
				sql='SELECT htid,uid FROM fullIds WHERE uid in (%s) ORDER BY FIELD(uid, %s)' 
			else:
				sql='SELECT htid,uid,url,location,ads_url,ads_id FROM fullIds WHERE uid in (%s) ORDER BY FIELD(uid, %s)' 
			for i in range(0,nb_query):	
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
		# expand metadata
		if not global_var['demo']:
			db=MySQLdb.connect(host=isthost,user=istuser,passwd=istpwd,db=istdb)
			c=db.cursor()
			sql='select i.url,i.location,ads.url,ads.id from images i left join ads on i.ads_id=ads.id where i.id in (%s) order by field (i.id,%s);' 
			for i in range(0,nb_query):	
				if not sim[i]: # empty
					continue
				query_num = [simj[4] for simj in sim[i]]
				in_p=', '.join(map(lambda x: '%s', query_num))
				sqlq = sql % (in_p,in_p)
				c.execute(sqlq, query_num*2)
				tmpresult = c.fetchall()
				sim[i]=[tmpresult[k]+sim[i][k][4:] for k in range(0,len(tmpresult))]
			db.close()
		
		output = []
		for i in range(0,nb_query):	
			output.append(dict())
			output[i]['similar_images']= OrderedDict([['number',len(sim[i])],['image_urls',[]],['cached_image_urls',[]],['page_urls',[]],['ht_ads_id',[]],['ht_images_id',[]],['sha1',[]],['distance',[]]])
			for simj in sim[i]:
				output[i]['similar_images']['image_urls'].append(simj[0])
				output[i]['similar_images']['cached_image_urls'].append(simj[1])
				output[i]['similar_images']['page_urls'].append(simj[2])
				output[i]['similar_images']['ht_ads_id'].append(simj[3])
				output[i]['similar_images']['ht_images_id'].append(simj[4])
				output[i]['similar_images']['sha1'].append(simj[5])
			output[i]['similar_images']['distance']=sim_score[i]
		outp = OrderedDict([['number',nb_query],['images',output]])
		json.dump(outp, open(outputname,'w'),indent=4, sort_keys=False)		
 
	print 'query time: ', time.time() - t0
