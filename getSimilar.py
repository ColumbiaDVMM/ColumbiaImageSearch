import os,sys,json
import struct,time
import MySQLdb

from array import *
#sys.path.append('libsvm-3.18/python')
#from svmutil import *
from collections import OrderedDict 
import math

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

if __name__ == '__main__':
	t0 = time.time()
	#currentDir = os.getcwd()
	#os.chdir('deepsentibank')
	if len(sys.argv)<2:
		print  "This program takes one or multiple images as input, and output similar images.\nUsage: python getSimilar.py image_path/image_path_list.txt [similar_image_number] [post_ranking_ratio] [get_duplicate=1] [CPU/GPU] [DEVICE_ID=0]"
		exit()
	img_filename = sys.argv[1]
	sim_limit = 100
	if len(sys.argv)>2:
		sim_limit = int(sys.argv[2])
	ratio = '0.001'
	if len(sys.argv)>3:
		ratio = sys.argv[3]
	get_dup = 1
	dupstr = '_dup'
	if len(sys.argv)>4:
		get_dup = int(sys.argv[4])
		if get_dup==0:
			dupstr=''
	device = 'CPU'
	if len(sys.argv)>5 and sys.argv[5]=='GPU':
		device = 'GPU'
		if len(sys.argv)>6 and sys.argv[6].find('DEVICE_ID=')>-1:
			device = device + ' ' + sys.argv[6]		
	feature_num = 4096
	testname = img_filename[:-4] + '-test.txt'
	protoname = img_filename[:-4] + '-test.prototxt'
	featurename = img_filename[:-4] + '-features'
	featurefilename = featurename+'_fc7.dat'
	outputname = img_filename[:-4] + '-sim_'+str(sim_limit)+'_'+ratio+dupstr+'.json'
		
	if not os.path.exists(outputname):
		simname = featurename + '_fc7-sim_'+ratio+'.txt'

		f = open(testname,'w')
		if img_filename[-4:]=='.txt':
			ins_num = 0
			for line in open(img_filename):
				imgname = line.replace('\n','')
				if len(imgname)>2:
					ins_num = ins_num + 1
					f.write(imgname+' 0\n')
		else:
			f.write(img_filename+' 0')
			ins_num = 1
		f.close()
		if os.name=='nt':
			prefix = ''
		else:
			prefix = './'
		if not os.path.exists(featurefilename):


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
			command = prefix+'extract_nfeatures caffe_sentibank_train_iter_250000 '+protoname+ ' fc7,prob '+featurename.replace('\\','/')+'_fc7,'+featurename.replace('\\','/')+'_prob '+str(iteration)+' '+device;
			print command
			os.system(command)
			print 'sentibank time: ', time.time() - t0
			#os.system(prefix+'getBiconcept caffe_sentibank_train_iter_250000 '+protoname+ ' fc7 '+featurename.replace('\\','/')+'_fc7 1 CPU')
			#os.system(prefix+'getBiconcept caffe_sentibank_train_iter_250000 '+protoname+ ' prob '+featurename.replace('\\','/')+'_prob 1 CPU')

			os.remove(protoname)
		os.remove(testname)
		if not os.path.exists(simname):
			command = prefix+'hashing '+featurefilename + ' 256 '+ratio;
			print command
			os.system(command)
			os.rename(featurename + '_fc7-sim.txt',simname)
		
		#os.remove(probfilename.dat)
		#os.remove(featurefilename)
		#print prob,feature
		#os.system('cd ..')
		#os.chdir(currentDir)
		f = open(simname);
		sim =[]
		sim_score=[]
		db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
		c=db.cursor()
		sql='SELECT NULL,location,NULL,NULL,htid,sha1 FROM uniqueIds WHERE id in (%s) ORDER BY FIELD(id, %s)' 

		count = 0
		for line in f:
			#sim_index.append([])
			nums=line.replace(' \n','').split(' ')
			onum = len(nums)/2			
			n = min(sim_limit,onum)
			query_num = []
			for i in range(0,n):
				query_num.append(int(nums[i])+1)
			in_p=', '.join(map(lambda x: '%s', query_num))
			sqlq = sql % (in_p,in_p)
			
			c.execute(sqlq, query_num*2)
			sim.append(c.fetchall())
			sim_score.append(nums[onum:onum+n])
			count = count + 1
			if count == ins_num:
				break
		f.close()
		
		
		# get_duplicate
		if get_dup:
			new_sim = []
			new_sim_score = []
			if not global_var['demo']:
				sql='SELECT htid,uid FROM fullIds WHERE uid in (%s) ORDER BY FIELD(uid, %s)' 
			else:
				sql='SELECT htid,uid,url,location,ads_url,ads_id FROM fullIds WHERE uid in (%s) ORDER BY FIELD(uid, %s)' 
			for i in range(0,ins_num):	
				new_sim.append([])
				new_sim_score.append([])
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
			for i in range(0,ins_num):	
				query_num = [simj[4] for simj in sim[i]]
				in_p=', '.join(map(lambda x: '%s', query_num))
				sqlq = sql % (in_p,in_p)
				c.execute(sqlq, query_num*2)
				tmpresult = c.fetchall()
				sim[i]=[tmpresult[k]+sim[i][k][4:] for k in range(0,len(tmpresult))]
			db.close()
		
		output = []
		for i in range(0,ins_num):	
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
		outp = OrderedDict([['number',ins_num],['images',output]])
		json.dump(outp, open(outputname,'w'),indent=4, sort_keys=False)		
 
	print 'query time: ', time.time() - t0
