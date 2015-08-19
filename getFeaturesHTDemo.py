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
import shutil
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

def get_htid_fromIST(roxy_imgname):
        db=MySQLdb.connect(host=isthost,user=istuser,passwd=istpwd,db=istdb)
        c=db.cursor()
        query="select id from images where location=\""+roxy_imgname+"\";"
        #print query
        c.execute(query) #Should we use id or htid here?
        remax = c.fetchall()
        return remax[0][0]

def exist_img_precompfeat(query_sha1):
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
	c=db.cursor()
	query="select id,htid from uniqueIds where sha1=\""+query_sha1+"\";"
	#print query
	c.execute(query) #Should we use id or htid here?
	remax = c.fetchall()
	#print remax
	if len(remax):
		feat_id = int(remax[0][0])
		ht_id = int(remax[0][1])
	else:
		feat_id = 0
		ht_id = 0
	db.close()
	return feat_id, ht_id

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
		print  "This program takes one or multiple images as input, and output similar images.\nUsage: python getFeaturesHTDemo.py image_path/image_path_list.txt [CPU/GPU] [DEVICE_ID=0]"
		exit()
	img_filename = sys.argv[1]
	global_var = json.load(open('global_var_all.json'))
	#print len(sys.argv)
	device = 'CPU'
	if len(sys.argv)>2 and sys.argv[2]=='GPU':
		device = 'GPU'
		if len(sys.argv)>3 and sys.argv[3].find('DEVICE_ID=')>-1:
			device = device + ' ' + sys.argv[3]
	feature_num = 4096
	testname = img_filename[:-4] + '-test.txt'
	idsname = img_filename[:-4] + '-ids.txt'
	protoname = img_filename[:-4] + '-test.prototxt'
	featurename = img_filename[:-4] + '-features'
	precomp_featurename = img_filename[:-4] + '-precomp-features'
	featurefilename = featurename+'_fc7.dat'
	fresh_featurefilename = featurename+'-fresh_fc7'
	precomp_featurefilename = featurename+'-precomp_fc7.dat'
	now=datetime.datetime.now()

	ins_num = 0
	always_recompute = 0;
		
	if not os.path.exists(featurename):
		# To maintain proper alignment of output
		all_img_filenames=[]
		precomp_img_filenames=[]
		f = open(testname,'w')
		f_pre = open(precomp_featurename,'wb')
		f_ids = open(idsname,'wb')
		if img_filename[-4:]=='.txt':
			for line in open(img_filename):
				imgname = line.replace('\n','')
				roxy_imgname="https://s3.amazonaws.com/roxyimages/"+imgname[imgname.rfind('/')+1:]
				if len(imgname)>2:
					f_img = open(imgname, 'rb') # TODO: check if image or web address, download if web
					sha1=hashlib.sha1(f_img.read()).hexdigest().upper()
					f_img.close()
					feat_id, ht_id = exist_img_precompfeat(sha1)
					if feat_id != 0:
						print "Found feature locally: ",feat_id, ht_id,"for image:",imgname
						#precomp_feats.append(feat_id)
						f_pre.write(struct.pack('i',feat_id))
						precomp_img_filenames.append(imgname)
						#f_ids.write(imgname+' '+str(feat_id)+'\n') # this our unique id not htid...
						print imgname,str(ht_id)
						f_ids.write(imgname+' '+roxy_imgname+' '+str(ht_id)+'\n')
					else: # should compute features for this img
						print "Could not find feature locally for image:",imgname
						ins_num = ins_num + 1
						ht_id = get_htid_fromIST(roxy_imgname)
						f_ids.write(imgname+' '+roxy_imgname+' '+str(ht_id)+'\n')
						f.write(imgname+' 0\n')
					all_img_filenames.append(imgname)
		else: 
			print img_filename,"should have a .txt extension."
	                f.close()
        	        f_pre.close() 
                	f_ids.close()
			quit()
		f.close()
		f_pre.close()
		f_ids.close()
		if os.name=='nt':
			prefix = ''
		else:
			prefix = './'
		nb_query=len(all_img_filenames)

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
				for img in all_img_filenames:
					if img in precomp_img_filenames: # read from pre
						one_feat = f_pre.read(feature_num*4)
					else:
						one_feat = f_fresh.read(feature_num*4)
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

		# We should get hash codes too...
		ins_num=ins_num+nb_precomp
		feature_filepath = "featuresDEMO_norm"

		command = './hashing_update ' + featurefilename + ' ' + str(ins_num)
		print command
		os.system(command)
		feature_norm_output_path = featurefilename[:-4] + '_norm'
		shutil.move(feature_norm_output_path, feature_filepath)

		hashbits_filepath = "featuresDEMO_itq_norm_256"
		itq_output_path = featurefilename[:-4] + '_itq_norm_256'
		shutil.move(itq_output_path, hashbits_filepath)
		os.remove(featurefilename)

		print "Output files are:",feature_filepath,hashbits_filepath
 
	print 'Total time: ', time.time() - t0

