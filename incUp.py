import os,sys,json,urllib2,math,shutil
import struct,time,multiprocessing
import MySQLdb
import subprocess as sub
import hashlib

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

def download_shell(args):
	url = args[0]
	filepath = args[1]
	#tm_start = time.time()
	if os.path.isfile(filepath):
		#print time.time()-tm_start
		return 1
	else:
		#tm_start = time.time()
		down_success = download_image(url,filepath)
		#print 'down time '+ str(time.time()-tm_start)
		return down_success
def download_image(url,filepath):
	retry = 0
	failed = 0
	try:
		imgfile = urllib2.urlopen(url,timeout=10)
	except Exception as e:
		print  url+' download error!'
		retry = 1
		pass
	except timeout as error:
		print url+' timeout!'
		retry = 1
		pass
	if retry:
		time.sleep(2)
		try:
			imgfile = urllib2.urlopen(url,timeout=10)
		except Exception as e:
			print url+' retry download error!'
			failed = 1
			pass
		except timeout as error:
			print url+' retry timeout!'
			failed = 1
			pass
	if failed:
		return 0
	else:
		savefailed = 0
		saveretry = 0
		try:
			f=open(filepath,'wb')
			f.write(imgfile.read())
			f.close()
		except Exception as e:
			print url+' save file retry!'
			saveretry = 1
			pass
		if saveretry:
			time.sleep(2)
			try:
				imgfile = urllib2.urlopen(url,timeout=10)
				f=open(filepath,'wb')
				f.write(imgfile.read())
				f.close()
			except Exception as e:
				print url+' save file failed!'
				savefailed = 1
				pass
		if savefailed:
			return 0
		else:
			return 1

if __name__ == '__main__':
	step_times = []
	step_times.append(time.time())
	timestr= time.strftime("%b-%d-%Y-%H-%M-%S", time.localtime(step_times[0]))

	from argparse import ArgumentParser

	parser = ArgumentParser(description='Incremental Update')
 
	parser.add_argument('-s','--startid',default=0,
                        help='''Manually providing starting memex image id for update''')
	parser.add_argument('-e','--endid',default=0,
                        help='''Manually providing ending memex image id for update''')

	# limiters
	parser.add_argument('-l','--limit',default=100000,type=int,
                        help='''Max number of images to download
                                (Default: 100,000)''')
 
  
	# parse input arguments
	args = parser.parse_args()

	limit = args.limit
	startid = args.startid
	endid = args.endid

	if startid == 0:
		db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
		c=db.cursor()
		c.execute('select htid from fullIds ORDER BY htid DESC limit 1;')
		remax = c.fetchall()
		if len(remax):
			startid = int(remax[0][0])
		else:
			startid = 0
		db.close()
		
	endid_str = ''
	if endid > startid:
		endid_str=' and id <= '+str(endid)
	update_path = 'update/'
	update_image_cache = os.path.join(update_path,'images')
	if not os.path.exists(update_path):
		os.mkdir(update_path)
		
	if not os.path.exists(update_image_cache):
		os.mkdir(update_image_cache)
	# get image URLs, ids, etc.
	db=MySQLdb.connect(host=isthost,user=istuser,passwd=istpwd,db=istdb)
	c=db.cursor()
	# sql='select id,location,importtime  from images where importtime >= DATE_FORMAT(''%s'', ''%s'') and location is not null order by importtime asc'
	# query_time = ['2015-04-13 15:00:00','%Y-%m-%d %H:%i:%s']
	# start_time = time.time()
	# c.execute(sql, query_time)
	#print "--- %s seconds ---" % (time.time() - start_time)

	sql='select id,location from images where id > ' + str(startid) + endid_str+' and location is not null order by id asc limit ' + str(limit)
	query_id = []
	start_time = time.time()
	c.execute(sql, query_id)
	print "query database: %s seconds ---" % (time.time() - start_time)

	re = c.fetchall()
	db.close()
	if len(re)<limit:
		print "Not enough images. Exiting"
		sys.exit("Error: Not enough images.")
	if len(re)>0:
		lastId=int(re[-1][0])
	else:
		lastId = startid

	update_suffix = timestr+'_'+str(startid)+'_'+str(lastId)
	print update_suffix
        update_logs_path = os.path.join(update_path,'logs')
        if not os.path.exists(update_logs_path):
                os.mkdir(update_logs_path)
        update_logfilename = os.path.join(update_logs_path,update_suffix+'.log')
        flog = open(update_logfilename,'w')

	num_url = len(re)
	print 'retrived %d image URLs' % num_url
	flog.write('retrived %d image URLs' % num_url+'\n')
	# remove existing ones
	retrived_ids = [int(img_item[0]) for img_item in re]
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
	c=db.cursor()
	sql='SELECT htid FROM fullIds WHERE htid in (%s);' 
	in_p=', '.join(map(lambda x: '%s', retrived_ids))
	sqlq = sql % (in_p)
	c.execute(sqlq, retrived_ids)
	existing_ids = c.fetchall()
	db.close()
	existing_ids=[int(i[0]) for i in existing_ids]
	imglist = [img_item for img_item in re if int(img_item[0]) not in existing_ids]
	num_url = len(imglist)
	print 'after remove existing: %d image URLs' % num_url
        flog.write('after remove existing: %d image URLs' % num_url+'\n')

	step_times.append(time.time())
	print 'Time for getting urls: ', str(step_times[-1]-step_times[-2]), 'seconds'
	flog.write('Time for getting urls: '+ str(step_times[-1]-step_times[-2])+ ' seconds\n')

	# download images
	pool = multiprocessing.Pool(10)
	download_arg=[];
	if not os.path.isdir(os.path.join(update_image_cache,str(startid))):
		os.mkdir(os.path.join(update_image_cache,str(startid)))
	for img_item in re:
		url = img_item[1]
		name = url.split('/')[-1]
		filepath = os.path.join(update_image_cache,str(startid),name)
		download_arg.append([url,filepath])

	download_indicator=pool.map(download_shell, download_arg)
	downloaded = []
	for i in range(0,len(re)):
		if download_indicator[i]:
			downloaded.append(re[i]+(download_arg[i][1],))

	num_downloaded = len(downloaded)
	print 'downloaded %d images' % num_downloaded
        flog.write('downloaded %d images' % num_downloaded+'\n')

	if not num_downloaded:
		exit(-1)

	step_times.append(time.time())
	print 'Time for downloading images: ', str(step_times[-1]-step_times[-2]), 'seconds'
        flog.write( 'Time for downloading images: '+ str(step_times[-1]-step_times[-2])+ ' seconds\n')

	# image integrity check
	readable_images = []
	integrity_path = os.path.join(update_path,'integrity_check')
	if not os.path.exists(integrity_path):
		os.mkdir(integrity_path)

	integrity_filepath = os.path.join(integrity_path,update_suffix+'.txt')
	f=open(integrity_filepath,'w')
	ok_tag = '[OK]'
	error_tag = '[ERROR]'
	png_tag = '0x89 0x50'
	unsp_tag = 'Unsupported color conversion request'
	for img_item in downloaded:
		command = 'jpeginfo -c '+ img_item[2]
		output, error = sub.Popen(command.split(' '), stdout=sub.PIPE, stderr=sub.PIPE).communicate()
		if output.find(ok_tag)<0:
			f.write(output)
			
		if output.find(error_tag)>=0 and output.find(png_tag)<0 and output.find(unsp_tag)<0:
			continue
			
		readable_images.append(img_item)

	f.close()
	num_readable = len(readable_images)
	print 'readable images: %d' % num_readable
        flog.write('readable images: %d' % num_readable + '\n')
	if not num_readable:
		exit(-1)
	step_times.append(time.time())
	print 'Time for checking integrity: ', str(step_times[-1]-step_times[-2]), 'seconds'	
        flog.write('Time for checking integrity: '+ str(step_times[-1]-step_times[-2])+ ' seconds\n')
	# compute sha1

	for i in range(0,num_readable):
		with open(readable_images[i][2], 'rb') as f:
			output=hashlib.sha1(f.read()).hexdigest().upper()
		readable_images[i]=readable_images[i]+(output,)

	step_times.append(time.time())
	print 'Time for computing sha1: ', str(step_times[-1]-step_times[-2]), 'seconds'	
        flog.write('Time for computing sha1: '+ str(step_times[-1]-step_times[-2])+ ' seconds\n')

	# get unique images 
	sha1_list = [img_item[3] for img_item in readable_images]
	unique_sha1 = sorted(set(sha1_list))
	unique_idx=[sha1_list.index(sha1) for sha1 in unique_sha1]
	full_idx=[unique_sha1.index(sha1) for sha1 in sha1_list]

	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
	c=db.cursor()
	c.execute('select id from uniqueIds ORDER BY id DESC limit 1;')
	remax = c.fetchall()
	if len(remax):
		umax = int(remax[0][0])
	else:
		umax = 0
		
	c.execute('select id from fullIds ORDER BY id DESC limit 1;')
	remax = c.fetchall()
	if len(remax):
		fmax = int(remax[0][0])
	else:
		fmax = 0
		
	sql='SELECT htid,sha1 FROM uniqueIds WHERE sha1 in (%s);' 
	in_p=', '.join(map(lambda x: '%s', unique_sha1))
	sqlq = sql % (in_p)
	c.execute(sqlq, unique_sha1)
	re = c.fetchall()
	db.close()

	old_uniques = [i[1] for i in re]
	old_uniques_htid = [int(i[0]) for i in re]

	new_uniques=[]
	unique_htid = []
	new_files=[]
	#new_id = cmax + 1
	for i in range(0,len(unique_sha1)):
		if unique_sha1[i] not in old_uniques:
			img_item=readable_images[unique_idx[i]]
			new_uniques.append((int(img_item[0]),img_item[1],img_item[3]))
			#new_id = new_id + 1
			new_files.append(img_item[2])
			unique_htid.append(new_uniques[-1][0])
		else:
			unique_htid.append(old_uniques_htid[old_uniques.index(unique_sha1[i])])

	new_fulls = []
	for i in range(0,num_readable):
		new_fulls.append((int(readable_images[i][0]),unique_htid[full_idx[i]]))

	num_new_unique=len(new_files)
	print 'new unique images: %d' % num_new_unique
        flog.write( 'new unique images: %d' % num_new_unique + '\n')

	step_times.append(time.time())
	print 'Time for getting unique images: ', str(step_times[-1]-step_times[-2]), 'seconds'	
        flog.write('Time for getting unique images: '+ str(step_times[-1]-step_times[-2])+ ' seconds\n')

	if num_new_unique:
	# extract features for unique images
		image_list_path = os.path.join(update_path,'image_list')
		if not os.path.exists(image_list_path):
			os.mkdir(image_list_path)

		image_list_filepath = os.path.join(image_list_path,update_suffix+'.txt')
		f=open(image_list_filepath,'w')
		f.writelines([filename+'\n' for filename in new_files])
		f.close()

		img_filename = image_list_filepath

		device='GPU'
		feature_num = 4096
		testname = img_filename[:-4] + '-test.txt'
		protoname = img_filename[:-4] + '-test.prototxt'
		featurename = img_filename[:-4] + '-features'
		featurefilename = featurename+'_fc7.dat'

		f = open(testname,'w')
		ins_num = 0
		for line in open(img_filename):
			imgname = line.replace('\n','')
			if len(imgname)>2:
				ins_num = ins_num + 1
				f.write(imgname+' 0\n')

		f.close()
		batch_size = min(64,ins_num)
		iteration = int(math.ceil(ins_num/float(batch_size)))
		print 'feature extraction image_number:', ins_num, 'batch_size:', batch_size, 'iteration:', iteration
	        flog.write('feature extraction image_number: '+str(ins_num)+' batch_size: '+str(batch_size)+ ' iteration: '+ str(iteration)+'\n')
		f = open('test.prototxt')
		proto = f.read()
		f.close()
		proto = proto.replace('test.txt',testname).replace('batch_size: 1','batch_size: '+str(batch_size))
		f = open(protoname,'w');
		f.write(proto)
		f.close()
		command = './extract_nfeatures_gpu caffe_sentibank_train_iter_250000 '+protoname+ ' fc7 '+featurename+'_fc7 '+str(iteration)+' '+device;
		print command
	        flog.write(command+'\n')
		os.system(command)
		os.remove(protoname)
		os.remove(testname)
		step_times.append(time.time())
		print 'Time for extracting features: ', str(step_times[-1]-step_times[-2]), 'seconds'	
	        flog.write( 'Time for extracting features: '+ str(step_times[-1]-step_times[-2])+' seconds\n')

	# compute feature norm, hashing codes and update the data

		feature_path = os.path.join(update_path,'features')
		if not os.path.exists(feature_path):
			os.mkdir(feature_path)

		feature_filepath = os.path.join(feature_path,update_suffix+'_norm')

		command = './hashing_update ' + featurefilename + ' ' + str(ins_num)
		print command
	        flog.write(command+'\n')
		os.system(command)
		feature_norm_output_path = featurefilename[:-4] + '_norm'
		shutil.move(feature_norm_output_path, feature_filepath)

		hashbits_path = os.path.join(update_path,'hash_bits')
		if not os.path.exists(hashbits_path):
			os.mkdir(hashbits_path)

		hashbits_filepath = os.path.join(hashbits_path,update_suffix+'_itq_norm_256')
		itq_output_path = featurefilename[:-4] + '_itq_norm_256'
		shutil.move(itq_output_path, hashbits_filepath)
		os.remove(featurefilename)
		step_times.append(time.time())
		print 'Time for updating the features and hash bits: ', str(step_times[-1]-step_times[-2]), 'seconds'	
	        flog.write( 'Time for updating the features and hash bits: '+ str(step_times[-1]-step_times[-2])+ ' seconds\n')
	#update MySQL
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
	c=db.cursor()
	if num_new_unique:
		insert_statement = "INSERT IGNORE INTO uniqueIds (htid, location, sha1) VALUES {}".format(','.join(map(str,new_uniques)))
		c.execute( insert_statement )
	insert_statement = "INSERT IGNORE INTO fullIds (htid, uid) VALUES {}".format(','.join(map(str,new_fulls)))
	c.execute( insert_statement )
	c.execute('select id from uniqueIds ORDER BY id DESC limit 1;')
	remax = c.fetchall()
	if len(remax):
		umax_new = int(remax[0][0])
	else:
		umax_new = 0
		
	c.execute('select id from fullIds ORDER BY id DESC limit 1;')
	remax = c.fetchall()
	if len(remax):
		fmax_new = int(remax[0][0])
	else:
		fmax_new = 0

	success = 0
	if umax_new-umax != num_new_unique:
		print 'Update failed! unique table size mismatch!',umax_new,umax,num_new_unique
	        flog.write('Update failed! unique table size mismatch!\n')
	elif fmax_new-fmax != num_readable:
		print 'Update failed! full table size mismatch!'
                flog.write('Update failed! full table size mismatch!\n')
	elif os.stat(hashbits_filepath).st_size!=num_new_unique*32:
		print 'Update failed! hash bits size mismatch!'
	        flog.write('Update failed! hash bits size mismatch!\n')
	elif os.stat(feature_filepath).st_size!=num_new_unique*16384:
		print 'Update failed! feature size mismatch!'
	        flog.write('Update failed! feature size mismatch!\n')
	else:
		success = 1
		db.commit()
	db.close()
	step_times.append(time.time())
	print 'Time for updating database: ', str(step_times[-1]-step_times[-2]), 'seconds'	
        flog.write('Time for updating database: '+str(step_times[-1]-step_times[-2])+' seconds\n')
	#post process
	if not success:
		if os.path.isfile(hashbits_filepath):
			os.remove(hashbits_filepath)
		if os.path.isfile(feature_filepath):
			os.remove(feature_filepath)
		#Todo: run alter table uniqueIds AUTO_INCREMENT = 1; alter table fullIds AUTO_INCREMENT = 1; in local mysql to reset the incrimental id
		print 'Total time: ', str(time.time()-step_times[0]), 'seconds'	
	        flog.write('Total time: '+ str(time.time()-step_times[0])+' seconds\n')
		flog.close()
		exit(-1)
	else:
		master_update_filepath =  'update_list.txt'
		with open(master_update_filepath, "a") as f:
			f.write(update_suffix+'\n')
		print update_suffix, 'Update is success!'
		print 'Total time: ', str(time.time()-step_times[0]), 'seconds'	
	        flog.write(update_suffix+'Update is success!'+'\n'+'Total time: '+ str(time.time()-step_times[0])+' seconds\n')
		flog.close()
		# post processing
		# delete img cache
		os.system('find img -name "*sim_*.txt" -exec rm -rf {} \;')
		os.system('find img -name "*sim_*.json" -exec rm -rf {} \;')
		#todo: merge features and hash files
		
		#vmtouch folder
		os.system('./cache.sh')
		exit(0)

