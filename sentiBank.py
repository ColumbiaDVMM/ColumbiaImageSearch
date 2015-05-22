import os,sys,json
import struct,time
from array import *
sys.path.append('libsvm-3.18/python')
from svmutil import *
from collections import OrderedDict 
import math

if __name__ == '__main__':
	t0 = time.time()
	#currentDir = os.getcwd()
	#os.chdir('deepsentibank')
	if len(sys.argv)<2:
		print  "This program takes one or multiple images as input, and output CNN features, SentiBank bi-concept probabilities and memex image attributes.\nUsage: python sentibank.py image_path/image_path_list.txt [CPU/GPU] [DEVICE_ID=0]"
		exit()
	img_filename = sys.argv[1]
	device = 'CPU'
	deviceid = 0
	if len(sys.argv)>2 and sys.argv[2]=='GPU':
		device = 'GPU'
		if len(sys.argv)>3 and sys.argv[3].find('DEVICE_ID=')>-1:
			device = device + ' ' + sys.argv[3]				
	feature_num = 4096
	classes = json.load(open('classes_memex.json'))
	class_num = len(classes)
	testname = img_filename[:-4] + '-test.txt'
	protoname = img_filename[:-4] + '-test.prototxt'
	featurename = img_filename[:-4] + '-features'
	outputname = img_filename[:-4] + '.json'
	if not os.path.exists(outputname):
		featurefilename = featurename+'_fc7.dat'
		probfilename = featurename+'_prob.dat'
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
		if not os.path.exists(featurefilename) or not os.path.exists(probfilename):


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
			#os.system(prefix+'getBiconcept caffe_sentibank_train_iter_250000 '+protoname+ ' fc7 '+featurename.replace('\\','/')+'_fc7 1 CPU')
			#os.system(prefix+'getBiconcept caffe_sentibank_train_iter_250000 '+protoname+ ' prob '+featurename.replace('\\','/')+'_prob 1 CPU')
			print 'sentibank time: ', time.time() - t0

			os.remove(protoname)
		os.remove(testname)
		feature_file = open(featurefilename,'rb')
		number = feature_num*ins_num
		feature = array('f')	
		feature.fromfile(feature_file,number)
		featuretmp=feature.tolist()
		feature = [[0]*feature_num]*ins_num
		for i in range(0,ins_num):
			feature[i]=featuretmp[i*feature_num:(i+1)*feature_num]
		feature_file.close()
		prob_file = open(probfilename,'rb')
		number = class_num*ins_num
		prob = array('f')	
		prob.fromfile(prob_file,number)
		probtmp=prob.tolist()
		prob = [[0]*class_num]*ins_num
		for i in range(0,ins_num):
			prob[i]=probtmp[i*class_num:(i+1)*class_num]
		prob_file.close()
		#os.remove(probfilename.dat)
		#os.remove(featurefilename)
		#print prob,feature
		#os.system('cd ..')
		#os.chdir(currentDir)
		cclass = json.load(open('classifier_list.json'))
		output = []
		for i in range(0,ins_num):	
			output.append({'features':feature[i],'attributes':dict()})
			#print i
			biconcept = dict()
			for j in range(0,class_num):
				biconcept[classes[j]]=prob[i][j]
			output[i]['bi-concepts'] = OrderedDict(sorted(biconcept.items(), key=lambda x: x[1], reverse=True))
			
		for cname in cclass:
			m = svm_load_model(cname+'.model')
			print 'predict',cname
			p_label, p_acc, p_val = svm_predict([1]*ins_num , feature, m, '-b 1 -q')
			for i in range(0,ins_num):
				output[i]['attributes'][cname]=p_val[i][0]
		outp = OrderedDict([['number',ins_num],['images',output]])
		json.dump(outp, open(outputname,'w'),indent=4, sort_keys=False)
	print 'SentiBank time: ', time.time() - t0
