import time
import json
import pickle

base_hdfs_path="./trial113"
#base_hdfs_path="hdfs://memex:/user/worker/crf/trial113"
all_ads=[]
all_attr=[]
all_vals=dict()

for part in xrange(0, 16):
 filename = base_hdfs_path+"/part-000"+"%02d" % part
 fp = open(filename)
 for line in fp:
	[ads_id,attr,val]=line.split('\t')
	#print val.rstrip()
	all_ads.append(ads_id)
	all_attr.append(attr)
	if attr not in all_vals.keys():
		all_vals[attr]=[]
	all_vals[attr].append(val.rstrip())
 fp.close()

print len(all_ads)
for one_attr in set(all_attr):
	print one_attr+":",set(all_vals[one_attr])
