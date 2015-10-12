from hadoop.io import SequenceFile
import time
import json
import pickle

#setimgkeys=set()
setvisualkeys=pickle.load(open( "setvisualkeys.p", "r" ))
visualvaluesdict=dict.fromkeys(list(setvisualkeys))
for visualkey in setvisualkeys:
 visualvaluesdict[visualkey]=set()

base_hdfs_path="hdfs://memex:/user/worker/crf/trial113"

for part in xrange(1, 16):
 filename = base_hdfs_path+"/part-r-000"+"%02d" % part
 reader = SequenceFile.Reader(filename)
 key_class = reader.getKeyClass()
 value_class = reader.getValueClass()
 key = key_class()
 value = value_class()
 position = reader.getPosition()
 while reader.next(key, value):
  if not reader.syncSeen():
    thisKey = key.toString()
    thisValue = value.toString()
    tmpj=json.loads(thisValue)
    #print tmpj
    for visualkey in setvisualkeys:
      try:
	#print list(tmpj['hasImagePart'].copy().keys())
	visualvaluesdict[visualkey]=visualvaluesdict[visualkey].union([tmpj['hasImagePart'][visualkey]['featureValue']])
      except:
	pass
  #print setimgkeys
  #print visualvaluesdict
  #time.sleep(10)
  position = reader.getPosition()
 reader.close()
 #print setimgkeys
 print visualvaluesdict

#pickle.dump(setimgkeys,open( "setimgkeys.p", "wb" ))
pickle.dump(visualvaluesdict,open( "visualvaluesdict.p", "wb" ))

#to be read again with setimgkeys=pickle.load(open( "setimgkeys.p", "r" ))
