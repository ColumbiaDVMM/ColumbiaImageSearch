# http://happybase.readthedocs.org/en/latest/user.html
import happybase,sys
import numpy as np
import pickle
import csv
connection = happybase.Connection('10.1.94.57')
# This fails...
#alltables = connection.tables()
tab = connection.table('aaron_memex_ht-images')

def getCC(images_ht): # only for undirected graph
  visited=[]
  CC=[]
  CCcount=0
  for pos,one_id in enumerate(images_ht['id']):
    if one_id not in visited:
      visited.append(int(one_id))
      CC.append([int(one_id)])
      #print "pos",pos
      #print "edges",images_ht['edges'][pos]
      CCqueue=images_ht['edges'][pos].split(',')
      while len(CCqueue)>0:
        tmp_id=int(CCqueue.pop())
        if tmp_id not in visited:
	  #print tmp_id
          visited.append(tmp_id)
          CC[CCcount].append(tmp_id)
	  #print CC
          try: # neighbor may not be in list, if we limited initial scan
            tmp_pos=images_ht['id'].index(tmp_id)
            CCqueue.extend(images_ht['edges'][tmp_pos].split(','))
          except:
            pass
      CCcount=CCcount+1
  return CC

if __name__ == '__main__':

  max_images=-1
  save_images=False
  our_batch_size=10000
  #images_ht={}
  #images_ht['id']=range(10)
  #images_ht['edges']=[]
  #images_ht['edges'].append('2,3')
  #images_ht['edges'].append('7,8,9')
  #images_ht['edges'].append('0,2')
  #images_ht['edges'].append('2,0')
  #images_ht['edges'].append('5,6')
  #images_ht['edges'].append('4,6')
  #images_ht['edges'].append('4,5')
  #images_ht['edges'].append('9,8,1')
  #images_ht['edges'].append('7,9,1')
  #images_ht['edges'].append('7,8,1')
  try:
    images_ht=pickle.load(open("ht_images"+str(max_images)+".pkl","rb"))
    print "Loaded precomputed list of images"
    #ok=True
  except:
    if max_images>0:
      print "Getting",str(max_images),"images."
    else:
      print "Getting all images."
    images_ht={}
    images_ht['id']=[]
    images_ht['edges']=[]
    #images_ht['URL']=[]
    for key, data in tab.scan(batch_size=our_batch_size,columns=('meta:columbia_near_dups',)):
      #print key, data
      if 'meta:columbia_near_dups' in data.keys():
        print len(images_ht['id']), key
        #print len(images_ht['id']), key, data['meta:columbia_near_dups'], data['meta:location']
        images_ht['id'].append(key)
        images_ht['edges'].append(data['meta:columbia_near_dups'])
        #images_ht['URL'].append(data['meta:location'])
      #else:
      #  print key,"not indexed."
      if len(images_ht['id'])>=max_images and max_images>0:
        print "Reached max images number",str(max_images),"Saving."
        pickle.dump(images_ht,open("ht_images"+str(max_images)+".pkl","wb"),2)
        break
  pickle.dump(images_ht,open("ht_images.pkl","wb"),2)
  #print images_ht['id'],images_ht['edges']#,images_ht['URL']
  # SHOULD CHECK EDGES REALLY DEFINE AN UNDIRECTED GRPAH...
  CC=getCC(images_ht)
  #print CC
  #pickle.dump(CC,open("CC_ht_images"+str(max_images)+".pkl","wb"),2)
  with open('CC_ht_images.csv', 'wb') as csvfile:
    CCwriter = csv.writer(csvfile, delimiter=',')
    for oneCC in CC:
      CCwriter.writerow(oneCC)
  csvfile.close()
  #create 
