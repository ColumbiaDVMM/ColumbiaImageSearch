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

def clean_htimages(images_ht,max_edges,max_id=None):
  for pos,key in enumerate(images_ht['id']):
  	if max_id:
	  # Cleaning corrupted rows
	  if int(key)>max_id:
	    print "Deleting row because id is too big",key
	    images_ht['id'].pop(pos)
	    images_ht['edges'].pop(pos)
	    continue
	edges_list=images_ht['edges'][pos].rsplit(",")
	# Discard too many edges
	if len(edges_list)>max_edges:
	  print "Deleting row because too many edges",key
	  images_ht['id'].pop(pos)
	  images_ht['edges'].pop(pos)
	  continue
	mod=0
	# Cleaning corrupted edges
	if max_id:
	  for one_edge in edges_list:
		if int(one_edge)>max_id:
			pos_edge=edges_list.index(one_edge)
			edges_list.pop(pos_edge)
			mod=mod+1
  return images_ht

if __name__ == '__main__':

  max_images=-1
  save_images=False
  our_batch_size=10000
  max_edges=100
  max_id=89830995
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
    if max_images>0:
	images_ht=pickle.load(open("ht_images"+str(max_images)+".pkl","rb"))
    else:	    
	images_ht=pickle.load(open("ht_images.pkl","rb"))
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
  #pickle.dump(images_ht,open("ht_images.pkl","wb"),2)
  #print images_ht['id'],images_ht['edges']#,images_ht['URL']
  # SHOULD CHECK EDGES REALLY DEFINE AN UNDIRECTED GRPAH...
  images_ht=clean_htimages(images_ht,max_edges,max_id)
  pickle.dump(images_ht,open("ht_images_cleanV2.pkl","wb"),2)
  CC=getCC(images_ht)
  #print CC
  #pickle.dump(CC,open("CC_ht_images"+str(max_images)+".pkl","wb"),2)
  with open('CC_ht_imagesV2.csv', 'wb') as csvfile:
    CCwriter = csv.writer(csvfile, delimiter=',')
    for oneCC in CC:
      CCwriter.writerow(oneCC)
  csvfile.close()
  #create 
