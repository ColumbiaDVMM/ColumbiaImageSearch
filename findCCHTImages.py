# http://happybase.readthedocs.org/en/latest/user.html
import happybase,sys
import numpy as np
import pickle
connection = happybase.Connection('10.1.94.57')
# This fails...
#alltables = connection.tables()
tab = connection.table('aaron_memex_ht-images')

def getCC(images_ht):
   visited=[]
   CC=[]
   CCcount=0
   for pos,one_id in enumerate(images_ht['id']):
	if one_id not in visited:
	  visited.append(one_id)
	  CC.append([one_id])
	  CCqueue=images_ht['edges'][pos].split(',')
	  while len(CCqueue)>0:
		tmp_id=CCqueue.pop()
		if tmp_id not in visited:
			visited.append(tmp_id)
			CC[CCcount].append(tmp_id)
			try: # neighbor may not be in list, if we limited initial scan
				tmp_pos=images_ht['id'].index(tmp_id)
				CCqueue.append(images_ht['edges'][tmp_pos])
			except:
				pass
	  CCcount=CCcount+1
   return CC

if __name__ == '__main__':

  max_images=100
  try:
    images_ht=pickle.load(open("ht_images"+str(max_images)+".pkl","rb"))
  except:
    images_ht={}
    images_ht['id']=[]
    images_ht['edges']=[]
    images_ht['URL']=[]
    for key, data in tab.scan():
      if 'meta:columbia_near_dups' in data.keys():
        print len(images_ht['id']), key, data['meta:columbia_near_dups'], data['meta:location']
        images_ht['id'].append(key)
        images_ht['edges'].append(data['meta:columbia_near_dups'])
        images_ht['URL'].append(data['meta:location'])
      else:
	print key,"not indexed."
      if len(images_ht['id'])>=max_images:
	print "Reached max images number",str(max_images),"Saving."
	pickle.dump(images_ht,open("ht_images"+str(max_images)+".pkl","wb"),2)
	break
  print images_ht['id'],images_ht['edges'],images_ht['URL']
  CC=getCC(images_ht)
  print CC
  pickle.dump(CC,open("CC_ht_images"+str(max_images)+".pkl","wb"),2)
