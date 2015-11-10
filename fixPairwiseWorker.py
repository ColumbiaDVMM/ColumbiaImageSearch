# One updated was not inserted in the update list.
# Making all subsequent ids incorrect when using get_precomp_feats...
# This file will remove similar images where query or similar ids are higher than the last correct id 

# http://happybase.readthedocs.org/en/latest/user.html
import happybase,sys
import numpy as np
import pickle
import csv
import time
import sys
connection = happybase.Connection('10.1.94.57')
# This fails...
#alltables = connection.tables()
tab = connection.table('aaron_memex_ht-images')

biggest_correct_htid=89830995
biggest_correct_uniqueid=22541770
step=4000000
max_worker=30
if len(sys.argv)>1:
	worker_id=sys.argv[1]
else:
	worker_id=0
print "I am worker",str(worker_id)
time.sleep(10)

if __name__ == '__main__':

  our_batch_size=1000
  b = tab.batch()
  batch_nbmod=0
  start_row=step*worker_id
  if worker_id>max_worker:
	end_row=None
  else:
	end_row=step*(worker_id+1)
  for key, data in tab.scan(row_start=str(start_row),row_stop=str(end_row),batch_size=our_batch_size,columns=('meta:columbia_near_dups','meta:columbia_near_dups_dist','meta:columbia_near_dups_biggest_dbid')):
      #print key, data
      if 'meta:columbia_near_dups' in data.keys():
        if int(key)>biggest_correct_htid: # remove all columbia_near_dups stuff
		print "key is too big we should delete all columbia_near_dups",key
		b.delete(key,columns=('meta:columbia_near_dups','meta:columbia_near_dups_dist','meta:columbia_near_dups_biggest_dbid'))
		batch_nbmod=batch_nbmod+1	
	else: # key is ok but are near_dups
		# update biggest id
		b.put(key,{'meta:columbia_near_dups_biggest_dbid' : ''+str(biggest_correct_uniqueid)+''})
		neighbors_list=data['meta:columbia_near_dups'].rsplit(',')
		dist_list=data['meta:columbia_near_dups_dist'].rsplit(',')
		print key,"Initially we have these neighbors",neighbors_list
		del_neighs=[]
        	for neigh in neighbors_list:
			#print neigh
			if int(neigh)>biggest_correct_htid:
				print "We should remove neighbor",neigh
				# we need to deal with biggest_id and dist fields too
				del_neigh=neighbors_list.index(neigh)
				neighbors_list.pop(del_neigh)
				dist_list.pop(del_neigh)
				del_neighs.append(neigh)
				# should we delete the neigh row too? We might delete twice...
				#b.delete(neigh,columns=('meta:columbia_near_dups','meta:columbia_near_dups_dist','meta:columbia_near_dups_biggest_dbid'))
		if len(del_neighs)>0:
			print key,"We have deleted these similar images:",del_neighs
			neighs_str = ','.join(map(str, neighbors_list))
			dist_str = ','.join(map(str, dist_list))
			print len(neighbors_list),neighs_str
			print len(dist_list),dist_str
			b.put(key,{'meta:columbia_near_dups' : ''+neighs_str+''})
			b.put(key,{'meta:columbia_near_dups_dist' : ''+dist_str+''})
			batch_nbmod=batch_nbmod+1
	if batch_nbmod>our_batch_size:
		# push batch
		print "Pushing batch of modification"
		b.send()
		batch_nbmod=0
		#quit()
