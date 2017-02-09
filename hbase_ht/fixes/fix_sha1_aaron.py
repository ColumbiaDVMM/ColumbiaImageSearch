import happybase
import time
import sys
import os
import json
sys.path.insert(0, os.path.abspath('../memex_tools'))
import sha1_tools
hbase_conn_timeout = None
tab_aaron_name = 'aaron_memex_ht-images'
tab_hash_name = 'image_id_sha1'
nb_threads = 2
pool = happybase.ConnectionPool(size=nb_threads,host='10.1.94.57',timeout=hbase_conn_timeout)
sha1_tools.pool = pool
global_var = json.load(open('../../conf/global_var_all.json'))
sha1_tools.global_var = global_var
sha1_tools.tab_aaron_name = tab_aaron_name

connection = happybase.Connection('10.1.94.57')
tab = connection.table(tab_aaron_name)
show=10000

count_null=0
count_null_sim=0
count_tot=0

done=False
start_row=None

while not done:
  try:
    for row in tab.scan(row_start=start_row,columns=['meta:sha1','meta:columbia_near_dups_sha1']):
      #sha1=row[1]['hash:sha1']
      sha1=row[1]['meta:sha1']
      start_row=row[0]
      count_tot+=1
      if count_tot%show==0:
          print "Scanned {} rows, currently at {}.".format(count_tot,row[0])
      if not sha1_tools.check_sha1(sha1):
          print "Corrupted sha1 in row {}".format(row[0])
          sys.stdout.flush()
          #tab.delete(row[0])
          count_null+=1
      for sim_sha1 in row[1]['meta:columbia_near_dups_sha1']:
          if not sha1_tools.check_sha1(sim_sha1):
              print "Corrupted sim sha1 in row {}".format(row[0])
              sys.stdout.flush()
              #tab.delete(row[0])
              count_null_sim+=1
    done=True
  except Exception as inst:
     print inst
     time.sleep(2)

print count_null,count_null_sim,count_tot
