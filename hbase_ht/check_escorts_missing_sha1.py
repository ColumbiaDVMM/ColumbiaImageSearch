import happybase
import time
import sys
import os
import json
sys.path.insert(0, os.path.abspath('../memex_tools'))
import sha1_tools
hbase_conn_timeout = None
nb_threads = 2
pool = happybase.ConnectionPool(size=nb_threads,host='10.1.94.57',timeout=hbase_conn_timeout)
sha1_tools.pool = pool

tab_escorts_images_name = 'escorts_images_cdrid_infos'
tab_escorts_missing_sha1_name = 'escorts_images_missing_sha1'
tab_hash_name = 'image_id_sha1'

global_var = json.load(open('../../conf/global_var_all.json'))
sha1_tools.global_var = global_var
#tab_aaron_name = 'aaron_memex_ht-images'
#sha1_tools.tab_aaron_name = tab_aaron_name

connection = happybase.Connection('10.1.94.57')
tab = connection.table(tab_escorts_missing_sha1_name)
show=10000

count_ok_from_tab_hash=0
count_tot=0

done=False
start_row=None

while not done:
  try:
    for row in tab.scan(row_start=start_row):
      #sha1=row[1]['hash:sha1']
      if 'info:image_id' in row[1].keys():
        image_id = row[1]['info:image_id']
        sha1 = get_SHA1_from_hbase_imagehash(image_id,tab_hash_name)
        if sha1_tools.check_sha1(sha1):
          print "Managed to get sha1 {} from image_id {} for cdr_id {}.".format(sha1,image_id,row[0])
          count_ok_from_tab_hash+=1
      start_row=row[0]
      count_tot+=1
      if count_tot%show==0:
        print "Scanned {} rows, currently at {}.".format(count_tot,row[0])
    done=True
  except Exception as inst:
     print inst
     time.sleep(2)

print count_ok_from_tab_hash,count_tot
