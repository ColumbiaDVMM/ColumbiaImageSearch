import happybase
import time
import sys

#tab_name="ht_images_cdrid_to_sha1_2016_old_crawler"
tab_name="image_id_sha1"
connection = happybase.Connection('10.1.94.57')
tab = connection.table(tab_name)
show=10000

count_null=0
count_tot=0

done=False
start_row='139864386'

while not done:
  try:
    for row in tab.scan(row_start=start_row):
      #sha1=row[1]['hash:sha1']
      sha1=row[1]['image:hash']
      start_row=row[0]
      count_tot+=1
      if count_tot%show==0:
          print "Scanned {} rows, currently at {}.".format(count_tot,row[0])
      if sha1=="NULL" or sha1=="null":
          print row
          sys.stdout.flush()
          tab.delete(row[0])
          count_null+=1
    done=True
  except Exception as inst:
     print inst
     time.sleep(2)

print count_null,count_tot
