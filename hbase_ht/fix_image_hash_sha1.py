import happybase
import time

#tab_name="ht_images_cdrid_to_sha1_2016_old_crawler"
tab_name="image_hash"
connection = happybase.Connection('10.1.94.57')
tab = connection.table(tab_name)

count_null=0
count_tot=0
for row in tab.scan():
    #sha1=row[1]['hash:sha1']
    sha1=row[1]['image:hash']
    count_tot+=1
    if sha1=="NULL":
        print row
        tab.delete(row[0])
        count_null+=1
print count_null,count_tot
