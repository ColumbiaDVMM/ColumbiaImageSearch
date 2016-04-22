import os,sys
# http://happybase.readthedocs.org/en/latest/user.html
import happybase
import MySQLdb
import json
import time
import requests
import shutil
import hashlib
from Queue import *
from threading import Thread

nb_threads=12
# HBase connection pool
hbase_conn_timeout = None
pool = happybase.ConnectionPool(size=12,host='10.1.94.57',timeout=hbase_conn_timeout)

batch_size=100000
imagedltimeout=2
tmp_img_dl_dir="tmp_img_dl"
start_img_fail="https://s3.amazonaws.com/memex-images/full"
row_start=None
#row_start="0FE98D4F5D6B03D59AD670AA06ACA4309DA1B139309903A46E5FA71008BE04FF"
#row_start="11AF5668A95D17484A5943827FDF425D548C7563DC2B064678F34B91947A6AFF"
# MySQL connection infos
global_var = json.load(open('../../conf/global_var_all.json'))
localhost=global_var['local_db_host']
localuser=global_var['local_db_user']
localpwd=global_var['local_db_pwd']
localdb=global_var['local_db_dbname']
suffix='_2015_oct_nov'
tab_samples_name='dig_isi_cdr2_ht_images'+suffix
# need to create these tables
tab_hash_name='image_hash'+suffix
tab_ht_images_infos='ht_images_infos'+suffix # need to create it
tab_missing_sha1_name='ht_images_missing_sha1'+suffix # need to create it
tab_missing_sim_name='ht_images_missing_sim'+suffix # need to create it
tab_cdrid_sha1_name='ht_images_cdrid_to_sha1'+suffix # need to create it
tab_columbia_sim_imgs_name='ht_columbia_similar_images'+suffix # need to create it
# end tables to be created

# save URL too
def saveInfos(sha1,img_cdr_id,parent_cdr_id,image_ht_id,ads_ht_id,s3_url,logf=None):
    # deal with obj_parent list
    if type(parent_cdr_id)==list:
        #if logf:
        #    logf.write("We have a list of obj_parent for image {} with cdr_id {}.\n".format(sha1,img_cdr_id))
        #else:
        #    print "We have a list of obj_parent for image {} with cdr_id {}.".format(sha1,img_cdr_id)
        for one_pcid in parent_cdr_id:
            saveInfos(sha1,img_cdr_id,str(one_pcid).strip(),image_ht_id,ads_ht_id,s3_url)
        return
    else: # single obj_parent case
        args=[img_cdr_id,parent_cdr_id,str(image_ht_id),str(ads_ht_id),str(s3_url)]
    with pool.connection(timeout=hbase_conn_timeout) as connection:
        tab_allinfos = connection.table(tab_ht_images_infos)
        row = tab_allinfos.row(str(sha1))
    hbase_fields=['info:all_cdr_ids','info:all_parent_ids','info:image_ht_ids','info:ads_ht_id','info:s3_url']
    if not row:
        # First insert
        first_insert="{"+', '.join(["\""+hbase_fields[x]+"\": \""+str(args[x]).strip()+"\"" for x in range(len(hbase_fields))])+"}"
        with pool.connection(timeout=hbase_conn_timeout) as connection:
            tab_allinfos = connection.table(tab_ht_images_infos)
            tab_allinfos.put(str(sha1), json.loads(first_insert))
    else:
        # Merge everything, except s3_url which should only be added if it is empty for now
        merge_hbase_fields=hbase_fields[:-2]
        try:
            split_row=[[str(tmp_field).strip() for tmp_field in row[field].split(',')] for field in hbase_fields if field in row]
            #print sha1
            check_presence=[str(args[i]).strip() in split_row[i] for i,field in enumerate(merge_hbase_fields)]
            if check_presence.count(True)<len(merge_hbase_fields):
                merged_tmp=[split_row[i].append(str(args[i]).strip()) for i in range(len(merge_hbase_fields))]
                merged=split_row
                #print "merged:",merged
                merge_insert="{"
                merge_insert+=', '.join(["\""+merge_hbase_fields[x]+"\": \""+','.join(merged[x])+"\"" for x in range(len(merge_hbase_fields))])
                if len(merged)<len(hbase_fields) or (len(merged)==len(hbase_fields) and not merged[len(hbase_fields)].startswith("https://s3") and s3_url.startswith("https://s3")):
                    merge_insert+=', \"'+hbase_fields[-1]+'\": \"'+s3_url+'\"'
                else: # used old s3_url
                    merge_insert+=', \"'+hbase_fields[-1]+'\": \"'+merged[len(hbase_fields)][0]+'\"'
                merge_insert+="}"
                with pool.connection(timeout=hbase_conn_timeout) as connection:
                    tab_allinfos = connection.table(tab_ht_images_infos)
                    tab_allinfos.put(str(sha1), json.loads(merge_insert))
            if len(split_row)<len(hbase_fields): # i.e. missing s3_url
                with pool.connection(timeout=hbase_conn_timeout) as connection:
                    tab_allinfos = connection.table(tab_ht_images_infos)
                    tab_allinfos.put(str(sha1), {'info:s3_url': str(s3_url)})
        except Exception as inst:
            print "[Error in saveInfos]:",inst
            print "sha1,args:",sha1,args
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            print "Image infos:",sha1,img_cdr_id,parent_cdr_id,image_ht_id,ads_ht_id
            #print "Split row:",split_row
            #print "Merge insert:",merge_insert
        else:
            pass
        #print "Image with infos ({},{},{},{}) already associated with sha1 {}.".format(img_cdr_id,parent_cdr_id,image_ht_id,ads_ht_id,sha1)

def fix_s3_url(sha1,all_cdr_ids):    
    for row_key in all_cdr_ids:
        with pool.connection(timeout=hbase_conn_timeout) as connection:
            tab_samples = connection.table(tab_samples_name)
            one_row = tab_samples.row(row_key)
        first_row = one_row[0]
        doc = one_row[1]['images:images_doc']
        jd = json.loads(doc)
        image_id=str(jd['crawl_data']['image_id']).strip()
        ad_id=str(jd['crawl_data']['memex_ht_id']).strip()
        parent_cdr_id=jd['obj_parent']
        s3_url=jd['obj_stored_url']
        if s3_url.startswith("https://s3"):
            print "Fixing {} with s3_url {}.".format(sha1.upper(),s3_url)
            saveInfos(sha1.upper(),one_row[0],parent_cdr_id,image_id,ad_id,s3_url)


def worker():
    while True:
        tupInp = q.get()
        fix_s3_url(tupInp[0], tupInp[1])
        q.task_done()

if __name__ == '__main__':
        
    tab = connection.table(tab_ht_images_infos)
    for one_row in tab.scan():
        if 'info:s3_url' in one_row[1].keys() and not one_row[1]['info:s3_url']:
            tupInp=(one_row[0],one_row[1]['all_cdr_ids'])
            q.put(tupInp)
    q.join()
    print "Done."


    
