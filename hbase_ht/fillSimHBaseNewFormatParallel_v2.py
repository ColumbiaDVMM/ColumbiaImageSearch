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

sys.path.insert(0, os.path.abspath('../memex_tools'))
import sha1_tools

nb_threads=12
# HBase connection pool
hbase_conn_timeout = None
pool = happybase.ConnectionPool(size=12,host='10.1.94.57',timeout=hbase_conn_timeout)
sha1_tools.pool = pool
sha1_tools.hbase_conn_timeout = hbase_conn_timeout

batch_size=10000
imagedltimeout=2
tmp_img_dl_dir="tmp_img_dl"
start_img_fail="https://s3.amazonaws.com/memex-images/full"
row_start=None
#row_start="0FE98D4F5D6B03D59AD670AA06ACA4309DA1B139309903A46E5FA71008BE04FF"
#row_start="11AF5668A95D17484A5943827FDF425D548C7563DC2B064678F34B91947A6AFF"
# MySQL connection infos
global_var = json.load(open('../../conf/global_var_all.json'))
sha1_tools.global_var = global_var
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

def mkpath(outpath):
    pos_slash=[pos for pos,c in enumerate(outpath) if c=="/"]
    for pos in pos_slash:
        try:
            os.mkdir(outpath[:pos])
        except:
            pass

def dlImage(url,logf=None):
    if url.startswith(start_img_fail):
        if logf:
            logf.write("Skipping image in failed s3 bucket.\n")
        else:
            print "Skipping image in failed s3 bucket."
        return None
    pos_slash=[pos for pos,c in enumerate(url) if c=="/"]
    file_img=url[pos_slash[-1]:]
    outpath=os.path.join(tmp_img_dl_dir,file_img)
    mkpath(outpath)
    #print "Downloading image from {} to {}.".format(url,outpath)
    try:
        r = requests.get(url, stream=True, timeout=imagedltimeout)
        if r.status_code == 200:
            with open(outpath, 'wb') as f:
                r.raw.decode_content = True
                shutil.copyfileobj(r.raw, f)
            return outpath
    except Exception as inst:
        if logf:
            logf.write("Download failed for img that should be saved at {} from url {}.\n".format(outpath,url))
        else:
            print "Download failed for img that should be saved at {} from url {}.".format(outpath,url)
        print inst 
        return None

def createHBaseTable(tab_name,cf):
    try:
        with pool.connection(timeout=hbase_conn_timeout) as connection:
            connection.create_table(tab_name, { cf: dict(), })
    except Exception as inst:
        print "[createHBaseTable] Error when creating table '{}'. {}".format(tab_name,inst)

def save_missing_sim(image_id):
    with pool.connection(timeout=hbase_conn_timeout) as connection:
        tab_missing_sim = connection.table(tab_missing_sim_name)
        if not tab_missing_sim.row(str(image_id)):
            tab_missing_sim.put(str(image_id), {'info:image_id': str(image_id)})

def getSimIds(image_id,logf=None):
    with pool.connection(timeout=hbase_conn_timeout) as connection:
        tab_aaron = connection.table('aaron_memex_ht-images')
        sim_row = tab_aaron.row(str(image_id))
        
    sim_ids = None
    if not sim_row:
        #print "Sim row is empty. Skipping."
        return sim_ids # Should compute similarity from API?
    if 'meta:columbia_near_dups' in sim_row:
        sim_ids=(sim_row['meta:columbia_near_dups'], sim_row['meta:columbia_near_dups_dist'])
    else:
        if logf:
            logf.write("Similarity not yet computed for image {}. Skipping\n".format(image_id))
        else:
            print "Similarity not yet computed for image {}. Skipping".format(image_id)
        save_missing_sim(image_id)
    return sim_ids
        
def saveSimPairs(sha1_sim_pairs):
    row_keys=[pair[0] for pair in sha1_sim_pairs]
    with pool.connection(timeout=hbase_conn_timeout) as connection:
        tab_similar = connection.table(tab_columbia_sim_imgs_name)
        sim_rows = tab_similar.rows(row_keys)
    if len(sim_rows)==len(row_keys): # everything already there
        return
    existing_pairs_key=[row[0] for row in sim_rows]
    new_sha1_sim_pairs=[pair for pair in sha1_sim_pairs if pair[0] not in existing_pairs_key]
    with pool.connection(timeout=hbase_conn_timeout) as connection:
        tab_similar = connection.table(tab_columbia_sim_imgs_name)
        b = tab_similar.batch()
        for pair in new_sha1_sim_pairs:
            b.put(str(pair[0]), {'info:dist': pair[1]})
        b.send()

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
                if len(merged)<len(hbase_fields) or (len(merged)==len(hbase_fields) and not merged[len(hbase_fields)-1][0].startswith("https://s3") and s3_url.startswith("https://s3")):
                    merge_insert+=', \"'+hbase_fields[-1]+'\": \"'+s3_url+'\"'
                else: # used old s3_url
                    merge_insert+=', \"'+hbase_fields[-1]+'\": \"'+merged[len(hbase_fields)-1][0]+'\"'
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

def processBatch(first_row,last_row):
    nb_img=0
    time_sha1=0
    time_save_info=0
    time_get_sim=0
    time_prep_sim=0
    time_save_sim=0
    start=time.time()
    done=False
    f = open("logFillSimNewFormatParallel_{}-{}.txt".format(first_row,last_row), 'wt', 0) # 0 for no buffering
    with pool.connection(timeout=hbase_conn_timeout) as connection:
      tab_samples = connection.table(tab_samples_name)
      while not done:
        try:
            for one_row in tab_samples.scan(row_start=first_row,row_stop=last_row):
                first_row = one_row[0]
                nb_img = nb_img+1
                cdr_id = one_row[0]
                doc = one_row[1]['images:images_doc']
                jd = json.loads(doc)
                image_id=str(jd['crawl_data']['image_id']).strip()
                ad_id=str(jd['crawl_data']['memex_ht_id']).strip()
                parent_cdr_id=jd['obj_parent'] # might be corrupted? might be a list?
                # get obj_stored_url and discard if not s3
                s3_url=jd['obj_stored_url']
                if not s3_url.startswith("https://s3"):
                    s3_url=""
                # get SHA1
                start_sha1=time.time()
                sha1hash=sha1_tools.get_SHA1_from_image_id_or_cdr_id(image_id,cdr_id,tab_hash_name,f)
                print sha1hash
                if sha1hash:
                    #print "Saving SHA1 {} for image ({},{}) in HBase".format(sha1hash,cdr_id,image_id)
                    sha1_tools.save_SHA1_to_hbase(image_id,cdr_id,sha1hash.upper(),tab_hash_name,tab_cdrid_sha1_name)
                else:
                    sha1_tools.save_missing_sha1(image_id,cdr_id,tab_missing_sha1_name)
                    #print "Could not get/compute SHA1 for {} {}.".format(image_id,cdr_id)
                time_sha1+=time.time()-start_sha1
                if not sha1hash: 
                    #time.sleep(1)
                    continue
                # get similar ids
                start_get_sim = time.time()
                sim_ids = getSimIds(image_id,f)
                time_get_sim+=time.time()-start_get_sim
                # save all infos
                start_save_info=time.time()
                saveInfos(sha1hash.upper(),one_row[0],parent_cdr_id,image_id,ad_id,s3_url,f)
                time_save_info+=time.time()-start_save_info
                if not sim_ids or not sim_ids[0]: 
                    #time.sleep(1)
                    continue
                #print sim_ids
                start_prep_sim=time.time()
                #f.write("Looking for sim_ids of image {}: {}\n".format(image_id,sim_ids))
                # Process sim_ids as batch
                sha1_sim_ids,missing_sha1s,new_sha1s=sha1_tools.get_batch_SHA1_from_imageids(sim_ids[0].split(','),tab_hash_name,f)
                sha1_tools.save_missing_SHA1_to_hbase_missing_sha1(missing_sha1s,tab_missing_sha1_name)
                sha1_tools.save_batch_SHA1_to_hbase_image_hash(new_sha1s,tab_hash_name)
                # prepare to save
                sha1_sim_pairs=[]
                sim_dists=sim_ids[1].split(',')
                for i,sha1_sim_id in enumerate(sha1_sim_ids):
                    if sha1_sim_id:
                        tup=("{}-{}".format(min(sha1hash,sha1_sim_id).upper(),max(sha1hash,sha1_sim_id).upper()),sim_dists[i])
                        sha1_sim_pairs.append(tup)
                #print sha1_sim_pairs
                sha1_sim_pairs=set(sha1_sim_pairs)
                time_prep_sim=time_prep_sim+time.time()-start_prep_sim
                #print sha1_sim_pairs
                start_save_sim=time.time()
                saveSimPairs(sha1_sim_pairs)
                time_save_sim=time_save_sim+time.time()-start_save_sim
                if nb_img%100==0:
                    f.write("Processed {} images. Total time : {}. Average time per image is {}.\n".format(nb_img,time.time()-start,float(time.time()-start)/nb_img))
                    f.write("Timing details: sha1:{}, save_info:{}, get_sim:{}, prep_sim:{}, save_sim:{}\n".format(float(time_sha1)/nb_img,float(time_save_info)/nb_img,float(time_get_sim)/nb_img,float(time_prep_sim)/nb_img,float(time_save_sim)/nb_img))
            done=True
        except Exception as inst:
            f.write("[Caught error] {}\n".format(inst))
            exc_type, exc_obj, exc_tb = sys.exc_info()  
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            f.write("{} in {} line {}.\n".format(exc_type, fname, exc_tb.tb_lineno))
            time.sleep(2)
    f.write('Batch done. ')
    f.write("Processed {} images. Total time : {}. Average time per image is {}.\n".format(nb_img,time.time()-start,float(time.time()-start)/nb_img))
    f.write("Timing details: sha1:{}, save_info:{}, get_sim:{}, prep_sim:{}, save_sim:{}\n".format(float(time_sha1)/nb_img,float(time_save_info)/nb_img,float(time_get_sim)/nb_img,float(time_prep_sim)/nb_img,float(time_save_sim)/nb_img))
    f.close()

def worker():
    while True:
        tupInp = q.get()
        processBatch(tupInp[0], tupInp[1])
        q.task_done()

if __name__ == '__main__':

    # need to create these tables
    createHBaseTable(tab_ht_images_infos,'info') # column family "info"
    createHBaseTable(tab_missing_sha1_name,'info') # column family "info"
    createHBaseTable(tab_missing_sim_name,'info') # column family "info"
    createHBaseTable(tab_columbia_sim_imgs_name,'info') # column family "info"
    createHBaseTable(tab_cdrid_sha1_name,'hash') # column family "hash"
    # end tables to be created


    q = Queue()
    for i in range(nb_threads):
        t=Thread(target=worker)
        t.daemon=True
        t.start()
    
    row_count=0
    first_row=None
    last_row=row_start

    try:
        with pool.connection() as connection:
            tab_samples = connection.table(tab_samples_name)
            for one_row in tab_samples.scan(row_start=last_row):
                row_count=row_count+1
                if row_count%(batch_size/10)==0:
                    print "Scanned {} rows so far.".format(row_count)
                    sys.stdout.flush()
                if first_row is None:
                    first_row=one_row[0]
                if row_count%batch_size==0:
                    last_row=one_row[0]
                    print "Pushing batch {}-{}".format(first_row,last_row)
                    sys.stdout.flush()
                    tupInp=(first_row,last_row)
                    first_row=None
                    q.put(tupInp)
                    time.sleep(60)
    except Exception as inst:
        print "[Caught error] {}\n".format(inst)
        exc_type, exc_obj, exc_tb = sys.exc_info()  
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print "{} in {} line {}.\n".format(exc_type, fname, exc_tb.tb_lineno)
        time.sleep(2)
    q.join()    
    print "Done."
