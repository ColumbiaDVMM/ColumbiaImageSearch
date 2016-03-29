import os,sys
# http://happybase.readthedocs.org/en/latest/user.html
import happybase
import MySQLdb
import json
import time
import requests
import shutil
import hashlib
#import numpy as np

tmp_img_dl_dir="tmp_img_dl"
imagedltimeout=2
start_img_fail="https://s3.amazonaws.com/memex-images/full"
#row_start=None
row_start="0FE98D4F5D6B03D59AD670AA06ACA4309DA1B139309903A46E5FA71008BE04FF"
# MySQL connection infos
global_var = json.load(open('../conf/global_var_all.json'))
localhost=global_var['local_db_host']
localuser=global_var['local_db_user']
localpwd=global_var['local_db_pwd']
localdb=global_var['local_db_dbname']

# HBase connection infos
connection = happybase.Connection('10.1.94.57')
# use fields: meta:columbia_near_dups, meta:columbia_near_dups_dist
tab_aaron = connection.table('aaron_memex_ht-images')
#use field: image:hash
tab_hash = connection.table('image_hash')
# use field: images:images_doc
#tab_samples = connection.table('dig_isi_cdr2_ht_images_sample')
tab_samples = connection.table('dig_isi_cdr2_ht_images_2016')
# save sha1 in 'ht_images_cdrid_to_sha1_sample'
#tab_cdr_hash = connection.table('ht_images_cdrid_to_sha1_sample')
tab_cdr_hash = connection.table('ht_images_cdrid_to_sha1_2016')
# save similarities in 'ht_columbia_similar_images_sample'
# key min_sha1-max_sha1,value dist in column info:dist
#tab_similar = connection.table('ht_columbia_similar_images_sample')
tab_similar = connection.table('ht_columbia_similar_images_2016')
# save all image info in 'ht_images_infos_sample' with sha1 as rowkey and
# a JSON of all_cdr_ids, all_parents_cdr_ids, all_cdr_docs, all_images_htid, all_images_htadsid.
# [check if column exist, if id already there, append]
#tab_allinfos = connection.table('ht_images_infos_sample')
tab_allinfos = connection.table('ht_images_infos_2016')

def mkpath(outpath):
    pos_slash=[pos for pos,c in enumerate(outpath) if c=="/"]
    for pos in pos_slash:
        try:
            os.mkdir(outpath[:pos])
        except:
            pass

def dlImage(url):
    if url.startswith(start_img_fail):
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
        print "Download failed for img that should be saved at {} from url {}.".format(outpath,url)
        print inst 
        return None

def getSHA1FromMySQL(image_id):
    res_sha1 = None
    if image_id:
      db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
      c=db.cursor()
      sql='SELECT sha1 FROM uniqueIds WHERE htid=\"{}\"'.format(image_id) 
      #print sql
      c.execute(sql)
      res=c.fetchall()
      if res:
        res_sha1=res[0][0]
    return res_sha1

def getSHA1FromFile(filepath):
    sha1 = hashlib.sha1()
    f = open(filepath, 'rb')
    try:
        sha1.update(f.read())
    finally:
        f.close()
    os.unlink(filepath)
    return sha1.hexdigest()

def computeSHA1(cdr_id):
    sha1hash = None
    # get image url
    one_row = tab_samples.row(cdr_id)
    #print one_row
    doc = one_row['images:images_doc']
    jd = json.loads(doc)
    one_url = jd['obj_stored_url']
    if not one_url:
        print "Could not get URL from cdrid {}.".format(cdr_id)
    else: # download
        localpath = dlImage(one_url)
        # compute sha1
        if localpath:
            sha1hash = getSHA1FromFile(localpath)
        else:
            print "Could not download image from URL {} of cdrid {}.".format(one_url,cdr_id)
    return sha1hash

def getSHA1(image_id,cdr_id):
    #print image_id,cdr_id
    hash_row = None
    if image_id:
        hash_row = tab_hash.row(str(image_id))
    sha1hash = None
    if hash_row:
        sha1hash = hash_row['image:hash']
    else:
        #print "HBase Hash row is empty. Trying to get SHA1 from MySQL."
        # Get hash from MySQL...
        sha1hash = getSHA1FromMySQL(image_id)
        # or recompute from image if failed.
        if not sha1hash and cdr_id:
            #print "Could not get SHA1 from MYSQL. Recomputing..."
            sha1hash = computeSHA1(cdr_id)
    if sha1hash:
        #print "Saving SHA1 {} for image ({},{}) in HBase".format(sha1hash,cdr_id,image_id)
        saveSHA1(image_id,cdr_id,sha1hash.upper())
    else:
        pass
        #print "Could not get/compute SHA1 for {} {}.".format(image_id,cdr_id)
    return sha1hash

def saveSHA1(image_id,cdr_id,sha1hash):
    # save in the two tables
    # old table indexed by htid 'tab_hash'
    tab_hash.put(str(image_id), {'image:hash': sha1hash})
    # new table indexed by cdrid
    if cdr_id:
        tab_cdr_hash.put(str(cdr_id), {'hash:sha1': sha1hash})

def getSimIds(image_id):
    sim_row = tab_aaron.row(str(image_id))
    sim_ids = None
    if not sim_row:
        #print "Sim row is empty. Skipping."
        return sim_ids # Should compute similarity from API?
    if 'meta:columbia_near_dups' in sim_row:
        sim_ids=(sim_row['meta:columbia_near_dups'], sim_row['meta:columbia_near_dups_dist'])
    else:
        print "Similarity not yet computed. Skipping"
    return sim_ids
        

def saveSimPairs(sha1_sim_pairs):
    for pair in sha1_sim_pairs:
        tab_similar.put(str(pair[0]), {'info:dist': pair[1]})

def saveInfos(sha1,img_cdr_id,parent_cdr_id,image_ht_id,ads_ht_id):
    row = tab_allinfos.row(str(sha1))
    hbase_fields=['info:all_cdr_ids','info:all_parent_ids','info:image_ht_ids','info:ads_ht_id']
    args=[img_cdr_id,parent_cdr_id,str(image_ht_id),str(ads_ht_id)]
    if not row:
        # First insert
        first_insert="{"+', '.join(["\""+hbase_fields[x]+"\": \""+args[x]+"\"" for x in range(len(hbase_fields))])+"}"
        tab_allinfos.put(str(sha1), json.loads(first_insert))
    else:
        # Merge everything
        split_row=[list(row[field].split(',')) for i,field in enumerate(hbase_fields)]
        #print sha1
        check_presence=[args[i] in row[field].split(',') for i,field in enumerate(hbase_fields)]
        if check_presence.count(True)<len(hbase_fields):
            merged_tmp=[split_row[i].append(args[i]) for i in range(len(hbase_fields))]
            merged=split_row
            #print "merged:",merged
            merge_insert="{"+', '.join(["\""+hbase_fields[x]+"\": \""+', '.join(merged[x])+"\"" for x in range(len(hbase_fields))])+"}"
            #print merge_insert
            tab_allinfos.put(str(sha1), json.loads(merge_insert))
        else:
            pass
            #print "Image with infos ({},{},{},{}) already associated with sha1 {}.".format(img_cdr_id,parent_cdr_id,image_ht_id,ads_ht_id,sha1)

if __name__ == '__main__':
    done=False
    last_row=row_start
    nb_img=0
    time_sha1=0
    time_save_info=0
    time_get_sim=0
    time_prep_sim=0
    time_save_sim=0
    start=time.time()
    while not done:
        try:
            for one_row in tab_samples.scan(row_start=last_row):
                last_row = one_row[0]
                nb_img = nb_img+1
                doc = one_row[1]['images:images_doc']
                jd = json.loads(doc)
                image_id=jd['crawl_data']['image_id']
                ad_id=jd['crawl_data']['memex_ht_id']
                parent_cdr_id=jd['obj_parent']
                # get SHA1
                start_sha1=time.time()
                sha1 = getSHA1(image_id,one_row[0])
                time_sha1=time_sha1+time.time()-start_sha1
                if not sha1:
                    #time.sleep(1)
                    continue
                # save all infos
                start_save_info=time.time()
                saveInfos(sha1.upper(),last_row,parent_cdr_id,image_id,ad_id)
                time_save_info=time_save_info+time.time()-start_save_info
                # get similar ids
                start_get_sim=time.time()
                sim_ids = getSimIds(image_id)
                time_get_sim=time_get_sim+time.time()-start_get_sim
                if not sim_ids:
                    #time.sleep(1)
                    continue
                #print sim_ids
                start_prep_sim=time.time()
                sha1_sim_ids=[]
                for sim_id in sim_ids[0].split(','):
                    if sim_id:
                        #print sim_id
                        # Would need to query ES to get the cdr_id...
                        sha1_sim_ids.append(getSHA1(sim_id,None))
                # prepare to save similarities
                # key should be: min(sha1,sim_sha1)-max(sha1,sim_sha1)
                # value in column info:dist is corresponding distance
                sha1_sim_pairs=[]
                sim_dists=sim_ids[1].split(',')
                for i,sha1_sim_id in enumerate(sha1_sim_ids):
                    if sha1_sim_id:
                        tup=("{}-{}".format(min(sha1,sha1_sim_id).upper(),max(sha1,sha1_sim_id).upper()),sim_dists[i])
                        sha1_sim_pairs.append(tup)
                #print sha1_sim_pairs
                sha1_sim_pairs=set(sha1_sim_pairs)
                time_prep_sim=time_prep_sim+time.time()-start_prep_sim
                #print sha1_sim_pairs
                start_save_sim=time.time()
                saveSimPairs(sha1_sim_pairs)
                time_save_sim=time_save_sim+time.time()-start_save_sim
                if nb_img%100==0:
                     print "Processed {} images. Average time per image is {}.".format(nb_img,float(time.time()-start)/nb_img)
                     print "Timing details: sha1:{}, save_info:{}, get_sim:{}, prep_sim:{}, save_sim:{}".format(float(time_sha1)/nb_img,float(time_save_info)/nb_img,float(time_get_sim)/nb_img,float(time_prep_sim)/nb_img,float(time_save_sim)/nb_img)
            done=True
        except Exception as inst:
            print "[Caught error] {}".format(inst)
            time.sleep(10)
