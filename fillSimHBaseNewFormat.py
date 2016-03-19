# http://happybase.readthedocs.org/en/latest/user.html
import happybase,sys
#import numpy as np
import json
import time
import MySQLdb

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
tab_samples = connection.table('dig_isi_cdr2_ht_images_sample')


def getSHA1(image_id):
    hash_row = tab_hash.row(str(image_id))
    sha1hash = None
    if hash_row:
        sha1hash = hash_row['image:hash']
    else:
        print "HBase Hash row is empty. Getting SHA1 from MySQL"
        # Get hash from MySQL or recompute from image
        db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
        c=db.cursor()
        sql='SELECT location,htid,sha1 FROM uniqueIds WHERE htid={}'.format(image_id) 
        c.execute(sql)
        res=c.fetchall()
        print res
        print res[0][2]
        print "Saving SHA1 {} for image {} in HBase".format(sha1hash,image_id)
    return sha1hash

def getSimIds(image_id):
    sim_row = tab_aaron.row(str(image_id))
    sim_ids = None
    if not sim_row:
        print "Sim row is empty. Skipping."
        return sim_ids # Should compute similarity from API?
    if 'meta:columbia_near_dups' in sim_row:
        sim_ids=(sim_row['meta:columbia_near_dups'], sim_row['meta:columbia_near_dups_dist'])
    else:
        print "Similarity not yet computed. Skipping"
    return sim_ids
        

if __name__ == '__main__':

    for one_row in tab_samples.scan():
        doc = one_row[1]['images:images_doc']
        jd = json.loads(doc)
        image_id=jd['crawl_data']['image_id']
        print image_id
        # TODO also get obj_parent, one_row[0] i.e. CDR_ID, crawl_data.memex_ht_id
        sha1 = getSHA1(image_id)
        if not sha1:
            time.sleep(1)
            continue
        sim_ids = getSimIds(image_id)
        if not sim_ids:
            time.sleep(1)
            continue
