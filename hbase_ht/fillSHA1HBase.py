import MySQLdb
import happybase
import json
import time

global_var = json.load(open('global_var_all.json'))
localhost=global_var['local_db_host']
localuser=global_var['local_db_user']
localpwd=global_var['local_db_pwd']
localdb=global_var['local_db_dbname']

connection = happybase.Connection('10.1.94.57')
tab_hash = connection.table('image_hash')
#tab_aaron = connection.table('aaron_memex_ht-images')
show=1000

def get_biggest_unique_id(): # Should be the biggest id currently in the DB for potential later update...
    db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
    c=db.cursor()
    sql='select id from uniqueIds order by id desc limit 1'
    start_time = time.time()
    c.execute(sql)
    re = c.fetchall()
    biggest_dbid = re[0][0]
    db.close()
    return biggest_dbid

def get_htid_SHA1_mapping(uniqueId):
    db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
    c=db.cursor()
    sql='select fullIds.htid,uniqueIds.sha1 from uniqueIds join fullIds on fullIds.uid=uniqueIds.htid where uniqueIds.id={}'.format(uniqueId)
    c.execute(sql)
    re = c.fetchall()
    htid_SHA1_mapping=[]
    for htid_SHA1_pair in re:
        htid_SHA1_mapping.append((htid_SHA1_pair[0],htid_SHA1_pair[1]))
    return htid_SHA1_mapping

def save_htid_SHA1_mapping(htid_SHA1_mapping):
    b = tab_hash.batch()
    for pair in htid_SHA1_mapping:
        b.put(str(pair[0]), {'image:hash': pair[1].upper()})
    b.send()

if __name__ == '__main__':
    # Scan uniqueIds and run on join query for each uniqueId ?
    start_time = time.time()
    biggest_unique_id=get_biggest_unique_id()
    print "Getting biggest_unique_id {} took {}s.".format(biggest_unique_id,time.time()-start_time)
    fill_start_time = time.time()
    htid_count=0
    for unique_id in range(biggest_unique_id):
        htid_SHA1_mapping=get_htid_SHA1_mapping(unique_id+1)
        save_htid_SHA1_mapping(htid_SHA1_mapping)
        htid_count=htid_count+len(htid_SHA1_mapping)
        if unique_id%show==0:
            elapsed=time.time()-fill_start_time
            print "Filled {} ht ids from {} unique ids in {}s. Average htid per s: {}".format(htid_count,unique_id,elapsed,float(htid_count)/elapsed)


