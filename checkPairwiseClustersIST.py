import os,sys,json
import struct,time
import MySQLdb
# http://happybase.readthedocs.org/en/latest/user.html
import happybase

from array import *
#sys.path.append('libsvm-3.18/python')
#from svmutil import *
from collections import OrderedDict 
import math
import ntpath
import hashlib
import datetime
import pickle
import csv

import json
global_var = json.load(open('global_var_all.json'))
isthost=global_var['ist_db_host']
istuser=global_var['ist_db_user']
istpwd=global_var['ist_db_pwd']
istdb=global_var['ist_db_dbname']
localhost=global_var['local_db_host']
localuser=global_var['local_db_user']
localpwd=global_var['local_db_pwd']
localdb=global_var['local_db_dbname']

sql_from_adid='select * from images i left join ads on i.ads_id=ads.id where ads.id=(%s);'
sql_from_htids='select * from uniqueIds where htid in (%s) order by id;'

def getBiggestDBId(): # Should be the biggest id currently in the DB for potential later update...
    db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
    c=db.cursor()
    sql='select id from uniqueIds order by id desc limit 1'
    query_id = []
    start_time = time.time()
    c.execute(sql, query_id)
    re = c.fetchall()
    biggest_dbid = re[0][0]
    db.close()
    #print "Biggest ID in LOCAL DB:",biggest_dbid
    return biggest_dbid # Biggest HT ID or unique ID?


def getImagesInfos(images_htid):
    db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
    c=db.cursor()
    query = sql_from_htids % (', '.join(map(lambda x: '%s', images_htid)),)
    #print query
    c.execute(query,images_htid)
    remax = c.fetchall()
    #print remax
    db.close()
    return remax

def getImagesHtIds(ad_id):
    db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
    c=db.cursor()
    query=sql_from_adid % (ad_id,)
    #print query
    c.execute(query) #Should we use id or htid here?
    remax = c.fetchall()
    #print remax
    res=None
    if len(remax)>0:
        res=[onere[0] for onere in remax]
    db.close()
    return res

def filterFromHBase(images_htid):
    connection = happybase.Connection('10.1.94.57')
    tab = connection.table('aaron_memex_ht-images')
    filter_images_htid=[]
    for oneid in images_htid:
        onerow=tab.row(str(oneid),columns=('meta:columbia_near_dups','meta:ads_id'))
        #print onerow
        if 'meta:columbia_near_dups' not in onerow.keys():
            filter_images_htid.append(oneid)
    connection.close()
    return filter_images_htid

def filter_near_dup(nums,dist_ths):
    onum=len(nums)/2
    temp_nums=[]
    for one_num in range(0,onum):
        if float(nums[onum+one_num])>dist_ths:
            return temp_nums
        temp_nums.insert(one_num,nums[one_num])
        temp_nums.insert(len(temp_nums),nums[onum+one_num])
    return temp_nums


if __name__ == '__main__':

    t0 = time.time()
    if len(sys.argv)>5:
        print  "This program fill the HT HBase with near duplicate links between similar images from the IST cluters GT file."
        exit()

    sim_limit = 10000 # Does not really matter, big enough to return all near duplicates.
    ratio = '0.0001'
    get_dup = 1
    dupstr = '_dup'
    near_dup = 1
    near_dup_th = 0.15
    neardupstr=''
    if near_dup:
        neardupstr='_neardup'+str(near_dup_th)

    feature_num = 4096
    pairwise_filename = "update/logs/ist_clustersgt_pairwise"
    simname = pairwise_filename + '-sim.txt'
    featurename = pairwise_filename + '-features'
    featurefilename = featurename+'_fc7.dat'
    now=datetime.datetime.now()
    logname = pairwise_filename+'.log'
    flog=open(logname, 'w')

    ins_num = 0
    always_recompute = 0;
    prefix = './' # should be empty for windows

    biggest_dbid=getBiggestDBId()
    print >>flog,biggest_dbid

    # Get data from CSV
    csv_filename='nov2015_qpr_truth__ids_images.csv'
    csv_file=open(csv_filename,'rb')
    csv_file.readline() # discard header
    csvreader = csv.reader(csv_file)
    ads_id=[]
    for row in csvreader:
        ads_id.append(row[5])
    print len(ads_id)


    all_images_infos=[]
    # Check for each ad if images index in HBase, if not add to be processed
    for ad_id in ads_id:
        images_htid=getImagesHtIds(ad_id)
        # Filter out images already in HBase
        filter_images_htid=filterFromHBase(images_htid)
        # Get these non processed images, images infos
        tmp_images_infos = getImagesInfos(filter_images_htid)
        if tmp_images_infos:
            all_images_infos.extend(tmp_images_infos)
    print >>flog,all_images_infos
    # Get these features
    f_pre = open(featurename,'wb')
    ht_ids=[]
    for one_img in all_images_infos:
        ht_ids.append(one_img[1])
        feat_id=one_img[3]
        f_pre.write(struct.pack('i',feat_id))
    f_pre.close()
    command = prefix+'get_precomp_feats '+featurename+' '+featurefilename+' >> '+logname+' 2>&1';
    print >>flog,command
    flog.close()
    os.system(command)
    flog=open(logname, 'a')

    # Compare with images in DB
    command = prefix+'hashing '+featurefilename + ' 256 '+ratio+' >> '+logname+' 2>&1';
    print >>flog,command
    flog.close()
    os.system(command)
    flog=open(logname, 'a')
    os.rename(featurename + '_fc7-sim.txt',simname)

    # Process output
    f = open(simname);
    sim =[]
    sim_score=[]
    db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
    c=db.cursor()
    sql='SELECT htid,id FROM uniqueIds WHERE id in (%s) ORDER BY FIELD(id, %s)'

    # get similar images
    count = 0
    for line in f:
        #sim_index.append([])
        nums=line.replace(' \n','').split(' ')
        if near_dup: #filter near duplicate here
            nums=filter_near_dup(nums,float(near_dup_th))
        #print nums
        onum = len(nums)/2
        n = min(sim_limit,onum)
        #print n
        if n==0: # no returned images, e.g. no near duplicate
            sim.append(())
            sim_score.append([])
            continue
        query_num = []
        for i in range(0,n):
            query_num.append(int(nums[i])+1)
        in_p=', '.join(map(lambda x: '%s', query_num))
        sqlq = sql % (in_p,in_p)
        #print sqlq
        c.execute(sqlq, query_num*2)
        sim.append(c.fetchall())
        sim_score.append(nums[onum:onum+n])
        count = count + 1
        if count == len(all_images_infos):
            break
    f.close()

    if get_dup:
        print >>flog,"Getting duplicates"
        new_sim = []
        new_sim_score = []
        dup_list = []
        sql='SELECT htid,uid FROM fullIds WHERE uid in (%s) ORDER BY FIELD(uid, %s)'
        for i in range(0,len(all_images_infos)):
            new_sim.append([])
            new_sim_score.append([])
            dup_list.append([])
            # We have to fill HBase rows for duplicates too.
            in_p=', '.join(map(lambda x: '%s', [ht_ids[i]]))
            sqlq = sql % (in_p,in_p)
            c.execute(sqlq, [ht_ids[i],ht_ids[i]])
            tmpresult = c.fetchall()
            for k in tmpresult:
                if k[0]!=ht_ids[i]:
                    dup_list[i].append(k[0])
            if not sim[i]: # empty
                continue
            query_num = [simj[0] for simj in sim[i]]
            in_p=', '.join(map(lambda x: '%s', query_num))
            sqlq = sql % (in_p,in_p)
            c.execute(sqlq, query_num*2) # query_num*2 duplicates the list.
            tmpresult = c.fetchall()
            #print len(tmpresult)
            p = 0
            for k in tmpresult:
                if sim[i][p][0]!=k[1]:
                    p = p+1
                new_sim[i].append(k[0])
                new_sim_score[i].append(str(max(0,float(sim_score[i][p]))))
        sim = new_sim
        sim_score = new_sim_score

    db.close()


    print >>flog,len(sim),sim
    print >>flog,len(dup_list),dup_list
    print >>flog,len(sim_score),sim_score
    print >>flog,len(ht_ids),ht_ids
    # Fill HBase
    # https://happybase.readthedocs.org/en/latest/user.html#performing-batch-mutations
    # This may hang?...
    connection = happybase.Connection('10.1.94.57')
    #connection.tables()
    # Connection timeout issue...
    # happybase.hbase.ttypes.IOError: IOError(_message="java.net.SocketTimeoutException: callTimeout=0, callDuration=17367:...
    tab = connection.table('aaron_memex_ht-images')
    b = tab.batch()
    for i in range(0,len(all_images_infos)):
        sim_str = ','.join(map(str, sim[i]))
        sim_dist = ','.join(map(str, sim_score[i]))
        b.put(''+str(ht_ids[i])+'',{'meta:columbia_near_dups' : ''+sim_str+''})
        b.put(''+str(ht_ids[i])+'',{'meta:columbia_near_dups_dist' : ''+sim_dist+''})
        b.put(''+str(ht_ids[i])+'',{'meta:columbia_near_dups_biggest_dbid' : ''+str(biggest_dbid)+''})
        for dup in dup_list[i]:
            b.put(''+str(dup)+'',{'meta:columbia_near_dups' : ''+sim_str+''})
                    b.put(''+str(dup)+'',{'meta:columbia_near_dups_dist' : ''+sim_dist+''})
                    b.put(''+str(dup)+'',{'meta:columbia_near_dups_biggest_dbid' : ''+str(biggest_dbid)+''})

    b.send()
    connection.close()

    #Cleaning
    os.remove(simname)
    os.remove(featurename)
    os.remove(featurefilename)

    print >>flog,'Pairwise compute time: ', time.time() - t0
    flog.close()