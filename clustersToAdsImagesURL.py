import pickle
#import requests
import os
import json
import MySQLdb
import codecs
from elasticsearch import Elasticsearch
import csv

sql_withurl='select i.id,i.url,i.location,ads.url,ads.id from images i left join ads on i.ads_id=ads.id where i.location in (%s) and ads.id=(%s) order by i.id;'
sql_nourl='select i.id,i.url,i.location,ads.url,ads.id from images i left join ads on i.ads_id=ads.id where ads.id=(%s) order by i.id;'

global_var = json.load(open('global_var_all.json'))
isthost=global_var['ist_db_host']
istuser=global_var['ist_db_user']
istpwd=global_var['ist_db_pwd']
istdb=global_var['ist_db_dbname']
localhost=global_var['local_db_host']
localuser=global_var['local_db_user']
localpwd=global_var['local_db_pwd']
localdb=global_var['local_db_dbname']

all_data=pickle.load(open("alldata_istgtv6.pkl","rb"))

actexpads=[(pos,adslist) for pos,adslist in enumerate(all_data['all_expanded_adsid']) if len(adslist)>0]
print len(actexpads)

csvfile=open('alladsinfos.csv', 'wb')
alladswriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)

def getFixAdUrl(adid,imagesurl):
    db=MySQLdb.connect(host=isthost,user=istuser,passwd=istpwd,db=istdb)
    c=db.cursor()
    if len(imagesurl)==1 and not imagesurl[0]: # empty
        sqlq = sql_nourl % (adid,)
        c.execute(sqlq)
    else:
        sqlq = sql_withurl % (', '.join(map(lambda x: '%s', imagesurl)),adid)
        c.execute(sqlq,imagesurl)
    #print sqlq
    tmpresult = c.fetchall()
    #print tmpresult
    if len(tmpresult)>0:
      adurl=str(tmpresult[0][3])
    else:
        print "Couldn't find images for this ad?",str(adid)
        adurl=''
    db.close()
    return adurl

for oneactexp in range(len(actexpads)):
    print oneactexp
    adid=all_data['all_adsid'][actexpads[oneactexp][0]]
    #adurl=all_data['all_adsurl'][actexpads[oneactexp][0]]
    imagesurl=all_data['all_imagesurl'][actexpads[oneactexp][0]]
    adurl=getFixAdUrl(adid,imagesurl)
    imagesid=all_data['all_imagesid'][actexpads[oneactexp][0]]
    for pos,oneimg in enumerate(imagesid):
        if adid and adurl and imagesid[pos] and imagesurl[pos]: # do not write corrupted rows
            alladswriter.writerow([adid,adurl,str(imagesid[pos]),imagesurl[pos]])
    expanded_imagesid=all_data['all_expanded_imagesid'][actexpads[oneactexp][0]]
    expanded_adsid=all_data['all_expanded_adsid'][actexpads[oneactexp][0]]
    expanded_imagesurl=all_data['all_expanded_imagesurl'][actexpads[oneactexp][0]]
    expanded_adsurl=all_data['all_expanded_adsurl'][actexpads[oneactexp][0]]
    for pos,oneimg in enumerate(expanded_imagesid):
        if expanded_adsid[pos] and expanded_adsurl[pos] and expanded_imagesid[pos] and expanded_imagesurl[pos]: # do not write corrupted rows
            alladswriter.writerow([expanded_adsid[pos],expanded_adsurl[pos],expanded_imagesid[pos],expanded_imagesurl[pos]])

csvfile.close()
