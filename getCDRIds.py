import os,sys,json
import struct,time
import MySQLdb

from array import *
from collections import OrderedDict 
import math
import ntpath
import hashlib
import datetime
import pickle

ist_down=False

from elasticsearch import Elasticsearch

os.chdir('/home/ubuntu/memex/')
global_var = json.load(open('./conf/global_var_all.json'))

isthost=global_var['ist_db_host']
istuser=global_var['ist_db_user']
istpwd=global_var['ist_db_pwd']
istdb=global_var['ist_db_dbname']

localhost=global_var['local_db_host']
localuser=global_var['local_db_user']
localpwd=global_var['local_db_pwd']
localdb=global_var['local_db_dbname']

ist_els_instance=global_var['ist_els_instance']
ist_els_user=global_var['ist_els_user']
ist_els_pass=global_var['ist_els_pass']

def buildPayload(ht_ids):
	payload = "{\n\"fields\":[\"_id\",\"obj_stored_url\",\"crawl_data.image_id\",\"timestamp\"],\
	\n\"query\":{\n\"bool\":{\n\"should\":["
	for i,ht_id in enumerate(ht_ids):
		if i==len(ht_ids)-1:
			print "last_sample"
			payload=payload+"{\"match\":{\"crawl_data.image_id\":\""+str(ht_id[0])+"\"}}"
		else:
			payload=payload+"{\"match\":{\"crawl_data.image_id\":\""+str(ht_id[0])+"\"}},"
	payload=payload+"\n]\n}\n},\n\"size\": 1000}" # How to make sure we retrieve all documents?
	# Should we scan/scroll?
	print payload
	return payload

def getCDRInfos(ht_ids):
	es = Elasticsearch('https://'+ist_els_user+':'+ist_els_pass+'@'+ist_els_instance)
	payload = buildPayload(ht_ids)
	response = es.search(index="memex-domains_2016.01.01",doc_type="escorts",body=payload)
	#print(response)
	#print response.keys()
	#print response['hits'].keys()
	print "Got "+str(response['hits']['total'])+" results."
	return response['hits']['hits']

def getUniqueCDR(cdr_doc,sha1,feat_id):
	unique_cdr_id=checkUniqueCDR(sha1)
	if not unique_cdr_id:
		# Need to insert doc as new unique
		unique_cdr_id=insertUniqueCDR(cdr_doc,sha1,feat_id)
	return unique_cdr_id	

def insertUniqueCDR(cdr_doc,sha1,feat_id):
	global localhost,localuser,localpwd,localdb
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
        c=db.cursor()
        query="insert into uniqueIdsCDR (location,htid,sha1,feat_id,cdr_id,timestamp) values \
		(\""+str(cdr_doc['fields']['obj_stored_url'][0])+"\","+str(cdr_doc['fields']['crawl_data.image_id'][0])+",\
		\""+str(sha1)+"\","+str(feat_id)+",\""+str(cdr_doc['_id'])+"\","+str(cdr_doc['fields']['timestamp'][0])+");"
	try:
	        c.execute(query)
		db.commit()
       		db.close()
        except:
	        print "Could not execute query:",query
		print "Is doc ",cdr_doc['_id'],"already present?"			
	return cdr_doc['_id']

def insertFullCDR(cdr_doc,unique_cdr_id):
	global localhost,localuser,localpwd,localdb
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
        c=db.cursor()
        query="insert into fullIdsCDR (htid,unique_cdr_id,cdr_id,timestamp) values \
		("+str(cdr_doc['fields']['crawl_data.image_id'][0])+",\""+str(unique_cdr_id)+"\",\
		\""+str(cdr_doc['_id'])+"\","+str(cdr_doc['fields']['timestamp'][0])+");"
	try:
	        c.execute(query)
		db.commit()
       		db.close()
        except:
	        print "Could not execute query:",query
		print "Is doc ",cdr_doc['_id'],"already present?"			

def checkUniqueCDR(sha1):
	global localhost,localuser,localpwd,localdb
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
	c=db.cursor()
	sql='SELECT cdr_id FROM uniqueIdsCDR where sha1="'+str(sha1)+'"'
	print sql
	c.execute(sql)
        tmpresult = c.fetchall()
	if len(tmpresult)==1: 
		return tmpresult[0][0]
	else:
		print str(sha1),"not present in DB."
		return None


def processCDRInfos(list_cdr_infos,ht_ids):
	allimages_ids=[ht_id[0] for ht_id in ht_ids]
	allfeat_ids=[ht_id[2] for ht_id in ht_ids]
	allsha1=[ht_id[1] for ht_id in ht_ids]
	for oneDoc in list_cdr_infos:
		# check if exists in unique, insert if not. Get unique_crd_id anyway
		cdr_image_id=oneDoc['fields']['crawl_data.image_id'][0]
		docIndex=allimages_ids.index(cdr_image_id)
		unique_cdr_id=getUniqueCDR(oneDoc,allsha1[docIndex],allfeat_ids[docIndex])
		print unique_cdr_id
		# always insert in full
		insertFullCDR(oneDoc,unique_cdr_id)
		print oneDoc['_id'],cdr_image_id,oneDoc['fields']['timestamp'][0],\
		      oneDoc['fields']['obj_stored_url'][0]

def getHTIds(num,start):
	global localhost,localuser,localpwd,localdb
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
	c=db.cursor()
	sql='SELECT location,htid,sha1,id FROM uniqueIds where id>'+str(start)+' ORDER BY id LIMIT '+str(num)
	c.execute(sql)
        tmpresult = c.fetchall()
	print tmpresult
        return [[k[1],k[2],k[3]] for k in tmpresult]


	

if __name__ == '__main__':
	t0 = time.time()
	ht_ids=getHTIds(10,0)
	print ht_ids
	list_cdr_infos=getCDRInfos(ht_ids)
	print list_cdr_infos
	processCDRInfos(list_cdr_infos,ht_ids)
