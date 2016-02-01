import os,sys,json
import struct,time
import MySQLdb
import urllib3
urllib3.disable_warnings()

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
	payload = "{\n\"fields\":[\"_id\",\"obj_stored_url\",\"obj_parent\",\"crawl_data.image_id\",\"timestamp\"],\
	\n\"query\":{\n\"bool\":{\n\"should\":["
	for i,ht_id in enumerate(ht_ids):
		if i==len(ht_ids)-1:
			payload=payload+"{\"match\":{\"crawl_data.image_id\":\""+str(ht_id[0])+"\"}}"
		else:
			payload=payload+"{\"match\":{\"crawl_data.image_id\":\""+str(ht_id[0])+"\"}},"
	payload=payload+"\n]\n}\n},\n\"size\": 10}" # How to make sure we retrieve all documents?
	# Should we scan/scroll?
	#print payload
	return payload

def getCDRInfos(ht_ids):
	es = Elasticsearch('https://'+ist_els_user+':'+ist_els_pass+'@'+ist_els_instance)
	payload = buildPayload(ht_ids)
	while True:
		try:
			response = es.search(index="memex-domains_2016.01.01",doc_type="escorts",body=payload)
		except:
			print "ElasticSearch error when requesting:",payload[:100],"... at:",str(datetime.now())
			# Could be just a timeout that will be solved next time we query...
			# Give ES a little rest first.
			time.sleep(30)
	#print(response)
	#print response.keys()
	#print response['hits'].keys()
	print "Got "+str(response['hits']['total'])+" results in "+str(response['took'])+"ms."
	return response['hits']['hits']

def getCDRInfosScan(ht_ids):
	cdr_infos=[]
	scroll_ids=[]
	es = Elasticsearch('https://'+ist_els_user+':'+ist_els_pass+'@'+ist_els_instance)
	payload = buildPayload(ht_ids)
	while True:
		try:
			print "ElasticSearch query at:",str(datetime.datetime.now())
			response = es.search(index="memex-domains_2016.01.01",doc_type="escorts",body=payload, search_type="scan", scroll="5m")
			break
		except:
			print "ElasticSearch error when requesting:",payload[:100],"... at:",str(datetime.datetime.now())
			# Could be just a timeout that will be solved next time we query...
			# Give ES a little rest first.
			time.sleep(30)
	#print(response)
	print "Got "+str(response['hits']['total'])+" results in "+str(response['took'])+"ms."
	scrollId = response['_scroll_id']
	scroll_ids.append(""+str(scrollId)+"")
	response = es.scroll(scroll_id=scrollId, scroll="5m")
	while len(response['hits']['hits'])>0:
		print "Getting "+str(len(response['hits']['hits']))+" docs."
		cdr_infos.extend(response['hits']['hits'])
		#print "Scroll id:",str(response['_scroll_id'])
		scrollId = response['_scroll_id']
		scroll_ids.append(""+str(scrollId)+"")
		while True:
			try:
				print "ElasticSearch query at:",str(datetime.datetime.now())
				response = es.scroll(scroll_id=scrollId, scroll="5m")
				break
			except:
				print "ElasticSearch error when requesting:",payload[:100],"... at:",str(datetime.datetime.now())
				# Could be just a timeout that will be solved next time we query...
				# Give ES a little rest first.
				time.sleep(30)
	#print ""+','.join(scroll_ids)+""
	#es.clear_scroll(scroll_id=""+', '.join(scroll_ids)+"")
	#es.clear_scroll(scroll_id="{"+','.join(scroll_ids)+"}")
	#es.clear_scroll(scroll_id=""+','.join(scroll_ids)+"")
	#print scroll_ids[0]
	#print scroll_ids[-1]
	#es.clear_scroll(scroll_id=scroll_ids[-1])
	# Never managed to have clear_scroll to work for now
	# Try to use first,last,list of 
	return cdr_infos
	#print response.keys()
	#print response['hits'].keys()

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
	        print "Could not execute unique insert query:",query
		print "Is doc",cdr_doc['_id'],"already present in unique ids table?"
	return cdr_doc['_id']

def insertFullCDR(cdr_doc,unique_cdr_id):
	global localhost,localuser,localpwd,localdb
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
        c=db.cursor()
        query="insert into fullIdsCDR (htid,unique_cdr_id,cdr_id,parent_cdr_id,timestamp) values \
		("+str(cdr_doc['fields']['crawl_data.image_id'][0])+",\""+str(unique_cdr_id)+"\",\
		\""+str(cdr_doc['_id'])+"\",\""+str(cdr_doc['fields']['obj_parent'])+"\","+str(cdr_doc['fields']['timestamp'][0])+");"
	try:
	        c.execute(query)
		db.commit()
       		db.close()
        except:
	        #print "Could not execute full insert query:",query
		#print "Is doc",cdr_doc['_id'],"already present in full ids table?"			
		pass

def checkUniqueCDR(sha1):
	global localhost,localuser,localpwd,localdb
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
	c=db.cursor()
	sql='SELECT cdr_id FROM uniqueIdsCDR where sha1="'+str(sha1)+'"'
	#print sql
	c.execute(sql)
        tmpresult = c.fetchall()
	if len(tmpresult)==1: 
		return tmpresult[0][0]
	else:
		#print str(sha1),"not present in DB. Should be added."
		return None


def processCDRInfos(list_cdr_infos,ht_ids):
	if len(list_cdr_infos)>0:
	  print "Processing",str(len(list_cdr_infos)),"docs."
	  allimages_ids=[ht_id[0] for ht_id in ht_ids]
	  allfeat_ids=[ht_id[2] for ht_id in ht_ids]
	  allsha1=[ht_id[1] for ht_id in ht_ids]
	  for oneDoc in list_cdr_infos:
		# check if exists in unique, insert if not. Get unique_crd_id anyway
		cdr_image_id=oneDoc['fields']['crawl_data.image_id'][0]
		docIndex=allimages_ids.index(cdr_image_id)
		unique_cdr_id=getUniqueCDR(oneDoc,allsha1[docIndex],allfeat_ids[docIndex])
		#print unique_cdr_id
		# always try to insert in full
		insertFullCDR(oneDoc,unique_cdr_id)
		#print oneDoc['_id'],cdr_image_id,oneDoc['fields']['timestamp'][0],\
		#      oneDoc['fields']['obj_stored_url'][0]
	else:
	  print "Empty CDR infos list!"

def getHTIds(num,start):
	global localhost,localuser,localpwd,localdb
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
	c=db.cursor()
	sql='SELECT u.location,f.htid,u.sha1,u.id FROM fullIds as f join uniqueIds as u on f.uid=u.htid where u.id>'+str(start)+' ORDER BY u.id LIMIT '+str(num)
	c.execute(sql)
        tmpresult = c.fetchall()
	#print tmpresult
        return [[k[1],k[2],k[3]] for k in tmpresult]
	

if __name__ == '__main__':
	num=1000
	start=139000
	while True:
		t0 = time.time()
		ht_ids=getHTIds(num,start)
		if len(ht_ids)>0:
			#print ht_ids
			list_cdr_infos=getCDRInfosScan(ht_ids)
			#print list_cdr_infos
			processCDRInfos(list_cdr_infos,ht_ids)
		else:
			print "Did not retrieve ht_ids starting at",str(START)
			break
		print "Processing "+str(num)+" took "+str(time.time()-t0)+"s."
		start=start+num
		
