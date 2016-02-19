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
	payload=payload+"\n]\n}\n}\n}" # No setting the size here makes each scroll be only 10*nb_primary_shards
	#print payload
	return payload

def getCDRInfos(ht_ids):
	es = Elasticsearch('https://'+ist_els_user+':'+ist_els_pass+'@'+ist_els_instance)
	payload = buildPayload(ht_ids)
	while True:
		try:
			response = es.search(index="memex-domains_2016.01.01",doc_type="escorts",body=payload)
		except Exception as inst:
   			#import pdb; pdb.set_trace()
			#print "ElasticSearch error when requesting:",payload[:100],"... at:",str(datetime.now())
			print "ElasticSearch error when requesting:",payload,"... at:",str(datetime.now())
			print ht_ids
			print inst
			# Could be just a timeout that will be solved next time we query...
			# Give ES a little rest first.
			time.sleep(10)
	#print(response)
	#print response.keys()
	#print response['hits'].keys()
	print "Got "+str(response['hits']['total'])+" results in "+str(response['took'])+"ms."
	return response['hits']['hits']

def getCDRInfosScan(ht_ids):
	t0=time.time()
	cdr_infos=[]
	scroll_ids=[]
	es = Elasticsearch('https://'+ist_els_user+':'+ist_els_pass+'@'+ist_els_instance)
	payload = buildPayload(ht_ids)
	while True:
		try:
			print "ElasticSearch query at:",str(datetime.datetime.now())
			#response = es.search(index="memex-domains_2016.01.01",doc_type="escorts",body=payload, search_type="scan", scroll="30m")
			response = es.search(index="memex-domains",doc_type="escorts",body=payload, search_type="scan", scroll="30m")
			break
		except Exception as inst:
   			#import pdb; pdb.set_trace()
			#print "ElasticSearch error when requesting:",payload[:100],"... at:",str(datetime.datetime.now())
			print "ElasticSearch error when requesting:",payload,"... at:",str(datetime.datetime.now())
			print ht_ids
			print inst
			if inst[0]==404:
				print "Breaking loop on error 404."
				break
			# Could be just a timeout that will be solved next time we query...
			# Give ES a little rest first.
			time.sleep(10)
	#print(response)
	print "Got "+str(response['hits']['total'])+" results in "+str(response['took'])+"ms."
	scrollId = response['_scroll_id']
	scroll_ids.append(""+str(scrollId)+"")
	while True:
		try:
			response = es.scroll(scroll_id=scrollId, scroll="30m")
			break
		except Exception as inst:
			print "ElasticSearch error when requesting first step of scrolling at:",str(datetime.datetime.now())
                        print inst
			if inst[0]==404:
				print "Breaking loop on error 404."
				break
                        time.sleep(10)
	while len(response['hits']['hits'])>0:
		print "Getting "+str(len(response['hits']['hits']))+" docs."
		cdr_infos.extend(response['hits']['hits'])
		#print "Scroll id:",str(response['_scroll_id'])
		scrollId = response['_scroll_id']
		scroll_ids.append(""+str(scrollId)+"")
		while True:
			try:
				print "ElasticSearch query at:",str(datetime.datetime.now())
				response = es.scroll(scroll_id=scrollId, scroll="30m")
				break
			except Exception as inst:
   				#import pdb; pdb.set_trace()
				#print "ElasticSearch error when requesting:",payload[:100],"... at:",str(datetime.datetime.now())
				print "ElasticSearch error when requesting:",payload,"... at:",str(datetime.datetime.now())
				print ht_ids
				print inst
				if inst[0]==404:
					print "Breaking loop on error 404."
					break
				# Could be just a timeout that will be solved next time we query...
				# Give ES a little rest first.
				time.sleep(10)
	#print ""+','.join(scroll_ids)+""
	#es.clear_scroll(scroll_id=""+', '.join(scroll_ids)+"")
	#es.clear_scroll(scroll_id="{"+','.join(scroll_ids)+"}")
	#es.clear_scroll(scroll_id=""+','.join(scroll_ids)+"")
	#print scroll_ids[0]
	#print scroll_ids[-1]
	#es.clear_scroll(scroll_id=scroll_ids[-1])
	# Never managed to have clear_scroll to work for now
	# Try to use first,last,list of 
	print "Got all cdr infos in "+str(time.time()-t0)+"s."
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
		\""+str(cdr_doc['_id'])+"\",\""+str(cdr_doc['fields']['obj_parent'][0])+"\","+str(cdr_doc['fields']['timestamp'][0])+");"
	try:
	        c.execute(query)
		db.commit()
       		db.close()
        except:
	        #print "Could not execute full insert query:",query
		#print "Is doc",cdr_doc['_id'],"already present in full ids table?"			
		pass

def insertFullCDRBulk(list_cdr_doc,list_unique_cdr_ids):
	global localhost,localuser,localpwd,localdb
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
        c=db.cursor()
        query="insert into fullIdsCDR (htid,unique_cdr_id,cdr_id,parent_cdr_id,timestamp) values "
	for i,cdr_doc in enumerate(list_cdr_doc):
		query+=" ("+str(cdr_doc['fields']['crawl_data.image_id'][0])+",\""+str(list_unique_cdr_ids[i])+"\",\
		\""+str(cdr_doc['_id'])+"\",\""+str(cdr_doc['fields']['obj_parent'][0])+"\","+str(cdr_doc['fields']['timestamp'][0])+")"
		if i==len(list_cdr_doc)-1:
			query+=";"
		else:
			query+=","
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
	  t0 = time.time()
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
	  print "Processing took "+str(time.time()-t0)+"s."
	else:
	  print "Empty CDR infos list!"

def processCDRInfosBulk(list_cdr_infos,ht_ids):
	if len(list_cdr_infos)>0:
	  print "Processing",str(len(list_cdr_infos)),"docs."
	  t0 = time.time()
	  allimages_ids=[ht_id[0] for ht_id in ht_ids]
	  allfeat_ids=[ht_id[2] for ht_id in ht_ids]
	  allsha1=[ht_id[1] for ht_id in ht_ids]
	  list_unique_cdr_ids=[]
	  for oneDoc in list_cdr_infos:
		# check if exists in unique, insert if not. Get unique_crd_id anyway
		cdr_image_id=oneDoc['fields']['crawl_data.image_id'][0]
		docIndex=allimages_ids.index(cdr_image_id)
		unique_cdr_id=getUniqueCDR(oneDoc,allsha1[docIndex],allfeat_ids[docIndex])
		#print unique_cdr_id
		list_unique_cdr_ids.append(unique_cdr_id)
	  insertFullCDRBulk(list_cdr_infos,list_unique_cdr_ids)
	  print "Processing took "+str(time.time()-t0)+"s."
	else:
	  print "Empty CDR infos list!"


# Could be improved by joining fullIdsCDR and looking for not yet inserted images...
def getHTIds(num,start):
	global localhost,localuser,localpwd,localdb
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
	c=db.cursor()
	sql='SELECT u.location,f.htid,u.sha1,u.id FROM fullIds as f join uniqueIds as u on f.uid=u.htid \
 left join uniqueIdsCDR as uidcdr on uidcdr.feat_id=u.id where u.id>'+str(start)+' and u.id<='+str(start+num)+' and \
 uidcdr.feat_id is null;'
#	sql='SELECT u.location,f.htid,u.sha1,u.id FROM fullIds as f join uniqueIds as u on f.uid=u.htid \
# left join uniqueIdsCDR as uidcdr on uidcdr.feat_id=u.id where u.id>'+str(start)+' and uidcdr.feat_id is null \
# order by u.id LIMIT '+str(num)+';'
	#sql='SELECT u.location,f.htid,u.sha1,u.id FROM fullIds as f join uniqueIds as u on f.uid=u.htid where u.id>'+str(start)+' ORDER BY u.id LIMIT '+str(num)
	c.execute(sql)
        tmpresult = c.fetchall()
	#print tmpresult
        return [[k[1],k[2],k[3]] for k in tmpresult]

def getLastId():
	global localhost,localuser,localpwd,localdb
	db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
	c=db.cursor()
	sql='SELECT u.id FROM uniqueIds as u order by u.id desc LIMIT 1;'
	c.execute(sql)
        tmpresult = c.fetchall()
	#print tmpresult
        return int(tmpresult[0][0])

	

if __name__ == '__main__':
	num=1000
	#start=(worker_id)*1210000
	#stop=(worker_id+1)*1210000
	#start=(worker_id)*3500000
	#stop=(worker_id+1)*3500000
	start=3000000
	stop=getLastId()
	while start<=stop:
		t0 = time.time()
		print "Getting "+str(num)+" unique ids from "+str(start)
		ht_ids=getHTIds(num,start)
		print "Getting ids took "+str(time.time()-t0)+"s."
		#print ht_ids
		print "[Batch info] samples number:",str(len(ht_ids))+", biggest unique id:",str(ht_ids[len(ht_ids)-1][2])
		if len(ht_ids)>0:
			#print ht_ids
			if len(ht_ids)>4096: # limit of match clause from ES
				list_cdr_infos=[]
				start=0
				while len(ht_ids[start:])>4096:
					print len(ht_ids[start:])
					list_cdr_infos.extend(getCDRInfosScan(ht_ids[start:start+4096]))
					start=start+4096
				print len(ht_ids[start:])
				if len(ht_ids[start:])>0:
					list_cdr_infos.extend(getCDRInfosScan(ht_ids[start:]))
			else:
				list_cdr_infos=getCDRInfosScan(ht_ids)
			print "Got "+str(len(list_cdr_infos))+" ES docs total."
			#print list_cdr_infos
			processCDRInfosBulk(list_cdr_infos,ht_ids)
		else:
			print "Did not retrieve ht_ids starting at",str(start)
			break
		#break
		print "Processing "+str(num)+" took "+str(time.time()-t0)+"s."
		#start=start+num
		start=ht_ids[len(ht_ids)-1][2]
		
