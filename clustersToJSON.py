import pickle
#import requests
import os
import json
import codecs
from elasticsearch import Elasticsearch

global_var = json.load(open('global_var_all.json'))

isthost=global_var['isi_els_ht']

all_data=pickle.load(open("alldata_istgtv4.pkl","rb"))
es = Elasticsearch(isthost)

actexpads=[(pos,adslist) for pos,adslist in enumerate(all_data['all_expanded_adsid']) if len(adslist)>0]
print len(actexpads)
nbreps=0
all_init_adids=set()
for oneactexp in range(len(actexpads)):
	print oneactexp
    if all_data['all_adsid'][actexpads[oneactexp][0]]
	all_init_adids.add(all_data['all_adsid'][actexpads[oneactexp][0]])
print len(all_init_adids)
for oneactexp in range(len(actexpads)):
	init_adid=all_data['all_adsid'][actexpads[oneactexp][0]]
	print init_adid,"expanded into:",actexpads[oneactexp][1]
	#print actexpads
	canopy_adsid = actexpads[oneactexp][1]
	noerror=True
	while noerror:
		try:
			canopy_adsid.remove('')
		except:
			noerror=False
	canopy_adsid.append(init_adid)
	#print canopy_adsid
	payload = "{\"query\": \n    {\n        \"terms\" : \n        {\n            \"offers.identifier\": [\n\""+"\",\"".join(map(str,canopy_adsid))+"\"\n            ]\n        }\n    }\n}\n"
	response = es.search(index="dig-ht-trial11",doc_type="adultservice",body=payload)
	#print response
	tmp_json = "resp_"+str(init_adid)+".json"
	#print tmp_json
	with codecs.open(tmp_json, 'w', 'utf8') as f:
	    f.write(json.dumps(response, sort_keys = True, ensure_ascii=False))
	os.system("cat "+tmp_json+" | jq '.hits.hits'| jq  -c '.[] | ._source' > canopy_"+str(init_adid)+".json") #does not work
	nbreps=nbreps+1
print nbreps