import pickle
import requests
import os
from elasticsearch import Elasticsearch

all_data=pickle.load(open("alldata_istgtv4.pkl","rb"))
#url = "https://darpamemex:darpamemex@esc.memexproxy.com/dig-ht-trial11/adultservice/_search"
#url = "https://esc.memexproxy.com/dig-ht-trial11/adultservice/_search"

es = Elasticsearch('https://darpamemex:darpamemex@esc.memexproxy.com')

# res = es.search(index="dig-clusters-qpr-01", body={"query": {"match_all": {}}})
#s = requests.Session()
#s.auth = ('darpamemex', 'darpamemex')
#s.headers.update({'x-test': 'true'})

#payload = "{\"query\": \n    {\n        \"terms\" : \n        {\n            \"offers.identifier\": [\n\"32675607\",\"32649628\",\"32596567\",\"32591455\",\"32698203\",\"32596171\",\"32596099\",\"1470478\",\"32649628\",\"31916418\",\"32285237\",\"31528714\",\"32285312\",\"31478763\",\"32297952\",\"31383303\",\"32750539\",\"26318037\",\"32591455\",\"1521670\",\"\",\"1586762\",\"26653042\",\"1674010\",\"34081279\",\"49331097\",\"32675607\",\"32649628\",\"32596567\",\"32591455\",\"32698203\",\"32672331\",\"32596171\",\"32596099\",\"32698203\",\"32649628\",\"32596171\",\"32672331\",\"32596099\",\"32591455\",\"32675607\",\"32596567\",\"32675607\",\"32649628\",\"32596567\",\"32591455\",\"32698203\",\"32672331\",\"32596171\",\"32596099\"\n            ]\n        }\n    }\n}\n"
#payload = "{\"query\": \n    {\n        \"terms\" : \n        {\n            \"offers.identifier\": [\n\"32675607\",\"32649628\",\"32596567\",\"32591455\",\"32698203\",\"32596171\",\"32596099\",\"1470478\",\"32649628\",\"31916418\",\"32285237\",\"31528714\",\"32285312\",\"31478763\",\"32297952\",\"31383303\",\"32750539\",\"26318037\",\"32591455\",\"1521670\",\"\",\"1586762\",\"26653042\",\"1674010\",\"34081279\",\"49331097\",\"32675607\",\"32649628\",\"32596567\",\"32591455\",\"32698203\",\"32672331\",\"32596171\",\"32596099\",\"32698203\",\"32649628\",\"32596171\",\"32672331\",\"32596099\",\"32591455\",\"32675607\",\"32596567\",\"32675607\",\"32649628\",\"32596567\",\"32591455\",\"32698203\",\"32672331\",\"32596171\",\"32596099
#headers = {
#    'authorization': "Basic ZGFycGFtZW1leDpkYXJwYW1lbWV4",
#    'cache-control': "no-cache",
#    'postman-token': "5c2227a0-e019-2ba4-d61f-875312920bf7"
#    }

actexpads=[(pos,adslist) for pos,adslist in enumerate(all_data['all_expanded_adsid']) if len(adslist)>0]
for oneactexp in range(len(actexpads)):
	init_adid=all_data['all_adsid'][actexpads[oneactexp][0]]
	print init_adid,"expanded into:",actexpads[oneactexp][1]
	payload = "{\"query\": \n    {\n        \"terms\" : \n        {\n            \"offers.identifier\": [\n\""+"\",\"".join(map(str,actexpads[oneactexp][1]))+"\"\n            ]\n        }\n    }\n}\n"
	print payload
	#response = requests.request("POST", url, data=payload, headers=headers)
	#response = requests.request("POST", url, data=payload, auth=requests.auth.HTTPBasicAuth('darpamemex', 'darpamemex'))
	#req = requests.Request('POST', url, data=payload, headers=header)	
	#req = requests.Request('POST', url, data=payload)
	#response = s.send(req.prepare())	
	#response = es.search(index="dig-ht-trial11/adultservice/_search",body=payload)
	response = es.search(index="dig-ht-trial11",doc_type="adultservice",body=payload)
	tmp_json = "resp_"+str(init_adid)+".json"
	f = open(tmp_json,"wt")
	f.write(response.text)
	os.system("cat "+tmp_json+" | jq '.hits.hits'| jq  -c '.[] |._source' > canopy_"+str(init_adid)+".json")
