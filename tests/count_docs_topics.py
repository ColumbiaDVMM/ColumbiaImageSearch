import sys
import json
from kafka import KafkaConsumer

keys_prefix = '/Users/svebor/Documents/Workspace/CodeColumbia/MEMEX/ColumbiaFaceSearch/data/keys/hg-kafka-'
group_id = 'count_ht_cdr31_ext_1'

#idk = '_id'
#topic = 'ht-cdr31'
#topic = "backpage-test-ext2"

idk = 'sha1'
topic = "backpage-test-images2"


broker_list = [
    '54.202.16.183:9093',
    '54.213.217.208:9093',
    '34.223.224.66:9093',
    '54.214.183.205:9093',
    '54.191.242.156:9093'
]

args = {
    "security_protocol":"SSL",
    "ssl_cafile":keys_prefix+"ca-cert.pem",
    "ssl_certfile":keys_prefix+"client-cert.pem",
    "ssl_keyfile":keys_prefix+"client-key.pem",
    "ssl_check_hostname":False,
    "max_poll_records": 10,
    "session_timeout_ms": 40000,
    "heartbeat_interval_ms": 4000,
    "request_timeout_ms": 80000,
    "consumer_timeout_ms": 100000
}

if __name__ == "__main__": 

	consumer = KafkaConsumer(
			topic,
			bootstrap_servers=broker_list,
			group_id=group_id,
			auto_offset_reset='earliest',
			**args
	)

	uniq_docs = dict()
	count = 0
	for c in consumer:
		x = json.loads(c.value)
		if count==0:
			print x.keys()
		sys.stdout.flush()
		id = x[idk]
		if id not in uniq_docs:
			uniq_docs[id] = 1
		else:
			uniq_docs[id] += 1
		count +=1
		if count%100 == 0:
			print count, len(uniq_docs.keys())

	print len(uniq_docs.keys())
	#print json.dumps(uniq_docs, indent=2)