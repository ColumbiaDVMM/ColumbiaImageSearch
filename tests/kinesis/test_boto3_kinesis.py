from __future__ import print_function


# # To avoid warnings when VERIFY_CERTIFICATES is False
# import urllib3
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# import requests
# from requests.packages.urllib3.exceptions import InsecureRequestWarning
# requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
# Does not seem to work, actually using
# export PYTHONWARNINGS="ignore:Unverified HTTPS request"

import boto3
import json
import os

ACCESS_KEY=os.getenv('AWS_ACCESS_KEY_ID')
SECRET_KEY=os.getenv('AWS_SECRET_ACCESS_KEY')
STREAM_NAME=os.getenv('KINESIS_STREAM_NAME', 'default-stream')
NB_SHARDS=os.getenv('NB_SHARDS', 2)
LIMIT_GET=os.getenv('LIMIT_GET', 10)
REGION_NAME=os.getenv('KINESIS_REGION_NAME', 'us-east-1') # REGION_NAME cannot be None
ENDPOINT_URL=os.getenv('ENDPOINT_URL')
USE_SSL=os.getenv('USE_SSL', True)
VERIFY_CERTIFICATES=os.getenv('VERIFY_CERTIFICATES', False)
SHARD_ITERATOR_TYPE="TRIM_HORIZON"

NB_TRIALS=3

def get_kinesis_client():
  kinesis_conf = {"aws_access_key_id": ACCESS_KEY,
                  "aws_secret_access_key": SECRET_KEY,
                  "region_name": REGION_NAME,
                  "endpoint_url": ENDPOINT_URL,
                  "use_ssl": USE_SSL,
                  "verify": VERIFY_CERTIFICATES}
  print(kinesis_conf)
  return boto3.client('kinesis', region_name=REGION_NAME, endpoint_url=ENDPOINT_URL,
                      aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY,
                      verify=VERIFY_CERTIFICATES, use_ssl=USE_SSL)


def get_random_sha1():
  from hashlib import sha1
  import random
  return sha1(str(random.getrandbits(256)).encode('utf-8')).hexdigest().upper()


if __name__ == "__main__":

  push = True
  nb_singles = 50
  nb_list = 20
  max_len_list = 50

  #READ
  from cufacesearch.ingester.kinesis_ingester import KinesisIngester
  # OK
  ki = KinesisIngester({'region_name': REGION_NAME, 'stream_name': STREAM_NAME, 'verbose': 6,
                        'endpoint_url': ENDPOINT_URL, 'verify_certificates': VERIFY_CERTIFICATES})

  for msg in ki.get_msg_json():
    print(msg)

  # PUSH
  if push:
    import random
    client = get_kinesis_client()

    for _ in range(nb_singles):
      one_sha1 = get_random_sha1()
      single_rec = [{'Data': json.dumps({'sha1': one_sha1}), 'PartitionKey': one_sha1}]
      client.put_records(Records=single_rec, StreamName=STREAM_NAME)

    for _ in range(nb_list):
      one_len_list = random.randint(0, max_len_list)
      list_sha1s = []
      for pos in range(one_len_list):
        list_sha1s.append(get_random_sha1())
      list_rec = [{'Data': json.dumps({'list_sha1s': list_sha1s}), 'PartitionKey': get_random_sha1()}]
      client.put_records(Records=list_rec, StreamName=STREAM_NAME)

