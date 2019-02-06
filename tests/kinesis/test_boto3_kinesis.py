from __future__ import print_function


# # To avoid warnings when VERIFY_CERTIFICATES is False
# Does not seem to work, using
# import urllib3
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# import requests
# from requests.packages.urllib3.exceptions import InsecureRequestWarning
# requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

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


  # tries = 0
  # client = get_kinesis_client()
  # print(client)
  # while tries < NB_TRIALS:
  #   tries += 1
  #   time.sleep(1)
  #   try:
  #     response = client.describe_stream(StreamName=STREAM_NAME)
  #     if response['StreamDescription']['StreamStatus'] == 'ACTIVE':
  #       break
  #   except Exception as inst:
  #     msg = 'Error #{}: while trying to describe kinesis stream : {}. {}'
  #     print(msg.format(tries, STREAM_NAME, inst))
  #     # botocore.exceptions.NoCredentialsError: Unable to locate credentials
  #     resp = client.create_stream(StreamName=STREAM_NAME, ShardCount=NB_SHARDS)
  #     print(resp)
  # else:
  #   raise RuntimeError('Stream is still not active, aborting...')
  #
  # shard_ids = []
  # stream_name = None
  # if response and 'StreamDescription' in response:
  #   stream_name = response['StreamDescription']['StreamName']
  #   for shard_id in response['StreamDescription']['Shards']:
  #     print(shard_id)
  #     shard_id = shard_id['ShardId']
  #     shard_iterator = client.get_shard_iterator(StreamName=stream_name,
  #                                                ShardId=shard_id,
  #                                                ShardIteratorType=SHARD_ITERATOR_TYPE)
  #     shard_ids.append({'shard_id': shard_id, 'shard_iterator': shard_iterator['ShardIterator']})
  #   print(shard_ids)
  #
  #
  # # for shard_id in shard_ids:
  # #   rec_response = client.get_records(ShardIterator=shard_id['shard_iterator'],
  # #                                     Limit=2)
  # #   if len(rec_response['Records']) == 0:
  # #     print("Nothing in shard {}".format(shard_id['shard_id']))
  # #     # TODO: try to put_records?
  # #     # sha1 ...
  # #     client.put_records(Records=[
  # #       {'Data': json.dumps({'sha1': 'aa785adca3fcdfe1884ae840e13c6d294a2414e8'}),
  # #        'PartitionKey': 'firstsha1'+str(shard_id)}],
  # #                        StreamName=stream_name)
  # #     # or list_sha1s
  # #     client.put_records(Records=[{'Data': json.dumps({'list_sha1s': [
  # #       'aecf5455a8ada1cc54642e2ad072cd9edfa56ff6', '184c256e763dc16d1bfbd943870c936daff1506b']}),
  # #                                  'PartitionKey': 'firstlistsha1s'+str(shard_id)}],
  # #                        StreamName=stream_name)
  # #   # break
  #
  # #RegisterStreamConsumer then SubscribeToShard
  # #Why isn't SubscribeToShard documented in boto3?
  #
  # # Should we just have a simple consumer that reads from all shards and store the sequence number
  # # Then call get_shard_iterator with ShardIteratorType='AT_SEQUENCE_NUMBER' or 'AFTER_SEQUENCE_NUMBER'?
  # # How to store this persistently?... The KCL uses a DynamoDB table ApplicationName
  # # If we cannot, it just adds many useless check for unprocessed sha1s on every restart
  # # but not really an issue...
  #
  # empty = 0
  # lim_get_rec = 5
  # # Actually iterate forever?
  # for shard_id in shard_ids:
  #   shard_it = shard_id['shard_iterator']
  #   print("Getting records starting from {} in shard {}".format(shard_it, shard_id['shard_id']))
  #
  #   rec_response = client.get_records(ShardIterator=shard_id['shard_iterator'],
  #                                     Limit=lim_get_rec)
  #
  #   while 'NextShardIterator' in rec_response:
  #     #NB: if len(rec_response['Records']) is < lim_get_rec we have reached end of stream
  #     if len(rec_response['Records']) > 0:
  #       for rec in rec_response['Records']:
  #         rec_json = json.loads(rec['Data'])
  #         sqn = rec['SequenceNumber']
  #         msg = 'Found record #{} {} with "{}" key in "{}" from shard {}.'
  #         if 'list_sha1s' in rec_json:
  #           print(msg.format(sqn, rec_json['list_sha1s'], 'list_sha1s', STREAM_NAME, shard_id['shard_id']))
  #         elif 'sha1' in rec_json:
  #           print(msg.format(sqn, rec_json['sha1'], 'sha1', STREAM_NAME, shard_id['shard_id']))
  #         else:
  #           print("Unknown record keys: {}".format(rec_json.keys()))
  #
  #       time.sleep(1)
  #
  #     else:
  #       print("Shard {} seems empty".format(shard_id['shard_id']))
  #       empty += 1
  #       break
  #
  #     shard_it = rec_response['NextShardIterator']
  #     print("Getting records starting from {} in shard {}".format(shard_it, shard_id['shard_id']))
  #     rec_response = client.get_records(ShardIterator=rec_response['NextShardIterator'],
  #                                       Limit=lim_get_rec)
  #
  #   if empty == len(shard_ids):
  #     print("All shards seem empty")
  #     break

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

