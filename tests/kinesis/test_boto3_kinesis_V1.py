from __future__ import print_function

import os
import boto3
import time
import json

STREAM_NAME=os.getenv('KINESIS_STREAM_NAME', 'default-stream')
REGION_NAME=os.getenv('KINESIS_REGION_NAME', 'us-east-1')
SHARD_ITERATOR_TYPE="TRIM_HORIZON"
NB_TRIALS=3

def get_kinesis_client():
  import os
  auth = {"aws_access_key_id": os.environ['AWS_ACCESS_KEY_ID'],
          "aws_secret_access_key": os.environ['AWS_SECRET_ACCESS_KEY']}
  print(auth)
  return boto3.client('kinesis', region_name=REGION_NAME)


if __name__ == "__main__":
  tries = 0
  client = get_kinesis_client()
  print(client)
  while tries < NB_TRIALS:
    tries += 1
    time.sleep(1)
    try:
      response = client.describe_stream(StreamName=STREAM_NAME)
      if response['StreamDescription']['StreamStatus'] == 'ACTIVE':
        break
    except Exception as inst:
      msg = 'Error #{}: while trying to describe kinesis stream : {}. {}'
      print(msg.format(tries, STREAM_NAME, inst))
  else:
    raise RuntimeError('Stream is still not active, aborting...')

  shard_ids = []
  stream_name = None
  if response and 'StreamDescription' in response:
    stream_name = response['StreamDescription']['StreamName']
    for shard_id in response['StreamDescription']['Shards']:
      shard_id = shard_id['ShardId']
      shard_iterator = client.get_shard_iterator(StreamName=stream_name,
                                                 ShardId=shard_id,
                                                 ShardIteratorType=SHARD_ITERATOR_TYPE)
      shard_ids.append({'shard_id': shard_id, 'shard_iterator': shard_iterator['ShardIterator']})
    print(shard_ids)


  for shard_id in shard_ids:
    rec_response = client.get_records(ShardIterator=shard_id['shard_iterator'],
                                      Limit=2)

    while 'NextShardIterator' in rec_response:
      rec_response = client.get_records(ShardIterator=rec_response['NextShardIterator'],
                                        Limit=10)
      if len(rec_response['Records']) == 0:
        print("Nothing in shard {}".format(shard_id['shard_id']))
        break

      for rec in rec_response['Records']:
        list_sha1 = json.loads(rec['Data'])
        msg = 'Found {} in record from shard {}.'
        print(msg.format(list_sha1['list_sha1s'], shard_id['shard_id']))
      time.sleep(1)
