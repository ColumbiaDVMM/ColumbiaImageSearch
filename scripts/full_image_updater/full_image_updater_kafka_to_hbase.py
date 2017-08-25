import json
from cufacesearch.indexer.hbase_indexer_minimal import HBaseIndexerMinimal
from kafka import KafkaConsumer
from argparse import ArgumentParser

# This script simulates the way updates were generated from the spark workflows but reading from a kafka topic

def store_img_infos(dict_sha1_infos, msg):
  dict_sha1_infos[msg['sha1']] = dict()
  # discard 'img_buffer', and 'sha1'
  for k in msg:
    if k != "img_buffer" and k != "sha1:
      dict_sha1_infos[msg['sha1']][k] = msg[k]
  return dict_sha1_infos

def cleanup_dict_infos(dict_sha1_infos, list_del_sha1s):
  for sha1 in list_del_sha1s:
    del dict_sha1_infos[sha1]
  return dict_sha1_infos

def get_dict_push(dict_sha1_infos, list_get_sha1s):
  dict_push = dict()
  for sha1 in list_get_sha1s:
    dict_push[sha1] = dict()
    # build column names properly i.e. appending 'info:'
    tmp_dict = dict_sha1_infos[sha1]
    for k in tmp_dict:
      dict_push[sha1]['info:'+k] = tmp_dict[k]
  return dict_push

if __name__ == "__main__":

  if __name__ == "__main__":
    # Get conf file
    parser = ArgumentParser()
    parser.add_argument("-c", "--conf", dest="conf_file", required=True)
    options = parser.parse_args()

  global_conf = json.loads(open(options.conf, 'rt'))
  print global_conf
  # could be a parameter of the script or in the conf.
  # could be 'info:cu_feat_id' (or 'info:featnorm_cu', 'info:hash256_cu')
  check_column = 'info:cu_feat_id'
  indexer = HBaseIndexerMinimal(global_conf)
  
  # Need to read from Kafka topic 'images_out_topic' of KafkaImageProcessor "KIP_images_out_topic" in conf
  topic = global_conf["KIP_images_out_topic"]
  servers = global_conf["KIP_servers"]
  group = global_conf["KIP_images_update_group"]
  consumer = KafkaConsumer(topic, group_id=group, bootstrap_servers=servers)

  list_sha1s_to_process = []
  dict_sha1_infos = dict()
  while True:
    list_sha1s_to_check = []

    for msg in consumer:
      # (decode?) json and get field 'sha1'
      list_sha1s_to_check.append(msg['sha1'])
      # store other fields to be able to push them too
      dict_sha1_infos = store_img_infos(dict_sha1_infos, msg)
      if len(list_sha1s_to_check) >= indexer.batch_update_size:
        break
    # Check if sha1 has 'info:cu_feat_id' (or 'info:featnorm_cu', 'info:hash256_cu') in HBase table 'sha1infos'
    # This call will only return rows with that have the check_column
    sha1s_rows = indexer.get_columns_from_sha1_rows(list_sha1s_to_check, [check_column])
    found_sha1_rows = set([row[0] for row in sha1s_rows])
    # Clean up 'dict_sha1_infos' deleting found_sha1_rows?
    dict_sha1_infos = cleanup_dict_infos(dict_sha1_infos, found_sha1_rows)
    notfound_sha1_rows = set(list_sha1s_to_check) - found_sha1_rows
    # Mark sha1 that were not found as to be processed
    for sha1 in notfound_sha1_rows:
      list_sha1s_to_process.append(sha1)
    # Push them to HBase by batch of 'batch_update_size'
    if len(list_sha1s_to_process) >= indexer.batch_update_size:
      # Trim here to push exactly a batch of 'batch_update_size'
      list_push = list_sha1s_to_process[:indexer.batch_update_size]
      # Gather corresponding sha1 infos
      dict_push = get_dict_push(dict_sha1_infos, list_push)
      # Push them
      indexer.push_dict_rows(dict_push, indexer.table_sha1infos_name)
      # Push update
      indexer.push_list_updates(list_push)
      # Gather any remaining sha1s and clean up infos
      if len(list_sha1s_to_process) > indexer.batch_update_size:
        list_sha1s_to_process = list_sha1s_to_process[indexer.batch_update_size:]
      else:
        list_sha1s_to_process = []
      dict_sha1_infos = cleanup_dict_infos(dict_sha1_infos, list_push)