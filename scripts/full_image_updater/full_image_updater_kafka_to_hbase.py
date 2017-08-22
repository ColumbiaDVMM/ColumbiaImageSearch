import json
from cufacesearch.indexer.hbase_indexer_minimal import HBaseIndexerMinimal
from kafka import KafkaConsumer
from argparse import ArgumentParser

# This script simulates the way updates were generated from the spark workflows but reading from a kafka topic

if __name__ == "__main__":

  if __name__ == "__main__":
    # Get conf file
    parser = ArgumentParser()
    parser.add_argument("-c", "--conf", dest="conf_file", required=True)
    options = parser.parse_args()

  global_conf = json.loads(open(options.conf, 'rt'))
  print global_conf
  # could be a parameter of the script or in the conf.
  check_column = 'info:cu_feat_id'
  indexer = HBaseIndexerMinimal(global_conf)
  
  # Need to read from Kafka topic 'images_out_topic' of KafkaImageProcessor "KIP_images_out_topic" in conf
  topic = global_conf["KIP_images_out_topic"]
  servers = global_conf["KIP_servers"]
  group = global_conf["KIP_images_update_group"]
  consumer = KafkaConsumer(topic, group_id=group, bootstrap_servers=servers)

  list_sha1s_to_process = []

  while True:
    list_sha1s_to_check = []
    for msg in consumer:
      # (decode?) json and get field 'sha1'
      list_sha1s_to_check.append(msg['sha1'])
      if len(list_sha1s_to_check) >= indexer.batch_update_size:
        break
    # Check if sha1 has 'info:cu_feat_id' (or 'info:featnorm_cu', 'info:hash256_cu') in HBase table 'sha1infos'
    sha1s_rows = indexer.get_columns_from_sha1_rows(list_sha1s_to_check, [check_column])
    # Keep those that don't
    for sha1_row in sha1s_rows:
      if check_column not in sha1_row:
        list_sha1s_to_process.append(sha1_row[0])
    if len(list_sha1s_to_process) >= indexer.batch_update_size:
      # Push them with HBase indexer push_list_updates(list_sha1s)
      indexer.push_list_updates(list_sha1s_to_process)
      list_sha1s_to_process = []
