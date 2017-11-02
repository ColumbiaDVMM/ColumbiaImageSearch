import os
import json
from argparse import ArgumentParser

if __name__ == "__main__":
  # Get config
  parser = ArgumentParser()
  parser.add_argument("-o", "--output_dir", dest="output_dir", required=True)
  options = parser.parse_args()

  # Initialization
  conf = dict()
  conf_name = os.environ['conf_name']
  extr_prefix = "EXTR_"
  check_ingester_prefix = "KIcheck_"
  proc_ingester_prefix = "KIproc_"
  hbase_prefix = "HBI_"

  conf[extr_prefix + 'check_ingester_prefix'] = check_ingester_prefix
  conf[extr_prefix + 'proc_ingester_prefix'] = proc_ingester_prefix
  conf[extr_prefix + 'indexer_prefix'] = hbase_prefix
  conf[extr_prefix + 'indexer_type'] = "hbase_indexer_minimal"

  # Extraction settings
  extr_type = os.environ['extr_type']
  if extr_type == "dlibface":
    # Extractions options
    featurizer_prefix = "DLIB_"
    conf[extr_prefix + 'featurizer_prefix'] = featurizer_prefix
    conf[extr_prefix + 'featurizer_type'] = "dlib"
    conf[extr_prefix + 'detector_type'] = "dlib"
    conf[extr_prefix + 'input_type'] = "face"
    conf[featurizer_prefix + 'pred_path'] = "./data/models/shape_predictor_68_face_landmarks.dat"
    conf[featurizer_prefix + 'rec_path'] = "./data/models/dlib_face_recognition_resnet_model_v1.dat"
  elif extr_type == "sbpycaffeimg":
    featurizer_prefix = "SBPY_"
    conf[extr_prefix + 'featurizer_prefix'] = featurizer_prefix
    conf[extr_prefix + 'featurizer_type'] = "sbpycaffe"
    conf[extr_prefix + 'detector_type'] = "full"
    conf[extr_prefix + 'input_type'] = "image"
    conf[featurizer_prefix + 'sbcaffe_path'] = "./data/models/caffe_sentibank_train_iter_250000",
    conf[featurizer_prefix + 'imgmean_path'] = "./data/models/imagenet_mean.npy"
  else:
    raise ValueError("Unknown extraction type: {}".format(extr_type))

  # HBase settings
  conf[hbase_prefix + 'host'] =  os.environ['hbase_host']
  conf[hbase_prefix + 'table_sha1infos'] = os.environ['table_sha1infos']
  conf[hbase_prefix + 'table_updateinfos'] = os.environ['table_updateinfos']
  conf[hbase_prefix + 'batch_update_size'] = os.environ['batch_update_size']
  conf[hbase_prefix + 'pool_thread'] = 1

  # Local input settings
  if os.environ['input_type'] == "local":
    conf[extr_prefix + 'file_input'] = True

  # Generic ingestion settings
  verbose = os.getenv('verbose', 0)
  conf[extr_prefix + "verbose"] = verbose
  conf[extr_prefix + "max_delay"] = 600
  conf[extr_prefix + "nb_threads"] = 2

  kafka_servers = json.loads(os.getenv('kafka_servers', ["kafka0.team-hg-memex.com:9093",
                                                         "kafka1.team-hg-memex.com:9093",
                                                         "kafka2.team-hg-memex.com:9093",
                                                         "kafka3.team-hg-memex.com:9093",
                                                         "kafka4.team-hg-memex.com:9093",
                                                         "kafka5.team-hg-memex.com:9093",
                                                         "kafka6.team-hg-memex.com:9093",
                                                         "kafka7.team-hg-memex.com:9093",
                                                         "kafka8.team-hg-memex.com:9093",
                                                         "kafka9.team-hg-memex.com:9093"]))
  kafka_security = json.loads(os.getenv('kafka_security', "{\"security_protocol\": \"SSL\", \"ssl_cafile\": \"./data/keys/hg-kafka-ca-cert.pem\", \"ssl_certfile\": \"./data/keys/hg-kafka-client-cert.pem\", \"ssl_keyfile\": \"./data/keys/hg-kafka-client-key.pem\", \"ssl_check_hostname\": false}"))
  consumer_options = json.loads(os.getenv('kafka_consumer_options', "{\"auto_offset_reset\": \"earliest\", \"max_poll_records\": 10, \"session_timeout_ms\": 300000, \"request_timeout_ms\": 600000, \"consumer_timeout_ms\": 600000}"))

  # Checker consumer
  conf[check_ingester_prefix + 'consumer_servers'] = kafka_servers
  conf[check_ingester_prefix + 'consumer_security'] = kafka_security
  conf[check_ingester_prefix + 'consumer_topics'] = os.environ['images_topic']
  conf[check_ingester_prefix + 'consumer_group'] = os.environ['extr_check_consumer_group']
  conf[check_ingester_prefix + 'consumer_options'] = consumer_options
  conf[check_ingester_prefix + 'verbose'] = verbose
  # Checker producer
  conf[check_ingester_prefix + 'producer_servers'] = kafka_servers
  conf[check_ingester_prefix + 'producer_security'] = kafka_security
  conf[check_ingester_prefix + 'producer_updates_out_topic'] = os.environ['updates_topic']

  # Processor consumer
  conf[proc_ingester_prefix + 'consumer_servers'] = kafka_servers
  conf[proc_ingester_prefix + 'consumer_security'] = kafka_security
  conf[proc_ingester_prefix + 'consumer_topics'] = os.environ['updates_topic']
  conf[proc_ingester_prefix + 'consumer_group'] = os.environ['extr_proc_consumer_group']
  conf[proc_ingester_prefix + 'consumer_options'] = consumer_options


  if not os.path.exists(options.output_dir):
    os.mkdir(options.output_dir)

  json.dump(conf, open(os.path.join(options.output_dir,'conf_extraction_'+conf_name+'.json'),'wt'), sort_keys=True, indent=4)