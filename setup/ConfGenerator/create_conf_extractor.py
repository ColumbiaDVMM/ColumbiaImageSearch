import os
import json
from argparse import ArgumentParser

if __name__ == "__main__":
  # Get config
  parser = ArgumentParser()
  parser.add_argument("-o", "--output_dir", dest="output_dir", required=True)
  options = parser.parse_args()

  # General environment variables
  # - conf_name (required)
  # - extr_type (required)
  # - input_type (required)
  # - extr_nb_threads (optional, default: 1)
  # Kafka related environment variables
  # - images_topic (required)
  # - extr_check_consumer_group (required)
  # - extr_proc_consumer_group (required)
  # - updates_topic
  # - kafka_servers (optional, default: memex HG kakfa brokers)
  # - kafka_security (optional)
  # Hbase related environment variables
  # - hbase_host (required)
  # - table_sha1infos (required)
  # - table_updateinfos (required)
  # - batch_update_size (required)
  # TODO: report this list in the docs.
  # Make sure the docker-compose propagate all these variables down, so we can generate conf files in docker...

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
    # conf[featurizer_prefix + 'pred_path'] = "./data/models/shape_predictor_68_face_landmarks.dat"
    # conf[featurizer_prefix + 'rec_path'] = "./data/models/dlib_face_recognition_resnet_model_v1.dat"
    conf[featurizer_prefix + 'pred_path'] = "/data/models/shape_predictor_68_face_landmarks.dat"
    conf[featurizer_prefix + 'rec_path'] = "/data/models/dlib_face_recognition_resnet_model_v1.dat"
  elif extr_type == "sbpycaffeimg":
    featurizer_prefix = "SBPY_"
    conf[extr_prefix + 'featurizer_prefix'] = featurizer_prefix
    conf[extr_prefix + 'featurizer_type'] = "sbpycaffe"
    conf[extr_prefix + 'detector_type'] = "full"
    conf[extr_prefix + 'input_type'] = "image"
    #conf[featurizer_prefix + 'sbcaffe_path'] = "./data/models/caffe_sentibank_train_iter_250000"
    #conf[featurizer_prefix + 'imgmean_path'] = "./data/models/imagenet_mean.npy"
    conf[featurizer_prefix + 'sbcaffe_path'] = "/data/models/caffe_sentibank_train_iter_250000"
    conf[featurizer_prefix + 'imgmean_path'] = "/data/models/imagenet_mean.npy"
  elif extr_type == "sbcmdlineimg":
    featurizer_prefix = "SBCMD_"
    conf[extr_prefix + 'featurizer_prefix'] = featurizer_prefix
    conf[extr_prefix + 'featurizer_type'] = "sbcmdline"
    conf[extr_prefix + 'detector_type'] = "full"
    conf[extr_prefix + 'input_type'] = "image"
    #TODO: what are the options required for sbcmdline?
    # conf[featurizer_prefix + 'sbcaffe_path'] = "./data/models/caffe_sentibank_train_iter_250000"
    # conf[featurizer_prefix + 'imgmean_path'] = "./data/models/imagenet_mean.npy"
    conf[featurizer_prefix + 'sbcaffe_path'] = "/data/models/caffe_sentibank_train_iter_250000"
    # What should it be?
    conf[featurizer_prefix + 'caffe_exec_path'] = ""
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

  use_kafka = True
  if os.environ['input_type'] == "hbase":
    use_kafka = False
    conf[extr_prefix + "ingestion_input"] = "hbase"

  # Generic ingestion settings
  verbose = os.getenv('verbose', 0)
  conf[extr_prefix + "verbose"] = int(verbose)
  conf[extr_prefix + "nb_threads"] = int(os.getenv('extr_nb_threads', 1))

  if use_kafka:
    conf[extr_prefix + "max_delay"] = int(os.getenv('extr_check_max_delay', 3600))

    kafka_servers = json.loads(os.getenv('kafka_servers', '["kafka0.team-hg-memex.com:9093",\
                                                           "kafka1.team-hg-memex.com:9093",\
                                                           "kafka2.team-hg-memex.com:9093",\
                                                           "kafka3.team-hg-memex.com:9093",\
                                                           "kafka4.team-hg-memex.com:9093",\
                                                           "kafka5.team-hg-memex.com:9093",\
                                                           "kafka6.team-hg-memex.com:9093",\
                                                           "kafka7.team-hg-memex.com:9093",\
                                                           "kafka8.team-hg-memex.com:9093",\
                                                           "kafka9.team-hg-memex.com:9093"]'))
    #kafka_security = json.loads(os.getenv('kafka_security', "{\"security_protocol\": \"SSL\", \"ssl_cafile\": \"./data/keys/hg-kafka-ca-cert.pem\", \"ssl_certfile\": \"./data/keys/hg-kafka-client-cert.pem\", \"ssl_keyfile\": \"./data/keys/hg-kafka-client-key.pem\", \"ssl_check_hostname\": false}"))
    env_kafka_security = os.getenv('kafka_security')
    if env_kafka_security:
      kafka_security = json.loads(env_kafka_security)
      conf[check_ingester_prefix + 'consumer_security'] = kafka_security
      conf[check_ingester_prefix + 'producer_security'] = kafka_security
      conf[proc_ingester_prefix + 'consumer_security'] = kafka_security

    consumer_options = json.loads(os.getenv('kafka_consumer_options', "{\"auto_offset_reset\": \"earliest\", \"max_poll_records\": 10, \"session_timeout_ms\": 300000, \"request_timeout_ms\": 600000, \"consumer_timeout_ms\": 600000}"))

    # Checker consumer
    conf[check_ingester_prefix + 'consumer_servers'] = kafka_servers
    conf[check_ingester_prefix + 'consumer_topics'] = os.environ['images_topic']
    conf[check_ingester_prefix + 'consumer_group'] = os.environ['extr_check_consumer_group']
    conf[check_ingester_prefix + 'consumer_options'] = consumer_options

    conf[check_ingester_prefix + 'producer_servers'] = kafka_servers
    conf[check_ingester_prefix + 'producer_updates_out_topic'] = os.environ['updates_topic']

    # Processor consumer
    conf[proc_ingester_prefix + 'consumer_servers'] = kafka_servers
    conf[proc_ingester_prefix + 'consumer_topics'] = os.environ['updates_topic']
    conf[proc_ingester_prefix + 'consumer_group'] = os.environ['extr_proc_consumer_group']
    conf[proc_ingester_prefix + 'consumer_options'] = consumer_options

  conf[check_ingester_prefix + 'verbose'] = verbose
  conf[check_ingester_prefix + 'pp'] = "KafkaUpdateChecker"
  # Checker producer

  conf[proc_ingester_prefix + 'verbose'] = verbose
  conf[proc_ingester_prefix + 'pp'] = "KafkaUpdateIngester"

  if not os.path.exists(options.output_dir):
    os.mkdir(options.output_dir)

  outpath = os.path.join(options.output_dir,'conf_extraction_'+conf_name+'.json')
  json.dump(conf, open(outpath,'wt'), sort_keys=True, indent=4)
  print("Saved conf at {}: {}".format(outpath, json.dumps(conf)))