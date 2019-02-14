import os
import json
import time
from argparse import ArgumentParser

DEFAULT_MAX_DELAY=3600
DEFAULT_NB_THREADS=1
DEFAULT_POOL_THREADS=1
DEFAULT_VERIFY_CERTIFICATES=1

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
  # TODO: Update IN and OUT hbase parameters
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
    conf[featurizer_prefix + 'caffe_exec_path'] = "/home/ubuntu/caffe_cpu/build/tools/extract_nfeatures"
  else:
    raise ValueError("Unknown extraction type: {}".format(extr_type))

  # HBase settings
  conf[hbase_prefix + 'host'] = os.environ['hbase_host'].strip()
  conf[hbase_prefix + 'table_sha1infos'] = os.environ['table_sha1infos'].strip()
  conf[hbase_prefix + 'table_updateinfos'] = os.environ['table_updateinfos'].strip()
  conf[hbase_prefix + 'batch_update_size'] = int(os.environ['batch_update_size'])
  conf[hbase_prefix + 'pool_thread'] = int(os.getenv('pool_thread', DEFAULT_POOL_THREADS))

  # Deal with newly exposed but optional parameters
  if os.getenv('skip_failed', False):
    conf[hbase_prefix + 'skip_failed'] = os.environ['skip_failed']
  if os.getenv('column_list_sha1s', False):
    conf[hbase_prefix + 'column_list_sha1s'] = os.environ['column_list_sha1s']
  if os.getenv('extr_column_family', False):
    conf[hbase_prefix + 'extr_column_family'] = os.environ['extr_column_family']
  if os.getenv('image_info_column_family', False):
    conf[hbase_prefix + 'image_info_column_family'] = os.environ['image_info_column_family']
  if os.getenv('image_url_column_name', False):
    conf[hbase_prefix + 'image_url_column_name'] = os.environ['image_url_column_name']
  if os.getenv('image_buffer_column_family', False):
    conf[hbase_prefix + 'image_buffer_column_family'] = os.environ['image_buffer_column_family']
  if os.getenv('image_buffer_column_name', False):
    conf[hbase_prefix + 'image_buffer_column_name'] = os.environ['image_buffer_column_name']
  if os.getenv('update_info_column_family', False):
    conf[hbase_prefix + 'update_info_column_family'] = os.environ['update_info_column_family']

  # "In" HBase settings if any
  env_in_prefix = "in_"
  if os.getenv(env_in_prefix+'table_sha1infos', False):
    in_hbase_prefix = 'IN'+hbase_prefix
    conf[extr_prefix + 'in_indexer_prefix'] = in_hbase_prefix
    conf[in_hbase_prefix + 'table_sha1infos'] = os.environ[env_in_prefix+'table_sha1infos'].strip()
    # Should we allow different HBase hosts?
    conf[in_hbase_prefix + 'host'] = os.environ['hbase_host'].strip()
    conf[in_hbase_prefix + 'pool_thread'] = int(os.getenv('table_in_pool_thread', 1))

    # Deal with newly exposed but optional parameters
    if os.getenv('skip_failed', False):
      conf[in_hbase_prefix + 'skip_failed'] = os.environ['skip_failed']
    env_key = env_in_prefix + 'extr_column_family'
    if os.getenv(env_key, False):
      conf[in_hbase_prefix + 'extr_column_family'] = os.environ[env_key]
    env_key = env_in_prefix + 'image_buffer_column_family'
    if os.getenv(env_key, False):
      conf[in_hbase_prefix + 'image_buffer_column_family'] = os.environ[env_key]
    env_key = env_in_prefix + 'image_buffer_column_name'
    if os.getenv(env_key, False):
      conf[in_hbase_prefix + 'image_buffer_column_name'] = os.environ[env_key]
    env_key = env_in_prefix + 'image_info_column_family'
    if os.getenv(env_key, False):
      conf[in_hbase_prefix + 'image_info_column_family'] = os.environ[env_key]
    env_key = env_in_prefix + 'image_url_column_name'
    if os.getenv(env_key, False):
      conf[in_hbase_prefix + 'image_url_column_name'] = os.environ[env_key]

  # Input settings
  # TODO: a bit confusing. Try to make this cleaner
  # Define
  # - input_type: local or what? url? Basically to set file_input to True if local
  # Confusing with image/face above...
  if os.getenv('input_type') == "local":
    conf[extr_prefix + 'file_input'] = True

  use_kafka = False
  # - image_ingestion_type: kinesis/kafka
  if os.environ['image_ingestion_type'] == "kafka":
    conf[extr_prefix + "image_ingestion_type"] = "kafka"
    use_kafka = True
  elif os.environ['image_ingestion_type'] == "kinesis":
    conf[extr_prefix + "image_ingestion_type"] = "kinesis"
  else:
    msg = "Unsupported 'image_ingestion_type: {}"
    raise ValueError(msg.format(os.environ['image_ingestion_type']))
  # - update_ingestion_type: kinesis/kafka/hbase
  if os.environ['update_ingestion_type'] == "hbase":
    conf[extr_prefix + "update_ingestion_type"] = "hbase"
  elif os.environ['update_ingestion_type'] == "kafka":
    conf[extr_prefix + "update_ingestion_type"] = "kafka"
    use_kafka = True
  elif os.environ['update_ingestion_type'] == "kinesis":
    conf[extr_prefix + "update_ingestion_type"] = "kinesis"

  # Deprecated
  # #conf[extr_prefix + 'ingester_type'] = os.environ['producer_type']
  # #conf[check_ingester_prefix + 'ingester_type'] = os.environ['producer_type']
  # if os.environ['producer_type'] == "kafka":
  #   use_kafka = True
  # elif os.environ['producer_type'] != "kinesis":
  #   raise ValueError("Producer in neither Kafka nor Kinesis")

  print("os.getenv('input_type'): {}".format(os.getenv('input_type')))
  print("os.environ['image_ingestion_type']: {}".format(os.environ['image_ingestion_type']))
  print("os.environ['update_ingestion_type']: {}".format(os.environ['update_ingestion_type']))
  print("use_kafka: {}".format(use_kafka))

  # Generic ingestion settings
  verbose = os.getenv('verbose', 0)
  conf[hbase_prefix + "verbose"] = int(verbose)
  conf[extr_prefix + "verbose"] = int(verbose)
  conf[extr_prefix + "nb_threads"] = int(os.getenv('extr_nb_threads', DEFAULT_NB_THREADS))
  conf[extr_prefix + "max_delay"] = int(os.getenv('extr_check_max_delay', DEFAULT_MAX_DELAY))

  if use_kafka:

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
    if conf[extr_prefix + "image_ingestion_type"] == "kafka":
      conf[check_ingester_prefix + 'consumer_servers'] = kafka_servers
      conf[check_ingester_prefix + 'consumer_options'] = consumer_options
      conf[check_ingester_prefix + 'pp'] = "KafkaImageIngester"
      conf[check_ingester_prefix + 'consumer_topics'] = os.environ['images_stream']
      conf[check_ingester_prefix + 'consumer_group'] = os.environ['extr_check_consumer_group']
      # This is now optional
      #if os.getenv('updates_topic', False):
      #conf[check_ingester_prefix + 'producer_updates_out_topic'] = os.environ['updates_topic']

    if conf[extr_prefix + "update_ingestion_type"] == "kafka":
      # Checker producers
      conf[check_ingester_prefix + 'producer_updates_out_topic'] = os.environ['updates_topic']
      conf[check_ingester_prefix + 'producer_servers'] = kafka_servers

      # Processor consumer
      conf[proc_ingester_prefix + 'consumer_servers'] = kafka_servers
      conf[proc_ingester_prefix + 'consumer_options'] = consumer_options
      conf[proc_ingester_prefix + 'pp'] = "KafkaUpdateIngester"
      conf[proc_ingester_prefix + 'consumer_topics'] = os.environ['updates_topic']
      conf[proc_ingester_prefix + 'consumer_group'] = os.environ['extr_proc_consumer_group']


  # We should have at least images from Kinesis
  if conf[extr_prefix + "image_ingestion_type"] == "kinesis":
    conf[check_ingester_prefix + 'pp'] = "KinesisImageIngester"
    conf[check_ingester_prefix + 'stream_name'] = os.environ['images_stream']
    conf[check_ingester_prefix + 'region_name'] = os.environ['region_name']
    conf[check_ingester_prefix + 'endpoint_url'] = os.getenv('endpoint_url')
    conf[check_ingester_prefix + 'aws_profile'] = os.getenv('aws_profile')
    verify_certificates = bool(int(os.getenv('verify_certificates', DEFAULT_VERIFY_CERTIFICATES)))
    conf[check_ingester_prefix + 'verify_certificates'] = verify_certificates

  if conf[extr_prefix + "update_ingestion_type"] == "kinesis":
    conf[proc_ingester_prefix + 'pp'] = "KinesisUpdateIngester"
    conf[proc_ingester_prefix + 'stream_name'] = os.environ['updates_topic']
    conf[proc_ingester_prefix + 'region_name'] = os.environ['region_name']
    conf[proc_ingester_prefix + 'endpoint_url'] = os.getenv('endpoint_url')
    conf[proc_ingester_prefix + 'aws_profile'] = os.getenv('aws_profile')
    verify_certificates = bool(int(os.getenv('verify_certificates', DEFAULT_VERIFY_CERTIFICATES)))
    conf[proc_ingester_prefix + 'verify_certificates'] = verify_certificates
  elif conf[extr_prefix + "update_ingestion_type"] == "hbase":
    # No parameters to pass?
    pass
  elif conf[extr_prefix + "update_ingestion_type"] == "kafka":
    pass
    # Kinesis + HBase? Or should we create a stream for the updates too?

  conf[check_ingester_prefix + 'verbose'] = verbose
  conf[proc_ingester_prefix + 'verbose'] = verbose

  if not os.path.exists(options.output_dir):
    os.mkdir(options.output_dir)

  outpath = os.path.join(options.output_dir,'conf_extraction_'+conf_name+'.json')
  json.dump(conf, open(outpath,'wt'), sort_keys=True, indent=4)
  print("Saved conf at {}: {}".format(outpath, json.dumps(conf)))
  time.sleep(5)
