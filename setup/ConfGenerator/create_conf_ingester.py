import os
import json
from argparse import ArgumentParser

if __name__ == "__main__":
  # Get config
  parser = ArgumentParser()
  parser.add_argument("-o", "--output_dir", dest="output_dir", required=True)
  options = parser.parse_args()

  # Environment variables:
  # - conf_name (required)
  # - input_type (required)
  # - images_topic (required, output topic)
  # If 'input_type' is local:
  # - input_path (optional, default: ./data/input_images/)
  # - source_zip (optional, e.g. to be used for an online dataset)
  # If 'input_type' is kafka:
  # - input_topic (required)
  # - input_consumer_group (required)
  # - input_obj_stored_prefix (required)
  # - input_nb_threads (optional, default: 4)
  # - kafka_servers (optional, default: memex HG kakfa brokers)
  # - kafka_security (optional)
  # If 'producer_type' is kinesis:
  # - producer_type
  # - producer_prefix
  # - images_stream
  # - aws_profile
  # - region_name
  # - endpoint_url
  # - verify_certificates
  # - use_ssl
  # - nb_trials
  # - create_stream
  # - nb_shards

  # TODO: define kafka producer type
  # TODO: report this list in the docs.
  # Make sure the docker-compose propagate all these variables down, so we can generate conf files in docker...

  # Initialization
  conf = dict()
  conf_name = os.environ['conf_name']
  input_type = os.environ['input_type']
  image_pushing_type = os.environ['image_pushing_type']
  image_ingester_prefix = os.getenv("image_ingester_prefix", "IMG_ING_")
  image_pusher_prefix = os.getenv("image_pusher_prefix", "IMG_PUSH_")
  #producer_prefix = os.environ['producer_prefix']
  #producer_type = os.environ['producer_type']

  # Local input settings
  #prefix = "LIP_"
  prefix = ""
  conf[prefix + 'image_ingester_prefix'] = image_ingester_prefix
  conf[prefix + 'image_pusher_prefix'] = image_pusher_prefix
  conf[prefix + 'image_pushing_type'] = image_pushing_type

  if input_type == "local":
    conf[prefix + 'input_path'] = os.getenv('input_path', './data/input_images/')
    source_zip = os.environ.get('source_zip')
    if source_zip:
      conf[prefix + 'source_zip'] = source_zip
  # Kafka input settings
  elif input_type == "kafka":
    #prefix = "KAIP_"
    conf[image_ingester_prefix + 'topic_name'] = os.environ['input_topic']
    conf[image_ingester_prefix + 'consumer_group'] = os.environ['input_consumer_group']
    conf[image_ingester_prefix + 'obj_stored_prefix'] = os.environ['input_obj_stored_prefix']
  # Kinesis input settings
  elif input_type == "kinesis":
    # NEVER TESTED
    #prefix = "KII_"
    conf[image_ingester_prefix + 'endpoint_url'] = os.getenv('endpoint_url', None)
    conf[image_ingester_prefix + 'region_name'] = os.environ['region_name']
    conf[image_ingester_prefix + 'stream_name'] = os.environ['input_stream']
    conf[image_ingester_prefix + 'verify_certificates'] = bool(int(os.getenv('verify_certificates', 1)))
    conf[image_ingester_prefix + 'use_ssl'] = bool(int(os.getenv('use_ssl', 1)))
    if os.getenv('aws_profile', False):
      conf[image_ingester_prefix + 'aws_profile'] = os.environ['aws_profile']
  else:
    raise ValueError("Unknown input type: {}".format(os.environ['input_type']))
  # Is this actually used? Fro what?
  conf[image_ingester_prefix + 'nb_threads'] = int(os.getenv('input_nb_threads', 4))

  if input_type == "kafka" or image_pushing_type == "kafka":
    env_kafka_security = os.getenv('kafka_security')
    if env_kafka_security:
      kafka_security = json.loads(env_kafka_security)

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
    if input_type == "kafka":
      conf[image_ingester_prefix + 'security'] = kafka_security
      conf[image_ingester_prefix + 'servers'] = kafka_servers
    if image_pushing_type == "kafka":
      conf[image_pusher_prefix + 'security'] = kafka_security
      conf[image_pusher_prefix + 'servers'] = kafka_servers

  if image_pushing_type == "kafka":
    conf[image_pusher_prefix + 'topic_name'] = os.environ['images_topic']


  if image_pushing_type == "kinesis":
    conf[image_pusher_prefix + 'stream_name'] = os.environ['images_stream']
    conf[image_pusher_prefix + 'region_name'] = os.environ['region_name']
    conf[image_pusher_prefix + 'endpoint_url'] = os.getenv('endpoint_url')
    conf[image_pusher_prefix + 'aws_profile'] = os.getenv('aws_profile')
    conf[image_pusher_prefix + 'create_stream'] = bool(int(os.getenv('create_stream', 0)))
    conf[image_pusher_prefix + 'verify_certificates'] = bool(int(os.getenv('verify_certificates', 1)))

  # - images_stream
  # - aws_profile
  # - region_name
  # - endpoint_url
  # - verify_certificates
  # - use_ssl
  # - nb_trials
  # - create_stream
  # - nb_shards

  # # Deprecated
  # # cdr_out_topic
  # cdr_out_topic = os.getenv('cdr_out_topic', None)
  # if cdr_out_topic:
  #   conf[prefix + 'producer_cdr_out_topic'] = cdr_out_topic

  # Generic ingestion settings
  conf[prefix + 'verbose'] = os.getenv('verbose', 0)
  conf[image_ingester_prefix + 'verbose'] = os.getenv('verbose', 0)
  conf[image_pusher_prefix + 'verbose'] = os.getenv('verbose', 0)

  if not os.path.exists(options.output_dir):
    os.mkdir(options.output_dir)

  outpath = os.path.join(options.output_dir,'conf_ingestion_'+conf_name+'.json')
  json.dump(conf, open(outpath,'wt'), sort_keys=True, indent=4)
  print("Saved conf at {}: {}".format(outpath, json.dumps(conf)))

