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
  # - kafka_servers (optional, default: memex HG kakfa brokers)
  # - kafka_security (optional)
  # TODO: report this list in the docs.
  # Make sure the docker-compose propagate all these variables down, so we can generate conf files in docker...

  # Initialization
  conf = dict()
  conf_name = os.environ['conf_name']


  # Local input settings
  if os.environ['input_type'] == "local":
    prefix = "LIKP_"
    conf[prefix + 'input_path'] = os.getenv('input_path', './data/input_images/')
    source_zip = os.environ.get('source_zip')
    if source_zip:
      conf[prefix + 'source_zip'] = source_zip

  # Kafka input settings
  if os.environ['input_type'] == "kafka":
    prefix = "KID_"
    conf[prefix + 'consumer_topics'] = os.environ['input_topic']
    conf[prefix + 'consumer_group'] = os.environ['input_consumer_group']

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

  # Generic ingestion settings
  conf[prefix + 'verbose'] = os.getenv('verbose', 0)
  conf[prefix + 'producer_servers'] = kafka_servers

  env_kafka_security = os.getenv('kafka_security')
  if env_kafka_security:
    kafka_security = json.loads(env_kafka_security)
    conf[prefix + 'producer_security'] = kafka_security
    conf[prefix + 'consumer_security'] = kafka_security

  #conf[prefix + 'producer_security'] = json.loads(os.getenv('kafka_security', "{\"security_protocol\": \"SSL\", \"ssl_cafile\": \"./data/keys/hg-kafka-ca-cert.pem\", \"ssl_certfile\": \"./data/keys/hg-kafka-client-cert.pem\", \"ssl_keyfile\": \"./data/keys/hg-kafka-client-key.pem\", \"ssl_check_hostname\": false}"))
  
  conf[prefix + 'producer_images_out_topic'] = os.environ['images_topic']

  if not os.path.exists(options.output_dir):
    os.mkdir(options.output_dir)

  outpath = os.path.join(options.output_dir,'conf_ingestion_'+conf_name+'.json')
  json.dump(conf, open(outpath,'wt'), sort_keys=True, indent=4)
  print("Saved conf at {}: {}".format(outpath, json.dumps(conf)))

