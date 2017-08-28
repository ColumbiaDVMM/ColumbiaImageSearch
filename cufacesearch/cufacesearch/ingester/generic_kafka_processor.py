import time
from kafka import KafkaConsumer, KafkaProducer
from ..common.conf_reader import ConfReader

# For debugging
#import logging
#logging.basicConfig(level=logging.DEBUG)

# Should we consider using Kafka Streams ?
base_path_keys = "../../data/keys/hg-kafka-"

class GenericKafkaProcessor(ConfReader):

  def __init__(self, global_conf_filename, prefix=""):
    super(GenericKafkaProcessor, self).__init__(global_conf_filename, prefix)

    # Initialize attributes
    self.consumer = None
    self.producer = None

    # Initialize stats attributes
    self.process_count = 0
    self.process_skip = 0
    self.process_failed = 0
    # Should we have separate timings for each cases?
    self.process_time = 0
    self.display_count = 100

    # Initialize everything
    self.init_consumer()
    self.init_producer()

  def get_param_type(self, param_key):
    spk = str(param_key)
    if spk in KafkaConsumer.DEFAULT_CONFIG:
      if KafkaConsumer.DEFAULT_CONFIG[spk] == None:
        param_type = str
      else:
        param_type = type(KafkaConsumer.DEFAULT_CONFIG[spk])
      return param_type
    raise ValueError("[{}] Unknown parameter: {}".format(self.pp, spk))

  def get_servers(self, dict_args, server_param):
    servers = self.get_param(server_param)
    if servers:
      if type(servers) == list:
        servers = [str(s) for s in servers]
      dict_args['bootstrap_servers'] = servers
    return dict_args

  def get_security(self, dict_args, security_param):
    security = self.get_param(security_param)
    if security:
      for sec_key in security:
        # Could now use self.get_param_type(sec_key) for casting...
        if str(sec_key) == "ssl_check_hostname":
          dict_args['ssl_check_hostname'] = bool(security[sec_key])
        else:
          dict_args[str(sec_key)] = str(security[sec_key])
    return dict_args

  def toc_process_ok(self, start_process):
    self.process_count += 1
    self.process_time += time.time() - start_process

  def toc_process_skip(self, start_process):
    self.process_skip += 1
    self.process_time += time.time() - start_process

  def toc_process_failed(self, start_process):
    self.process_failed += 1
    self.process_time += time.time() - start_process

  def print_stats(self, msg):
    tot = self.process_count + self.process_failed + self.process_skip
    if tot % self.display_count == 0:
      avg_process_time = self.process_time / max(1, tot)
      print_msg = "[%s] (%s:%d:%d) process count: %d, skipped: %d, failed: %d, time: %f"
      print print_msg % (self.pp, msg.topic, msg.partition, msg.offset, self.process_count, self.process_skip, self.process_failed, avg_process_time)

  def set_pp(self):
    self.pp = "GenericKafkaProcessor"

  def init_consumer(self):
    ## Required parameters
    topic = str(self.get_required_param('consumer_topics'))
    # NB: topic could be a list?
    if type(topic) == list:
      topic = [str(t) for t in topic]

    ## Optional parameters
    dict_args = dict()
    # see all options at: http://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
    # could also have parameters for key_deserializer, value_deserializer
    dict_args = self.get_servers(dict_args, 'consumer_servers')
    dict_args = self.get_security(dict_args, 'consumer_security')
    # group
    group = self.get_param('consumer_group')
    if group:
      dict_args['group_id'] = str(group)
    # other options...
    options = self.get_param('consumer_options')
    if options:
      for opt_key in options:
        # Try to properly cast options here
        dict_args[str(opt_key)] = self.get_param_type(opt_key)(options[opt_key])

    # Instantiate consumer
    if self.verbose > 0:
      print "[{}] Starting consumer for topic '{}' with parameters {}".format(self.pp, topic, dict_args)

    self.consumer = KafkaConsumer(topic, **dict_args)


  def init_producer(self):
    # Gather optional parameters
    dict_args = dict()
    dict_args = self.get_servers(dict_args, 'producer_servers')
    dict_args = self.get_security(dict_args, 'producer_security')
    # Instantiate producer
    self.producer = KafkaProducer(**dict_args)