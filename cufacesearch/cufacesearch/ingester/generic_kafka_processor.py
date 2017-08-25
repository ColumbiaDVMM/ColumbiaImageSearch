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