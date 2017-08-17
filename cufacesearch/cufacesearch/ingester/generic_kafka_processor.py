import json
from kafka import KafkaConsumer, KafkaProducer

class GenericKafkaProcessor():

  def __init__(self, global_conf_filename, prefix=""):
    self.pp = "GenericKafkaProcessor"
    self.set_pp()

    print '[{}.init: info] reading configuration: {}'.format(self.pp, global_conf_filename)
    self.global_conf_filename = global_conf_filename
    self.prefix = prefix
    self.global_conf = json.load(open(global_conf_filename, 'rt'))

    # Initialize attributes
    self.consumer = None
    self.producer = None
    self.verbose = 0

    # Read conf
    self.read_conf()

    # Initialize everything
    self.init_consumer()
    self.init_producer()

  def read_conf(self):
    # read some generic parameters
    verbose = self.get_param('verbose')
    if verbose:
      self.verbose = int(verbose)

  def get_param(self, param):
    key_param = self.prefix + param
    if key_param in self.global_conf:
      return self.global_conf[key_param]
    if self.verbose>0:
      print '[{}.get_param: info] could not find {} in configuration'.format(self.pp, key_param)

  def get_required_param(self, param):
    param_value = self.get_param(param)
    if param_value is None:
      msg = '[{}.get_required_param: error] {} not defined in configuration'.format(self.pp, param)
      print msg
      raise ValueError(msg)

  def init_consumer(self):
    topic = self.get_required_param('topic')
    servers = self.get_required_param('servers')
    group = self.get_param('group')
    # see all options at: http://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
    # could also have parameters for key_deserializer, value_deserializer
    self.consumer = KafkaConsumer(topic,  group_id=group, bootstrap_servers=servers)

  def init_producer(self):
    servers = self.get_required_param('servers')
    # Should the producer inherit from as in threading.Thread?
    # https://github.com/dpkp/kafka-python/blob/master/example.py
    self.producer = KafkaProducer(bootstrap_servers=servers)