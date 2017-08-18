from kafka import KafkaConsumer, KafkaProducer
from ..common.conf_reader import ConfReader

class GenericKafkaProcessor(ConfReader):

  def __init__(self, global_conf_filename, prefix=""):
    super(GenericKafkaProcessor, self).__init__(global_conf_filename, prefix)

    # Initialize attributes
    self.consumer = None
    self.producer = None
    self.verbose = 2

    # Initialize everything
    self.init_consumer()
    self.init_producer()

  def set_pp(self):
    self.pp = "GenericKafkaProcessor"

  def init_consumer(self):
    # topic could be a list?
    topic = str(self.get_required_param('consumer_topics'))
    servers = self.get_param('consumer_servers')
    group = self.get_param('consumer_group')
    # see all options at: http://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
    # could also have parameters for key_deserializer, value_deserializer
    if servers:
      self.consumer = KafkaConsumer(topic,  group_id=group, bootstrap_servers=servers)
    else:
      self.consumer = KafkaConsumer(topic, group_id=group)

  def init_producer(self):
    servers = self.get_param('producer_servers')
    # Should the producer inherit from as in threading.Thread?
    # https://github.com/dpkp/kafka-python/blob/master/example.py
    if servers:
      self.producer = KafkaProducer(bootstrap_servers=servers)
    else:
      self.producer = KafkaProducer()