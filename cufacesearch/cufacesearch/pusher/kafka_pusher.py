from __future__ import print_function

import sys
import time
import json
import socket
from datetime import datetime
from kafka import KafkaProducer
from cufacesearch.common.conf_reader import ConfReader

# For debugging
#import logging
#logging.basicConfig(level=logging.DEBUG)

# Should we consider using Kafka Streams ?
# TODO: This should be listed in defaults
base_path_keys = "../../data/keys/hg-kafka-"

# NB: Use KafkaPusher class name to avoid confusion with original KafkaProducer...
# TODO: Should we have a GenericPusher class that exposes the `send` message method?

class KafkaPusher(ConfReader):
  """KafkaPusher
  """

  def __init__(self, global_conf_filename, prefix="", pid=None):
    """KafkaPusher constructor

    :param global_conf_filename: configuration file or dictionary
    :type global_conf_filename: str, dict
    :param prefix: prefix in configuration file
    :type prefix: str
    :type prefix: str
    :param pid: process id
    :type pid: int
    """
    # When running as deamon, save process id
    self.pid = pid
    self.verbose = 1

    super(KafkaPusher, self).__init__(global_conf_filename, prefix)

    # Set print prefix
    self.set_pp(pp=self.get_param("pp"))
    self.client_id = socket.gethostname() + '-' + self.pp

    print('[{}: log] verbose level is: {}'.format(self.pp, self.verbose))

    # Initialize attributes
    self.producer = None

    # Initialize stats attributes
    self.push_count = 0
    self.last_display = 0
    self.display_count = 1000
    self.start_time = time.time()

    # Initialize everything
    self.init_producer()

  def get_servers(self, dict_args, server_param):
    """Get (list of) Kafka server(s) and fill dictionary of arguments

    :param dict_args: dictionary of arguments
    :type dict_args: dict
    :param server_param: string of the named servers parameter
    :type server_param: str
    :return: dictionary of arguments filled with server(s) parameters
    :rtype: dict
    """
    servers = self.get_param(server_param)
    if servers:
      if type(servers) == list:
        servers = [str(s) for s in servers]
      dict_args['bootstrap_servers'] = servers
    return dict_args

  def get_security(self, dict_args, security_param):
    """Get security information and fill dictionary of arguments

    :param dict_args: dictionary of arguments
    :type dict_args: dict
    :param server_param: string of the named security parameter
    :type server_param: str
    :return: dictionary of arguments filled with security parameters
    :rtype: dict
    """
    security = self.get_param(security_param)
    if security:
      for sec_key in security:
        if str(sec_key) == "ssl_check_hostname":
          dict_args['ssl_check_hostname'] = bool(security[sec_key])
        else:
          dict_args[str(sec_key)] = str(security[sec_key])
    return dict_args

  def set_pp(self, pp=None):
    """Set pretty print name

    :param pp: pretty print name, default will be `KafkaPusher`
    :type pp: str
    """
    if pp is not None:
      self.pp = pp
    else:
      self.pp = "KafkaPusher"

  def init_producer(self):
    """Initialize KafkaProducer
    """
    print("[{}: log] Initializing producer...".format(self.pp))
    # Gather optional parameters
    dict_args = dict()
    #dict_args = self.get_servers(dict_args, 'producer_servers')
    #dict_args = self.get_security(dict_args, 'producer_security')
    dict_args = self.get_servers(dict_args, 'servers')
    dict_args = self.get_security(dict_args, 'security')
    # Instantiate producer
    try:
      self.producer = KafkaProducer(**dict_args)
    except Exception as inst:
      msg = "[{}: ERROR] Could not initialize producer with arguments {}. Error was: {}"
      raise RuntimeError(msg.format(self.pp, dict_args, inst))
    self.topic_name = self.get_required_param("topic_name")

  def send(self, msg):
    """Push `msg` to `self.topic_name`

    :param msg: message to be pushed
    :type msg: str, dict
    """
    # Check msg has been JSON dumped
    if isinstance(msg, dict):
      msg = json.dump(msg).encode('utf-8')
    self.producer.send(self.topic_name, msg)
    if self.verbose > 1:
      self.push_count += 1
      self.print_stats()

  def print_stats(self):
    """Print statistics of producer
    """
    if self.push_count - self.last_display > self.display_count:
      display_time = datetime.today().strftime('%Y/%m/%d-%H:%M.%S')
      print_msg = "[{} at {}] Push count: {}"
      print(print_msg.format(self.pp, display_time, self.push_count))
      sys.stdout.flush()
      self.last_display = self.push_count