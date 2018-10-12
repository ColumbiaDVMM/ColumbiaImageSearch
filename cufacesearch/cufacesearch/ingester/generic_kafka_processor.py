from __future__ import print_function

import sys
import time
import socket
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from ..common.conf_reader import ConfReader

# For debugging
#import logging
#logging.basicConfig(level=logging.DEBUG)

# Should we consider using Kafka Streams ?
base_path_keys = "../../data/keys/hg-kafka-"

class GenericKafkaProcessor(ConfReader):
  """GenericKafkaProcessor
  """

  def __init__(self, global_conf_filename, prefix="", pid=None):
    """GenericKafkaProcessor constructor

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

    super(GenericKafkaProcessor, self).__init__(global_conf_filename, prefix)

    # Set print prefix
    self.set_pp(pp=self.get_param("pp"))
    self.client_id = socket.gethostname() + '-' + self.pp

    print('[{}: log] verbose level is: {}'.format(self.pp, self.verbose))

    # Initialize attributes
    self.consumer = None
    self.producer = None

    # Initialize stats attributes
    self.process_count = 0
    self.process_skip = 0
    self.process_failed = 0
    # Should we have separate timings for each cases?
    self.process_time = 0
    self.start_time = time.time()
    self.display_count = 1000
    self.last_display = 0

    # Initialize everything
    self.init_consumer()
    self.init_producer()

  def get_param_type(self, param_key):
    """Get type of parameter ``param_key``

    :param param_key: name of parameter
    :type param_key: str
    :return: type
    :raise ValueError: if parameter is unknown i.e. not in KafkaConsumer.DEFAULT_CONFIG
    """
    spk = str(param_key)
    if spk in KafkaConsumer.DEFAULT_CONFIG:
      if KafkaConsumer.DEFAULT_CONFIG[spk] == None:
        param_type = str
      else:
        param_type = type(KafkaConsumer.DEFAULT_CONFIG[spk])
      return param_type
    raise ValueError("[{}] Unknown parameter: {}".format(self.pp, spk))

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
        # Could now use self.get_param_type(sec_key) for casting...
        if str(sec_key) == "ssl_check_hostname":
          dict_args['ssl_check_hostname'] = bool(security[sec_key])
        else:
          dict_args[str(sec_key)] = str(security[sec_key])
    return dict_args

  def toc_process_ok(self, start_process, end_time=None):
    """Log one process completed

    :param start_process: start time of process (in seconds since epoch)
    :type start_process: int
    :param end_time: end time of process, if 'None' will be set to now (in seconds since epoch)
    :type end_time: int
    """
    if end_time is None:
      end_time = time.time()
    self.process_time += abs(end_time - start_process)
    self.process_count += 1

  def toc_process_skip(self, start_process, end_time=None):
    """Log one skipped process

    :param start_process: start time of process (in seconds since epoch)
    :type start_process: int
    :param end_time: end time of process, if 'None' will be set to now (in seconds since epoch)
    :type end_time: int
    """
    if end_time is None:
      end_time = time.time()
    self.process_time += abs(end_time - start_process)
    self.process_skip += 1

  def toc_process_failed(self, start_process, end_time=None):
    """Log one process failed

    :param start_process: start time of process (in seconds since epoch)
    :type start_process: int
    :param end_time: end time of process, if 'None' will be set to now (in seconds since epoch)
    :type end_time: int
    """
    if end_time is None:
      end_time = time.time()
    self.process_time += abs(end_time - start_process)
    self.process_failed += 1

  def print_stats(self, msg):
    """Print statistics of ingester

    :param msg: Kafka record
    :type msg: collections.namedtuple
    """
    tot = self.process_count + self.process_failed + self.process_skip
    if tot - self.last_display > self.display_count:
      # also use os.times() https://stackoverflow.com/questions/276281/cpu-usage-per-process-in-python
      display_time = datetime.today().strftime('%Y/%m/%d-%H:%M.%S')
      avg_process_time = self.process_time / max(1, tot)
      print_msg = "[%s:%s] (%s:%d:%d) process count: %d, skipped: %d, failed: %d, time: %f"
      print(print_msg % (self.pp, display_time, msg.topic, msg.partition, msg.offset, self.process_count,
                         self.process_skip, self.process_failed, avg_process_time))
      sys.stdout.flush()
      self.last_display = tot
      # Commit manually offsets here to improve offsets saving?
      # Is this causing some wrong offset sums?
      try:
        self.consumer.commit()
      except Exception as inst:
        # Could get the following error if processing time is too long.
        # Adjust "max_poll_records", "session_timeout_ms" and "request_timeout_ms" in "consumer_options" field in conf
        # CommitFailedError: Commit cannot be completed since the group has already rebalanced and assigned the
        # partitions to another member. This means that the time between subsequent calls to poll() was longer
        # than the configured session.timeout.ms, which typically implies that the poll loop is spending too much
        # time message processing. You can address this either by increasing the session timeout or by reducing the
        # maximum size of batches returned in poll() with max.poll.records.
        print("[{}: warning] Commit failed, with error {}".format(self.pp, inst))

  def set_pp(self, pp=None):
    """Set pretty print name

    :param pp: pretty print name, default will be `GenericKafkaProcessor`
    :type pp: str
    """
    if pp is not None:
      self.pp = pp
    else:
      self.pp = "GenericKafkaProcessor"

  def init_consumer(self):
    """Initialize ``self.consumer``
    """
    # Get topic
    #topic = self.get_required_param('consumer_topics')
    print("[{}: log] Initializing consumer...".format(self.pp))
    topic = self.get_param('consumer_topics')
    if topic is None:
      msg = "[{}: warning] Could not initialize consumer as no 'consumer_topics' was provided"
      print(msg.format(self.pp))
      return


    # NB: topic could be a list
    if type(topic) == list:
      topic = [str(t) for t in topic]
    else:
      topic = str(topic)

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

    # Also set client_id, using hostname and self.pp
    # Beware: issue if 'client_id' has ':' in it?
    dict_args['client_id'] = self.client_id

    # Instantiate consumer
    if self.verbose > 0:
      print("[{}: log] Starting consumer for topic '{}' with parameters {}".format(self.pp, topic, dict_args))
      sys.stdout.flush()

    self.consumer = KafkaConsumer(topic, **dict_args)


  def init_producer(self):
    """Initialize ``self.producer``
    """
    print("[{}: log] Initializing producer...".format(self.pp))
    # Gather optional parameters
    dict_args = dict()
    dict_args = self.get_servers(dict_args, 'producer_servers')
    dict_args = self.get_security(dict_args, 'producer_security')
    # Instantiate producer
    try:
      self.producer = KafkaProducer(**dict_args)
    except Exception as inst:
      # Would be OK for ingester that do not output to kafka...
      print("[{}: warning] Could not initialize producer with arguments {}. Error was: {}".format(self.pp, dict_args, inst))
