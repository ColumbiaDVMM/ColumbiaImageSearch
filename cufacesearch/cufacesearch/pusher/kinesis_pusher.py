from __future__ import print_function
import sys
import time
import json
import boto3
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from datetime import datetime
#from cufacesearch.common.conf_reader import ConfReader
from ..common.conf_reader import ConfReader

# Cannot be imported?
#from botocore.errorfactory import ExpiredIteratorException


def get_random_sha1():
  from hashlib import sha1
  import random
  return sha1(str(random.getrandbits(256)).encode('utf-8')).hexdigest().upper()

# TODO: Should we have a GenericPusher class that exposes the `send` message method?


class KinesisPusher(ConfReader):
  """KinesisPusher
  """

  def __init__(self, global_conf, prefix="", pid=None):
    """KinesisPusher constructor

    :param global_conf: configuration file or dictionary
    :type global_conf: str, dict
    :param prefix: prefix in configuration file
    :type prefix: str
    :type prefix: str
    :param pid: process id
    :type pid: int
    """
    # When running as deamon, save process id
    self.pid = pid
    self.verbose = 1

    super(KinesisPusher, self).__init__(global_conf, prefix)

    # Set print prefix
    self.set_pp(pp=self.get_param("pp"))
    print('[{}: log] verbose level is: {}'.format(self.pp, self.verbose))

    # Initialize attributes
    self.client = None
    self.shard_iters = dict()
    self.shard_infos = dict()
    self.stream_name = self.get_required_param('stream_name')

    # Initialize stats attributes
    self.push_count = 0
    self.last_display = 0
    self.display_count = 1000
    self.start_time = time.time()

    # Initialize everything
    self.init_pusher()


  def set_pp(self, pp=None):
    """Set pretty print name

    :param pp: pretty print name, default will be `KinesisPusher`
    :type pp: str
    """
    if pp is not None:
      self.pp = pp
    else:
      self.pp = "KinesisPusher"


  def init_client(self):
    """Initialize Kinesis client.
    """
    region_name = self.get_required_param('region_name')
    aws_profile = self.get_param('aws_profile', None)
    # This is mostly to be able to test locally
    endpoint_url = self.get_param('endpoint_url', None)
    verify = self.get_param('verify_certificates', True)
    use_ssl = self.get_param('use_ssl', True)

    # Use session and profile
    self.session = boto3.Session(profile_name=aws_profile, region_name=region_name)
    self.client = self.session.client('kinesis', endpoint_url=endpoint_url, verify=verify,
                                      use_ssl=use_ssl)

  def init_pusher(self):
    """Initialize stream shards infos
    """
    self.init_client()

    # Get stream initialization related parameters
    nb_trials = self.get_param('nb_trials', 3)

    # Check stream is active
    tries = 0
    while tries < nb_trials:
      tries += 1
      try:
        response = self.client.describe_stream(StreamName=self.stream_name)
        if response['StreamDescription']['StreamStatus'] == 'ACTIVE':
          break
      # Can we catch ResourceNotFound and create stream here?
      except Exception as inst:
        # Create stream
        if self.get_param('create_stream', False):
          try:
            nb_shards = self.get_param('nb_shards', 2)
            self.client.create_stream(StreamName=self.stream_name, ShardCount=nb_shards)
          except:
            msg = "[{}: Warning] Trial #{}: could not create kinesis stream : {}. {}"
            print(msg.format(self.pp, tries, self.stream_name, inst))
        msg = "[{}: Warning] Trial #{}: could not describe kinesis stream : {}. {}"
        print(msg.format(self.pp, tries, self.stream_name, inst))
        time.sleep(1)
    else:
      msg = "[{}: ERROR] Stream {} not active after {} trials. Aborting..."
      raise RuntimeError(msg.format(self.pp, self.stream_name, nb_trials))

  def send(self, msg):
      """Push `msg` to `self.stream_name`

      :param msg: message to be pushed
      :type msg: str, dict
      """
      # Check if msg was already JSON dumped
      if isinstance(msg, dict):
        msg = json.dump(msg).encode('utf-8')
      # Use a random sha1 as partition key
      single_rec = [{'Data': msg, 'PartitionKey': get_random_sha1()}]
      self.client.put_records(Records=single_rec, StreamName=self.stream_name)
      self.push_count += 1
      if self.verbose > 1:
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