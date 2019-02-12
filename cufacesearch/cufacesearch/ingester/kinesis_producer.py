from __future__ import print_function

import time
import json
import boto3
# Cannot be imported?
#from botocore.errorfactory import ExpiredIteratorException
from ..common.conf_reader import ConfReader


def get_random_sha1():
  from hashlib import sha1
  import random
  return sha1(str(random.getrandbits(256)).encode('utf-8')).hexdigest().upper()

class KinesisProducer(ConfReader):
  """KinesisProducer
  """

  def __init__(self, global_conf, prefix="", pid=None):
    """KinesisProducer constructor

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

    super(KinesisProducer, self).__init__(global_conf, prefix)

    # Set print prefix
    self.set_pp(pp=self.get_param("pp"))
    print('[{}: log] verbose level is: {}'.format(self.pp, self.verbose))

    # Initialize attributes
    self.client = None
    self.shard_iters = dict()
    self.shard_infos = dict()
    self.stream_name = self.get_required_param('stream_name')

    # Initialize everything
    self.init_producer()


  def set_pp(self, pp=None):
    """Set pretty print name

    :param pp: pretty print name, default will be `KinesisIngester`
    :type pp: str
    """
    if pp is not None:
      self.pp = pp
    else:
      self.pp = "KinesisProducer"


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

  def init_producer(self):
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
            msg = "[{}: warning] Trial #{}: could not create kinesis stream : {}. {}"
            print(msg.format(self.pp, tries, self.stream_name, inst))
        msg = "[{}: warning] Trial #{}: could not describe kinesis stream : {}. {}"
        print(msg.format(self.pp, tries, self.stream_name, inst))
        time.sleep(1)
    else:
      msg = "[{}: ERROR] Stream {} not active after {} trials. Aborting..."
      raise RuntimeError(msg.format(self.pp, self.stream_name, nb_trials))


  def send(self, stream_name, msg):
      """Push `msg` to `stream_name`
      """
      if stream_name != self.stream_name:
        msg = "[{}: ERROR] Trying to push to {} while was initialized with {}"
        raise ValueError(msg.format(self.pp, stream_name, self.stream_name))
      # TODO: what is a good partition key?
      single_rec = [{'Data': json.dumps(msg), 'PartitionKey': get_random_sha1()}]
      self.client.put_records(Records=single_rec, StreamName=stream_name)