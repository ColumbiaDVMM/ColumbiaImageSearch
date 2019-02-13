from __future__ import print_function

import os
import time
import json
import boto3
from datetime import datetime
# Cannot be imported?
#from botocore.errorfactory import ExpiredIteratorException
from ..common.conf_reader import ConfReader


class KinesisIngester(ConfReader):
  """KinesisIngester
  """

  def __init__(self, global_conf, prefix="", pid=None):
    """KinesisIngester constructor

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

    super(KinesisIngester, self).__init__(global_conf, prefix)

    # Set print prefix
    self.set_pp(pp=self.get_param("pp"))
    print('[{}: log] verbose level is: {}'.format(self.pp, self.verbose))

    # Initialize attributes
    self.client = None
    self.shard_iters = dict()
    self.shard_infos = dict()
    self.stream_name = self.get_required_param('stream_name')

    # Initialize everything
    self.init_consumer()


  def set_pp(self, pp=None):
    """Set pretty print name

    :param pp: pretty print name, default will be `KinesisIngester`
    :type pp: str
    """
    if pp is not None:
      self.pp = pp
    else:
      self.pp = "KinesisIngester"

  def get_shard_infos_filename(self):
    """Get shard infos filename.

    :return: shard infos filename
    :rtype: string
    """
    return self.get_param('shard_infos_filename', self.pp+'_'+self.stream_name+'.json')

  def get_shard_iterator(self, shard_id):
    shard_iterator_type = self.get_param('shard_iterator_type', "TRIM_HORIZON")
    # Try to get iterator based on latest processed sequence number if available
    if shard_id in self.shard_infos:
      sqn = self.shard_infos[shard_id]['sqn']
      shard_iterator = self.client.get_shard_iterator(StreamName=self.stream_name,
                                                      ShardId=shard_id,
                                                      StartingSequenceNumber=sqn,
                                                      ShardIteratorType='AFTER_SEQUENCE_NUMBER')
    else:
      shard_iterator = self.client.get_shard_iterator(StreamName=self.stream_name,
                                                      ShardId=shard_id,
                                                      ShardIteratorType=shard_iterator_type)
    return shard_iterator['ShardIterator']


  def init_client(self):
    """Initialize Kinesis client.
    """
    region_name = self.get_required_param('region_name')
    aws_profile = self.get_param('aws_profile', None)
    # This is mostly to be able to test locally
    endpoint_url = self.get_param('endpoint_url', None)
    verify = self.get_param('verify_certificates', True)
    use_ssl = self.get_param('use_ssl', True)

    # # Should we get these in another way?
    # aws_access_key_id = self.get_param('aws_access_key_id', None)
    # aws_secret_access_key = self.get_param('aws_secret_access_key', None)
    #
    # # Are there other parameters we should pass?
    # self.client = boto3.client('kinesis', region_name=region_name, endpoint_url=endpoint_url,
    #                            aws_access_key_id=aws_access_key_id,
    #                            aws_secret_access_key=aws_secret_access_key,
    #                            verify=verify, use_ssl=use_ssl)

    # Use session and profile
    self.session = boto3.Session(profile_name=aws_profile, region_name=region_name)
    self.client = self.session.client('kinesis', endpoint_url=endpoint_url, verify=verify,
                                      use_ssl=use_ssl)


  def init_consumer(self):
    """Initialize stream shards infos
    """
    self.init_client()

    # Get stream initialization related parameters
    nb_trials = self.get_param('nb_trials', 3)
    sifn = self.get_shard_infos_filename()

    # Check stream properties
    tries = 0
    while tries < nb_trials:
      tries += 1
      try:
        response = self.client.describe_stream(StreamName=self.stream_name)
        if response['StreamDescription']['StreamStatus'] == 'ACTIVE':
          break
      except Exception as inst:
        msg = "[{}: warning] Trial #{}: could not describe kinesis stream : {}. {}"
        print(msg.format(self.pp, tries, self.stream_name, inst))
        time.sleep(1)
    else:
      msg = "[{}: ERROR] Stream {} not active after {} trials. Aborting..."
      raise RuntimeError(msg.format(self.pp, self.stream_name, nb_trials))

    # Try to reload latest processed sequence number from disk
    if os.path.isfile(sifn):
      with open(sifn) as sif:
        self.shard_infos = json.load(sif)

    if response and 'StreamDescription' in response:
      for shard_id in response['StreamDescription']['Shards']:
        sh_id = shard_id['ShardId']
        self.shard_iters[sh_id] = self.get_shard_iterator(sh_id)

    if len(self.shard_iters) > 0:
      msg = "[{}: log] Initialization OK for stream '{}' with {} shards"
      print(msg.format(self.pp, self.stream_name, len(self.shard_iters)))
    else:
      msg = "[{}: ERROR] Initialization FAILED for stream '{}' with {} shards"
      raise RuntimeError(msg.format(self.pp, self.stream_name, len(self.shard_iters)))


  def get_msg_json(self):
      """Generator of JSON messages from the consumer.

      :yield: JSON message
      """
      lim_get_rec = self.get_param('lim_get_rec', 10)
      sleep_time = self.get_param('sleep_time', 10)
      sifn = self.get_shard_infos_filename()
      empty = 0
      sleep_count = 0

      # Iterate over shards
      for sh_id in self.shard_iters:
        sh_it = self.shard_iters[sh_id]

        if self.verbose > 2:
          msg = "[{}: log] Getting records starting from {} in shard {}"
          print(msg.format(self.pp, sh_it, sh_id))

        # If iterator has expired, we would get botocore.errorfactory.ExpiredIteratorException:
        # An error occurred (ExpiredIteratorException) when calling the GetRecords operation: Iterator expired.
        # The iterator was created at time XXX while right now it is XXX which is further in the future than the tolerated delay of 300000 milliseconds.
        try:
          rec_response = self.client.get_records(ShardIterator=sh_it, Limit=lim_get_rec)
        #except ExpiredIteratorException as inst:
        except Exception as inst:
          # Iterator may have expired...
          if self.verbose > 0:
            msg = "[{}: WARNING] Could not get records starting from {} in shard {}. {}"
            print(msg.format(self.pp, sh_it, sh_id, inst))
          self.shard_iters[sh_id] = self.get_shard_iterator(sh_id)
          continue

        while 'NextShardIterator' in rec_response:
          records = rec_response['Records']
          if len(records) > 0:
            sleep_count = 0
            for rec in records:
              rec_json = json.loads(rec['Data'])
              yield rec_json

              # Store `sqn`. Is there anything else we should store?
              # Maybe number of records read for sanity check
              # Start read time too?
              sqn = rec['SequenceNumber']
              if sh_id in self.shard_infos:
                self.shard_infos[sh_id]['sqn'] = sqn
                self.shard_infos[sh_id]['nb_read'] += 1
              else:
                self.shard_infos[sh_id] = dict()
                self.shard_infos[sh_id]['sqn'] = sqn
                self.shard_infos[sh_id]['start_read'] = datetime.now().isoformat()
                self.shard_infos[sh_id]['nb_read'] = 0

            # len(records) < lim_get_rec means we have reached end of stream
            # This test avoid making one more `get_records` call
            if len(records) < lim_get_rec:
              empty += 1
              break

          else:
            if self.verbose > 3:
              msg = "[{}: log] Shard {} seems empty"
              print(msg.format(self.pp, sh_id))
            empty += 1
            break

          # Iterate in same shard
          sh_it = rec_response['NextShardIterator']
          # Update iterator
          self.shard_iters[sh_id] = sh_it
          if self.verbose > 4:
            msg = "[{}: log] Getting records starting from {} in shard {}"
            print(msg.format(self.pp, sh_it, sh_id))
          rec_response = self.client.get_records(ShardIterator=sh_it, Limit=lim_get_rec)

        if empty == len(self.shard_iters):
          if self.verbose > 1:
            msg = "[{}: log] All shards seem empty or fully processed."
            print(msg.format(self.pp))

          # Dump current self.shard_infos
          if self.verbose > 1:
            msg = "[{}: log] shard_infos: {}"
            print(msg.format(self.pp, self.shard_infos))
          with open(sifn, 'w') as sif:
            json.dump(self.shard_infos, sif)

          # Sleep?
          time.sleep(sleep_time*max(sleep_count+1, sleep_time))
          sleep_count += 1
          break
