from __future__ import print_function

import os
import time
import json
import boto3
from datetime import datetime
# Cannot be imported?
#from botocore.errorfactory import ExpiredIteratorException
from ..common.conf_reader import ConfReader

# TODO: Should we have a GenericIngester class that exposes the `get_msg_json` message method?

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
    """Get shard iterator

    :param shard_id: shard identifier
    :return: shard iterator
    """

    # Try to get iterator based on latest processed sequence number if available
    if shard_id in self.shard_infos:
      try:
        sqn = str(self.shard_infos[shard_id]['sqn'])
        if self.verbose > 5:
          msg = "[{}.get_shard_iterator] Initializing from previous SequenceNumber {}"
          print(msg.format(self.pp, sqn))
        shard_iterator = self.client.get_shard_iterator(StreamName=self.stream_name,
                                                        ShardId=shard_id,
                                                        StartingSequenceNumber=sqn,
                                                        ShardIteratorType='AFTER_SEQUENCE_NUMBER')
        return shard_iterator['ShardIterator']
      except Exception as inst:
        msg = "[{}.get_shard_iterator] Could not initialize from previous SequenceNumber {}. {}"
        print(msg.format(self.pp, sqn, inst))
      # Could fail with
      #botocore.errorfactory.InvalidArgumentException: An error occurred (InvalidArgumentException) when calling the GetShardIterator operation: StartingSequenceNumber 49592949124142737608129152630877400786080442094840709122 used in GetShardIterator on shard shardId-000000000000 in stream test-local-kinesis-caltech101 under account 000000000000 is invalid because it did not come from this stream.
    shard_iterator_type = self.get_required_param('shard_iterator_type')
    if self.verbose > 5:
      msg = "[{}.get_shard_iterator] Initializing with type {}"
      print(msg.format(self.pp, shard_iterator_type))

    shard_iterator = self.client.get_shard_iterator(StreamName=self.stream_name,
                                                    ShardId=shard_id,
                                                    ShardIteratorType=shard_iterator_type)
    return shard_iterator['ShardIterator']


  def init_client(self):
    """Initialize Kinesis client.
    """
    region_name = self.get_required_param('region_name')
    aws_profile = self.get_param('aws_profile', default=None)
    # This is mostly to be able to test locally
    endpoint_url = self.get_param('endpoint_url', default=None)
    verify = self.get_param('verify_certificates', default=True)
    use_ssl = self.get_param('use_ssl', default=True)

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
      if self.verbose > 5:
        msg = "[{}: log] Entering get_msg_json"
        print(msg.format(self.pp))

      lim_get_rec = self.get_required_param('lim_get_rec')
      sleep_time = self.get_required_param('sleep_time')
      sifn = self.get_shard_infos_filename()
      nb_shards = len(self.shard_iters)
      sleep_count = 0

      try:
        while True:
          empty = 0
          if self.verbose > 5:
            msg = "[{}: log] Entering while loop in get_msg_json"
            print(msg.format(self.pp))

          # Iterate over shards
          for sh_num in range(nb_shards):
            sh_id = self.shard_iters.keys()[sh_num]
            sh_it = self.shard_iters[sh_id]
            if sh_it is None:
              self.shard_iters[sh_id] = self.get_shard_iterator(sh_id)
              sh_it = self.shard_iters[sh_id]

            if self.verbose > 2:
              msg = "[{}: log] Getting records starting from {} in shard {}"
              print(msg.format(self.pp, sh_it, sh_id))

            # If iterator has expired, we would get botocore.errorfactory.ExpiredIteratorException:
            # An error occurred (ExpiredIteratorException) when calling the GetRecords operation: Iterator expired.
            # The iterator was created at time XXX while right now it is XXX which is further in the future than the tolerated delay of 300000 milliseconds.
            # Also beware of get_records limits of 2MB per second per shard
            try:
              rec_response = self.client.get_records(ShardIterator=sh_it, Limit=lim_get_rec)
            #except ExpiredIteratorException as inst:
            except Exception as inst:
              # Iterator may have expired...
              if self.verbose > 0:
                msg = "[{}: WARNING] Could not get records starting from {} in shard {}. {}"
                print(msg.format(self.pp, sh_it, sh_id, inst))
              if self.verbose > 5:
                msg = "[{}: log] Invalidating shard iterator for shard {}"
                print(msg.format(self.pp, sh_id))
              self.shard_iters[sh_id] = None
              continue

            # To iterate in same shard
            if 'NextShardIterator' in rec_response:
              sh_it = rec_response['NextShardIterator']
              if sh_it is None:
                if self.verbose > 5:
                  msg = "[{}: log] Invalidating shard iterator for shard {}"
                  print(msg.format(self.pp, sh_id))
                self.shard_iters[sh_id] = None
              else:
                # Update iterator. Is this working?
                if self.verbose > 4:
                  msg = "[{}: log] Found valid next shard iterator {} for shard {}"
                  print(msg.format(self.pp, sh_it, sh_id))
                self.shard_iters[sh_id] = sh_it
              # if self.verbose > 4:
              #   msg = "[{}: log] Loop getting records. Starting from {} in shard {}"
              #   print(msg.format(self.pp, sh_it, sh_id))
              # Try catch this?
              # rec_response = self.client.get_records(ShardIterator=sh_it, Limit=lim_get_rec)
            else:
              if self.verbose > 5:
                msg = "[{}: log] Invalidating shard iterator for shard {}"
                print(msg.format(self.pp, sh_id))
              self.shard_iters[sh_id] = None

            records = rec_response['Records']

            if len(records) > 0:
              if self.verbose > 3:
                # msg = "[{}: log] Found message at SequenceNumber {} in shard {}: {}"
                # print(msg.format(self.pp, sqn, sh_id, rec_json))
                msg = "[{}: log] Got {} records"
                print(msg.format(self.pp,len(records)))
              sleep_count = 0
              for rec in records:
                rec_json = json.loads(rec['Data'])
                sqn = str(rec['SequenceNumber'].decode("utf-8"))
                if self.verbose > 5:
                  #msg = "[{}: log] Found message at SequenceNumber {} in shard {}: {}"
                  #print(msg.format(self.pp, sqn, sh_id, rec_json))
                  msg = "[{}: log] Found message at SequenceNumber {} in shard {}"
                  print(msg.format(self.pp, sqn, sh_id))

                # Store `sqn`. Is there anything else we should store?
                # Maybe number of records read for sanity check
                # Start read time too?
                if sh_id in self.shard_infos:
                  self.shard_infos[sh_id]['sqn'] = sqn
                  self.shard_infos[sh_id]['nb_read'] += 1
                else:
                  self.shard_infos[sh_id] = dict()
                  self.shard_infos[sh_id]['sqn'] = sqn
                  self.shard_infos[sh_id]['start_read'] = datetime.now().isoformat()
                  self.shard_infos[sh_id]['nb_read'] = 1

                yield rec_json

              if self.verbose > 5:
                msg = "[{}: log] Finished looping on {} records"
                print(msg.format(self.pp,len(records)))


            if self.shard_iters[sh_id] is None:
              empty += 1
              if self.verbose > 3:
                msg = "[{}: log] Shard {} seems empty"
                print(msg.format(self.pp, sh_id))

            # if self.verbose > 5:
            #   msg = "[{}: log] Invalidating shard iterator for shard {}"
            #   print(msg.format(self.pp, sh_id))
            # self.shard_iters[sh_id] = None

          # Dump current self.shard_infos
          if self.verbose > 1:
            msg = "[{}: log] shard_infos: {}"
            print(msg.format(self.pp, self.shard_infos))
          with open(sifn, 'w') as sif:
            json.dump(self.shard_infos, sif)

          # Sleep?
          if empty == len(self.shard_iters):
            if self.verbose > 1:
              msg = "[{}: log] All shards seem empty or fully processed."
              print(msg.format(self.pp))
            time.sleep(min(sleep_count + 1, sleep_time))
            sleep_count += 1
          else:
            time.sleep(1)
      except Exception as inst:
        msg = "[{}: ERROR] get_msg_json failed with error {}"
        print(msg.format(self.pp, inst))