import boto3
import botocore
# TODO: use botocore to properly catch exceptions as below...
# except botocore.exceptions.ClientError as e:
# error_code = int(e.response['Error']['Code'])
import cStringIO as sio
import cPickle as pickle
from .generic_storer import GenericStorer
from cufacesearch.common.error import full_trace_error

default_prefix = "S3ST_"

class S3Storer(GenericStorer):

  def __init__(self, global_conf_in, prefix=default_prefix):
    super(S3Storer, self).__init__(global_conf_in, prefix)
    self.set_pp(pp="S3Storer")

    # This assumes you have set the corresponding profile in ~/.aws/credentials
    self.bucket_name = self.get_required_param('bucket_name')
    self.region = self.get_param('aws_region', default=None)
    self.aws_profile = self.get_param('aws_profile', default=None)
    # we can define a prefix, i.e. folder in a bucket as a parameter
    self.aws_prefix = self.get_param('aws_prefix', default='')
    self.session = None

    try:
      self.setup()
    except botocore.exceptions.ProfileNotFound:
      raise ValueError("Could not find AWS profile: {}".format(self.aws_profile))

  def setup(self):
    self.session = boto3.Session(profile_name=self.aws_profile, region_name=self.region)
    self.s3 = self.session.resource('s3')
    # Try to access first to make sure
    try:
      self.s3.meta.client.head_bucket(Bucket=self.bucket_name)
    except botocore.exceptions.ClientError as e:
      err_msg = "[{}: error] Could not check bucket '{}' using profile '{}' in region '{}'"
      full_trace_error(err_msg.format(self.pp, self.bucket_name, self.aws_profile, self.region))
      raise e
    self.bucket = self.s3.Bucket(self.bucket_name)
    if self.verbose > 0:
      msg = "[{}: log] Initialized with bucket '{}' and profile '{}' in region '{}'."
      print(msg.format(self.pp, self.bucket_name, self.aws_profile, self.region))

  def save(self, key, obj):
    # Pickle and save to s3 bucket
    buffer = sio.StringIO(pickle.dumps(obj))
    save_key = key
    if self.aws_prefix:
      save_key = '/'.join([self.aws_prefix, key])
    self.bucket.upload_fileobj(buffer, save_key)
    if self.verbose > 1:
      print("[{}: log] Saved file: {}".format(self.pp, save_key))

  def load(self, key, silent=False):
    # Load a pickle object from s3 bucket
    try:
      buffer = sio.StringIO()
      load_key = key
      if self.aws_prefix:
        load_key = '/'.join([self.aws_prefix, key])
      self.bucket.download_fileobj(load_key, buffer)
      # buffer has been filled, offset is at the end, seek to beginning for unpickling
      buffer.seek(0)
      obj = pickle.load(buffer)
      if self.verbose > 1:
        print("[{}: log] Loaded file: {}".format(self.pp, load_key))
      return obj
    except Exception as e:
      if self.verbose > 1 and not silent:
        err_msg = "[{}: error ({}: {})] Could not load object with key: {}"
        print(err_msg.format(self.pp, type(e), e, load_key))

  def list_prefix(self, prefix_path):
    if self.aws_prefix:
      prefix_path = '/'.join([self.aws_prefix, prefix_path])
    for obj in self.bucket.objects.filter(Prefix=prefix_path):
      yield obj

  # This would be used to load all codes
  def get_all_from_prefix(self, prefix_path):
    if self.aws_prefix:
      prefix_path = '/'.join([self.aws_prefix, prefix_path])
    for obj in self.list_prefix(prefix_path):
      yield self.load(obj.key)

if __name__ == "__main__":
  s3_conf = {"aws_profile": "cuimagesearch", "bucket_name": "dig-cu-imagesearchindex"}

  s3s = S3Storer(s3_conf, prefix="")