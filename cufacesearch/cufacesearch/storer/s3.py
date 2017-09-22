import boto3
import botocore # TODO: to properly catch exceptions...
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

    # This assumes you have the corresponding profile in ~/.aws/credentials
    self.aws_profile = self.get_param('aws_profile')
    self.bucket_name = self.get_required_param('bucket_name')

    self.session = None
    self.setup()

  def set_pp(self):
    self.pp = "S3Storer"

  def setup(self):
    self.session = boto3.Session(profile_name=self.aws_profile)
    self.s3 = self.session.resource('s3')
    # Try to access first to make sure
    try:
      self.s3.meta.client.head_bucket(Bucket=self.bucket_name)
    except botocore.exceptions.ClientError as e:
      err_msg = "[{}: error] Could not check bucket {} using profile {}"
      full_trace_error(err_msg.format(self.pp, self.bucket_name, self.aws_profile))
      raise e
    self.bucket = self.s3.Bucket(self.bucket_name)
    if self.verbose > 0:
      print "[{}: log] Initialized with bucket '{}' and profile '{}'.".format(self.pp, self.bucket_name, self.aws_profile)

  def save(self, key, obj):
    # Pickle and save to s3 bucket
    buffer = sio.StringIO(pickle.dumps(obj))
    self.bucket.upload_fileobj(buffer, key)
    if self.verbose > 1:
      print "[{}: log] Saved file: {}".format(self.pp, key)


  def load(self, key):
    # Load a pickle object from s3 bucket
    try:
      buffer = sio.StringIO()
      self.bucket.download_fileobj(key, buffer)
      obj = pickle.load(buffer)
      if self.verbose > 1:
        print "[{}: log] Loaded file: {}".format(self.pp, key)
      return obj
    except Exception as e:
      err_msg = "[{}: error ({}: {})] Could not load object with key: {}"
      print err_msg.format(self.pp, type(e), e, key)

  def list_prefix(self, prefix_path):
    for obj in self.bucket.objects.filter(Prefix=prefix_path):
      yield obj

  # This would be used to load all codes
  def get_all_from_prefix(self, prefix_path):
    for obj in self.list_prefix(prefix_path):
      yield self.load(obj.key)

if __name__ == "__main__":
  s3_conf = {"aws_profile": "cuimagesearch", "bucket_name": "dig-cu-imagesearchindex"}

  s3s = S3Storer(s3_conf, prefix="")