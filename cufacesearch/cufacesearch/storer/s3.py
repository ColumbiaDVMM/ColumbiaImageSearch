import boto3
import botocore
# TODO: use botocore to properly catch exceptions as below...
# except botocore.exceptions.ClientError as e:
# error_code = int(e.response['Error']['Code'])
# Are cStringIO.StringIO and io.BytesIO compatible enough for our usage?...
try:
  from cStringIO import StringIO as sio
except: # python 3
  # io.StringIO only accept true strings...
  from io import BytesIO as sio
try:
  import cPickle as pickle
except: # python 3
  import pickle
from .generic_storer import GenericStorer
from cufacesearch.common.error import full_trace_error

default_prefix = "S3ST_"

class S3Storer(GenericStorer):
  """S3Storer class.
  """

  def __init__(self, global_conf_in, prefix=default_prefix):
    """S3Storer constructor

    :param global_conf_in: configuration file or dictionary
    :type global_conf_in: str, dict
    :param prefix: prefix in configuration
    :type prefix: str
    """
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
    """Setup S3Storer
    """
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

  def _get_s3obj_key_noprefix(self, s3_obj):
    """Get clean object key from s3 object

    :param s3_obj: s3 object
    :type s3_obj: :class:`S3.Object`
    :return: cleaned up key
    :rtype: str
    """
    key = s3_obj.key
    if self.aws_prefix:
      # self.aws_prefix could appear later in key, we just want to remove the prefix one...
      key = self.aws_prefix.join(key.split(self.aws_prefix)[1:])
    return key


  def save(self, key, obj):
    """Save object ``obj`` at location ``key``

    :param key: location to save
    :type key: str
    :param obj: object to save, will be pickled.
    :type obj: object
    """
    # Pickle and save to s3 bucket
    #buffer = sio.StringIO(pickle.dumps(obj))
    buffer = sio(pickle.dumps(obj))
    save_key = key
    if self.aws_prefix:
      save_key = '/'.join([self.aws_prefix, key])
    self.bucket.upload_fileobj(buffer, save_key)
    if self.verbose > 1:
      print("[{}: log] Saved file: {}".format(self.pp, save_key))

  def load(self, key, silent=False):
    """Load from location ``key``

    :param key: location
    :type key: str
    :param silent: whether load fails silently
    :type silent: bool
    :return: loaded object
    :rtype: object
    """
    # Load a pickle object from s3 bucket
    try:
      #buffer = sio.StringIO()
      buffer = sio()
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
    """List all objects starting with ``prefix_path``

    Args:
      prefix_path (str): prefix path

    Yields:
      :class:`S3.Object`: s3 object
    """
    # NB: used Google style documentation here to get yield type recognized
    # """List all files in ``prefix_path``
    #
    # :param prefix_path: prefix path
    # :type prefix_path: str
    # :yield: object
    # """
    # Should this actually return _get_s3obj_key_noprefix(obj) to be more similar to local storer?
    if self.aws_prefix:
      prefix_path = '/'.join([self.aws_prefix, prefix_path])
    for obj in self.bucket.objects.filter(Prefix=prefix_path):
      yield obj

  # This would be used to load all codes
  def get_all_from_prefix(self, prefix_path):
    """Get all objects in ``prefix_path``

    Args:
      prefix_path (str): prefix path

    Yields:
      object: object
    """
    # NB: used Google style documentation here to get yield type recognized
    # """Get all objects in ``prefix_path``
    #
    # :param prefix_path: prefix path
    # :type prefix_path: str
    # :yield: object
    # """
    # NB: here object is a python object loaded from pickle not an S3 Object
    if self.aws_prefix:
      prefix_path = '/'.join([self.aws_prefix, prefix_path])
    for obj in self.list_prefix(prefix_path):
      yield self.load(obj.key)

if __name__ == "__main__":
  s3_conf = {"aws_profile": "cuimagesearch", "bucket_name": "dig-cu-imagesearchindex"}

  s3s = S3Storer(s3_conf, prefix="")
