import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

class UnknownImageFormat(Exception):
  pass

ImageMIMETypes = dict()
ImageMIMETypes['GIF'] = "image/gif"
ImageMIMETypes['PNG'] = "image/png"
ImageMIMETypes['JPEG'] = "image/jpeg"

DEFAULT_RETRIES = 3
DEFAULT_BOF = 0.2
DEFAULT_FL = [500, 502, 503, 504]
SESSION = requests.Session()
RETRIES_SETTINGS = Retry(total=DEFAULT_RETRIES, backoff_factor=DEFAULT_BOF,
                         status_forcelist=DEFAULT_FL)
SESSION.mount('http://', HTTPAdapter(max_retries=RETRIES_SETTINGS))
SESSION.mount('https://', HTTPAdapter(max_retries=RETRIES_SETTINGS))


def get_SHA1_from_data(data):
  sha1hash = None
  import hashlib
  try:
    sha1 = hashlib.sha1()
    sha1.update(data)
    sha1hash = sha1.hexdigest().upper()
  except:
    print "Could not read data to compute SHA1."
  return sha1hash


def get_image_size_and_format(input_data):
  # adapted from https://github.com/scardine/image_size
  """
  Return (width, height, format) for a given img file content stream.
  No external dependencies except the struct module from core.
  """
  import struct

  height = -1
  width = -1
  data = input_data.read(25)

  if data[:6] in ('GIF87a', 'GIF89a'):
    # GIFs
    tmp_w, tmp_h = struct.unpack("<HH", data[6:10])
    width = int(tmp_w)
    height = int(tmp_h)
    img_format = 'GIF'
  elif data.startswith('\211PNG\r\n\032\n') and (data[12:16] == 'IHDR'):
    # PNGs
    tmp_w, tmp_h = struct.unpack(">LL", data[16:24])
    width = int(tmp_w)
    height = int(tmp_h)
    img_format = 'PNG'
  elif data.startswith('\211PNG\r\n\032\n'):
    # older PNGs?
    tmp_w, tmp_h = struct.unpack(">LL", data[8:16])
    width = int(tmp_w)
    height = int(tmp_h)
    img_format = 'PNG'
  elif data.startswith('\377\330'):
    # JPEG
    img_format = 'JPEG'
    msg = " raised while trying to decode as JPEG."
    input_data.seek(0)
    input_data.read(2)
    byte_read = input_data.read(1)
    try:
      while byte_read and ord(byte_read) != 0xDA:
        while ord(byte_read) != 0xFF:
          byte_read = input_data.read(1)
        while ord(byte_read) == 0xFF:
          byte_read = input_data.read(1)
        if (ord(byte_read) >= 0xC0 and ord(byte_read) <= 0xC3):
          input_data.read(3)
          tmp_h, tmp_w = struct.unpack(">HH", input_data.read(4))
          break
        else:
          input_data.read(int(struct.unpack(">H", input_data.read(2))[0]) - 2)
        byte_read = input_data.read(1)
      width = int(tmp_w)
      height = int(tmp_h)
    except struct.error:
      raise UnknownImageFormat("StructError" + msg)
    except ValueError:
      raise UnknownImageFormat("ValueError" + msg)
    except Exception as err:
      raise UnknownImageFormat(err.__class__.__name__ + msg)
  elif data.startswith('<?xml'):
    img_format = 'SVG'
    # We could try to use viewbox or width and height in tag svg to estimate width and height?
  # We could also try to support BMP, RIFF...
  else:
    raise UnknownImageFormat("Sorry, don't know how to get information from this file.")

  return width, height, img_format


def get_SHA1_img_type_from_B64(base64str):
  img_buffer = get_buffer_from_B64(base64str)
  sha1, img_type, _, _ = get_SHA1_img_info_from_buffer(img_buffer)
  return sha1, img_type

def get_SHA1_from_buffer(img_buffer):
  img_buffer.seek(0)
  return get_SHA1_from_data(img_buffer.read())

def get_SHA1_img_info_from_buffer(img_buffer):
  width, height, img_type = get_image_size_and_format(img_buffer)
  img_buffer.seek(0)
  sha1 = get_SHA1_from_data(img_buffer.read())
  return sha1, img_type, width, height


def buffer_to_B64(img_buffer):
  import base64
  # make sure buffer is at beginning
  img_buffer.seek(0)
  data = img_buffer.read()
  b64_from_data = base64.b64encode(data)
  return b64_from_data


def get_buffer_from_B64(base64str):
  from cStringIO import StringIO
  import base64
  return StringIO(base64.b64decode(base64str))

def get_buffer_from_filepath(filepath):
  from cStringIO import StringIO
  with open(filepath, 'rb') as f_in:
    return StringIO(f_in.read())

def get_buffer_from_URL(img_url, verbose=0, image_dl_timeout=4, retries=DEFAULT_RETRIES):
  # Sometime fails with a timeout, now using retries
  #   see: https://stackoverflow.com/questions/15431044/can-i-set-max-retries-for-requests-request
  # SSLError: [Errno 1] _ssl.c:510:
  # error:14090086:SSL routines:SSL3_GET_SERVER_CERTIFICATE:certificate verify failed
  # should we pass verify=False to make sure we will download the image...
  from cStringIO import StringIO
  if verbose > 0:
    print "Downloading image from {}".format(img_url)
  if retries != 0:
    if retries != DEFAULT_RETRIES:
      retries_settings = Retry(total=retries, backoff_factor=DEFAULT_BOF,
                               status_forcelist=DEFAULT_FL)
      SESSION.mount('http://', HTTPAdapter(max_retries=retries_settings))
      SESSION.mount('https://', HTTPAdapter(max_retries=retries_settings))
    req = SESSION.get(img_url, timeout=10)
    #r = s.get(img_url, timeout=image_dl_timeout)
  else:
    req = requests.get(img_url, timeout=image_dl_timeout)
  if req.status_code == 200:
    if int(req.headers['content-length']) == 0:
      del req
      raise ValueError("Empty image.")
    else:
      img_buffer = StringIO(req.content)
      return img_buffer
  else:
    raise ValueError("Incorrect status code: {}".format(req.status_code))


def load_image_from_buffer(img_buffer):
  """Load an image from a buffer. Can deal with GIF and alpha channel

  :param img_buffer: image buffer
  :type img_buffer: buffer
  :returns: the loaded image
  :rtype: :class:`numpy.ndarray`
  """
  import numpy as np
  from skimage import io as skio
  img = skio.imread(img_buffer)
  # Deal with GIF
  if len(img.shape) == 4:
    # Get first 'frame' of GIF
    img = np.squeeze(img[1, :, :, :])
  # Deal with alpha channel in PNG
  if img.shape[-1] == 4:
    img = img[:, :, :3]
  return img

# Should we use boto3 to download from s3?
# http://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Bucket.download_fileobj
# Should we write a get_buffer_from_S3(key) or get_buffer_from_S3(bucket, key) ?
# should the bucket be initialized only once, where?
# to be thread each thread should use it's own session...
# see: https://boto3.readthedocs.io/en/latest/guide/resources.html#multithreading-multiprocessing