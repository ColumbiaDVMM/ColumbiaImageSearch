class UnknownImageFormat(Exception):
  pass

ImageMIMETypes = dict()
ImageMIMETypes['GIF'] = "image/gif"
ImageMIMETypes['PNG'] = "image/png"
ImageMIMETypes['JPEG'] = "image/jpeg"

import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

default_retries = 10
default_bof = 0.2
default_fl = [500, 502, 503, 504]
s = requests.Session()
retries_settings = Retry(total=default_retries, backoff_factor=default_bof, status_forcelist=default_fl)
s.mount('http://', HTTPAdapter(max_retries=retries_settings))
s.mount('https://', HTTPAdapter(max_retries=retries_settings))

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


def get_image_size_and_format(input):
  # adapted from https://github.com/scardine/image_size
  """
  Return (width, height, format) for a given img file content stream.
  No external dependencies except the struct modules from core.
  """
  import struct

  height = -1
  width = -1
  format = None
  data = input.read(25)

  if data[:6] in ('GIF87a', 'GIF89a'):
    # GIFs
    w, h = struct.unpack("<HH", data[6:10])
    width = int(w)
    height = int(h)
    format = 'GIF'
  elif data.startswith('\211PNG\r\n\032\n') and (data[12:16] == 'IHDR'):
    # PNGs
    w, h = struct.unpack(">LL", data[16:24])
    width = int(w)
    height = int(h)
    format = 'PNG'
  elif data.startswith('\211PNG\r\n\032\n'):
    # older PNGs?
    w, h = struct.unpack(">LL", data[8:16])
    width = int(w)
    height = int(h)
    format = 'PNG'
  elif data.startswith('\377\330'):
    # JPEG
    format = 'JPEG'
    msg = " raised while trying to decode as JPEG."
    input.seek(0)
    input.read(2)
    b = input.read(1)
    try:
      while (b and ord(b) != 0xDA):
        while (ord(b) != 0xFF): b = input.read(1)
        while (ord(b) == 0xFF): b = input.read(1)
        if (ord(b) >= 0xC0 and ord(b) <= 0xC3):
          input.read(3)
          h, w = struct.unpack(">HH", input.read(4))
          break
        else:
          input.read(int(struct.unpack(">H", input.read(2))[0]) - 2)
        b = input.read(1)
      width = int(w)
      height = int(h)
    except struct.error:
      raise UnknownImageFormat("StructError" + msg)
    except ValueError:
      raise UnknownImageFormat("ValueError" + msg)
    except Exception as e:
      raise UnknownImageFormat(e.__class__.__name__ + msg)
  else:
    raise UnknownImageFormat("Sorry, don't know how to get information from this file.")

  return width, height, format


def get_SHA1_img_type_from_B64(base64str):
  img_buffer = get_buffer_from_B64(base64str)
  sha1, img_type, width, height = get_SHA1_img_info_from_buffer(img_buffer)
  return sha1, img_type


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


def get_buffer_from_URL(img_url, verbose=0, image_dl_timeout=4, retries=default_retries):
  # Sometime fails with a timeout, now using retries
  #   see: https://stackoverflow.com/questions/15431044/can-i-set-max-retries-for-requests-request
  import requests
  from cStringIO import StringIO
  if verbose > 0:
    print "Downloading image from {}".format(img_url)
  if retries !=0:
    if retries != default_retries:
      retries_settings = Retry(total=retries, backoff_factor=default_bof, status_forcelist=default_fl)
      s.mount('http://', HTTPAdapter(max_retries=retries_settings))
      s.mount('https://', HTTPAdapter(max_retries=retries_settings))
    r = s.get(img_url, timeout=image_dl_timeout)
  else:
    r = requests.get(img_url, timeout=image_dl_timeout)
  if r.status_code == 200:
    if int(r.headers['content-length']) == 0:
      del r
      raise ValueError("Empty image.")
    else:
      img_buffer = StringIO(r.content)
      return img_buffer