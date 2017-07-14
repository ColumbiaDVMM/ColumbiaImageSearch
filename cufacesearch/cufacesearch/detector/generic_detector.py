class GenericFaceDetector(object):

  image_dl_timeout = 4
  verbose = 1

  def __init__(self):
    pass

  def get_SHA1_from_data(self, data):
    sha1hash = None
    import hashlib
    try:
      sha1 = hashlib.sha1()
      sha1.update(data)
      sha1hash = sha1.hexdigest().upper()
    except:
      print "Could not read data to compute SHA1."
    return sha1hash


  def detect_from_url(self, img_url, upsample=1):
    import requests
    from cStringIO import StringIO
    if self.verbose > 0:
      print "Downloading image from {}".format(img_url)
    r = requests.get(img_url, timeout=self.image_dl_timeout)
    if r.status_code == 200:
      if int(r.headers['content-length']) == 0:
        del r
        raise ValueError("Empty image.")
      else:
        img_buffer = StringIO(r.content)
        return self.detect_from_buffer(img_buffer, upsample)


  def detect_from_b64(self, base64str, up_sample=0):
    from cStringIO import StringIO
    import base64
    img_buffer = StringIO(base64.b64decode(base64str))
    return self.detect_from_buffer(img_buffer, up_sample)


  def detect_from_buffer(self, img_buffer, up_sample=0):
    from skimage import io as skio
    sha1 = self.get_SHA1_from_data(img_buffer.read())
    img_buffer.seek(0)
    img = skio.imread(img_buffer)
    return sha1, img, self.detect_from_img(img, up_sample)


  def detect_from_img(self, img, up_sample=0):
    """ This method implementation will depend on the actual detector being used.

    Args:
      img: image as a scikit-image
      up_sample: integer to specifiy if we should perform upsampling (optional)

    Returns:
      list: the detections as a list of dict with keys "left", "top", "right", "bottom"
    """
    raise NotImplementedError('detect_from_img')