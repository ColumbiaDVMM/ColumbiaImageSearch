from ..imgio.imgio import get_buffer_from_URL, get_buffer_from_B64, get_SHA1_img_info_from_buffer
from skimage import io as skio

def get_detector(detector_type):
  if detector_type == "dblib_detector":
    import dblib_detector
    return dblib_detector.DLibFaceDetector()
  else:
    raise ValueError("[{}: error] unknown 'detector' {}.".format("get_detector", detector_type))

class GenericFaceDetector(object):

  image_dl_timeout = 4
  verbose = 1

  def __init__(self):
    pass


  def detect_from_url(self, img_url, up_sample=1, image_dl_timeout=4):
    return self.detect_from_buffer(get_buffer_from_URL(img_url, image_dl_timeout=image_dl_timeout), up_sample=up_sample)


  def detect_from_b64(self, base64str, up_sample=0):
    return self.detect_from_buffer(get_buffer_from_B64(base64str), up_sample)


  # You may need to override this method if your detector needs cannot work with a skimage
  def detect_from_buffer(self, img_buffer, up_sample=0):
    sha1, img_type, width, height = get_SHA1_img_info_from_buffer(img_buffer)
    img_buffer.seek(0)
    img = skio.imread(img_buffer)
    return sha1, img_type, width, height, img, self.detect_from_img(img, up_sample)

  def detect_from_buffer_noinfos(self, img_buffer, up_sample=0):
    img = skio.imread(img_buffer)
    return img, self.detect_from_img(img, up_sample)


  # You have to override this method.
  def detect_from_img(self, img, up_sample=0):
    """ This method implementation will depend on the actual detector being used.

    Args:
      img: image as a scikit-image
      up_sample: integer to specifiy if we should perform upsampling (optional)

    Returns:
      list: the detections as a list of dict with keys "left", "top", "right", "bottom"
    """
    raise NotImplementedError('detect_from_img')