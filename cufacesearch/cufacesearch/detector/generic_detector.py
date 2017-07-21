from ..imgio.imgio import get_buffer_from_URL, get_buffer_from_B64, get_SHA1_img_info_from_buffer

class GenericFaceDetector(object):

  image_dl_timeout = 4
  verbose = 1

  def __init__(self):
    pass


  def detect_from_url(self, img_url, upsample=1):
    return self.detect_from_buffer(get_buffer_from_URL(img_url), upsample)


  def detect_from_b64(self, base64str, up_sample=0):
    return self.detect_from_buffer(get_buffer_from_B64(base64str), up_sample)


  # You may need to override this method if your detector needs cannot work with a skimage
  def detect_from_buffer(self, img_buffer, up_sample=0):
    sha1, img_type, width, height = get_SHA1_img_info_from_buffer(img_buffer)
    img_buffer.seek(0)
    from skimage import io as skio
    img = skio.imread(img_buffer)
    return sha1, img_type, width, height, img, self.detect_from_img(img, up_sample)

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