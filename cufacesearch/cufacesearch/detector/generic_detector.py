from ..imgio.imgio import get_buffer_from_URL, get_buffer_from_B64, get_SHA1_img_info_from_buffer
from skimage import io as skio
import numpy as np

default_upsampling = 1
default_image_dl_timeout = 1

# Could be moved to a factory
def get_detector(detector_type):
  #if detector_type == "dlib_detector":
  if detector_type == "dlib":
    import dlib_detector
    return dlib_detector.DLibFaceDetector()
  elif detector_type == "full":
    return None
  else:
    raise ValueError("[{}: error] unknown 'detector' {}.".format("get_detector", detector_type))

def get_bbox_str(bbox):
  # Build bbox string as left_top_right_bottom_score
  return "_".join(["{}"]*5).format(bbox["left"], bbox["top"], bbox["right"], bbox["bottom"], bbox["score"])


class GenericFaceDetector(object):

  def __init__(self):
    pass

  def detect_from_url(self, img_url, up_sample=default_upsampling, image_dl_timeout=default_image_dl_timeout):
    """ Detect faces in image at URL 'img_url'.

    :param img_url: full URL of the image to process
    :param up_sample: number of upsampling before detection to allow small faces detection.
    :param image_dl_timeout: timeout in seconds for image download.
    :return:
    """
    return self.detect_from_buffer(get_buffer_from_URL(img_url, image_dl_timeout=image_dl_timeout), up_sample=up_sample)

  def detect_from_b64(self, base64str, up_sample=default_upsampling):
    return self.detect_from_buffer(get_buffer_from_B64(base64str), up_sample)

  # You may need to override this method if your detector needs cannot work with a skimage
  def detect_from_buffer(self, img_buffer, up_sample=default_upsampling):
    sha1, img_type, width, height = get_SHA1_img_info_from_buffer(img_buffer)
    img_buffer.seek(0)
    img = skio.imread(img_buffer)
    return sha1, img_type, width, height, img, self.detect_from_img(img, up_sample)

  def detect_from_buffer_noinfos(self, img_buffer, up_sample=default_upsampling):
    img = skio.imread(img_buffer)
    # Deal with GIF
    if len(img.shape) == 4:
      # Get first 'frame' of GIF
      img = np.squeeze(img[1, :, :, :])
    # Deal with alpha channel in PNG
    if img.shape[:-1]==4:
      img = img[:, :, :3]
    return img, self.detect_from_img(img, up_sample)

  # You have to override this method in the child class.
  def detect_from_img(self, img, up_sample=default_upsampling):
    """ This method implementation will depend on the actual detector being used.

    Args:
      img: image as a scikit-image
      up_sample: integer to specifiy if we should perform upsampling (optional)

    Returns:
      list: the detections as a list of dict with keys "left", "top", "right", "bottom"
    """
    raise NotImplementedError('detect_from_img')