from ..imgio.imgio import get_buffer_from_URL, get_buffer_from_filepath, get_buffer_from_B64, get_SHA1_img_info_from_buffer
from skimage import io as skio
import numpy as np

default_upsampling = 1
default_image_dl_timeout = 1

# Could be moved to a factory
def get_detector(detector_type):
  """Get detector of ``detector_type``

  :param detector_type:
  :type detector_type: string
  :return: detector
  """
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
  """Generic face detector. To be inherited by any other detector.
  """

  def __init__(self):
    pass

  def load_image_from_buffer(self, img_buffer):
    """ Load an image from a buffer. Can deal with GIF and alpha channel

    :param img_buffer: image buffer
    :type img_buffer: buffer
    :returns: the loaded image
    :rtype: :class:`numpy.ndarray`
    """
    img = skio.imread(img_buffer)
    # Deal with GIF
    if len(img.shape) == 4:
      # Get first 'frame' of GIF
      img = np.squeeze(img[1, :, :, :])
    # Deal with alpha channel in PNG
    if img.shape[-1] == 4:
      img = img[:, :, :3]
    return img

  def detect_from_filepath(self, img_file_path, up_sample=default_upsampling, image_dl_timeout=default_image_dl_timeout,
                      with_infos=True):
    """ Run face detection in image at local path 'img_file_path'.

    The path has to be readable from within the docker.

    Args:
      img_file_path (str): full path of the image to process
      up_sample (int): number of upsampling before detection to allow small faces detection. [optional]
      image_dl_timeout (int): timeout in seconds for image download. [optional]
      with_infos (bool): wether or not we also . [optional]

    Returns:
      output (tuple): (optionally image infos), loaded image, and the detections as a list of dict with keys "left", "top", "right", "bottom"
    """
    if with_infos:
      return self.detect_from_buffer(get_buffer_from_filepath(img_file_path), up_sample=up_sample)
    else:
      return self.detect_from_buffer_noinfos(get_buffer_from_filepath(img_file_path), up_sample=up_sample)

  def detect_from_url(self, img_url, up_sample=default_upsampling, image_dl_timeout=default_image_dl_timeout,
                      with_infos=True):
    """ Run face detection in image at URL 'img_url'.

    :param img_url: full URL of the image to process
    :type img_url: string
    :param up_sample: number of upsampling before detection to allow small faces detection [optional]
    :type up_sample: int
    :param image_dl_timeout: timeout in seconds for image download. [optional]
    :type image_dl_timeout: int
    :param with_infos: wether or not we also get image info. [optional]
    :type with_infos: bool
    :returns: (optionally image infos), loaded image, and the detections as a list of dict with keys "left", "top", "right", "bottom"
    :rtype: tuple

    Args:
      img_url (str): full URL of the image to process
      up_sample (int): number of upsampling before detection to allow small faces detection. [optional]
      image_dl_timeout (int): timeout in seconds for image download. [optional]
      with_infos (bool): wether or not we also . [optional]

    Returns:
      output (tuple): (optionally image infos), loaded image, and the detections as a list of dict with keys "left", "top", "right", "bottom"
    """
    if with_infos:
      return self.detect_from_buffer(get_buffer_from_URL(img_url, image_dl_timeout=image_dl_timeout),
                                     up_sample=up_sample)
    else:
      return self.detect_from_buffer_noinfos(get_buffer_from_URL(img_url, image_dl_timeout=image_dl_timeout),
                                     up_sample=up_sample)

  def detect_from_b64(self, img_base64str, up_sample=default_upsampling, with_infos=True):
    """ Run face detection in image encoded in base64 'base64str'.

    Args:
      img_base64str (str): base64 encoded string of the image to process
      up_sample (int): number of upsampling before detection to allow small faces detection.
      with_infos (bool): wether or not we also . [optional]

    Returns:
      output (tuple): (optionally image infos), loaded image, and the detections as a list of dict with keys "left", "top", "right", "bottom"
    """
    if with_infos:
      return self.detect_from_buffer(get_buffer_from_B64(img_base64str), up_sample)
    else:
      return self.detect_from_buffer_noinfos(get_buffer_from_B64(img_base64str), up_sample)

  # You may need to override this method if your detector needs cannot work with a skimage
  def detect_from_buffer(self, img_buffer, up_sample=default_upsampling):
    """ Detect faces in an image and extract image infos.

    Args:
      img_buffer (buffer): image buffer
      up_sample (int): integer to specifiy if we should perform upsampling (optional)

    Returns:
      output (tuple): (sha1, img_type, width, height, img, detections) with 'img' being the loaded image and 'detections' the detections as a list of dict with keys "left", "top", "right", "bottom"
    """
    sha1, img_type, width, height = get_SHA1_img_info_from_buffer(img_buffer)
    img_buffer.seek(0)
    img = self.load_image_from_buffer(img_buffer)
    return sha1, img_type, width, height, img, self.detect_from_img(img, up_sample)

  def detect_from_buffer_noinfos(self, img_buffer, up_sample=default_upsampling):
    """ Detect faces in an image but do not extract image infos.

    Args:
      img_buffer (buffer): image buffer
      up_sample (int): integer to specifiy if we should perform upsampling (optional)

    Returns:
      output (tuple): (img, detections) with 'img' being the loaded image and 'detections' the detections as a list of dict with keys "left", "top", "right", "bottom"
    """
    img = self.load_image_from_buffer(img_buffer)
    return img, self.detect_from_img(img, up_sample)

  #NB: You have to override this method in the child class.
  def detect_from_img(self, img, up_sample=default_upsampling):
    """ This method implementation will depend on the actual detector being used.

    Args:
      img: image as a scikit-image
      up_sample (int): integer to specifiy if we should perform upsampling (optional)

    Returns:
      detections (list): the detections as a list of dict with keys "left", "top", "right", "bottom"
    """
    raise NotImplementedError('detect_from_img')