"""Generic detector
"""

from ..imgio.imgio import get_buffer_from_URL, get_buffer_from_filepath, get_buffer_from_B64, \
  get_SHA1_img_info_from_buffer, load_image_from_buffer

DEFAULT_UPSAMPLING = 1
DEFAULT_IMAGE_DL_TIMEOUT = 1

class GenericDetector(object):
  """To be inherited by any other detector.
  """

  def __init__(self):
    pass

  def detect_from_filepath(self, img_file_path, up_sample=DEFAULT_UPSAMPLING, with_infos=True):
    """Run detection in image at local path ``img_file_path``.
     The path has to be readable from within the docker

    :param img_file_path: full path of the image to process
    :type img_file_path: string
    :param up_sample: number of upsampling before detection to allow small detection. [optional]
    :type up_sample: int
    :param with_infos: wether or not we also compute image info. [optional]
    :type with_infos: bool

    :returns: (optionally image infos), loaded image, and the detections as a list of dict with
     keys: ``left``, ``top``, ``right``, ``bottom``, ``score``
    :rtype: tuple
    """
    # """
    # Args:
    #   img_file_path (str): full path of the image to process
    #   up_sample (int): number of upsampling before detection to allow small detection. [optional]
    #   with_infos (bool): wether or not we also compute image info. [optional]
    #
    #  Returns:
    #   output (tuple): (optionally image infos), loaded image, and the detections as a list of dict
    # with keys "left", "top", "right", "bottom"
    # """
    if with_infos:
      return self.detect_from_buffer(get_buffer_from_filepath(img_file_path), up_sample=up_sample)
    return self.detect_from_buffer_noinfos(get_buffer_from_filepath(img_file_path),
                                           up_sample=up_sample)

  def detect_from_url(self, img_url, up_sample=DEFAULT_UPSAMPLING,
                      image_dl_timeout=DEFAULT_IMAGE_DL_TIMEOUT, with_infos=True):
    """Run detection in image at URL ``img_url``.

    :param img_url: full URL of the image to process
    :type img_url: string
    :param up_sample: number of upsampling before detection to allow small detection [optional]
    :type up_sample: int
    :param image_dl_timeout: timeout in seconds for image download. [optional]
    :type image_dl_timeout: int
    :param with_infos: wether or not we also compute image info. [optional]
    :type with_infos: bool
    :returns: (optionally image infos), loaded image, and the detections as a list of dict with
     keys: ``left``, ``top``, ``right``, ``bottom``, ``score``
    :rtype: tuple

    """
    # """
    # Args:
    #   img_url (str): full URL of the image to process
    #   up_sample (int): number of upsampling before detection to allow small detection [optional]
    #   image_dl_timeout (int): timeout in seconds for image download. [optional]
    #   with_infos (bool): wether or not we also compute image info. [optional]
    #
    # Returns:
    #   output (tuple): (optionally image infos), loaded image, and the detections as a list of dict
    #  with keys "left", "top", "right", "bottom"
    # """
    if with_infos:
      return self.detect_from_buffer(get_buffer_from_URL(img_url,
                                                         image_dl_timeout=image_dl_timeout),
                                     up_sample=up_sample)
    return self.detect_from_buffer_noinfos(get_buffer_from_URL(img_url,
                                                               image_dl_timeout=image_dl_timeout),
                                           up_sample=up_sample)

  def detect_from_b64(self, img_base64str, up_sample=DEFAULT_UPSAMPLING, with_infos=True):
    """Run detection in image encoded in base64 `1img_base64str``

    :param img_base64str: base64 encoded string of the image to process
    :type img_base64str: string
    :param up_sample: number of upsampling before detection to allow small detection [optional]
    :type up_sample: int
    :param with_infos: wether or not we also get image info. [optional]
    :type with_infos: bool
    :returns: (optionally image infos), loaded image, and the detections as a list of dict with
     keys: ``left``, ``top``, ``right``, ``bottom``, ``score``
    :rtype: tuple
    """
    # """
    # Args:
    #   img_base64str (str): base64 encoded string of the image to process
    #   up_sample (int): number of upsampling before detection to allow small faces detection.
    #   with_infos (bool): wether or not we also compute image info. [optional]
    #
    # Returns:
    #   output (tuple): (optionally image infos), loaded image, and the detections as a list of dict
    #  with keys "left", "top", "right", "bottom"
    # """
    if with_infos:
      return self.detect_from_buffer(get_buffer_from_B64(img_base64str), up_sample)
    return self.detect_from_buffer_noinfos(get_buffer_from_B64(img_base64str), up_sample)

  # You may need to override this method if your detector cannot work with a skimage
  def detect_from_buffer(self, img_buffer, up_sample=DEFAULT_UPSAMPLING):
    """Run detection in image ``img_buffer`` and extract image infos.

    :param img_buffer: image buffer
    :type img_buffer: :class:`numpy.ndarray`
    :param up_sample: number of upsampling before detection to allow small detection [optional]
    :type up_sample: int
    :returns: (sha1, img_type, width, height, img, detections) with `img` being the loaded image and
     `detections` the detections as a list of dict with keys: ``left``, ``top``, ``right``,
     ``bottom``, ``score``
    :rtype: tuple
    """
    # """
    # Args:
    #   img_buffer (numpy.ndarray): image buffer
    #   up_sample (int): integer to specifiy if we should perform upsampling [optional]
    #
    # Returns:
    #   output (tuple): (sha1, img_type, width, height, img, detections) with 'img' being the loaded
    # image and 'detections' the detections as a list of dict with keys "left", "top", "right",
    # "bottom"
    # """
    sha1, img_type, width, height = get_SHA1_img_info_from_buffer(img_buffer)
    img_buffer.seek(0)
    img = load_image_from_buffer(img_buffer)
    return sha1, img_type, width, height, img, self.detect_from_img(img, up_sample)

  def detect_from_buffer_noinfos(self, img_buffer, up_sample=DEFAULT_UPSAMPLING):
    """Run detection in image ``img_bufer`` but do not extract image infos.

    :param img_buffer: image buffer
    :type img_buffer: :class:`numpy.ndarray`
    :param up_sample: number of upsampling before detection to allow small detection [optional]
    :type up_sample: int
    :returns: img, detections with `img` being the loaded image and `detections` the detections as a
     list of dict with keys: ``left``, ``top``, ``right``, ``bottom``, ``score``
    :rtype: tuple
    """
    # """
    # Args:
    #   img_buffer (buffer): image buffer
    #   up_sample (int): integer to specifiy if we should perform upsampling (optional)
    #
    # Returns:
    #   output (tuple): (img, detections) with 'img' being the loaded image and 'detections' the
    # detections as a list of dict with keys `left``, ``top``, ``right``, ``bottom``, ``score``
    # """
    img = load_image_from_buffer(img_buffer)
    return img, self.detect_from_img(img, up_sample)

  #NB: You have to override this method in the child class.
  def detect_from_img(self, img, up_sample=DEFAULT_UPSAMPLING):
    """ This method implementation will depend on the actual detector being used.

    :param img_buffer: image buffer
    :type img_buffer: :class:`numpy.ndarray`
    :param up_sample: number of upsampling before detection to allow small detection [optional]
    :type up_sample: int
    :returns: detections as a list of dict with keys: ``left``, ``top``, ``right``, ``bottom``,
     ``score``
    :rtype: list
    :raises NotImplementedError: if not overwritten
    """
    # """
    # Args:
    #   img (numpy.ndarray): image loaded with scikit-image
    #   up_sample (int): integer to specifiy if we should perform upsampling (optional)
    #
    # Returns:
    #   detections (list): the detections as a list of dict with keys ``left``, ``top``, ``right``,
    # ``bottom``, ``score``)
    # """
    raise NotImplementedError('detect_from_img')
