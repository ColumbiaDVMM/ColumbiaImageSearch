"""Utility functions for detection.
"""

from ..detector import dlib_detector

def show_bbox_from_URL(img_url, bbox, close_after=None):
  """Show the bounding box in ``bbox`` on image at ``img_url``

  :param img_url: image URL
  :type img_url: string
  :param bbox: bounding box tuple (left, top, right, bottom) or dictionary with keys: ``left``,
   ``top``, ``right``, ``bottom``
  :type bbox: Union[tuple, dict]
  :param close_after: time in seconds to wait before closing image window. ``None`` (default) to
   leave window open
  :type close_after: int
  """
  from ..imgio.imgio import get_buffer_from_URL
  from PIL import Image
  img_buffer = get_buffer_from_URL(img_url)
  img = Image.open(img_buffer)
  show_bbox(img, bbox, close_after)


def show_bbox(img, bbox, close_after=None):
  """Show the bounding box defined in ``bbox`` on the image ``img`` using matplotlib.

  :param img: image
  :type img: :class:`numpy.ndarray`
  :param bbox: dictionary with keys: ``left``, ``top``, ``right``, ``bottom``
  :type bbox: dict
  :param close_after: time in seconds to wait before closing image window. ``None`` (default) to
   leave window open
  :type close_after: int
  """
  import matplotlib.pyplot as plt
  import matplotlib.patches as patches

  if isinstance(type(bbox), dict()):
    bbox = [bbox["left"], bbox["top"], bbox["right"], bbox["bottom"]]

  # Create figure and axes
  _, ax = plt.subplots(1)

  # Display the image
  ax.imshow(img)
  rect = patches.Rectangle((bbox[0], bbox[1]),
                           bbox[2] - bbox[0], bbox[3] - bbox[1],
                           linewidth=2, edgecolor='r', facecolor='none')

  # Add the patch to the Axes
  ax.add_patch(rect)

  if close_after:
    plt.show(block=False)
    import time
    time.sleep(close_after)
    plt.close()
  else:
    plt.show()

def build_bbox_str_list(bbox):
  """Build bounding box string representation from ``bbox``

  :param bbox: dictionary with keys: ``left``, ``top``, ``right``, ``bottom``
  :type bbox: dict
  :return: bounding box string list [left, top, width, height]
  :rtype: string
  """
  width = bbox['right'] - bbox['left']
  height = bbox['bottom'] - bbox['top']
  bbox_str_list = []
  bbox_str_list.append(str(max(0, bbox['left'])))
  bbox_str_list.append(str(max(0, bbox['top'])))
  bbox_str_list.append(str(width))
  bbox_str_list.append(str(height))
  return bbox_str_list

# Moved to imgio
# def load_image_from_buffer(img_buffer):
#   """Load an image from a buffer. Can deal with GIF and alpha channel
#
#   :param img_buffer: image buffer
#   :type img_buffer: buffer
#   :returns: the loaded image
#   :rtype: :class:`numpy.ndarray`
#   """
#   import numpy as np
#   from skimage import io as skio
#   img = skio.imread(img_buffer)
#   # Deal with GIF
#   if len(img.shape) == 4:
#     # Get first 'frame' of GIF
#     img = np.squeeze(img[1, :, :, :])
#   # Deal with alpha channel in PNG
#   if img.shape[-1] == 4:
#     img = img[:, :, :3]
#   return img

def get_detector(detector_type):
  """Get detector of ``detector_type``

  :param detector_type: request detector (``dlib`` or ``full`` i.e. no detector)
  :type detector_type: string
  :return: detector
  """
  if detector_type == "dlib":
    return dlib_detector.DLibFaceDetector()
  elif detector_type == "full":
    return None
  else:
    raise ValueError("[{}: error] unknown 'detector' {}.".format("get_detector", detector_type))

def get_bbox_str(bbox):
  """Build bounding box string as left_top_right_bottom_score

  :param bbox: dictionary with keys: ``left``, ``top``, ``right``, ``bottom``, ``score``
  :type bbox: dict
  :return: bounding box string
  :rtype: string
  """
  #
  return "_".join(["{}"]*5).format(bbox["left"], bbox["top"], bbox["right"], bbox["bottom"],
                                   bbox["score"])
