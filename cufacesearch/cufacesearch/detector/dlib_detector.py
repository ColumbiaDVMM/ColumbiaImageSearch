"""
Face detection using DLib_.

.. _DLib: http://dlib.net/

"""

from ..detector.generic_detector import GenericDetector, DEFAULT_UPSAMPLING

class DLibFaceDetector(GenericDetector):
  """Face detector using the `DLib face detection model`_

  .. _`DLib face detection model`: http://dlib.net/python/index.html?highlight=get_frontal_face_detector#dlib.get_frontal_face_detector

  """

  def __init__(self):
    super(DLibFaceDetector, self).__init__()
    import dlib
    self.detector = dlib.get_frontal_face_detector()

  def detect_from_img(self, img, up_sample=DEFAULT_UPSAMPLING):
    """Detect face in ``img``

    :param img: image
    :type img: :class:`numpy.ndarray`
    :param up_sample: up sampling factor
    :type up_sample: int
    :return: list of detection dictionary with keys: ``left``, ``top``, ``right``, ``bottom``,
     ``score``
    :rtype: list
    """
    dets, scores, _ = self.detector.run(img, up_sample, 0)
    return [{"left": d.left(), "top": d.top(), "right": d.right(), "bottom": d.bottom(),
             "score": scores[i]} for i, d in enumerate(dets)]
