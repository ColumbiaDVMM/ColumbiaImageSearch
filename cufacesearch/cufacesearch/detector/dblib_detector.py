from .generic_detector import GenericFaceDetector

class DLibFaceDetector(GenericFaceDetector):

  def __init__(self):
    import dlib
    self.detector = dlib.get_frontal_face_detector()

  # v1 without score
  # def detect_from_img(self, img, up_sample=1):
  #   return [{
  #     "left": d.left(),
  #     "top": d.top(),
  #     "right": d.right(),
  #     "bottom": d.bottom()
  #   } for d in self.detector(img, up_sample)]

  def detect_from_img(self, img, up_sample=1):
    return [{
      "left": d.left(),
      "top": d.top(),
      "right": d.right(),
      "bottom": d.bottom(),
      "score": score
    } for d, score, idx in self.detector.run(img, up_sample, 0)]
