
class DLibFeaturizer(object):

  def __init__(self, conf):
    #TODO read predictor_path, face_rec_model_path from conf
    import dlib
    self.sp = dlib.shape_predictor(predictor_path)
    self.facerec = dlib.face_recognition_model_v1(face_rec_model_path)

  def featurize(self, img, d):
    shape = self.sp(img, d)
    return self.facerec.compute_face_descriptor(img, shape)