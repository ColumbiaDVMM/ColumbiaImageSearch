def get_featurizer(featurizer_type, global_conf_filename):
  if featurizer_type == "dlib_featurizer":
    from dblib_featurizer import DLibFeaturizer
    return DLibFeaturizer(global_conf_filename)
  else:
    raise ValueError("[{}: error] unkown 'featurizer' {}.".format("get_featurizer", featurizer_type))

class GenericFeaturizer():

  def __init__(self, conf):
    pass

  def featurize(self, img, face_bbox):
    """" This method takes the image and face bounding box returned by and detector and generates a feature vector
    representing of the face. A 'featurizer' has usually been trained on some specific types of detection, so the
    same detector that has been used during training should be used here. """
    pass