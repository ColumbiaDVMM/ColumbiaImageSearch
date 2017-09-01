from ..common.conf_reader import ConfReader

test_list_sha1 = "200000115DD121F32E87C116C243166C08F99984,10000007CA53847D9D9D124305ADB144B6CC5E2E,000001EC5DD154E58B72326EFC26A41A4C8E9586,000001BF13372B9665A89ED25E8948FC7F99F7F1,000000D29139258BD3716C94A68CFF54A8A7C033,0000007031E3FA80C97940017253BEAB542EA334,000000A5DF1457034A05D8F3458695AE66AA9E77,000000A7CAD184BD6BC00DAF032E5F7C818C39D0"

def get_featurizer(featurizer_type, global_conf):
  #if featurizer_type == "dlib_featurizer":
  # This only a face featurizer... Should we name it "dlib_face_dlib_feat" as it should be run only on a dlib_detected_face
  #if featurizer_type == "dlib_face_dlib_feat":
  if featurizer_type == "dlib":
    from dblib_featurizer import DLibFeaturizer
    return DLibFeaturizer(global_conf)
  else:
    raise ValueError("[{}:error] Unknown 'featurizer' {}.".format("get_featurizer", featurizer_type))

class GenericFeaturizer(ConfReader):

  def __init__(self, global_conf_in, prefix=""):
    super(GenericFeaturizer, self).__init__(global_conf_in, prefix)

  def featurize(self, img, bbox=None, img_type="scikit"):
    """" This method takes an image (by default considering it was loaded from scikit...)
    and optionally an object (e.g. face) bounding box returned by a detector.
    It generates a feature vector representing of the image or object.
    An object or face 'featurizer' has usually been trained on some specific type of detection, so the
    same detector that has been used during training should be used to generate the bounding box passed here. """
    raise NotImplementedError("[{}:error] 'featurize' method was not overridden.".format(self.pp))