from cufacesearch.common.conf_reader import ConfReader

default_prefix = "ST_"

def get_storer(storer_type, global_conf_in, prefix):
  if storer_type == "local":
    from local import LocalStorer
    return LocalStorer(global_conf_in, prefix)
  elif storer_type == "s3":
    from s3 import S3Storer
    return S3Storer(global_conf_in, prefix)
  else:
    raise ValueError("[{}: error] unknown 'storer' {}.".format("get_storer", storer_type))

class GenericStorer(ConfReader):

  def __init__(self, global_conf_in, prefix=default_prefix):
    super(GenericStorer, self).__init__(global_conf_in, prefix)
    self.set_pp()

  def set_pp(self):
    self.pp = "GenericStorer"

  def setup(self):
    pass

  # Storer need to implement these methods
  def save(self, key, obj):
    raise NotImplementedError()

  def load(self, key):
    raise NotImplementedError()

  # This would be used to load all codes
  def get_all_from_prefix(self, prefix_path):
    raise NotImplementedError()

  # Should we add a get_latest to get the latest model ?