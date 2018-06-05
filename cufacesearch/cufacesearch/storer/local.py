import os
import glob
import cPickle as pickle
from cufacesearch.storer.generic_storer import GenericStorer
from cufacesearch.common.dl import mkpath

default_prefix = "LOCALST_"

class LocalStorer(GenericStorer):
  """LocalStorer class
  """

  def __init__(self, global_conf_in, prefix=default_prefix):
    """LocalStorer constructor

    :param global_conf_in: configuration file or dictionary
    :type global_conf_in: str, dict
    :param prefix: prefix in configuration
    :type prefix: str
    """
    super(LocalStorer, self).__init__(global_conf_in, prefix)
    self.set_pp(pp="LocalStorer")
    self.base_path = self.get_required_param('base_path')
    # Be sure base_path ends by "/" for mkpath
    if self.base_path[-1] != "/":
      self.base_path = self.base_path+"/"
    self.setup()

  def setup(self):
    """Setup LocalStorer
    """
    # create base path dir
    mkpath(self.base_path)
    if self.verbose > 0:
      print("[{}: log] Initialized with base_path '{}'".format(self.pp, self.base_path))

  def get_full_path(self, key):
    """Get full path for location ``key``

    :param key: location
    :type key: str
    :return: full path
    :rtype: str
    """
    return os.path.join(self.base_path, key)

  def save(self, key, obj):
    """Save object ``obj`` at location ``key``

    :param key: location to save
    :type key: str
    :param obj: object to save, will be pickled.
    :type obj: object
    """
    # Pickle and save to disk
    full_path = self.get_full_path(key)
    mkpath(full_path)
    pickle.dump(obj, open(full_path, 'wb'))
    if self.verbose > 1:
      print "[{}: log] Saved file: {}".format(self.pp, full_path)

  def load(self, key, silent=False):
    """Load from location ``key``

    :param key: location
    :type key: str
    :param silent: whether load fails silently
    :type silent: bool
    :return: loaded object
    :rtype: Object
    """
    # Load a pickle object from disk
    try:
      full_path = self.get_full_path(key)
      obj = pickle.load(open(full_path, 'rb'))
      if self.verbose > 1:
        print("[{}: log] Loaded file: {}".format(self.pp, full_path))
      return obj
    except Exception as e:
      if self.verbose > 0 and not silent:
        err_msg = "[{}: error ({}: {})] Could not load object from path: {}"
        print(err_msg.format(self.pp, type(e), e, full_path))


  def list_prefix(self, prefix_path):
    """List all files in ``prefix_path``

    :param prefix_path: prefix path
    :type prefix_path: str
    :yield: one file path
    """
    for filepath in glob.glob(self.get_full_path(prefix_path)+"*"):
      yield filepath

  # This would be used to load all codes
  def get_all_from_prefix(self, prefix_path):
    """Get all objects in ``prefix_path``

    :param prefix_path: prefix path
    :type prefix_path: str
    :yield: object
    """
    for filepath in self.list_prefix(prefix_path):
      obj = self.load(filepath)
      if obj:
        yield obj

if __name__ == "__main__":
  local_conf = {"base_path": "./store/", "verbose": 2}
  lst = LocalStorer(local_conf, prefix="")