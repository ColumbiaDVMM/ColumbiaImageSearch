import json

class ConfReader(object):
  """Common class to read parameters from a configuration file."""

  def __init__(self, global_conf_in, prefix=""):
    """ Initialize class to read parameters from a configuration file.

    :param global_conf_in: configuration parameters file or dictionary.
    :param prefix: prefix to prepend to get parameters of the current class.
    """
    self.pp = None
    self.set_pp()
    self.verbose = 0
    if type(global_conf_in)==dict:
      print '[{}.init: info] got dictionary configuration.'.format(self.pp)
      self.global_conf = global_conf_in
    else:
      print '[{}.init: info] reading configuration from file: {}'.format(self.pp, global_conf_in)
      self.global_conf = json.load(open(global_conf_in, 'rt'))
    self.prefix = prefix
    self.read_conf()

  def set_pp(self):
    """ Sets pretty print name 'self.pp'.

    Should be overridden in any child class.
    """
    self.pp = "ConfReader"

  def read_conf(self):
    """ Read generic parameters from configuration file.

    Currently, just reads 'verbose'. Can be overriden in any child class to get specific parameters.
    """
    # read some generic parameters
    verbose = self.get_param('verbose')
    if verbose:
      self.verbose = int(verbose)

  def get_param(self, param):
    """ Read parameter 'param' from configuration file.

    :param param: name of parameter to read (without prefix).
    :return: parameter 'param' value (None if not found)
    """
    key_param = self.prefix + param
    if key_param in self.global_conf:
      if self.verbose > 1:
        found_msg = '[{}.get_param: info] found {} with value {} in configuration'
        print found_msg.format(self.pp, key_param, self.global_conf[key_param])
      return self.global_conf[key_param]
    if self.verbose > 0:
      print '[{}.get_param: info] could not find {} in configuration'.format(self.pp, key_param)

  def get_required_param(self, param):
    """ Read required parameter 'param' from configuration file.

    :param param: name of parameter to read (without prefix).
    :return: parameter 'param' value
    :raise ValueError: if parameter 'param' (with prefix appended) cannot be find.
    """
    param_value = self.get_param(param)
    if param_value is None:
      msg = '[{}.get_required_param: error] {} not defined in configuration'.format(self.pp, param)
      print msg
      raise ValueError(msg)
    return param_value
