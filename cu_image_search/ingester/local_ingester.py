from generic_ingester import GenericIngester

class LocalIngester(GenericIngester):

  def initialize_source(self):
    """ Use information contained in `self.global_conf` to initialize `self.source`
    """
    self.data_dir = self.global_conf['LI_in_data_dir']
    self.allowed_patterns = self.global_conf['LI_allowed_patterns']
    self.list_files = []
    self.filled_list = False

  def fill_list_files(self):
    import os
    import fnmatch

    print "[LocalIngester.fill_list_files: log] Looking for files in {}".format(self.data_dir)

    for base, dirs, files in os.walk(self.data_dir):
      validated_files = []
      for pattern in self.allowed_patterns:
        validated_files.extend(fnmatch.filter(files, pattern))
      self.list_files.extend([os.path.join(base, f) for f in validated_files])

    self.list_files.sort(key=os.path.getmtime)
    self.filled_list = True

    print "[LocalIngester.fill_list_files: log] Found {} files".format(len(self.list_files))


  def get_batch(self):
    """ Should return a list of (id, path, path) querying for `batch_size` samples from `self.source` from `start`

    Build batch based on start and creation time of files.
    """
    if not self.filled_list:
      self.fill_list_files()


    return [(idx, self.list_files[idx], self.list_files[idx]) for idx in range(self.start, min(self.start+self.batch_size, len(self.list_files)))]
