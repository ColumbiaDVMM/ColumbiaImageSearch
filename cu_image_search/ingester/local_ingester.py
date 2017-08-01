from generic_ingester import GenericIngester

class LocalIngester(GenericIngester):

  def initialize_source(self):
    """ Use information contained in `self.global_conf` to initialize `self.source`
    """
    self.data_dir = self.global_conf['LI_in_data_dir']

  def get_batch(self):
    """ Should return a list of (id, path, None) querying for `batch_size` samples from `self.source` from `start`

    Build batch based on start and creation time of files.
    """
    import glob
    import os

    files = glob.glob(self.data_dir)
    files.sort(key=os.path.getmtime)

    return [(idx, files[idx], None) for idx in range(self.start, self.start+self.batch_size)]
