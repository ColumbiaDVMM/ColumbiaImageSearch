import json

class GenericIngester():
    """ Generic class defining an ingester object.
    """

    def __init__(self,global_conf):
        self.start = None
        self.batch_size = None
        self.source = None
        self.fail_less_than_batch = True
        self.verbose = 0
        self.global_conf = json.load(open(global_conf,'rt'))
        self.read_conf()
        self.initialize_source()
        
    def read_conf(self):
        if "verbose" in self.global_conf:
            self.verbose = self.global_conf["verbose"]
        if "ingester_verbose" in self.global_conf:
            self.verbose = self.global_conf["ingester_verbose"]
        if "fail_less_than_batch" in self.global_conf:
            self.fail_less_than_batch = self.global_conf["fail_less_than_batch"]
        if "batch_size" in self.global_conf:
            self.batch_size = self.global_conf["batch_size"]

    def initialize_source(self):
        """ Use information contained in `self.global_conf` to initialize `self.source`
        """
        pass

    def set_verbose(self,verbose):
        """ Set verbose level. 

        :param verbose: verbose level (default: 0 for silent)
        :type verbose: integer
        """
        self.verbose = verbose

    def set_fail_less_than_batch(self,fail_less_than_batch):
        """ Set boolean `fail_less_than_batch` to define what to do when less than batch_size images are retrieved.

        :param fail_less_than_batch: determine if an ingester will fail if less than `batch_size` samples can be retrieved (default: True)
        :type fail_less_than_batch: boolean
        """
        self.fail_less_than_batch = fail_less_than_batch

    def set_start(self,start):
        """ Defines `start` value.
        """
        self.start = start

    def set_batch_size(self,batch_size):
        """ Defines batch_size i.e. number of samples retrieve by a `get_batch` call.
        """
        self.batch_size = batch_size

    def get_batch(self):
        """ Should return a list of (id,url,other_data) querying for `batch_size` samples from `self.source` from `start`
        """
        return [(None,None,None)]
