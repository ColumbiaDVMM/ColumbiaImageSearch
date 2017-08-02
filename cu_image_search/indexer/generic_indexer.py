import json
from datetime import datetime


class GenericIndexer():
    """ Generic class defining an indexer object.
    """

    def __init__(self,global_conf_filename):
        self.backend = None
        self.verbose = 0
        self.initializing = True
        self.last_refresh = datetime.now()
        self.global_conf_filename = global_conf_filename
        self.global_conf = json.load(open(global_conf_filename,'rt'))
        self.read_conf()
        self.initialize_indexer_backend()
        if "verbose" in self.global_conf:
            self.verbose = self.global_conf["verbose"]
        if "indexer_verbose" in self.global_conf:
            self.verbose = self.global_conf["indexer_verbose"]


    def read_conf(self):
        pass


    def initialize_indexer_backend(self):
        """ Use information contained in `self.global_conf` to initialize `self.backend`
        """
        pass


    def is_indexed(self,sha1):
        # query index with single SHA1
        pass


    def are_indexed(self,sha1_list):
        # query index with list of SHA1
        pass


    def get_feature(self,sha1):
        # get one feature
        pass


    def get_hashcode(self,sha1):
        # get one hashcode
        pass


    def set_verbose(self,verbose):
        """ Set verbose level. 
        """
        self.verbose = verbose


    def get_next_batch_start(self):
        """ Returns `start` value for next update.
        """
        return None


    def index_batch(self,batch):
        """ Should index a batch in the form of a list of (id,url,other_data)
        """
        pass 
