import json
from datetime import datetime

class GenericIndexer():
    """ Generic class defining an indexer object.
    """

    def __init__(self, global_conf_filename):
        self.global_conf_filename = global_conf_filename
        self.global_conf = json.load(open(global_conf_filename,'rt'))
        self.read_conf()


    def read_conf(self):
        pass


    def index_batch(self,batch):
        """ Should index a batch in the form of a list of (id,url,other_data)
        """
        pass 
