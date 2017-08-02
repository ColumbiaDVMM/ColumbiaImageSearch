import json
import sys
import traceback

class Updater():
    """ This class enables updating the index of available images, 
    getting new images from an ingester and pushing them to an indexer.
    """

    def __init__(self,global_conf_filename):
        """ Creates an `Updater` based on the values in the json file `global_conf_file`.

        :param global_conf_file: path to json file with parameters configuration.
        """
        self.ingester = None
        self.indexer = None
        self.has_indexed = False
        self.global_conf_filename = global_conf_filename
        self.global_conf = json.load(open(self.global_conf_filename,'rt'))
        self.init_ingester()
        self.init_indexer()
        self.refresh_URL = 'http://127.0.0.1:5000/cu_image_search/refresh'

    def init_ingester(self):
        """ Initialize `ingester` from `global_conf['ingester']` value.

        Currently supported ingester types are:
        - mysql_ingester
        - cdr_ingester
        """
        field = 'UP_ingester'
        if field not in self.global_conf:
            raise ValueError("[Updater: error] "+field+" is not defined in configuration file.")
        if self.global_conf[field]=="mysql_ingester":
            from ..ingester.mysql_ingester import MySQLIngester
            self.ingester = MySQLIngester(self.global_conf_filename)
        elif self.global_conf[field]=="cdr_ingester":
            from ..ingester.cdr_ingester import CDRIngester
            self.ingester = CDRIngester(self.global_conf_filename)
        elif self.global_conf[field] == "local_ingester":
            from ..ingester.local_ingester import LocalIngester
            self.ingester = LocalIngester(self.global_conf_filename)
        elif self.global_conf[field]=="hbase_ingester":
            from ..ingester.hbase_ingester import HBaseIngester
            self.ingester = HBaseIngester(self.global_conf_filename)
        else:
            raise ValueError("[Updater: error] unkown 'ingester' {}.".format(self.global_conf[field]))

    def init_indexer(self):
        """ Initialize `indexer` from `global_conf['UP_indexer']` value.

        Currently supported indexer types are:
        - local_indexer
        - hbase_indexer
        """
        field = 'UP_indexer'
        if field not in self.global_conf:
            raise ValueError("[Updater: error] "+field+" is not defined in configuration file.")
        if self.global_conf[field]=="local_indexer":
            from ..indexer.local_indexer import LocalIndexer
            self.indexer = LocalIndexer(self.global_conf_filename)
        elif self.global_conf[field]=="hbase_indexer":
            from ..indexer.hbase_indexer import HBaseIndexer
            self.indexer = HBaseIndexer(self.global_conf_filename)
        else:
            raise ValueError("[Updater: error] unkown 'indexer' {}.".format(self.global_conf[field]))

    def refresh_API(self):
        """ Forces a refresh of the API index.
        """
        import requests
        r = requests.get(self.refresh_URL)
        if r.status_code == 200:
            print("[Updater: refresh_API] refresh call return: {}".format(r.content))
        return True

    def run_update(self):
        """ Runs an update.
        """
        try:
            self.has_indexed = False
            start = self.indexer.get_next_batch_start()
            print "[run_update: log] Set start to {}".format(start)
            sys.stdout.flush()
            self.ingester.set_start(start)
            self.ingester.set_fail_less_than_batch(False)
            batch = self.ingester.get_batch()
            print "[run_update: log] Got batch of length {}".format(len(batch))
            sys.stdout.flush()
            if len(batch)>0:
                #print batch
                batch_indexed = self.indexer.index_batch(batch)
                print "[run_update: log] Indexed batch of length {}".format(len(batch))
                if batch_indexed:
                    self.has_indexed = True
                sys.stdout.flush()
        except Exception as inst:
            print "[Updater.run_udpate: error] {}".format(inst)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print "*** print_tb:"
            traceback.print_tb(exc_traceback, file=sys.stdout)
            print "*** print_exception:"
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            sys.stdout.flush()
