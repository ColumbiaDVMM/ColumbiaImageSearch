import json
import sys
import traceback

class Updater():
    """ This class enables updating the index of available images.
    """

    def __init__(self,global_conf_filename):
        """ Creates an `Updater` based on the values in the json file `global_conf_file`.

        :param global_conf_file: path to json file with parameters configuration.
        """
        self.indexer = None
        self.global_conf_filename = global_conf_filename
        self.global_conf = json.load(open(self.global_conf_filename,'rt'))
        self.init_indexer()

    def init_indexer(self):
        """ Initialize `indexer` from `global_conf['UP_indexer']` value.

        Currently supported indexer types are:
        - hbase_indexer
        """
        field = 'UP_indexer'
        if field not in self.global_conf:
            raise ValueError("[Updater: error] "+field+" is not defined in configuration file.")
        elif self.global_conf[field]=="hbase_indexer":
            from ..indexer.hbase_indexer import HBaseIndexer
            self.indexer = HBaseIndexer(self.global_conf_filename)
        else:
            raise ValueError("[Updater: error] unkown 'indexer' {}.".format(self.global_conf[field]))

    def run_update(self):
        """ Runs an update.
        """
        try:
            # needs to read table 'escorts_images_updates', rows starting with 'index_update_' and not marked as indexed.
            update_id, list_sha1s = self.indexer.get_next_batch()
            if update_id:
                rows_batch = self.indexer.get_columns_from_sha1_rows(list_sha1s.split(','), columns=["info:s3_url"])
                # who marks the update as started?
                print rows_batch
                clean_batch = [(row[0], row[1]["info:s3_url"]) for row in rows_batch]
                self.indexer.index_batch_sha1(clean_batch)
            else:
                print("Nothing to update!")
            # when done mark update_id as processed.
        except Exception as inst:
            print "[Updater.run_udpate: error] {}".format(inst)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print "*** print_tb:"
            traceback.print_tb(exc_traceback, file=sys.stdout)
            print "*** print_exception:"
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
