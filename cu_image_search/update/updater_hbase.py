import json
import sys
import traceback
import requests

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
        self.has_indexed = False
        # could/should be loaded from conf
        self.refresh_URL = 'http://127.0.0.1:5000/cu_image_search/refresh'

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


    def refresh_API(self):
        """ Forces a refresh of the API index.
        """
        r = requests.get(self.refresh_URL)
        if r.status_code == 200:
            print("[Updater: refresh_API] refresh call return: {}".format(r.content))
        return True


    def run_update(self):
        """ Runs an update.
        """
        try:
            # needs to read table 'escorts_images_updates', rows starting with 'index_update_' and not marked as indexed.
            update_id, str_list_sha1s = self.indexer.get_next_batch()
            if update_id:
                list_sha1s = str_list_sha1s.split(',')
                print("[Updater.run_update: log] Update {} has {} images.".format(update_id, len(list_sha1s)))
                # also get 'info:image' 'info:featnorm_cu', and 'info:hash256_cu' or all self.extractions_columns
                rows_batch = self.indexer.get_columns_from_sha1_rows(list_sha1s, columns=["info:s3_url"])
                # deal with precomputed features (and hashcodes)

                if rows_batch:
                    clean_batch = [(row[0], row[1]["info:s3_url"]) for row in rows_batch]
                    batch_indexed = self.indexer.index_batch_sha1(clean_batch, update_id)
                    if batch_indexed:
                        self.has_indexed = True
                else:
                    print("[Updater.run_update: log] Did not get any urls for this update ({}) images.".format(update_id))
                    print("[Updater.run_update: log] We were looking for the images urls in table {}.".format(self.indexer.table_sha1infos_name))
            else:
                print("[Updater.run_update: log] Nothing to update!")
        except Exception as inst:
            print "[Updater.run_udpate: error] {}".format(inst)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print "*** print_tb:"
            traceback.print_tb(exc_traceback, file=sys.stdout)
            print "*** print_exception:"
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
