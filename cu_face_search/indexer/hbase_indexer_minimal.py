import os
import sys
import time
import base64
import shutil
import happybase
import numpy as np
from datetime import datetime
from socket import timeout

TTransportException = happybase._thriftpy.transport.TTransportException
TException = happybase._thriftpy.thrift.TException
max_errors = 5
# reading a lot of data from HBase at once can be unstable
batch_size = 100 

class HBaseIndexerMinimal(object):

    def __init__(self, global_conf_filename, prefix="HBFI_"):
        self.verbose = 0
        self.prefix = prefix
        self.last_refresh = datetime.now()
        self.global_conf_filename = global_conf_filename
        self.global_conf = json.load(open(global_conf_filename,'rt'))
        self.read_conf()
        
    def get_param(self, param):
        key_param = self.prefix+param
        if key_param in self.global_conf:
            return self.global_conf[key_param]
        if self.verbose:
            print '[get_param: info] could not find {} in configuration'.format(key_param)


    def read_conf(self):
        """ Reads configuration parameters.

        Will read parameters 'host', 'table_sha1infos'...
        from self.global_conf.
        """
        # HBase conf
        self.hbase_host = self.get_param('host')
        self.table_sha1infos_name = self.get_param('table_sha1infos')        
        self.nb_threads = 1
        param_nb_threads = self.get_param('pool_thread')
        if param_nb_threads:
            self.nb_threads = param_nb_threads
        self.pool = happybase.ConnectionPool(size=self.nb_threads, host=self.hbase_host)

        # # Extractions configuration (TO BE IMPLEMENTED)
        # self.extractions_types = self.get_param('extractions_types')
        # self.extractions_columns = self.get_param('extractions_columns')
        # if len(self.extractions_columns) != len(self.extractions_types):
        #     raise ValueError("[HBaseIndexerMinimal.read_conf: error] Dimensions mismatch {} vs. {} for extractions_columns vs. extractions_types".format(len(self.extractions_columns),len(self.extractions_types)))



    def refresh_hbase_conn(self, calling_function, sleep_time=0):
        # this can take 4 seconds...
        start_refresh = time.time()
        dt_iso = datetime.utcnow().isoformat()
        print("[HBaseIndexerMinimal.{}: {}] caught timeout error or TTransportException. Trying to refresh connection pool.".format(calling_function, dt_iso))
        time.sleep(sleep_time)
        self.pool = happybase.ConnectionPool(size=self.nb_threads, host=self.hbase_host)
        print("[HBaseIndexerMinimal.refresh_hbase_conn: log] Refreshed connection pool in {}s.".format(time.time()-start_refresh))


    def check_errors(self, previous_err, function_name, inst=None):
        if previous_err >= max_errors:
            raise Exception("[HBaseIndexerMinimal: error] function {} reached maximum number of error {}. Error was: {}".format(function_name, max_errors, inst))
        return None


    def get_rows_by_batch(self, list_queries, table_name, columns=None, previous_err=0, inst=None):
        self.check_errors(previous_err, "get_rows_by_batch", inst)
        try:
            with self.pool.connection() as connection:
                hbase_table = connection.table(table_name)
                # slice list_queries in batches of batch_size to query
                rows = []
                nb_batch = 0
                for batch_start in range(0,len(list_queries), batch_size):
                    batch_list_queries = list_queries[batch_start:min(batch_start+batch_size,len(list_queries))]
                    rows.extend(hbase_table.rows(batch_list_queries, columns=columns))
                    nb_batch += 1
                if self.verbose:
                    print("[get_rows_by_batch: log] got {} rows using {} batches.".format(len(rows), nb_batch))
                return rows
        except Exception as inst:
            # try to force longer sleep time...
            self.refresh_hbase_conn("get_rows_by_batch", sleep_time=4)
            return self.get_rows_by_batch(list_queries, table_name, columns, previous_err+1, inst)


    def get_columns_from_sha1_rows(self, list_sha1s, columns, previous_err=0, inst=None):
        rows = None
        self.check_errors(previous_err, "get_columns_from_sha1_rows", inst)
        if list_sha1s:
            try:
                rows = self.get_rows_by_batch(list_sha1s, self.table_sha1infos_name, columns=columns)
            except Exception as inst: # try to catch any exception
                print "[get_columns_from_sha1_rows: error] {}".format(inst)
                self.refresh_hbase_conn("get_columns_from_sha1_rows")
                return self.get_columns_from_sha1_rows(list_sha1s, columns, previous_err+1, inst)
        return rows


    # # Something like this could be used to get precomptued face features
    # # But we should get a JSON listing all faces found features and parse it...
    # # (TO BE IMPLEMENTED)
    # def get_precomp_from_sha1(self, list_sha1s, list_type):
    #     """ Retrieves the 'list_type' extractions results from HBase for the image in 'list_sha1s'.

    #     :param list list_sha1s: list of sha1s of the images for which the extractions are requested.
    #     :param list list_type: list of the extractions requested. They have to be a subset of *self.extractions_types*
    #     :returns (list, list) (res, ok_ids): *res* contains the extractions, *ok_ids* the ids of the 'list_sha1s' for which we retrieved something.
    #     """
    #     res = []
    #     ok_ids = []
    #     print("[get_precomp_from_sha1] list_sha1s: {}.".format(list_sha1s))
    #     rows = self.get_full_sha1_rows(list_sha1s)
    #     # check if we have retrieved rows and extractions for each sha1
    #     retrieved_sha1s = [row[0] for row in rows]
    #     print("[get_precomp_from_sha1] retrieved_sha1s: {}.".format(list_sha1s))
    #     # building a list of ok_ids and res for each extraction type
    #     ok_ids = [[] for i in range(len(list_type))]
    #     res = [[] for i in range(len(list_type))]
    #     list_columns = self.get_columns_name(list_type)
    #     print("[get_precomp_from_sha1] list_columns: {}.".format(list_columns))
    #     for i,sha1 in enumerate(retrieved_sha1s):
    #         for e in range(len(list_type)):
    #             if list_columns[e] in rows[i][1]:
    #                 print("[get_precomp_from_sha1] {} {} {} {}.".format(i,sha1,e,list_columns[e]))
    #                 ok_ids[e].append(list_sha1s.index(sha1))
    #                 res[e].append(np.frombuffer(base64.b64decode(rows[i][1][list_columns[e]]), np.float32))
    #                 #res[e].append(rows[i][1][list_columns[e]])
    #     return res, ok_ids


    # def get_columns_name(self, list_type):
    #     list_columns = []
    #     if self.extractions_types:
    #         for e, extr in enumerate(list_type):
    #             if extr not in self.extractions_types:
    #                 raise ValueError("[HBaseIndexerMinimal.get_columns_name: error] Unknown extraction type \"{}\".".format(extr))
    #             pos = self.extractions_types.index(extr)
    #             list_columns.append(self.extractions_columns[pos])
    #     else:
    #         raise ValueError("[HBaseIndexerMinimal.get_columns_name: error] extractions_types were not loaded")
    #     return list_columns
