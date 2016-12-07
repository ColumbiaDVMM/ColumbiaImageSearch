import os
import sys
import time
import json
import happybase
from datetime import datetime
from socket import timeout

from generic_indexer import GenericIndexer

TTransportException = happybase._thriftpy.transport.TTransportException
max_errors = 5
batch_size = 100
# just so that we don't read all batches before actually doing anything
max_index_batches = 10

class HBaseIndexer(GenericIndexer):


    def read_conf(self):
        """ Reads configuration parameters.

        Will read from self.global_conf parameters:
        - 'HBI_host'
        - 'HBI_table_sha1infos'
        - 'HBI_table_updatesinfos'
        - 'HBI_pool_thread'
        """
        # HBase host and infos
        self.hbase_host = self.global_conf['HBI_host']
        self.table_sha1infos_name = self.global_conf['HBI_table_sha1infos']
        self.table_updateinfos_name = self.global_conf['HBI_table_updatesinfos']
        # HBase connection pool
        self.nb_threads = 1
        if 'HBI_pool_thread' in self.global_conf:
            self.nb_threads = self.global_conf['HBI_pool_thread']
        self.pool = happybase.ConnectionPool(size=self.nb_threads,host=self.hbase_host)
        # Indexing attributes
        self.cu_feat_id_column = "info:cu_feat_id" # could be loaded from conf
        self.index_batches = []
        self.last_batch = None


    def refresh_hbase_conn(self, calling_function, sleep_time=2):
        dt_iso = datetime.utcnow().isoformat()
        print_err = "[HBaseIndexer.{}: {}] caught timeout error or TTransportException. Trying to refresh connection pool."
        print(print_err.format(calling_function, dt_iso))
        time.sleep(sleep_time)
        self.pool = happybase.ConnectionPool(size=self.nb_threads,host=self.hbase_host)


    def check_errors(self, previous_err, function_name, inst=None):
        if previous_err >= max_errors:
            dt_iso = datetime.utcnow().isoformat()
            print_err = "[HBaseIndexer: error at {}] function {} reached maximum number of error {}. Error was: {}"
            raise Exception(print_err.format(dt_iso, function_name, max_errors, inst))
        return None


    def get_rows_by_batch(self, list_queries, table_name, columns=None, previous_err=0, inst=None):
        # still get timeout, TTransportException and IOError issues
        self.check_errors(previous_err, "get_rows_by_batch", inst)
        try:
            with self.pool.connection() as connection:
                hbase_table = connection.table(table_name)
                # slice list_queries in batches of batch_size to query
                rows = []
                nb_batch = 0
                for batch_start in range(0,len(list_queries),batch_size):
                    batch_list_queries = list_queries[batch_start:min(batch_start+batch_size,len(list_queries))]
                    rows.extend(hbase_table.rows(batch_list_queries, columns=columns))
                    nb_batch += 1
                print("[get_rows_by_batch] got {} rows using {} batches.".format(len(rows), nb_batch))
                return rows
        except (timeout or TTransportException or IOError) as inst:
            # try to force longer sleep time...
            self.refresh_hbase_conn("get_rows_by_batch", sleep_time=4)
            return self.get_rows_by_batch(list_queries, table_name, columns, previous_err+1, inst)


    def get_full_sha1_rows(self, list_sha1s):
        rows = None
        if list_sha1s:
            return self.get_rows_by_batch(list_sha1s, self.table_sha1infos_name)
        return rows


    def get_columns_from_sha1_rows(self, list_sha1s, columns):
        rows = None
        if list_sha1s:
            return self.get_rows_by_batch(list_sha1s, self.table_sha1infos_name, columns=columns)
        return rows

        
    def write_batch(self, batch, tab_out_name, previous_err=0, inst=None):
        self.check_errors(previous_err, "write_batch", inst)
        try:
            # batch is composed of tuples (row_key, dict())
            # where entries in the dict are key-value pairs as {'column_name': column_value}
            with self.pool.connection() as connection:
                tab_out = connection.table(tab_out_name)
                # this will write out every batch_size automatically
                batch_write = tab_out.batch(batch_size=batch_size)
                for row in batch:
                    batch_write.put(row[0], row[1])
                # send last batch
                batch_write.send()
        except (timeout or TTransportException or IOError) as inst:
            self.refresh_hbase_conn("write_batch")
            return self.write_batch(batch, tab_out_name, previous_err+1, inst)


    def push_cu_feats_id(self, rows_cu_feat_ids):
        batch = []
        for sha1, cu_feat_id_dict in rows_cu_feat_ids:
            batch.append((sha1, cu_feat_id_dict))
        print batch[0]
        print batch[-1]
        self.write_batch(batch, self.table_sha1infos_name)
        

    def finalize_batch_indexing(self, batch, update_id):
        rows_cu_feat_ids = self.get_columns_from_sha1_rows(batch, [self.cu_feat_id_column])
        self.push_cu_feats_id(rows_cu_feat_ids)


    def index_batch_sha1(self, batch, update_id):
        """ Index an update batch in the form of a list of (sha1, url)
        """
        # Download images
        start_time = time.time() 
        # mark update as started (actually single write)
        self.write_batch([(update_id, {'info:started': 'True'})], self.table_updateinfos_name)
        print "[HBaseIndexer.index_batch_sha1: log] Starting udpate {}".format(update_id)
        # we would actually do something with this batch (compute features/hashcodes etc)
        self.finalize_batch_indexing(batch, update_id)
        print "[HBaseIndexer.index_batch: log] Indexed batch in {}s.".format(time.time()-start_time)
        # mark update as completed (actually single write)
        self.write_batch([(update_id, {'info:indexed': 'True'})], self.table_updateinfos_name)
        return True


    def get_next_batch(self, only_not_indexed=False):
        """ Get next update batch. 

        :returns (update_id, list_sha1s): returns a batch to be indexed.
        """
        try:
            if not self.index_batches:
                with self.pool.connection() as connection:
                    table_updateinfos = connection.table(self.table_updateinfos_name)
                    # need to specify batch size to avoid timeout
                    start_row = 'index_update_'
                    if self.last_batch:
                        start_row = self.last_batch
                    for row in table_updateinfos.scan(row_start=start_row, row_stop='index_update_~', batch_size=batch_size):
                        # always push batch for test, we will basically just rewrite the same data
                        self.index_batches.append((row[0], row[1]["info:list_sha1s"]))
                        # just get a few batches at once
                        if len(self.index_batches) >= max_index_batches:
                            print("We got {} batches, moving on.".format(max_index_batches))
                            break
            if self.index_batches:
                batch = self.index_batches.pop()
                self.last_batch = batch[0]
            else:
                batch = (None, None)
        except timeout as inst:
            self.refresh_hbase_conn("get_next_batch", sleep_time=4)
            return self.get_next_batch()
        return batch

