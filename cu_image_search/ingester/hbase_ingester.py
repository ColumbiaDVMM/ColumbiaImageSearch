import os
import sys
import time
import datetime
import happybase
from generic_ingester import GenericIngester

class HBaseIngester(GenericIngester):

    def initialize_source(self):
        """ Use information contained in `self.global_conf` to initialize HBase config.

        Parameters used are: 
        - HBI_host, HBI_table_timestamp, HBI_table_cdrinfos, HBI_table_sha1infos, HBI_extractions_types
        HBI_extractions_columns, HBI_in_url_column, HBI_sha1_column
        """
        self.hbase_host = self.global_conf['HBI_host']
        self.table_timestamp_name = self.global_conf['HBI_table_timestamp']
        self.table_cdrinfos_name = self.global_conf['HBI_table_cdrinfos']
        self.table_sha1infos_name = self.global_conf['HBI_table_sha1infos']
        self.extractions_types = self.global_conf['HBI_extractions_types']
        self.extractions_columns = self.global_conf['HBI_extractions_columns']
        self.in_url_column = self.global_conf['HBI_in_url_column']
        self.sha1_column = self.global_conf['HBI_sha1_column']
        if len(self.extractions_columns) != len(self.extractions_types):
            raise ValueError("[HBaseIngester.initialize_source: error] Dimensions mismatch {} vs. {} for extractions_columns vs. extractions_types".format(len(self.extractions_columns),len(self.extractions_types)))
        self.nb_threads = 2
        if 'HBI_pool_thread' in self.global_conf:
            self.nb_threads = self.global_conf['HBI_pool_thread']
        self.pool = happybase.ConnectionPool(size=self.nb_threads,host=self.hbase_host)

    def get_cdr_ids_from_tscdrids(self,ts_cdr_ids):
        """ Get list of cdr_ids from list of ts_cdr_ids.  

        :param ts_cdr_ids: list of ts_cdr_ids from which the cdr ids should be extracted.
        :type ts_cdr_ids: list
        :return: return list of cdr ids
        :rtype: list
        """
        return [ts_cdr_id.split("_")[1] for ts_cdr_id in ts_cdr_ids]

    def get_cdr_ids_indexed(self,cdr_ids):
        """ Look for cdr_ids in 'table_cdrinfos_name'.

        :param cdr_ids: list of cdr ids
        :type cdr_ids: list
        :return: return boolean list 'indexed', and list of existing sha1 'sha1s'
        :rtype: list, list
        """
        # initialize indexed and sha1s
        indexed = [None]*len(cdr_ids)
        sha1s = [None]*len(cdr_ids)
        # look for cdr ids
        if cdr_ids:
            with self.pool.connection() as connection:
                # check existing
                table_cdrinfos = connection.table(self.table_cdrinfos_name)
                existing_rows = table_cdrinfos.rows(cdr_ids)
                for row in existing_rows:
                    indexed[cdr_ids.index(row[0])] = True
                    # check sha1
                    if self.sha1_column in row[1]:
                        sha1s[cdr_ids.index(row[0])] = row[1][self.sha1_column]
        return indexed, sha1s


    def fill_images_infos(self,new_rows,cdr_ids,extractions,images_infos):
        """ Fill the list of images to be indexed. 
        Only up to 'self.batch_size'.

        :param new_rows: list of rows 'table_timestamp'
        :type new_rows: list
        :param cdr_ids: list of cdr ids corresponding to rows
        :type cdr_ids: list
        :param extractions: list of extractions to be applied for each image
        :type extractions: list
        :param images_infos: current 'images_infos' list as tuples of (cdr_id,url,[extractions_needed,ts_cdrid_row_key,other_data])
        :type images_infos: list
        :return: return update 'images_infos'
        :rtype: list
        """
        if new_rows:
            # only insert up to batch_size
            nb_ins = min(self.batch_size-len(images_infos),len(new_rows))
            for i in range(nb_ins):
                images_infos.append((cdr_ids[i],new_rows[i][1][self.in_url_column],[extractions[i],new_rows[i][0],new_rows[i][1]]))
        return images_infos

    def check_extractions_rows(self,candidate_rows,sha1s):
        """ Check if all extractions have been applied to candidate_rows.
        Returns the rows and corresponding extractions that were not yet applied.
        """
        new_rows = []
        new_extractions = []
        with self.pool.connection() as connection:
            table_sha1infos = connection.table(self.table_sha1infos_name)
            candidate_rows_sha1s = table_sha1infos.rows(sha1s)
        found_candidates = [sha1s.index(crs_row[0]) for crs_row in candidate_rows_sha1s]
        # what to do with images with no rows found in table_sha1infos?
        # it would mean they are currently being updated?
        for i,row in enumerate(candidate_rows_sha1s):
            tmp_new_extractions = []
            for i,extr in enumerate(self.extractions_columns):
                if extr not in row[1]:
                    tmp_new_extractions.append(self.extractions_types[i])
            # we have an incomplete row
            if tmp_new_extractions:
                new_rows.append(candidate_rows[found_candidates[i]])
                new_extractions.append(tmp_new_extractions)
        new_rows, new_extractions

    def get_batch(self):
        """ Should return a list of (id,url,other_data) querying for `batch_size` samples from `self.source` from `start`
        """
        if self.batch_size is None:
            print "[HBaseIngester.get_batch: error] Parameter 'batch_size' not set."
            return None
        # We should have a cdr_ingester prior to this to actually pull data out of the cdr.
        # Look at 'table_timestamp' to get 'self.batch_size' samples not yet indexed 
        # i.e. not yet in 'table_cdrinfos', 
        # or corresponding sha1 not in 'table_sha1infos', 
        # or all self.extractions not computed for that sha1.
        # other_data should actually contain what has to be computed (features, hashcodes, ocr, exif)
        images_infos = []
        scan_done = False
        last_added = None
        start_row = None
        # while we don't have enough images or did not reach end.
        while not scan_done and len(images_infos)<self.batch_size:
            with self.pool.connection() as connection:
                table_timestamp = connection.table(self.table_timestamp_name)
                # get self.batch_size rows up to self.start
                # self.start should be the last row-key that was indexed previously
                rows = []
                for row in table_timestamp.scan(row_start=start_row,row_stop=str(self.start),batch_size=self.batch_size):
                    rk = row[0]
                    rd = row[1]
                    #print rk,rd,row
                    rows.append((rk,rd))
                    if len(rows)==self.batch_size:
                        break
                scan_done = True
            print len(rows) 
            ts_cdr_ids = [row[0] for row in rows]
            print ts_cdr_ids
            # look if cdr infos exist in 'table_cdrinfos_name'
            cdr_ids = self.get_cdr_ids_from_tscdrids(ts_cdr_ids)
            print cdr_ids
            indexed, sha1s = self.get_cdr_ids_indexed(cdr_ids)
            print indexed, sha1s
            # if not indexed push, filling up images_infos
            pos_indexed = [i for i,idx in enumerate(indexed) if idx]
            pos_not_indexed = [i for i,idx in enumerate(indexed) if not idx]
            if pos_not_indexed:
                new_rows = [rows[pos] for pos in pos_not_indexed]
                new_cdr_ids = [cdr_ids[pos] for pos in pos_not_indexed]
                # fill images_infos with all extractions
                images_infos = self.fill_images_infos(new_rows,new_cdr_ids,[self.extractions_types]*len(new_rows),images_infos)
                # stop scanning if we have a full batch
                if len(images_infos)==self.batch_size:
                    break
            # everything below should run mostly for first check but not incremental update.
            # if exist, checks if sha1 is extracted.
            if pos_indexed:    
                sha1s_indexed = [sha1s[pos] for pos in pos_indexed]
                sha1s_extracted_pos_tmp = [i for i,sha1 in enumerate(sha1s_indexed) if sha1]
                # if not probably failed sha1 just skip [or maybe push to missing sha1s... Think of another process that checks these missings sha1s?]
                # if sha1 extracted, check if extractions columns are present. If so skip.
                sha1s_extracted_pos = [pos_indexed[sha1_ept] for sha1_ept in sha1s_extracted_pos_tmp]
                candidate_rows = [rows[sha1_ep] for sha1_ep in sha1s_extracted_pos]
                candidate_cdr_ids = [cdr_ids[sha1_ep] for sha1_ep in sha1s_extracted_pos]
                candidate_sha1s = [sha1s[sha1_ep] for sha1_ep in sha1s_extracted_pos]
                new_rows, new_extractions = self.check_extractions_rows(candidate_rows,candidate_sha1s)
                new_rows_ids = [i for i,row in enumerate(candidate_rows) if row in new_rows]
                new_cdr_ids = [ccdr for i,ccdr in enumerate(candidate_cdr_ids) if i in new_rows_ids]
                # otherwise push to images_infos with informations about which extractions should be run
                images_infos = self.fill_images_infos(new_rows,new_cdr_ids,new_extractions,images_infos)
            # stop scanning if we have a full batch
            if len(images_infos)==self.batch_size:
                break
            start_row = rows[-1][0]
        if len(images_infos)<self.batch_size and self.fail_less_than_batch:
            print "[HBaseIngester.get_batch: error] Not enough images ("+str(len(images_infos))+")"
            return None
        return images_infos
        
