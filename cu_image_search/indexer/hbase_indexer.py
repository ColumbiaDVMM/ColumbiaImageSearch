import os
import sys
import time
import base64
import shutil
import happybase
import numpy as np
from datetime import datetime
from generic_indexer import GenericIndexer
from socket import timeout
from ..memex_tools.image_dl import mkpath
from ..memex_tools.sha1_tools import get_SHA1_from_file, get_SHA1_from_data
from ..memex_tools.binary_file import read_binary_file, write_binary_file

TTransportException = happybase._thriftpy.transport.TTransportException
max_errors = 10

class HBaseIndexer(GenericIndexer):


    def read_conf(self):
        """ Reads configuration parameters.

        Will read parameters 'HBI_image_downloader', 'HBI_hasher', 
        'HBI_feature_extractor', 'LI_master_update_filepath' and 'LI_base_update_path'...
        from self.global_conf.
        """
        self.image_downloader_type = self.global_conf['HBI_image_downloader']
        self.hasher_type = self.global_conf['HBI_hasher']
        self.feature_extractor_type = self.global_conf['HBI_feature_extractor']
        self.hbase_host = self.global_conf['HBI_host']
        self.table_cdrinfos_name = self.global_conf['HBI_table_cdrinfos']
        self.table_sha1infos_name = self.global_conf['HBI_table_sha1infos']
        self.table_updateinfos_name = self.global_conf['HBI_table_updatesinfos']
        self.table_sim_name = self.global_conf['HBI_table_sim']
        self.extractions_types = self.global_conf['HBI_extractions_types']
        self.extractions_columns = self.global_conf['HBI_extractions_columns']
        self.in_url_column = self.global_conf['HBI_in_url_column']
        self.sha1_column = self.global_conf['HBI_sha1_column']
        self.features_dim = self.global_conf["FE_features_dim"]
        self.bits_num = self.global_conf['HA_bits_num']
        self.sha1_featid_mapping_filename = self.global_conf['HBI_sha1_featid_mapping_filename']
        self.cu_feat_id_column = "info:cu_feat_id" # could be loaded from conf
        self.discarded_column = "image_discarded" # could be loaded from conf
        self.refresh_batch_size = self.global_conf['batch_size']
        if len(self.extractions_columns) != len(self.extractions_types):
            raise ValueError("[HBaseIngester.read_conf: error] Dimensions mismatch {} vs. {} for extractions_columns vs. extractions_types".format(len(self.extractions_columns),len(self.extractions_types)))
        self.nb_threads = 2
        if 'HBI_pool_thread' in self.global_conf:
            self.nb_threads = self.global_conf['HBI_pool_thread']
        self.pool = happybase.ConnectionPool(size=self.nb_threads,host=self.hbase_host)
        # How to deal with additionals extractions?
        # Where should we store the update infos? 
        # Start of batch being indexed should be end criterion for next batch.
        self.FORCE_REFRESH = False # only use once to fix indexing issue
        self.merging = False
        self.refreshing = False
        self.initializing = False
        self.refresh_inqueue = False
        self.index_batches = []
        self.sha1_featid_mapping = []
        self.initialize_sha1_mapping()

    def initialize_sha1_mapping(self):
        if self.refresh_inqueue:
            self.refresh_inqueue = False
        self.initializing = True
        previous_count = 0
        if self.sha1_featid_mapping:
            previous_count = len(self.sha1_featid_mapping)
        self.sha1_featid_mapping = []
        # read from self.sha1_featid_mapping_filename
        try:
            print "[HBaseIndexer.initialize_sha1_mapping: log] Loading sha1_featid_mapping...",
            with open(self.sha1_featid_mapping_filename,'rt') as sha1_fid:
                self.sha1_featid_mapping = [line.strip() for line in sha1_fid]
            if len(self.sha1_featid_mapping) < previous_count:
                print "[HBaseIndexer.initialize_sha1_mapping: warning] Initialized sha1_featid_mapping with less images than before. ({} vs.{})".format(previous_count, len(self.sha1_featid_mapping))
            self.set_sha1_indexed = set(self.sha1_featid_mapping)
            print "Done."
            sys.stdout.flush()
            self.initializing = False
            self.last_refresh = datetime.now()
            if self.refresh_inqueue:
                return self.initialize_sha1_mapping()
        except Exception as inst:
            print "FAILED"
            print "[HBaseIndexer.initialize_sha1_mapping: error] Could not initialize sha1_featid_mapping from {}.\n{}".format(self.sha1_featid_mapping_filename,inst)

    def save_sha1_mapping(self):
        try:
            with open(self.sha1_featid_mapping_filename,'wt') as sha1_fid:
                for sha1 in self.sha1_featid_mapping:
                    sha1_fid.write(sha1.strip()+'\n')
        except Exception as inst:
            print "[HBaseIndexer.save_sha1_mapping: error] Could not save sha1_featid_mapping to {}.\n{}".format(self.sha1_featid_mapping_filename,inst)


    def initialize_indexer_backend(self):
        """ Initialize backend.
        """
        print "[HBaseIndexer: log] initialized with:\n\
                    \t- image_downloader: {}\n\
                    \t- feature_extractor_type: {}\n\
                    \t- hasher_type: {}".format(self.image_downloader_type,
                        self.feature_extractor_type,self.hasher_type)
        # Initialize image_downloader, feature_extractor and hasher
        self.initialize_image_downloader()
        self.initialize_feature_extractor()
        self.initialize_hasher()
        self.check_alignment()
        self.db = None


    def initialize_image_downloader(self):
        if self.image_downloader_type=="file_downloader":
            from ..image_downloader.file_downloader import FileDownloader
            self.image_downloader = FileDownloader(self.global_conf_filename)
        else:
            raise ValueError("[HBaseIndexer.initialize_indexer_backend error] Unsupported image_downloader_type: {}.".format(self.image_downloader_type))        


    def initialize_feature_extractor(self):
        if self.feature_extractor_type=="sentibank_cmdline":
            from ..feature_extractor.sentibank_cmdline import SentiBankCmdLine
            self.feature_extractor = SentiBankCmdLine(self.global_conf_filename)
        else:
            raise ValueError("[HBaseIndexer.initialize_feature_extractor: error] Unknown feature_extractor_type: {}.".format(self.feature_extractor_type))


    def initialize_hasher(self):
        if self.hasher_type=="hasher_cmdline":
            from ..hasher.hasher_cmdline import HasherCmdLine
            self.hasher = HasherCmdLine(self.global_conf_filename)
            self.init_master_uf_fn = os.path.join(self.hasher.base_update_path, self.hasher.master_update_file)
        else:
            raise ValueError("[HBaseIndexer.initialize_hasher: error] Unknown hasher_type: {}.".format(self.hasher_type))


    def refresh_hbase_conn(self, calling_function, sleep_time=2):
        print("[HBaseIndexer.{}] caught timeout error or TTransportException. Trying to refresh connection pool.".format(calling_function))
        time.sleep(sleep_time)
        self.pool = happybase.ConnectionPool(size=self.nb_threads,host=self.hbase_host)


    def get_ids_from_sha1s(self, list_sha1s):
        found_ids = []
        for sha1 in list_sha1s:
            pos = None
            try:
                pos = self.sha1_featid_mapping.index(sha1)
            except:
                pass
            found_ids.append((pos,sha1))
        return found_ids


    def check_errors(self, previous_err, function_name, inst=None):
        if previous_err >= max_errors:
            raise Exception("[HBaseIndexer: error] function {} reached maximum number of error {}. Error was: {}".format(function_name, max_errors, inst))
        return None


    def get_ids_from_sha1s_hbase(self, list_sha1s, previous_err=0, inst=None):
        found_ids = []
        self.check_errors(previous_err, "get_ids_from_sha1s_hbase", inst)
        if list_sha1s:
            try:
                with self.pool.connection() as connection:
                    table_sha1infos = connection.table(self.table_sha1infos_name)
                    rows = table_sha1infos.rows(list_sha1s)
                    for row in rows:
                        if "info:cu_feat_id" in row[1]:
                            found_ids.append((long(row[1]["info:cu_feat_id" ]),str(row[0])))
            except (timeout or TTransportException or IOError) as inst:
                self.refresh_hbase_conn("get_ids_from_sha1s_hbase")
                return self.get_ids_from_sha1s_hbase(list_sha1s, previous_err+1, inst)
        return found_ids


    def get_full_sha1_rows(self, list_sha1s, previous_err=0, inst=None):
        rows = None
        self.check_errors(previous_err, "get_full_sha1_rows", inst)
        if list_sha1s:
            try:
                with self.pool.connection() as connection:
                    table_sha1infos = connection.table(self.table_sha1infos_name)
                    rows = table_sha1infos.rows(list_sha1s)
            except (timeout or TTransportException or IOError) as inst:
                self.refresh_hbase_conn("get_full_sha1_rows")
                return self.get_full_sha1_rows(list_sha1s, previous_err+1, inst)
        return rows


    def get_columns_from_sha1_rows(self, list_sha1s, columns, previous_err=0, inst=None):
        rows = None
        self.check_errors(previous_err, "get_columns_from_sha1_rows", inst)
        if list_sha1s:
            try:
                with self.pool.connection() as connection:
                    table_sha1infos = connection.table(self.table_sha1infos_name)
                    # this throws a socket timeout?...
                    rows = table_sha1infos.rows(list_sha1s, columns=columns)
            except (timeout or TTransportException or IOError) as inst:
                self.refresh_hbase_conn("get_columns_from_sha1_rows")
                return self.get_columns_from_sha1_rows(list_sha1s, columns, previous_err+1, inst)
        return rows


    def get_similar_images_from_sha1(self, list_sha1s, previous_err=0, inst=None):
        rows = None
        self.check_errors(previous_err, "get_similar_images_from_sha1", inst)
        print "[HBaseIndexer.get_similar_images_from_sha1: log] list_sha1s: {}".format(list_sha1s)
        if list_sha1s:
            try:
                with self.pool.connection() as connection:
                    table_sha1_sim = connection.table(self.table_sim_name)
                    rows = table_sha1_sim.rows(list_sha1s)
            except (timeout or TTransportException or IOError) as inst:
                self.refresh_hbase_conn("get_similar_images_from_sha1")
                return self.get_similar_images_from_sha1(list_sha1s, previous_err+1, inst)
        return rows


    def get_sim_infos(self, list_ids):
        #print("[HBaseIndexer.get_sim_infos: log] list_ids: {}".format(list_ids))
        # we could also discard id in list_ids if it is out of range...
        try:
            list_sha1s = [self.sha1_featid_mapping[int(i)] for i in list_ids]
        except Exception as inst:
            # if this fails, it should mean a refresh happened and we need to refresh too 'sha1_featid_mapping'
            print("[HBaseIndexer.get_sim_infos: log] caugh error: {}. The index might need to be refreshed, thus 'sha1_featid_mapping' is out of date.".format(inst))
            self.initialize_sha1_mapping()
            #while self.merging or self.initializing:
            #    print("[HBaseIndexer.get_sim_infos: log] Waiting for 'sha1_featid_mapping' to be upated.")
            #    time.sleep(1)
            # something else (bad) is happening if this fails again
            list_sha1s = [self.sha1_featid_mapping[int(i)] for i in list_ids]
        return self.get_full_sha1_rows(list_sha1s)


    def get_precomp_from_sha1(self, list_sha1s, list_type):
        """ Retrieves the 'list_type' extractions results from HBase for the image in 'list_sha1s'.

        :param list_sha1s: list of sha1s of the images for which the extractions are requested.
        :type list_sha1s: list
        :param list_type: list of the extractions requested. They have to be a subset of *self.extractions_types*
        :type list_sha1s: list
        :returns res, ok_ids: *res* contains the extractions, *ok_ids* the ids of the 'list_sha1s' for which we retrieved something.
        """
        res = []
        ok_ids = []
        print "[get_precomp_from_sha1] list_sha1s: {}.".format(list_sha1s)
        rows = self.get_full_sha1_rows(list_sha1s)
        # check if we have retrieved rows and extractions for each sha1
        retrieved_sha1s = [row[0] for row in rows]
        print "[get_precomp_from_sha1] retrieved_sha1s: {}.".format(list_sha1s)
        # building a list of ok_ids and res for each extraction type
        ok_ids = [[] for i in range(len(list_type))]
        res = [[] for i in range(len(list_type))]
        list_columns = self.get_columns_name(list_type)
        print "[get_precomp_from_sha1] list_columns: {}.".format(list_columns)
        for i,sha1 in enumerate(retrieved_sha1s):
            for e in range(len(list_type)):
                if list_columns[e] in rows[i][1]:
                    print "[get_precomp_from_sha1] {} {} {} {}.".format(i,sha1,e,list_columns[e])
                    ok_ids[e].append(list_sha1s.index(sha1))
                    res[e].append(rows[i][1][list_columns[e]])
        return res,ok_ids
   
        
    def write_batch(self, batch, tab_out_name, previous_err=0, inst=None):
        self.check_errors(previous_err, "write_batch", inst)
        try:
            # batch is composed of tuples (row_key, dict())
            # where entries in the dict are key-value pairs as {'column_name': column_value}
            with self.pool.connection() as connection:
                tab_out = connection.table(tab_out_name)
                batch_write = tab_out.batch()
                #print "Pushing batch from {}.".format(batch[0][0])
                for row in batch:
                    #print "[HBaseIndexer.write_batch: log] Pushing row {} with keys {}".format(row[0],row[1].keys())
                    batch_write.put(row[0], row[1])
                batch_write.send()
        except (timeout or TTransportException or IOError) as inst:
            self.refresh_hbase_conn("write_batch")
            return self.write_batch(batch, previous_err+1, inst)


    def get_columns_name(self, list_type):
        list_columns = []
        for e, extr in enumerate(list_type):
            if extr not in self.extractions_types:
                raise ValueError("[HBaseIndexer.get_columns_name: error] Unknown extraction type \"{}\".".format(extr))
            pos = self.extractions_types.index(extr)
            list_columns.append(self.extractions_columns[pos])
        return list_columns


    ##-- When reading FROM hbase to save locally
    def save_refresh_batch(self, refresh_batch, base_update_path, update_id):
        ''' Save features and hashcodes of refresh_batch to files in base_update_path folder
        and list corresponding sha1s.

        :param refresh_batch: batch as list of (sha1, base64feat, base64hash)
        :param base_update_path: where to save features and hashcodes files
        :param update_id: identifier of current update

        :returns tmp_sha1_featid_mapping, features_fn, hashcodes_fn: 
            tmp_sha1_featid_mapping: the list of new sha1s
            features_fn: filename with new features
            features_fn: filename with new hash codes
        '''
        import base64
        import numpy as np
        tmp_sha1_featid_mapping = []
        features_fn = None
        hashcodes_fn = None
        if refresh_batch:
            list_feats = []
            list_hashcodes = []
            for row in refresh_batch:
                tmp_sha1_featid_mapping.append(row[0].strip())
                list_feats.append(np.frombuffer(base64.b64decode(row[1]),np.float32))
                list_hashcodes.append(np.frombuffer(base64.b64decode(row[2]),np.uint8))
            mkpath(os.path.join(base_update_path,'features/'))
            mkpath(os.path.join(base_update_path,'hash_bits/'))
            # save features in base_update_path/features
            features_fn = os.path.join(base_update_path,'features', update_id+'_norm')
            write_binary_file(features_fn,list_feats)
            # save hashcodes in base_update_path/hash_bits
            hashcodes_fn = os.path.join(base_update_path,'hash_bits', update_id+'_itq_norm_'+str(self.bits_num))
            write_binary_file(hashcodes_fn,list_hashcodes)
        # returns tmp_sha1_featid_mapping
        return tmp_sha1_featid_mapping, features_fn, hashcodes_fn


    def merge_refresh_batch(self, refresh_batch):
        if refresh_batch:
            print "[HBaseIndexer.merge_refresh_batch: log] We have a batch of {} images from {}.".format(len(refresh_batch),refresh_batch[0][0])
            # [Create a temporary HasherCmdLine] have a temporary "master_update" file for that batch
            from ..hasher.hasher_cmdline import HasherCmdLine
            tmp_hasher = HasherCmdLine(self.global_conf_filename)
            tmp_update_id = str(time.time())+'_'+refresh_batch[0][0]
            tmp_hasher.master_update_file = "update_"+tmp_update_id
            tm_uf_fn = os.path.join(self.hasher.base_update_path, tmp_hasher.master_update_file)
            with open(tm_uf_fn,'wt') as tm_uf:
                tm_uf.write(tmp_update_id+'\n')
            # save features (and hashcodes) and compress features, and get temporary mapping sha1 - feat_id. 
            # refresh_batch: batch as list of (sha1, base64feat, base64hash)
            tmp_sha1_featid_mapping, tmp_features_fn, tmp_hashcodes_fn = self.save_refresh_batch(refresh_batch, self.hasher.base_update_path, tmp_update_id)
            tmp_hasher.compress_feats()
            self.finalize_batch_indexing(self, tmp_sha1_featid_mapping, tmp_update_id, tm_uf_fn)
    ##--


    def merge_update_files(self, previous_files, tmp_update_id, out_update_id, m_uf_fn, tmp_hasher):
        start_merge = time.time()
        print("[HBaseIndexer.merge_update_files: log] merging files in folder {}".format(self.hasher.base_update_path))
        # use shutil.copyfileobj for comp features
        out_comp_fn = os.path.join(self.hasher.base_update_path,'comp_features',out_update_id+'_comp_norm')
        new_comp_feat_fn = os.path.join(tmp_hasher.base_update_path,'comp_features',tmp_update_id+'_comp_norm')
        if previous_files:
            prev_comp_feat_fn = os.path.join(self.hasher.base_update_path,'comp_features',previous_files[0]+'_comp_norm')
            comp_idx_shift = os.stat(prev_comp_feat_fn).st_size
        else:
            comp_idx_shift = 0
        self.merging = True
        mkpath(out_comp_fn)
        if previous_files:
            shutil.move(prev_comp_feat_fn,out_comp_fn)
        with open(out_comp_fn,'ab') as out_comp, open(new_comp_feat_fn,'rb') as new_comp:
                shutil.copyfileobj(new_comp, out_comp)
        out_hash_fn = os.path.join(self.hasher.base_update_path,'hash_bits',out_update_id+'_itq_norm_'+str(self.bits_num))
        mkpath(out_hash_fn)
        with open(out_hash_fn,'wb') as out_hash:
            if previous_files:
                prev_hashcode_fn = os.path.join(self.hasher.base_update_path,'hash_bits',previous_files[0]+'_itq_norm_'+str(self.bits_num))
                with open(prev_hashcode_fn,'rb') as prev_hash:
                    shutil.copyfileobj(prev_hash, out_hash)
            new_hashcode_fn = os.path.join(tmp_hasher.base_update_path,'hash_bits',tmp_update_id+'_itq_norm_'+str(self.bits_num))
            with open(new_hashcode_fn,'rb') as new_hash:
                shutil.copyfileobj(new_hash, out_hash)
        # need to read and shift tmp_update_id comp_idx
        out_comp_idx_fn = os.path.join(self.hasher.base_update_path,'comp_idx',out_update_id+'_compidx_norm')
        mkpath(out_comp_idx_fn)
        with open(out_comp_idx_fn,'wb') as out_comp_idx:
            if previous_files:
                prev_comp_idx_fn = os.path.join(self.hasher.base_update_path,'comp_idx',previous_files[0]+'_compidx_norm')
                with open(prev_comp_idx_fn,'rb') as prev_hash:
                    shutil.copyfileobj(prev_hash, out_comp_idx)
            new_comp_idx_fn = os.path.join(tmp_hasher.base_update_path,'comp_idx',tmp_update_id+'_compidx_norm')
            arr = np.fromfile(new_comp_idx_fn, dtype=np.uint64)
            arr += comp_idx_shift
            # discarding the first value as it would be equal to the last one of 'prev_hash'
            arr[1:].tofile(out_comp_idx)
        # update master file
        mkpath(m_uf_fn)
        with open(m_uf_fn, 'wt') as m_uf:
            m_uf.write(out_update_id+'\n')
        print("[HBaseIndexer.merge_update_files: log] Merge update files took {}s.".format(time.time()-start_merge))
        return m_uf_fn


    def cleanup_update(self, previous_files, update_id, tmp_hasher):
        # cleanup features
        new_feat_fn = os.path.join(tmp_hasher.base_update_path,'features',update_id+'_norm')
        os.remove(new_feat_fn)
        # cleanup comp features
        # new procedure moves this file
        #prev_comp_feat_fn = os.path.join(self.hasher.base_update_path,'comp_features',previous_files[0]+'_comp_norm')
        #os.remove(prev_comp_feat_fn)
        new_comp_feat_fn = os.path.join(tmp_hasher.base_update_path,'comp_features',update_id+'_comp_norm')
        os.remove(new_comp_feat_fn)
        shutil.rmtree(os.path.join(tmp_hasher.base_update_path,'comp_features'))
        # cleanup hashcodes
        if previous_files:
            prev_hashcode_fn = os.path.join(self.hasher.base_update_path,'hash_bits',previous_files[0]+'_itq_norm_'+str(self.bits_num))
            os.remove(prev_hashcode_fn)
        new_hashcode_fn = os.path.join(tmp_hasher.base_update_path,'hash_bits',update_id+'_itq_norm_'+str(self.bits_num))
        os.remove(new_hashcode_fn)
        shutil.rmtree(os.path.join(tmp_hasher.base_update_path,'hash_bits'))
        # cleanup comp_idx
        if previous_files:
            prev_comp_idx_fn = os.path.join(self.hasher.base_update_path,'comp_idx',previous_files[0]+'_compidx_norm')
            os.remove(prev_comp_idx_fn)
        new_comp_idx_fn = os.path.join(tmp_hasher.base_update_path,'comp_idx',update_id+'_compidx_norm')
        os.remove(new_comp_idx_fn)
        shutil.rmtree(os.path.join(tmp_hasher.base_update_path,'comp_idx'))
        # cleanup temporary master file
        tm_uf_fn = os.path.join(tmp_hasher.base_update_path, tmp_hasher.master_update_file)
        os.remove(tm_uf_fn)
        # cleanup tmp update folder
        shutil.rmtree(os.path.join(tmp_hasher.base_update_path))


    def push_cu_feats_id(self, rows_update, cu_feat_ids, previous_err=0, inst=None):
        self.check_errors(previous_err, "push_cu_feats_id", inst)
        if len(rows_update)!=len(cu_feat_ids):
            raise ValueError("[HBaseIndexer.push_cu_feats_id: error] dimensions mismatch rows_update ({}) vs. cu_feat_ids ({})".format(len(rows_update),len(cu_feat_ids)))
        # use write_batch for this?
        try:
            with self.pool.connection() as connection:
                table_sha1infos = connection.table(self.table_sha1infos_name)
                with table_sha1infos.batch() as b:
                    for i, sha1 in enumerate(rows_update):
                        b.put(sha1, {self.cu_feat_id_column: str(cu_feat_ids[i])})
                b.send()
        except (timeout or TTransportException or IOError) as inst:
            self.refresh_hbase_conn("push_cu_feats_id")
            self.push_cu_feats_id(rows_update, cu_feat_ids, previous_err+1, inst)


    def check_alignment(self):
        # check alignment
        nb_indexed = len(self.sha1_featid_mapping)
        try:
            max_id = self.hasher.get_max_feat_id()
        except Exception as inst:
            # first batch
            if nb_indexed == 0:
                return nb_indexed
            else:
                raise ValueError("[HBaseIndexer.check_alignment: error] Could not retrieve max_id from hasher. Error was: {}".format(inst))
        if max_id != nb_indexed:
            print "[HBaseIndexer.check_alignment: error] We have {} features, {} listed in sha1_featid_mapping.".format(max_id, nb_indexed)
            # we could try to recover 
            # if max_id > nb_indexed (an update crashed after merging files but before updating and saving sha1_featid_mapping): 
            # - cut hasbits based at nb_indexed hashcodes
            # - cut comp_features based on comp_idx for nb_indexed [may not be needed if it is this step which failed]
            # if nb_indexed > max_id (unlikely, file have been tampered manually?):
            # - self.sha1_featid_mapping at max_id
            raise ValueError("[HBaseIndexer.check_alignment:error] max_id!=nb_indexed: {} vs. {}.".format(max_id, nb_indexed))
        return nb_indexed
        

    def finalize_batch_indexing(self, tmp_sha1_featid_mapping, tmp_update_id, tmp_hasher):
        nb_indexed = self.check_alignment()
        # Look for previous updates
        previous_files = []
        m_uf_fn = os.path.join(self.hasher.base_update_path, self.hasher.master_update_file)
        if os.path.isfile(m_uf_fn):
            with open(m_uf_fn, 'rt') as m_uf:
                for line in m_uf:
                    previous_files.append(line.strip())
        # should we actually impose a limit on the compress feature file dimension?
        if len(previous_files)>1:
            raise ValueError("[HBaseIndexer.finalize_batch_indexing:error] was expecting a single file, found {}.".format(len(previous_files)))
        # do a merge, i.e. concatenate hashcodes and compressed features. 
        out_update_id = str(time.time())+'_'+tmp_sha1_featid_mapping[0]
        self.merge_update_files(previous_files, tmp_update_id, out_update_id, m_uf_fn, tmp_hasher)
        # all files have been merged in out_update_id now,
        # we can delete files created by tmp_update_id and previous_files
        self.cleanup_update(previous_files, tmp_update_id, tmp_hasher)
        # update and save sha1 mapping
        self.sha1_featid_mapping.extend(tmp_sha1_featid_mapping)
        self.set_sha1_indexed = set(self.sha1_featid_mapping)
        self.save_sha1_mapping()
        if previous_files:
            self.merging = False
        # push cu_feat_id to hbase
        cu_feat_ids = [nb_indexed+i for i in range(len(tmp_sha1_featid_mapping))]
        self.push_cu_feats_id(tmp_sha1_featid_mapping, cu_feat_ids)


    def build_extractions_rows(self, new_files_id, extractions_names, extractions_vals):
        """ Creates batch output.

        :param new_files_id: list of images sha1.
        :type new_files_id: list
        :param extractions_names: list extractions names.
        :type extractions_names: list
        :param extractions_vals: list of list of extractions.
        :type extractions_vals: list
        :returns batch_output: list of tuple (*sha1*, *row_data*), where *row_data* contains the extractions.
        """
        if len(extractions_names) != len(extractions_vals):
            raise ValueError("[HBaseIndexer.build_extractions_rows: error] len(extractions_names): {} != {}: len(extractions_vals) ".format(len(extractions_names), len(extractions_vals)))
        batch_output = []
        extractions_columns_names = []
        for extr in extractions_names:
            extractions_columns_names.append(self.extractions_columns[self.extractions_types.index(extr)])
        for i, sha1 in enumerate(new_files_id):
            row_data = dict()
            for j, extr in enumerate(extractions_names):
                row_data[extractions_columns_names[j]] = base64.b64encode(extractions_vals[j][i])
            batch_output.append((sha1, row_data))
        return batch_output


    def cleanup_images(self, readable_images):
        for image in readable_images:
            os.remove(image[-1])


    def index_batch_sha1(self, batch, update_id):
        """ Index an update batch in the form of a list of (sha1, url)
        """
        # Download images
        start_time = time.time() 
        self.write_batch([(update_id, {'info:started': 'True'})], self.table_updateinfos_name)
        print "[HBaseIndexer.index_batch_sha1: log] Starting udpate {}".format(update_id)
        #readable_images = self.image_downloader.download_images(batch, update_id)
        readable_images = self.image_downloader.download_images_parallel_integritycheck(batch, update_id)
        # now each batch sample is (sha1, url, filename)
        new_sb_files = []
        new_files_id = []
        existing_cu_feat_ids = []
        existing_sha1 = []
        start_check_new_time = time.time()
        # fix to speed up indexing for now.
        update_existing = False
        # # v2.0
        # if "sentibank" in self.extractions_types:
        #     check_images_sha1 = [image[0].rstrip() for image in readable_images]
        #     already_indexed_sha1 = []
        #     for i, indexed_image_sha1 in enumerate(self.sha1_featid_mapping):
        #         if indexed_image_sha1 in check_images_sha1:
        #             print "[HBaseIndexer.index_batch_sha1: warning] tried to re-index image with sha1: {}".format(indexed_image_sha1)
        #             # will call push_cu_feats_id for these images to make sure they are marked as indexed.
        #             existing_cu_feat_ids.append(i)
        #             existing_sha1.append(indexed_image_sha1)
        #             already_indexed_sha1.append(indexed_image_sha1)
        #     for i, image_toindex_sha1 in enumerate(check_images_sha1):
        #         if image_toindex_sha1 not in already_indexed_sha1:
        #             new_sb_files.append(readable_images[i][-1])
        #             new_files_id.append(image_toindex_sha1)
        # v3.0
        if "sentibank" in self.extractions_types:
            check_images_sha1 = [image[0].rstrip() for image in readable_images]
            set_check_images_sha1 = set(check_images_sha1)
            set_new_sha1 = set_check_images_sha1 - self.set_sha1_indexed
            for i,sha1 in enumerate(check_images_sha1):
                if sha1 in set_new_sha1:
                    new_sb_files.append(readable_images[i][-1])
                    new_files_id.append(sha1)
                else:
                    print "[HBaseIndexer.index_batch_sha1: warning] tried to re-index image with sha1: {}".format(sha1)
                    # will call push_cu_feats_id for these images to make sure they are marked as indexed.
                    # this is slow...
                    if update_existing:
                        existing_cu_feat_ids.append(self.sha1_featid_mapping.index(sha1))
                        existing_sha1.append(sha1)
        if update_existing and existing_sha1 and existing_cu_feat_ids:
            print("[HBaseIndexer.index_batch_sha1: warning] Found {} images already indexed.".format(len(existing_sha1)))
            self.push_cu_feats_id(existing_sha1, existing_cu_feat_ids)
        print("[HBaseIndexer.index_batch_sha1: log] Checked existing images in {}s.".format(time.time()-start_check_new_time))
        sys.stdout.flush()
        if new_sb_files:
            start_extract_time = time.time()
            print "[HBaseIndexer.index_batch_sha1: log] Will extract features for {} images out of the {} of this batch.".format(len(new_files_id), len(batch))
            # [Create a temporary HasherCmdLine] have a temporary "master_update" file for that batch
            from ..hasher.hasher_cmdline import HasherCmdLine
            tmp_hasher = HasherCmdLine(self.global_conf_filename)
            tmp_hasher.master_update_file = "update_"+update_id
            # avoid overwriting if retrying same update somehow...
            tmp_hasher.base_update_path = self.hasher.base_update_path+update_id+"_"+str(time.time())
            tm_uf_fn = os.path.join(tmp_hasher.base_update_path, tmp_hasher.master_update_file)
            mkpath(tm_uf_fn)
            with open(tm_uf_fn,'wt') as tm_uf:
                tm_uf.write(update_id+'\n')
            # Compute features
            features_filename, ins_num = self.feature_extractor.compute_features(new_sb_files, update_id)
            # Compute hashcodes
            hashbits_filepath = tmp_hasher.compute_hashcodes(features_filename, ins_num, update_id)
            # need to move features file
            norm_features_filename = os.path.join(tmp_hasher.base_update_path,'features',update_id+'_norm')
            mkpath(norm_features_filename)
            shutil.move(features_filename[:-4]+"_norm", norm_features_filename)
            hashbits_filename = os.path.join(tmp_hasher.base_update_path,'hash_bits',update_id+'_itq_norm_'+str(self.hasher.bits_num))
            mkpath(hashbits_filename)
            shutil.move(hashbits_filepath, hashbits_filename)
            # read features and hashcodes and pushback for insertion
            feats, feats_ok_ids = read_binary_file(norm_features_filename, "feats", new_files_id, self.features_dim*4, np.float32)
            hashcodes, hash_ok_ids = read_binary_file(hashbits_filename, "hashcode", new_files_id, self.bits_num/8, np.uint8)
            print("[HBaseIndexer.index_batch_sha1: log] Extracted features and hashcodes in {}s.".format(time.time()-start_extract_time))
            if len(feats_ok_ids) != len(new_files_id) or len(hash_ok_ids) != len(new_files_id):
                print("[HBaseIndexer.index_batch_sha1: error] Dimensions mismatch. Are we missing features? {} vs. {}, or hashcodes {} vs. {}.".format(len(feats_ok_ids),len(new_files_id),len(hash_ok_ids),len(new_files_id)))
                self.cleanup_images(readable_images)
                # mark update as corrupted?
                self.write_batch([(update_id, {'info:corrupted': 'True'})], self.table_updateinfos_name)
                return False
            new_sha1_rows = self.build_extractions_rows(new_files_id, ["sentibank", "hashcode"], [feats, hashcodes])
            print "[HBaseIndexer.index_batch: log] writing batch of new images from {} to table {}.".format(new_sha1_rows[0][0], self.table_sha1infos_name)
            self.write_batch(new_sha1_rows, self.table_sha1infos_name) 
            tmp_hasher.compress_feats()
            self.finalize_batch_indexing(new_files_id, update_id, tmp_hasher)
            print "[HBaseIndexer.index_batch: log] Indexed batch in {}s.".format(time.time()-start_time)
        else:
            print("[HBaseIndexer.index_batch_sha1: log] No new/readable images to index for batch starting with row {}!".format(batch[0]))
        self.cleanup_images(readable_images)
        # mark update has completed
        self.write_batch([(update_id, {'info:indexed': 'True'})], self.table_updateinfos_name)
        return True


    def get_next_batch(self):
        """ Get next update batch. 

        :returns (update_id, list_sha1s): returns a batch to be indexed.
        """
        try:
            if not self.index_batches:
                with self.pool.connection() as connection:
                    table_updateinfos = connection.table(self.table_updateinfos_name)
                    for row in table_updateinfos.scan(row_start='index_update_', row_stop='index_update_~'):
                        if "info:indexed" not in row[1] and 'info:started' not in row[1] and 'info:corrupted' not in row[1]:
                            self.index_batches.append((row[0], row[1]["info:list_sha1s"]))
            if self.index_batches:
                batch = self.index_batches.pop()
            else:
                # we could look at the one marked as 'info:started' if they are not finished
                # but started from a long time ago in this case.
                batch = (None, None)
        except timeout as inst:
            print("[HBaseIndexer.get_next_batch] caught timeout error or TTransportException. Trying to refresh connection pool. Error was: {}".format(inst))
            self.pool = happybase.ConnectionPool(size=self.nb_threads,host=self.hbase_host)
            return self.get_next_batch()
        return batch


    ### Deprecated. Was used when ingesting form CDR was done in a python script and not with a Spark job.
    # def format_for_sha1_infos(self, list_images):
    #     sha1_infos = []
    #     unique_sha1 = set()
    #     # keys are "info:all_cdr_ids", "info:all_parent_ids", "info:all_htids", "info:featnorm_cu", "info:s3_url", "info:hash256_cu"
    #     for image in list_images:
    #         tmp = dict()
    #         sha1 = image[-1]
    #         #print "{}: {}".format(sha1,image)
    #         unique_sha1.add(sha1)
    #         tmp["info:all_cdr_ids"] = image[0]
    #         image_dict = image[2][2]
    #         tmp["info:all_parent_ids"] = image_dict["info:obj_parent"]
    #         # this field may be absent
    #         if "info:crawl_data.image_id" in image_dict:
    #             tmp["info:all_htids"] = image_dict["info:crawl_data.image_id"]
    #         tmp["info:s3_url"] = image[1]
    #         sha1_infos.append((sha1,tmp))
    #     return sha1_infos,unique_sha1

    # ## Deprecated
    # def refresh_hash_index(self, skip=False):
    #     start_row = None
    #     self.refreshing = True
    #     # when running in batch mode, to restart from failure point
    #     if skip and self.sha1_featid_mapping:
    #         start_row = self.sha1_featid_mapping[-1]
    #     list_type = ["sentibank", "hashcode"]
    #     list_columns = self.get_columns_name(list_type)
    #     all_needed_columns = list_columns+["info:all_cdr_ids", "info:all_parent_ids"]
    #     refresh_batch = []
    #     done = False
    #     scanned_rows = 0
    #     refresh_start = time.time()
    #     while not done:
    #         try:    
    #             with self.pool.connection() as connection:
    #                 table_sha1infos = connection.table(self.table_sha1infos_name)
    #                 batch_start = time.time()
    #                 for row in table_sha1infos.scan(row_start=start_row,batch_size=self.refresh_batch_size,columns=all_needed_columns+[self.cu_feat_id_column]):
    #                     scanned_rows += 1
    #                     if scanned_rows % self.refresh_batch_size == 0:
    #                         elapsed_refresh = time.time() - refresh_start
    #                         print "[HBaseIndexer.refresh_hash_index: log] Scanned {} rows so far. Total refresh time: {}. Average per row: {}.".format(scanned_rows,elapsed_refresh,elapsed_refresh/scanned_rows)
    #                         sys.stdout.flush()
    #                     # use column 'cu_featid' in hbase table escorts_images_sha1_infos to check if already indexed 
    #                     # and use list to get total number of features for sanity check, 
    #                     # because we cannot get that from the hbase table easily...
    #                     # too popular images have been discarded and marked with 'discarded_column', never index them
    #                     if (self.cu_feat_id_column not in row[1] or self.FORCE_REFRESH) and (self.discarded_column not in row[1]):
    #                         found_columns = [column for column in all_needed_columns if column in row[1]]
    #                         if len(found_columns)==len(all_needed_columns): # we have features and hashcodes
    #                             refresh_batch.append((row[0],row[1][list_columns[0]],row[1][list_columns[1]]))
    #                     # merge if we have a complete batch
    #                     if len(refresh_batch)>=self.refresh_batch_size:
    #                         print "[HBaseIndexer.refresh_hash_index: log] Pushing batch built in {}s.".format(time.time()-batch_start)
    #                         self.merge_refresh_batch(refresh_batch)
    #                         refresh_batch = []
    #                         batch_start = time.time()
    #                     start_row = row[0]
    #                 # last batch
    #                 if refresh_batch:
    #                     self.merge_refresh_batch(refresh_batch)
    #                 done = True
    #         except Exception as inst:
    #             print "[HBaseIndexer.refresh_hash_index: log] Caught Exception: {}.".format(inst)
    #             time.sleep(5)
    #     self.refreshing = False
    #     if self.refresh_inqueue:
    #         self.refresh_inqueue = False
    #         return self.refresh_hash_index()


    # ## Deprecated
    # def get_next_batch_start(self):
    #     """ Get start value for next update batch. 

    #     :returns timestamp_cdrid: First row timestamp_cdrid in 'table_updateinfos_name'.
    #     """
    #     with self.pool.connection() as connection:
    #         table_updateinfos = connection.table(self.table_updateinfos_name)
    #         for row in table_updateinfos.scan():
    #             return row[0]


    # ## Deprecated
    # def group_by_sha1(self,list_images,extractions=None):
    #     out = []
    #     sha1_list = []
    #     #print "[group_by_sha1: log] list_images {}.".format(list_images)
    #     for tmp in list_images:
    #         if tmp[0] not in sha1_list:
    #             sha1_list.append(tmp[0])
    #             if extractions and tmp[0] in extractions:
    #                 tmp = self.add_extractions_to_row(tmp,extractions[tmp[0]])
    #             out.append(tmp)
    #         else:
    #             pos = sha1_list.index(tmp[0])
    #             row = out[pos]
    #             # merge
    #             new_row = self.merge_two_rows(row,tmp)
    #             if extractions and tmp[0] in extractions:
    #                 new_row = self.add_extractions_to_row(new_row,extractions[tmp[0]])
    #             # update
    #             out[pos] = new_row
    #     return out

    # ## Deprecated
    # def add_extractions_to_row(self, row, extractions):
    #     """ Add newly extracted columns to row.

    #     :param row: tuple (*sha1*, *row_data*), where *row_data* is a dictionary.
    #     :type row: tuple
    #     :param extractions: dictionary of extractions, keys should be columns name.
    #     :type extractions: dict
    #     :returns updated_row: tuple (*sha1*, *row_data*), where *row_data* now contains the extractions.
    #     """
    #     if len(row) != 2:
    #         raise ValueError("[HBaseIndexer.add_extractions_to_row: error] row is incorrect, not a tuple (sha1, dict). row was: {}".format(row))
    #     out_values = row[1]
    #     if type(out_values) != dict:
    #         raise ValueError("[HBaseIndexer.add_extractions_to_row: error] row data is not a dictionary. row was: {}".format(row))
    #     #print "[HBaseIndexer.add_extractions_to_row: log] adding extractions {} to row {}.".format(extractions.keys(),row[0])
    #     for extr in extractions.keys():
    #         out_values[extr] = extractions[extr]
    #     return (row[0], out_values)


    # ## Deprecated
    # def merge_two_rows(self,row,tmp):
    #     if row[0]!=tmp[0]:
    #         raise ValueError("[HBaseIndexer.merge_two_rows: error] tried to merge two rows with different keys {} and {}.".format(row[0],tmp[0]))
    #     all_keys = set(row[1].keys())
    #     all_keys.union(set(tmp[1].keys()))
    #     out = dict()
    #     for key in all_keys:
    #         # some values are list and should be merged
    #         if key in row[1] and key in tmp[1] and key != 'info:s3_url' and key not in self.extractions_columns:
    #             out[key] = ','.join(list(set(row[1][key].split(',')) | set(tmp[1][key].split(','))))               
    #         # for others a single value should be kept: info:s3_url, and the extractions.
    #         else:
    #             if key in row[1]:
    #                 out[key] = row[1][key]
    #             elif key in tmp[1]:
    #                 out[key] = tmp[1][key]       
    #     out_row = (row[0],out)
    #     return out_row


    # ## Deprecated
    # def get_new_unique_images(self, sha1_images):
    #     # get sha1 list
    #     sha1_list = [img_item[-1] for img_item in sha1_images]
    #     # get new files and new_fulls
    #     new_files = []
    #     new_fulls = []
    #     old_to_be_merged = []
    #     unique_sha1 = sorted(set(sha1_list))
    #     print "[HBaseIndexer.get_new_unique_images: log] We have {} unique images.".format(len(unique_sha1))
    #     unique_idx = [sha1_list.index(sha1) for sha1 in unique_sha1]
    #     full_idx = [unique_sha1.index(sha1) for sha1 in sha1_list]
    #     # get what we already have indexed in 'table_sha1infos_name'
    #     res, ok_ids = self.get_precomp_from_sha1(list(unique_sha1),self.extractions_types)
    #     # check which images already exists and what extractions are present
    #     for i,sha1 in enumerate(unique_sha1):
    #         missing_extr = []
    #         for e,extr in enumerate(self.extractions_types):
    #             if i not in ok_ids[e]:
    #                 # we don't have this extraction for that image
    #                 missing_extr.append(extr)
    #         all_orig_ids = [k for k,j in enumerate(full_idx) if j==i]
    #         # we already have all extractions
    #         if not missing_extr:
    #             # we should just merge the cdr_ids and obj_parent etc
    #             old_to_be_merged.extend([sha1_images[k] for k in all_orig_ids])
    #         else:
    #             # we need to compute new extractions
    #             orig_pos = unique_idx[i]
    #             new_files.append((sha1_images[orig_pos][-2],missing_extr))
    #             new_fulls.append([sha1_images[k] for k in all_orig_ids])
    #     return new_files, new_fulls, old_to_be_merged


    # ## Deprecated
    # def index_batch(self,batch):
    #     """ Index a batch in the form of a list of (cdr_id,url,[extractions,ts_cdrid,other_data])
    #     """
    #     # Download images
    #     start_time = time.time() 
    #     timestr = time.strftime("%b-%d-%Y-%H-%M-%S", time.localtime(start_time))
    #     #startid = str(batch[0][0])
    #     #lastid = str(batch[-1][0])
    #     #update_id = timestr+'_'+startid+'_'+lastid
    #     update_id = timestr
    #     print "[HBaseIndexer.index_batch: log] Starting udpate {}".format(update_id)
    #     readable_images = self.image_downloader.download_images(batch,update_id)
    #     #print readable_images
    #     if not readable_images:
    #         print "[HBaseIndexer.index_batch: log] No readable images!"
    #         return None
    #     # Compute sha1
    #     sha1_images = [img+(get_SHA1_from_file(img[-1]),) for img in readable_images]
    #     # Now that we have sha1s, check if we actually don't already have all extractions
    #     new_files, new_fulls, old_to_be_merged = self.get_new_unique_images(sha1_images)
    #     #print "[HBaseIndexer.index_batch: log] new_files: {}".format(new_files)
    #     #print "[HBaseIndexer.index_batch: log] new_fulls: {}".format(new_fulls)
    #     #print "[HBaseIndexer.index_batch: log] old_to_be_merged: {}".format(old_to_be_merged)
    #     # Compute missing extractions 
    #     new_sb_files = []
    #     new_files_id = []
    #     for i,nf_extr in enumerate(new_files):
    #         nf,extr = nf_extr
    #         if "sentibank" in extr and new_fulls[i][0][-1] not in new_files_id:
    #             new_sb_files.append(nf)
    #             new_files_id.append(new_fulls[i][0][-1])
    #     extractions = dict()
    #     if new_sb_files:
    #         #print "[HBaseIndexer.index_batch: log] new_sb_files: {}".format(new_sb_files)
    #         #print "[HBaseIndexer.index_batch: log] new_files_id: {}".format(new_files_id)
    #         # Compute features
    #         features_filename, ins_num = self.feature_extractor.compute_features(new_sb_files, update_id)
    #         # Compute hashcodes
    #         hashbits_filepath = self.hasher.compute_hashcodes(features_filename, ins_num, update_id)
    #         norm_features_filename = features_filename[:-4]+"_norm"
    #         # read features and hashcodes and pushback for insertion
    #         #print "Initial features at {}, normalized features {} and hashcodes at {}.".format(features_filename,norm_features_filename,hashbits_filepath)
    #         feats,feats_ok_ids = read_binary_file(norm_features_filename,"feats",new_files_id,self.features_dim*4,np.float32)
    #         hashcodes,hash_ok_ids = read_binary_file(hashbits_filepath,"hashcodes",new_files_id,self.bits_num/8,np.uint8)
    #         #print "Norm features {}\n Hashcodes {}".format(feats,hashcodes)
    #         # should we update local hasher here?
    #         # cleanup
    #         os.remove(norm_features_filename)
    #         os.remove(hashbits_filepath)
    #         for new_file in new_sb_files:
    #             os.remove(new_file)
    #         # need to cleanup images too
    #         if len(feats_ok_ids)!=len(new_files_id) or len(hash_ok_ids)!=len(new_files_id):
    #             print "[HBaseIndexer.index_batch: error] Dimensions mismatch. Are we missing features {} vs. {}, or hashcodes {} vs. {}.".format(len(feats_ok_ids),len(new_files_id),len(hash_ok_ids),len(new_files_id))
    #             return False
    #         for i,sha1 in enumerate(new_files_id):
    #             extractions[sha1] = dict()
    #             sb_col_name = self.extractions_columns[self.extractions_types.index("sentibank")]
    #             hash_col_name = self.extractions_columns[self.extractions_types.index("hashcode")]
    #             extractions[sha1][sb_col_name] = base64.b64encode(feats[i])
    #             extractions[sha1][hash_col_name] = base64.b64encode(hashcodes[i])
    #     # Need to update self.table_cdrinfos_name and self.table_sha1infos_name
    #     # in self.table_sha1infos_name, 
    #     # merge "info:crawl_data.image_id", "info:doc_id", "info:obj_parent"
    #     # into "info:all_htids", "info:all_cdr_ids", "info:all_parent_ids"
    #     # merge old_to_be_merged, no new extractions just push back cdr ids
    #     if old_to_be_merged:
    #         insert_cdrid_sha1 = []
    #         for one_full in old_to_be_merged:
    #             one_val = {self.sha1_column: one_full[-1]}
    #             for tmpk in ["info:obj_parent","info:obj_stored_url"]: 
    #                 if tmpk in one_full[2][2]:
    #                     one_val[tmpk] = one_full[2][2][tmpk]
    #             insert_cdrid_sha1.append((one_full[0],one_val))
    #         if insert_cdrid_sha1:
    #             # First, insert sha1 in self.table_cdrinfos_name
    #             print "[HBaseIndexer.index_batch: log] writing batch from cdr id {} to table {}.".format(insert_cdrid_sha1[0][0],self.table_cdrinfos_name)
    #             self.write_batch(insert_cdrid_sha1,self.table_cdrinfos_name)
    #         old_sha1_format, unique_sha1 = self.format_for_sha1_infos(old_to_be_merged)
    #         #print "[HBaseIndexer.index_batch: log] old_sha1_format: {}".format(old_sha1_format)
    #         #print "[HBaseIndexer.index_batch: log] unique_sha1: {}".format(unique_sha1)
    #         # get corresponding rows
    #         sha1_rows = self.get_full_sha1_rows(unique_sha1)
    #         #print "[HBaseIndexer.index_batch: log] sha1_rows: {}".format(sha1_rows)
    #         # merge
    #         old_sha1_format.extend(sha1_rows)
    #         sha1_rows_merged = self.group_by_sha1(old_sha1_format)
    #         # push merged old images infos
    #         #print "[HBaseIndexer.index_batch: log] sha1_rows_merged: {}".format(sha1_rows_merged)
    #         if sha1_rows_merged:
    #             print "[HBaseIndexer.index_batch: log] writing batch to update old images from {} to table {}.".format(sha1_rows_merged[0][0],self.table_sha1infos_name)
    #             self.write_batch(sha1_rows_merged,self.table_sha1infos_name) 
    #     if new_fulls:
    #         # new_fulls is a list of list
    #         flatten_fulls = []
    #         insert_cdrid_sha1 = []
    #         for one_full_list in new_fulls:
    #             for one_full in one_full_list:
    #                 #print "[HBaseIndexer.index_batch: log] flattening row {} with sha1 {}.".format(one_full[0],one_full[-1])
    #                 flatten_fulls.extend((one_full,))
    #                 one_val = {self.sha1_column: one_full[-1]}
    #                 for tmpk in ["info:obj_parent","info:obj_stored_url"]: 
    #                     if tmpk in one_full[2][2]:
    #                         one_val[tmpk] = one_full[2][2][tmpk]
    #                 insert_cdrid_sha1.append((one_full[0],one_val))
    #         if insert_cdrid_sha1:
    #             # First, insert sha1 in self.table_cdrinfos_name
    #             print "[HBaseIndexer.index_batch: log] writing batch from cdr id {} to table {}.".format(insert_cdrid_sha1[0][0],self.table_cdrinfos_name)
    #             self.write_batch(insert_cdrid_sha1,self.table_cdrinfos_name)
    #         # Then insert sha1 row.
    #         #print "[HBaseIndexer.index_batch: log] flatten_fulls: {}".format(flatten_fulls)
    #         new_sha1_format, unique_sha1 = self.format_for_sha1_infos(flatten_fulls)
    #         new_sha1_rows_merged = self.group_by_sha1(new_sha1_format,extractions)
    #         if new_sha1_rows_merged:
    #             # Finally udpate self.table_updateinfos_name
    #             #print "[HBaseIndexer.index_batch: log] new_sha1_rows_merged: {}".format(new_sha1_rows_merged)
    #             print "[HBaseIndexer.index_batch: log] writing batch of new images from {} to table {}.".format(new_sha1_rows_merged[0][0],self.table_sha1infos_name)
    #             self.write_batch(new_sha1_rows_merged,self.table_sha1infos_name) 
    #     print "[HBaseIndexer.index_batch: log] indexed batch in {}s.".format(time.time()-start_time)

