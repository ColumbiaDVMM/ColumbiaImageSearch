import os
import sys
import time
import MySQLdb
from generic_indexer import GenericIndexer
from ..memex_tools.sha1_tools import get_SHA1_from_file, get_SHA1_from_data

class HBaseIndexer(GenericIndexer):

    def read_conf(self):
        """ Reads configuration parameters.

        Will read parameters 'HBI_image_downloader', 'HBI_hasher', 
        'HBI_feature_extractor', 'LI_master_update_filepath' and 'LI_base_update_path'
        from self.global_conf.
        """
        self.image_downloader_type = self.global_conf['HBI_image_downloader']
        self.hasher_type = self.global_conf['HBI_hasher']
        self.feature_extractor_type = self.global_conf['HBI_feature_extractor']
        self.table_sha1infos_name = self.global_conf['HBI_table_sha1infos']
        self.table_updateinfos_name = self.global_conf['HBI_table_updatesinfos']
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
        # How to deal with additionals extractions?
        # Where should we store the update infos? 
        # Start of batch being indexed should be end criterion for next batch.

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
        else:
            raise ValueError("[HBaseIndexer.initialize_hasher: error] Unknown hasher_type: {}.".format(self.hasher_type))

    def get_next_batch_start(self):
        """ Get start value for next update batch. 

        :returns timestamp_cdrid: First row timestamp_cdrid in 'table_updateinfos_name'.
        """
        with self.pool.connection() as connection:
            table_updateinfos = connection.table(self.table_updateinfos_name)
            for row in table_updateinfos.scan():
                return row[0]

    def get_precomp_from_cdrids(self,list_cdrids,list_type):
        # what to do with missing sha1s here?
        sha1_list = self.get_sha1s_from_cdrids(list_cdrids)
        res = self.get_precomp_from_sha1(self,sha1_list,list_type)

    def get_precomp_from_sha1(self,list_sha1s,list_type):
        res = []
        if list_sha1s:
            with self.pool.connection() as connection:
                table_sha1infos = connection.table(self.table_sha1infos_name)
                rows = table_sha1infos.rows(list_sha1s)
            # check if we have retrieved rows and extractions for each sha1
            retrieved_sha1s = [row[0] for row in rows]
            # building a list of ok_ids and res for each extraction type
            ok_ids = []*len(list_type)
            res = []*len(list_type)
            for i,sha1 in enumerate(retrieved_sha1s):
                for extr in list_type:
                    if extr in self.extractions_types:
                        extr_column = self.extractions_columns[self.extractions_types.index(extr)]
                        if extr_column in rows[i][1]:
                            ok_ids[e].append(list_sha1s.index(sha1))
                            res[e].append(rows[i][1][extr_column]])
                    else:
                        raise ValueError("[HBaseIndexer.get_precomp_from_sha1: error] Unknown extraction type: {}.".format(extr))
        return res,ok_ids

    def get_new_unique_images(self,sha1_images):
        new_uniques = []
        new_fulls = []
        old_to_be_merged = []
        # get unique images 
        sha1_list = [img_item[-1] for img_item in sha1_images]
        unique_sha1 = sorted(set(sha1_list))
        print "[HBaseIndexer.get_new_unique_images: log] We have {} unique images.".format(len(unique_sha1))
        ## only first time appearing?
        unique_idx = [sha1_list.index(sha1) for sha1 in unique_sha1]
        full_idx = [unique_sha1.index(sha1) for sha1 in sha1_list]
        # first build superset of extractions
        extractions = set()
        for tmp_img in sha1_images:
            extractions.add(tmp_img[2][0])
        list_type = list(extractions)
        # get what we already have indexed in 'table_sha1infos_name'
        res, ok_ids = self.get_precomp_from_sha1(list(unique_sha1),list_type)
        # check which images already exists and what extractions are present
        for i,sha1 in enumerate(unique_sha1):
            missing_extr = []
            for e,extr in enumerate(list_type):
                if i not in ok_ids[e]:
                    # we don't have this extraction for that image
                    missing_extr.append(extr)
            all_orig_ids = [k for k,j in enumerate(full_idx) if j==i]
            # we already have all extractions
            if not missing_extr:
                # we should just merge the cdr_ids and obj_parent etc
                old_to_be_merged.extend([sha1_images[k] for k in all_orig_ids])
            else:
                # we need to compute new extractions
                orig_pos = unique_idx[i]
                new_files.append((sha1_images[orig_pos][-2],missing_extr))
                new_fulls.append([sha1_images[k] for k in all_orig_ids])

        return new_files, new_fulls, old_to_be_merged
        

    def index_batch(self,batch):
        """ Index a batch in the form of a list of (cdr_id,url,[extractions,ts_cdrid,other_data])
        """
        # Download images
        timestr= time.strftime("%b-%d-%Y-%H-%M-%S", time.localtime(time.time()))
        startid = str(batch[0][0])
        lastid = str(batch[-1][0])
        update_id = timestr+'_'+startid+'_'+lastid
        print "[HBaseIndexer.index_batch: log] Starting udpate {}".format(update_id)
        readable_images = self.image_downloader.download_images(batch,update_id)
        #print readable_images
        # Compute sha1
        sha1_images = [img+(get_SHA1_from_file(img[-1]),) for img in readable_images]
        # Now that we have sha1s, check if we actually don't already have all extractions
        new_files, new_fulls, old_to_be_merged = self.get_new_unique_images(sha1_images)
        print "[HBaseIndexer.index_batch: log] new_files: {}".format(new_files)
        print "[HBaseIndexer.index_batch: log] new_fulls: {}".format(new_fulls)
        print "[HBaseIndexer.index_batch: log] old_to_be_merged: {}".format(old_to_be_merged)
        # Compute missing extractions 
        new_sb_files = []
        for nf,extr in new_files:
            if "sentibank" in extr:
                new_sb_files.append(nf)
        if new_sb_files:
            print "[HBaseIndexer.index_batch: log] new_sb_files: {}".format(new_sb_files)
            # Compute features
            features_filename, ins_num = self.feature_extractor.compute_features(new_files, update_id)
            # Compute hashcodes
            hashbits_filepath = self.hasher.compute_hashcodes(features_filename, ins_num, update_id)

        # Check at what point features are normalized
        # Insert new ids
        


        
