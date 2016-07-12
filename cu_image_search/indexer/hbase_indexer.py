import os
import sys
import time
import base64
import happybase
import numpy as np
from generic_indexer import GenericIndexer
from ..memex_tools.sha1_tools import get_SHA1_from_file, get_SHA1_from_data
from ..memex_tools.binary_file import read_binary_file

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
        self.hbase_host = self.global_conf['HBI_host']
        self.table_cdrinfos_name = self.global_conf['HBI_table_cdrinfos']
        self.table_sha1infos_name = self.global_conf['HBI_table_sha1infos']
        self.table_updateinfos_name = self.global_conf['HBI_table_updatesinfos']
        self.extractions_types = self.global_conf['HBI_extractions_types']
        self.extractions_columns = self.global_conf['HBI_extractions_columns']
        self.in_url_column = self.global_conf['HBI_in_url_column']
        self.sha1_column = self.global_conf['HBI_sha1_column']
        self.features_dim = self.global_conf["FE_features_dim"]
        self.bits_num = self.global_conf['HA_bits_num']
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
        pass
        ## what to do with missing sha1s here?
        #sha1_list = self.get_sha1s_from_cdrids(list_cdrids)
        #res = self.get_precomp_from_sha1(self,sha1_list,list_type)

    def get_full_sha1_rows(self,list_sha1s):
        rows = None
        if list_sha1s:
            with self.pool.connection() as connection:
                table_sha1infos = connection.table(self.table_sha1infos_name)
                rows = table_sha1infos.rows(list_sha1s)
        return rows

    def get_precomp_from_sha1(self,list_sha1s,list_type):
        res = []
        ok_ids = []
        rows = self.get_full_sha1_rows(list_sha1s)
        # check if we have retrieved rows and extractions for each sha1
        retrieved_sha1s = [row[0] for row in rows]
        # building a list of ok_ids and res for each extraction type
        ok_ids = [[]]*len(list_type)
        res = [[]]*len(list_type)
        for i,sha1 in enumerate(retrieved_sha1s):
            for e,extr in enumerate(self.extractions_types):
                #print i,sha1,e,extr
                #print len(ok_ids)
                extr_column = self.extractions_columns[self.extractions_types.index(extr)]
                if extr_column in rows[i][1]:
                    ok_ids[e].append(list_sha1s.index(sha1))
                    res[e].append(rows[i][1][extr_column])
        return res,ok_ids

    def get_new_unique_images(self,sha1_images):
        new_files = []
        new_fulls = []
        old_to_be_merged = []
        # get unique images 
        sha1_list = [img_item[-1] for img_item in sha1_images]
        unique_sha1 = sorted(set(sha1_list))
        print "[HBaseIndexer.get_new_unique_images: log] We have {} unique images.".format(len(unique_sha1))
        unique_idx = [sha1_list.index(sha1) for sha1 in unique_sha1]
        full_idx = [unique_sha1.index(sha1) for sha1 in sha1_list]
        # get what we already have indexed in 'table_sha1infos_name'
        res, ok_ids = self.get_precomp_from_sha1(list(unique_sha1),self.extractions_types)
        # check which images already exists and what extractions are present
        for i,sha1 in enumerate(unique_sha1):
            missing_extr = []
            for e,extr in enumerate(self.extractions_types):
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

    def format_for_sha1_infos(self,list_images):
        sha1_infos = []
        unique_sha1 = set()
        # keys are "info:all_cdr_ids", "info:all_parent_ids", "info:all_htids", "info:featnorm_cu", "info:s3_url", "info:hash256_cu"
        for image in list_images:
            tmp = dict()
            sha1 = image[-1]
            print "{}: {}".format(sha1,image)
            unique_sha1.add(sha1)
            tmp["info:all_cdr_ids"] = image[0]
            tmp["info:all_parent_ids"] = image[2][2]["info:obj_parent"]
            tmp["info:all_htids"] = image[2][2]["info:crawl_data.image_id"]
            tmp["info:s3_url"] = image[1]
            sha1_infos.append((sha1,tmp))
        return sha1_infos,unique_sha1
            
    def add_extractions_to_row(self,row,extractions):
        """ Add newly extracted columns to row.

        :param row: tuple (sha1,row_data)
        :type row: tuple
        :param extractions: dictionary of extractions, keys should be columns name.
        :type extractions: dict
        """
        out_values = row[1]
        for extr in extractions.keys():
            out_values[extr] = extractions[extr]
        return (row[0],out_values)

    def merge_two_rows(self,row,tmp):
        if row[0]!=tmp[0]:
            raise ValueError("[HBaseIndexer.merge_two_rows: error] tried to merge two rows with different keys {} and {}.".format(row[0],tmp[0]))
        all_keys = set(row[1].keys())
        all_keys.union(set(tmp[1].keys()))
        out = dict()
        for key in all_keys:
            # some values are list and should be merged
            if key in row[1] and key in tmp[1] and key != 'info:s3_url' and key not in self.extractions_columns:
                out[key] = ','.join(list(set(row[1][key].split(',')) | set(tmp[1][key].split(','))))               
            # for others a single value should be kept: info:s3_url, and the extractions.
            else:
                if key in row[1]:
                    out[key] = row[1][key]
                elif key in tmp[1]:
                    out[key] = tmp[1][key]       
        out_row = (row[0],out)
        return out_row

    def group_by_sha1(self,list_images,extractions=None):
        out = []
        sha1_list = []
        print "[group_by_sha1: log] list_images {}.".format(list_images)
        for tmp in list_images:
            if tmp[0] not in sha1_list:
                sha1_list.append(tmp[0])
                if extractions and tmp[0] in extractions:
                    tmp = self.add_extractions_to_row(row,extractions[tmp[0]])
                out.append(tmp)
            else:
                pos = sha1_list.index(tmp[0])
                row = out[pos]
                # merge
                new_row = self.merge_two_rows(row,tmp)
                if extractions and tmp[0] in extractions:
                    new_row = self.add_extractions_to_row(row,extractions[tmp[0]])
                # update
                out[pos] = new_row
        return out
        
    def write_batch(self,batch,tab_out_name):
        with self.pool.connection() as connection:
            tab_out = connection.table(tab_out_name)
            batch_write = tab_out.batch()
            print "Pushing batch from {}.".format(batch[0][0])
            for row in batch:
                batch_write.put(row[0],row[1])
        batch_write.send()

    def index_batch(self,batch):
        """ Index a batch in the form of a list of (cdr_id,url,[extractions,ts_cdrid,other_data])
        """
        # Download images
        timestr = time.strftime("%b-%d-%Y-%H-%M-%S", time.localtime(time.time()))
        #startid = str(batch[0][0])
        #lastid = str(batch[-1][0])
        #update_id = timestr+'_'+startid+'_'+lastid
        update_id = timestr
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
        new_files_id = []
        for i,nf_extr in enumerate(new_files):
            nf,extr = nf_extr
            if "sentibank" in extr and new_fulls[i][0][-1] not in new_files_id:
                new_sb_files.append(nf)
                new_files_id.append(new_fulls[i][0][-1])
        if new_sb_files:
            print "[HBaseIndexer.index_batch: log] new_sb_files: {}".format(new_sb_files)
            print "[HBaseIndexer.index_batch: log] new_files_id: {}".format(new_files_id)
            # Compute features
            features_filename, ins_num = self.feature_extractor.compute_features(new_sb_files, update_id)
            # Compute hashcodes
            hashbits_filepath = self.hasher.compute_hashcodes(features_filename, ins_num, update_id)
            norm_features_filename = features_filename[:-4]+"_norm"
            # read features and hashcodes and pushback for insertion
            print "Initial features at {}, normalized features {} and hashcodes at {}.".format(features_filename,norm_features_filename,hashbits_filepath)
            feats,feats_ok_ids = read_binary_file(norm_features_filename,"feats",new_files_id,self.features_dim*4,np.float32)
            hashcodes,hash_ok_ids = read_binary_file(hashbits_filepath,"hashcodes",new_files_id,self.bits_num/8,np.uint8)
            print "Norm features {}\n Hashcodes {}".format(feats,hashcodes)
            if len(feats_ok_ids)!=len(new_files_id) or len(hash_ok_ids)!=len(new_files_id):
                print "[HBaseIndexer.index_batch: error] Dimensions mismatch. Are we missing features {} vs. {}, or hashcodes {} vs. {}.".format(len(feats_ok_ids),len(new_files_id),len(hash_ok_ids),len(new_files_id))
                return False
            extractions = dict()
            for i,sha1 in enumerate(new_files_id):
                extractions[sha1] = dict()
                sb_col_name = self.extractions_columns[self.extractions_types.index("sentibank")]
                hash_col_name = self.extractions_columns[self.extractions_types.index("hashcode")]
                extractions[sha1][sb_col_name] = base64.b64encode(feats[i])
                extractions[sha1][hash_col_name] = base64.b64encode(hashcodes[i])
        # Need to update self.table_cdrinfos_name and self.table_sha1infos_name
        # in self.table_sha1infos_name, 
        # merge "info:crawl_data.image_id", "info:doc_id", "info:obj_parent"
        # into "info:all_htids", "info:all_cdr_ids", "info:all_parent_ids"
        # merge old_to_be_merged, no new extractions just merge ids
        old_sha1_format, unique_sha1 = self.format_for_sha1_infos(old_to_be_merged)
        print "[HBaseIndexer.index_batch: log] old_sha1_format: {}".format(old_sha1_format)
        print "[HBaseIndexer.index_batch: log] unique_sha1: {}".format(unique_sha1)
        # get corresponding rows
        sha1_rows = self.get_full_sha1_rows(unique_sha1)
        #print "[HBaseIndexer.index_batch: log] sha1_rows: {}".format(sha1_rows)
        # merge
        old_sha1_format.extend(sha1_rows)
        sha1_rows_merged = self.group_by_sha1(old_sha1_format)
        # push merged old images infos
        print "[HBaseIndexer.index_batch: log] sha1_rows_merged: {}".format(sha1_rows_merged)
        # insert new images
        print "[HBaseIndexer.index_batch: log] writing batch from {} to table {}.".format(sha1_rows_merged[0][0],self.table_sha1infos_name)
        self.write_batch(sha1_rows_merged,self.table_sha1infos_name) 
        # new_fulls is a list of list
        flatten_fulls = []
        insert_cdrid_sha1 = []
        for one_full_list in new_fulls:
            for one_full in one_full_list:
                flatten_fulls.extend(one_full)
                insert_cdrid_sha1.append((one_full[0],{self.sha1_column: one_ful[-1]}))
        # First, insert sha1 in self.table_cdrinfos_name
        self.write_batch(insert_cdrid_sha1,self.table_cdrinfos_name)
        # Then insert sha1 row.
        new_sha1_format, unique_sha1 = self.format_for_sha1_infos(flatten_fulls)
        new_sha1_rows_merged = self.group_by_sha1(new_sha1_format,extractions)
        # Finally udpate self.table_updateinfos_name
        print "[HBaseIndexer.index_batch: log] new_sha1_rows_merged: {}".format(new_sha1_rows_merged)
        print "[HBaseIndexer.index_batch: log] writing batch from {} to table {}.".format(new_sha1_rows_merged[0][0],self.table_sha1infos_name)
        #self.write_batch(sha1_rows_merged,self.table_sha1infos_name) 
        
        


        
