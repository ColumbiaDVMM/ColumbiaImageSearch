import os
import sys
import time
import base64
import shutil
import happybase
import numpy as np
from generic_indexer import GenericIndexer
from ..memex_tools.sha1_tools import get_SHA1_from_file, get_SHA1_from_data
from ..memex_tools.binary_file import read_binary_file, write_binary_file

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
        self.sha1_featid_mapping_filename = self.global_conf['HBI_sha1_featid_mapping_filename']
        self.initialize_sha1_mapping()
        self.refresh_batch_size = 1000
        if len(self.extractions_columns) != len(self.extractions_types):
            raise ValueError("[HBaseIngester.initialize_source: error] Dimensions mismatch {} vs. {} for extractions_columns vs. extractions_types".format(len(self.extractions_columns),len(self.extractions_types)))
        self.nb_threads = 2
        if 'HBI_pool_thread' in self.global_conf:
            self.nb_threads = self.global_conf['HBI_pool_thread']
        self.pool = happybase.ConnectionPool(size=self.nb_threads,host=self.hbase_host)
        # How to deal with additionals extractions?
        # Where should we store the update infos? 
        # Start of batch being indexed should be end criterion for next batch.

    def initialize_sha1_mapping(self):
        self.sha1_featid_mapping = []
        # read from self.sha1_featid_mapping_filename
        try:
            with open(self.sha1_featid_mapping_filename,'rt') as sha1_fid:
                self.sha1_featid_mapping = [line.strip() for line in sha1_fid]
        except Exception as inst:
            print "[HBaseIndexer.initialize_sha1_mapping: error] Could not initialize sha1_featid_mapping from {}.\n{}".format(sha1_featid_mapping_filename,inst)

    def save_sha1_mapping(self):
        with open(self.sha1_featid_mapping_filename,'wt') as sha1_fid:
            for sha1 in self.sha1_featid_mapping:
                sha1_fid.write(sha1.strip()+'\n')
        except Exception as inst:
            print "[HBaseIndexer.save_sha1_mapping: error] Could not save sha1_featid_mapping to {}.\n{}".format(sha1_featid_mapping_filename,inst)


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

    def get_ids_from_sha1s(self,list_sha1s):
        # we should have a list of all indexed sha1, 
        # where the index corresponds to position of the hashcode and features in the binary file.
        pass

    def get_full_sha1_rows(self,list_sha1s):
        rows = None
        if list_sha1s:
            with self.pool.connection() as connection:
                table_sha1infos = connection.table(self.table_sha1infos_name)
                rows = table_sha1infos.rows(list_sha1s)
        return rows

    def get_precomp_from_sha1(self,list_sha1s,list_type):
        """ Retrieves the 'list_type' extractions results from HBase for the image in 'list_sha1s'.

        :param list_sha1s: list of sha1s of the images for which the extractions are requested.
        :type list_sha1s: list
        :param list_type: list of the extractions requested. They have to be a subset of *self.extractions_types*
        :type list_sha1s: list
        :returns res,ok_ids: *res* contains the extractions, *ok_ids* the ids of the 'list_sha1s' for which we retrieved something.
        """
        res = []
        ok_ids = []
        rows = self.get_full_sha1_rows(list_sha1s)
        # check if we have retrieved rows and extractions for each sha1
        retrieved_sha1s = [row[0] for row in rows]
        # building a list of ok_ids and res for each extraction type
        ok_ids = [[] for i in range(len(list_type))]
        res = [[] for i in range(len(list_type))]
        list_columns = self.get_columns_name(list_type)
        for i,sha1 in enumerate(retrieved_sha1s):
            for e in range(len(list_type)):
                if list_columns[e] in rows[i][1]:
                    #print "[get_precomp_from_sha1] {} {} {} {}.".format(i,sha1,e,list_columns[e])
                    ok_ids[e].append(list_sha1s.index(sha1))
                    res[e].append(rows[i][1][list_columns[e]])
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
            #print "{}: {}".format(sha1,image)
            unique_sha1.add(sha1)
            tmp["info:all_cdr_ids"] = image[0]
            image_dict = image[2][2]
            tmp["info:all_parent_ids"] = image_dict["info:obj_parent"]
            # this field may be absent
            if "info:crawl_data.image_id" in image_dict:
                tmp["info:all_htids"] = image_dict["info:crawl_data.image_id"]
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
        #print "[HBaseIndexer.add_extractions_to_row: log] adding extractions {} to row {}.".format(extractions.keys(),row[0])
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
        #print "[group_by_sha1: log] list_images {}.".format(list_images)
        for tmp in list_images:
            if tmp[0] not in sha1_list:
                sha1_list.append(tmp[0])
                if extractions and tmp[0] in extractions:
                    tmp = self.add_extractions_to_row(tmp,extractions[tmp[0]])
                out.append(tmp)
            else:
                pos = sha1_list.index(tmp[0])
                row = out[pos]
                # merge
                new_row = self.merge_two_rows(row,tmp)
                if extractions and tmp[0] in extractions:
                    new_row = self.add_extractions_to_row(new_row,extractions[tmp[0]])
                # update
                out[pos] = new_row
        return out
        
    def write_batch(self,batch,tab_out_name):
        with self.pool.connection() as connection:
            tab_out = connection.table(tab_out_name)
            batch_write = tab_out.batch()
            #print "Pushing batch from {}.".format(batch[0][0])
            for row in batch:
                #print "[HBaseIndexer.write_batch: log] Pushing row {} with keys {}".format(row[0],row[1].keys())
                batch_write.put(row[0],row[1])
            batch_write.send()

    def get_columns_name(self,list_type):
        list_columns = []
        for e,extr in enumerate(list_type):
            if extr not in self.extractions_types:
                raise ValueError("[HBaseIndexer.get_columns_name: error] Unknown extraction type \"{}\".".format(extr))
            pos = self.extractions_types.index(extr)
            list_columns.append(self.extractions_columns[pos])
        return list_columns

    def save_refresh_batch(self,refresh_batch,base_update_path,tmp_udpate_id):
        import base64
        tmp_sha1_featid_mapping = []
        if refresh_batch:
            list_feats = []
            list_hashcodes = []
            for row in refresh_batch:
                tmp_sha1_featid_mapping.append(row[0].strip())
                list_feats.append(base64.b64decode(row[1]))
                list_hashcodes.append(base64.b64decode(row[2]))
            # save features in base_update_path/features
            write_binary_file(os.path.join(base_update_path,'features',tmp_udpate_id+'_norm'),list_feats)
            # save hashcodes in base_update_path/hash_bits
            write_binary_file(os.path.join(base_update_path,'hash_bits',tmp_udpate_id+'_itq_norm_'+str(self.bits_num)),list_hashcodes)
        # returns tmp_sha1_featid_mapping
        return tmp_sha1_featid_mapping

    def merge_refresh_batch(self,refresh_batch):
        if refresh_batch:
            # [Create a temporary HasherCmdLine] have a temporary "master_update" file for that batch
            from ..hasher.hasher_cmdline import HasherCmdLine
            tmp_hasher = HasherCmdLine(self.global_conf_filename)
            tmp_udpate_id = str(time.time())+'_'+refresh_batch[0][0]
            tmp_hasher.master_update_file = "update_"+tmp_udpate_id
            with open(os.path.join(self.hasher.base_update_path,tmp_hasher.master_update_file),'wt') as tm_uf:
                tm_uf.write(tmp_udpate_id+'\n')
            # save features (and hashcodes) and compress features, have a temporary mapping sha1 - feat_id. 
            tmp_sha1_featid_mapping = self.save_refresh_batch(refresh_batch,self.hasher.base_update_path,tmp_udpate_id)
            tmp_hasher.compress_feats()
            # - For idx files and mapping sha1 - feat_id need to shift by last compressed feats end idx in self.hasher
            max_id = self.hasher.get_max_feat_id()
            nb_indexed = len(self.sha1_featid_mapping)
            print "We have {} features, {} listed in sha1_featid_mapping.".format(max_id,nb_indexed)
            if max_id != nb_indexed:
                raise ValueError("[HBaseIndexer.merge_refresh_batch:error] max_id!=nb_indexed: {} vs. {}.".format(max_id,nb_indexed))
            # - Merge with previous updates, i.e. concatenate hashcodes and compressed features. 
            print "Should merge files listed in {} to new file listed in {}.".format(self.hasher.master_update_file,tmp_hasher.master_update_file)
            previous_files = []
            with open(self.hasher.base_update_path,self.hasher.master_update_file, 'rt') as m_uf:
                for line in m_uf:
                    previous_files.append(line.strip())
            # should we actually impose a limit on the file dimension: XXGB??
            if len(previous_files)>1:
                raise ValueError("[HBaseIndexer.merge_refresh_batch:error] was expecting a single file, found {}.".format(len(previous_files)))
            if previous_files:
                # actually do a merge
                # TODO put this in a method
                out_update_id = str(time.time())+'_'+refresh_batch[0][0]
                # use shutil.copyfileobj for comp features
                out_comp_fn = os.path.join(self.hasher.base_update_path,'comp_features',out_update_id+'_comp_norm')
                with open(out_comp_fn,'wb') as out_comp:
                    prev_comp_feat_fn = os.path.join(self.hasher.base_update_path,'comp_features',previous_files[0]+'_comp_norm')
                    new_comp_feat_fn = os.path.join(self.hasher.base_update_path,'comp_features',tmp_udpate_id+'_comp_norm')
                    comp_idx_shift = os.stat(comp_feat_fn).st_size
                    with open(prev_comp_feat_fn,'rb') as prev_comp, open(new_comp_feat_fn,'rb') as new_comp:
                            shutil.copyfileobj(prev_comp, out_comp)
                            shutil.copyfileobj(new_comp, out_comp)
                # use shutil.copyfileobj for and hashcodes
                out_hash_fn = os.path.join(self.hasher.base_update_path,'hashcodes',out_update_id+'_itq_norm_'+str(self.bits_num))
                with open(out_hash_fn,'wb') as out_hash:
                    prev_hashcode_fn = os.path.join(self.hasher.base_update_path,'hashcodes',previous_files[0]+'_itq_norm_'+str(self.bits_num))
                    new_hashcode_fn = os.path.join(self.hasher.base_update_path,'hashcodes',tmp_udpate_id+'_itq_norm_'+str(self.bits_num))
                    with open(prev_hashcode_fn,'rb') as prev_hash, open(new_hashcode_fn,'rb') as new_hash:
                            shutil.copyfileobj(prev_hash, out_hash)
                            shutil.copyfileobj(new_hash, out_hash)
                # but need to read and shift tmp_udpate comp_idx using what?
                out_comp_idx_fn = os.path.join(self.hasher.base_update_path,'comp_idx',out_update_id+'_itq_norm_'+str(self.bits_num))
                with open(out_comp_idx_fn,'wb') as out_comp_idx:
                    prev_comp_idx_fn = os.path.join(self.hasher.base_update_path,'comp_idx',previous_files[0]+'_itq_norm_'+str(self.bits_num))
                    new_comp_idx_fn = os.path.join(self.hasher.base_update_path,'comp_idx',tmp_udpate_id+'_itq_norm_'+str(self.bits_num))
                    with open(prev_comp_idx_fn),'rb') as prev_hash:
                            shutil.copyfileobj(prev_hash, out_hash)
                    arr = np.fromfile(new_comp_idx_fn, dtype=np.uint64)
                    arr += comp_idx_shift
                    arr.tofile(out_hash)
                # update sha1_featid_mapping
                self.sha1_featid_mapping.extend(tmp_sha1_featid_mapping)
                self.save_sha1_mapping()
                self.hasher.master_update_file = "update_"+out_update_id
                # - delete features file and any temporary file.

            else: # first batch, just copy
                # double check that shift_id and nb_indexed == 0?
                with open(self.hasher.base_update_path,self.hasher.master_update_file, 'wt') as m_uf:
                    m_uf.write(tmp_udpate_id+'\n')
            

    def refresh_hash_index(self):
        start_row = None
        list_type = ["sentibank","hashcode"]
        list_columns = self.get_columns_name(list_type)
        refresh_batch = []
        with self.pool.connection() as connection:
            table_sha1infos = connection.table(self.table_sha1infos_name)
            for row in table_sha1infos.scan(row_start=start_row,batch_size=self.refresh_batch_size):
                if row[0] not in self.sha1_featid_mapping: # new sha1
                    found_columns = [column for column in list_columns if column in row[1]]
                    if len(found_columns)==len(list_type): # we have features and hashcodes
                        refresh_batch.append((row[0],row[1][list_columns[0]],row[1][list_columns[1]]))
                # merge if we have a complete batch
                if len(refresh_batch)>=self.refresh_batch_size:
                    self.merge_refresh_batch(refresh_batch)
                    refresh_batch = []
                start_row = row[0]
            # last batch
            if refresh_batch:
                self.merge_refresh_batch(refresh_batch)
        

    def index_batch(self,batch):
        """ Index a batch in the form of a list of (cdr_id,url,[extractions,ts_cdrid,other_data])
        """
        # Download images
        start_time = time.time() 
        timestr = time.strftime("%b-%d-%Y-%H-%M-%S", time.localtime(start_time))
        #startid = str(batch[0][0])
        #lastid = str(batch[-1][0])
        #update_id = timestr+'_'+startid+'_'+lastid
        update_id = timestr
        print "[HBaseIndexer.index_batch: log] Starting udpate {}".format(update_id)
        readable_images = self.image_downloader.download_images(batch,update_id)
        #print readable_images
        if not readable_images:
            print "[HBaseIndexer.index_batch: log] No readable images!"
            return None
        # Compute sha1
        sha1_images = [img+(get_SHA1_from_file(img[-1]),) for img in readable_images]
        # Now that we have sha1s, check if we actually don't already have all extractions
        new_files, new_fulls, old_to_be_merged = self.get_new_unique_images(sha1_images)
        #print "[HBaseIndexer.index_batch: log] new_files: {}".format(new_files)
        #print "[HBaseIndexer.index_batch: log] new_fulls: {}".format(new_fulls)
        #print "[HBaseIndexer.index_batch: log] old_to_be_merged: {}".format(old_to_be_merged)
        # Compute missing extractions 
        new_sb_files = []
        new_files_id = []
        for i,nf_extr in enumerate(new_files):
            nf,extr = nf_extr
            if "sentibank" in extr and new_fulls[i][0][-1] not in new_files_id:
                new_sb_files.append(nf)
                new_files_id.append(new_fulls[i][0][-1])
        extractions = dict()
        if new_sb_files:
            #print "[HBaseIndexer.index_batch: log] new_sb_files: {}".format(new_sb_files)
            #print "[HBaseIndexer.index_batch: log] new_files_id: {}".format(new_files_id)
            # Compute features
            features_filename, ins_num = self.feature_extractor.compute_features(new_sb_files, update_id)
            # Compute hashcodes
            hashbits_filepath = self.hasher.compute_hashcodes(features_filename, ins_num, update_id)
            norm_features_filename = features_filename[:-4]+"_norm"
            # read features and hashcodes and pushback for insertion
            #print "Initial features at {}, normalized features {} and hashcodes at {}.".format(features_filename,norm_features_filename,hashbits_filepath)
            feats,feats_ok_ids = read_binary_file(norm_features_filename,"feats",new_files_id,self.features_dim*4,np.float32)
            hashcodes,hash_ok_ids = read_binary_file(hashbits_filepath,"hashcodes",new_files_id,self.bits_num/8,np.uint8)
            #print "Norm features {}\n Hashcodes {}".format(feats,hashcodes)
            # should we update local hasher here?
            # cleanup
            os.remove(norm_features_filename)
            os.remove(hashbits_filepath)
            for new_file in new_sb_files:
                os.remove(new_file)
            # need to cleanup images too
            if len(feats_ok_ids)!=len(new_files_id) or len(hash_ok_ids)!=len(new_files_id):
                print "[HBaseIndexer.index_batch: error] Dimensions mismatch. Are we missing features {} vs. {}, or hashcodes {} vs. {}.".format(len(feats_ok_ids),len(new_files_id),len(hash_ok_ids),len(new_files_id))
                return False
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
        # merge old_to_be_merged, no new extractions just push back cdr ids
        if old_to_be_merged:
            insert_cdrid_sha1 = []
            for one_full in old_to_be_merged:
                one_val = {self.sha1_column: one_full[-1]}
                for tmpk in ["info:obj_parent","info:obj_stored_url"]: 
                    if tmpk in one_full[2][2]:
                        one_val[tmpk] = one_full[2][2][tmpk]
                insert_cdrid_sha1.append((one_full[0],one_val))
            if insert_cdrid_sha1:
                # First, insert sha1 in self.table_cdrinfos_name
                print "[HBaseIndexer.index_batch: log] writing batch from cdr id {} to table {}.".format(insert_cdrid_sha1[0][0],self.table_cdrinfos_name)
                self.write_batch(insert_cdrid_sha1,self.table_cdrinfos_name)
            old_sha1_format, unique_sha1 = self.format_for_sha1_infos(old_to_be_merged)
            #print "[HBaseIndexer.index_batch: log] old_sha1_format: {}".format(old_sha1_format)
            #print "[HBaseIndexer.index_batch: log] unique_sha1: {}".format(unique_sha1)
            # get corresponding rows
            sha1_rows = self.get_full_sha1_rows(unique_sha1)
            #print "[HBaseIndexer.index_batch: log] sha1_rows: {}".format(sha1_rows)
            # merge
            old_sha1_format.extend(sha1_rows)
            sha1_rows_merged = self.group_by_sha1(old_sha1_format)
            # push merged old images infos
            #print "[HBaseIndexer.index_batch: log] sha1_rows_merged: {}".format(sha1_rows_merged)
            if sha1_rows_merged:
                print "[HBaseIndexer.index_batch: log] writing batch to update old images from {} to table {}.".format(sha1_rows_merged[0][0],self.table_sha1infos_name)
                self.write_batch(sha1_rows_merged,self.table_sha1infos_name) 
        if new_fulls:
            # new_fulls is a list of list
            flatten_fulls = []
            insert_cdrid_sha1 = []
            for one_full_list in new_fulls:
                for one_full in one_full_list:
                    #print "[HBaseIndexer.index_batch: log] flattening row {} with sha1 {}.".format(one_full[0],one_full[-1])
                    flatten_fulls.extend((one_full,))
                    one_val = {self.sha1_column: one_full[-1]}
                    for tmpk in ["info:obj_parent","info:obj_stored_url"]: 
                        if tmpk in one_full[2][2]:
                            one_val[tmpk] = one_full[2][2][tmpk]
                    insert_cdrid_sha1.append((one_full[0],one_val))
            if insert_cdrid_sha1:
                # First, insert sha1 in self.table_cdrinfos_name
                print "[HBaseIndexer.index_batch: log] writing batch from cdr id {} to table {}.".format(insert_cdrid_sha1[0][0],self.table_cdrinfos_name)
                self.write_batch(insert_cdrid_sha1,self.table_cdrinfos_name)
            # Then insert sha1 row.
            #print "[HBaseIndexer.index_batch: log] flatten_fulls: {}".format(flatten_fulls)
            new_sha1_format, unique_sha1 = self.format_for_sha1_infos(flatten_fulls)
            new_sha1_rows_merged = self.group_by_sha1(new_sha1_format,extractions)
            if new_sha1_rows_merged:
                # Finally udpate self.table_updateinfos_name
                #print "[HBaseIndexer.index_batch: log] new_sha1_rows_merged: {}".format(new_sha1_rows_merged)
                print "[HBaseIndexer.index_batch: log] writing batch of new images from {} to table {}.".format(new_sha1_rows_merged[0][0],self.table_sha1infos_name)
                self.write_batch(new_sha1_rows_merged,self.table_sha1infos_name) 
        print "[HBaseIndexer.index_batch: log] indexed batch in {}s.".format(time.time()-start_time)
                
        


        
