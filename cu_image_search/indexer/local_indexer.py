import os
import time
from generic_indexer import GenericIndexer
from ..memex_tools.sha1_tools import get_SHA1_from_file, get_SHA1_from_data

# Make that a local file indexer scheme
class LocalIndexer(GenericIndexer):

    def read_conf(self):
        """ Reads configuration parameters.

        Will read parameters 'LI_image_downloader', 'LI_hasher', 
        'LI_feature_extractor', 'LI_master_update_filepath' and 'LI_base_update_path'
        from self.global_conf.
        """
        self.image_downloader_type = self.global_conf['LI_image_downloader']
        self.hasher_type = self.global_conf['LI_hasher']
        self.feature_extractor_type = self.global_conf['LI_feature_extractor']
        self.master_update_filepath = self.global_conf['LI_master_update_filepath']
        self.base_update_path = self.global_conf['LI_base_update_path']
        self.save_index_path = self.global_conf['LI_save_index_path']
        # New things we should save?
        self.set_indexed_sha1 = set()
        self.list_indexed_sha1 = []
        self.max_file_id = 0
        self.unique_images = []
        self.full_images = []

    def initialize_indexer_backend(self):
        """ Initialize backend.
        """
        print "[LocalIndexer: log] initialized with:\n\
                    \t- image_downloader: {}\n\
                    \t- feature_extractor_type: {}\n\
                    \t- hasher_type: {}".format(self.image_downloader_type,
                        self.feature_extractor_type,self.hasher_type)
        # Initialize feature_extractor and hasher
        self.initialize_feature_extractor()
        self.initialize_hasher()
        # Load from save index path?
        self.load_from_disk()

    def initialize_feature_extractor(self):
        if self.feature_extractor_type=="sentibank_cmdline":
            from ..feature_extractor.sentibank_cmdline import SentiBankCmdLine
            self.feature_extractor = SentiBankCmdLine(self.global_conf_filename)
        elif self.feature_extractor_type == "sentibank_tensorflow":
            from ..feature_extractor.sentibank_tensorflow import SentiBankTensorflow
            self.feature_extractor = SentiBankTensorflow(self.global_conf_filename)
        else:
            raise ValueError("[LocalIndexer.initialize_feature_extractor: error] Unknown feature_extractor_type: {}.".format(self.feature_extractor_type))

    def initialize_hasher(self):
        if self.hasher_type=="hasher_cmdline":
            from ..hasher.hasher_cmdline import HasherCmdLine
            self.hasher = HasherCmdLine(self.global_conf_filename)
        elif self.hasher_type == "hasher_swig":
            from ..hasher.hasher_swig import HasherSwig
            print "[HBaseIndexer.initialize_hasher: log] Setting hasher_type to hasher_swig."
            self.hasher = HasherSwig(self.global_conf_filename)
        else:
            raise ValueError("[LocalIndexer.initialize_hasher: error] Unknown hasher_type: {}.".format(self.hasher_type))

    def get_next_batch_start(self):
        """ Get start value for next update batch
        :returns htid: Biggest htid in local database.
        """
        return self.max_file_id

    def get_precomp_from_ids(self,list_ids,list_type):
        res = []
        if "feats" in list_type:
            res.append(self.hasher.get_precomp_feats(list_ids))
        if "hashcodes" in list_type:
            res.append(self.hasher.get_precomp_hashcodes(list_ids))
        return res

    def get_precomp_from_sha1(self,list_sha1_id, list_type):
        list_ids_sha1_found = self.get_ids_from_sha1s(list_sha1_id)
        list_ids = [x[0] for x in list_ids_sha1_found]
        res = self.get_precomp_from_ids(list_ids, list_type)
        final_res = []
        for one_type_res in res:
            found_ids = one_type_res[1]
            final_res.append((one_type_res[0],[x[1] for i,x in enumerate(list_ids_sha1_found) if i in found_ids]))
        return final_res


    def get_ids_from_sha1s(self, sha1_list):
        nb_sha1 = len(sha1_list)
        uniques_ids = [None]*nb_sha1
        sha1_found = 0
        for i, indexed_sha1 in self.list_indexed_sha1:
            try:
                sha1_pos = sha1_list.index(indexed_sha1)
                uniques_ids[sha1_pos] = i
                sha1_found += 1
                if sha1_found == nb_sha1:
                    break
            except:
                pass
        if sha1_found != nb_sha1:
            print "[get_ids_from_sha1s: warning] We do not find all sha1s: {} found out of {}".format(sha1_found, nb_sha1)
        return uniques_ids


    def get_old_unique_ids(self, unique_sha1):
        # Get sha1s already indexed
        old_uniques = []
        old_uniques_id = []
        for sha1 in unique_sha1:
            if sha1 in self.set_indexed_sha1:
                old_uniques.append(sha1)
                old_uniques_id.append(self.list_indexed_sha1.index[sha1])
        return old_uniques, old_uniques_id

    def get_new_unique_images(self, sha1_images):
        # get unique images 
        sha1_list = [img_item[-1] for img_item in sha1_images]
        unique_sha1 = sorted(set(sha1_list))
        print "[LocalIndexer.get_new_unique_images: log] We have {} unique images.".format(len(unique_sha1))
        unique_idx = [sha1_list.index(sha1) for sha1 in unique_sha1]
        full_idx = [unique_sha1.index(sha1) for sha1 in sha1_list]
        
        old_uniques, old_uniques_id = self.get_old_unique_ids(unique_sha1)
        
        new_uniques=[]
        unique_id = []
        new_files=[]
        for i in range(0,len(unique_sha1)):
            if unique_sha1[i] not in old_uniques:
                img_item = sha1_images[unique_idx[i]]
                new_uniques.append((int(img_item[0]), img_item[1], img_item[-1]))
                new_files.append(img_item[-2])
                unique_id.append(new_uniques[-1][0])
            else:
                unique_id.append(old_uniques_id[old_uniques.index(unique_sha1[i])])
        new_fulls = []
        for i in range(0,len(sha1_images)):
            new_fulls.append((int(sha1_images[i][0]),unique_id[full_idx[i]]))
        print "[LocalIndexer.get_new_unique_images: log] We have {} new unique images.".format(len(new_files))
        return new_files, new_uniques, new_fulls

    def insert_new_uniques(self, new_uniques):
        """ Insert new_uniques ids in the local database.

        :param new_uniques: list of tuples (image_id, location, sha1) to be inserted.
        :type new_uniques: list
        """
        self.unique_images.extend(new_uniques)
        for _, _, sha1 in new_uniques:
            self.set_indexed_sha1.add(sha1)
            self.list_indexed_sha1.append(sha1)
        self.max_unique_id = len(self.set_indexed_sha1)

    def insert_new_fulls(self, new_fulls):
        """ Insert new_fulls ids in the local database.

        :param new_fulls: list of tuples (image_id, unique_id) to be inserted.
        :type new_fulls: list
        """
        self.full_images.extend(new_fulls)
        self.max_file_id += len(new_fulls)

    def check_batch(self,umax,umax_new,num_new_unique,fmax_new,fmax,num_readable,hashbits_filepath,feature_filepath):
        if umax_new-umax != num_new_unique:
            print 'Update failed! unique infos size mismatch!',umax_new,umax,num_new_unique
        # TODO: replace hard coded values by hash_nb_bits/8 and feats_nb_dim*4.
        elif os.stat(hashbits_filepath).st_size!=num_new_unique*32:
            print 'Update failed! hash bits size mismatch!',os.stat(hashbits_filepath).st_size,num_new_unique*32
        elif os.stat(feature_filepath).st_size!=num_new_unique*16384:
            print 'Update failed! feature size mismatch!',os.stat(feature_filepath).st_size,num_new_unique*16384
        elif fmax_new-fmax != num_readable:
            print 'Update warning! full infos size mismatch!',fmax_new,fmax,num_readable
        else:
            return True
        return False

    def update_master_file(self,update_id):
        """ Appends `update_id` to the `master_update_filepath`.
        """
        with open(self.master_update_filepath, "a") as f:
                f.write(update_id+'\n')

    def finalize_update(self,success,hashbits_filepath,feature_filepath,update_id):
        """ Finalize update.

        If success, will update the `update_master_file` with current update_id
        and compress the features.

        If failure, will delete `hashbits_filepath` and `feature_filepath`.
        """
        if not success:
            if os.path.isfile(hashbits_filepath):
                os.remove(hashbits_filepath)
            if os.path.isfile(feature_filepath):
                os.remove(feature_filepath)
        else:
            self.update_master_file(update_id)
            self.hasher.compress_feats()
            self.save_to_disk()
            

    def index_batch(self, batch):
        """ Index a batch in the form of a list of (id, path, other_data)
        """
        # Download images
        timestr= time.strftime("%b-%d-%Y-%H-%M-%S", time.localtime(time.time()))
        update_id = timestr
        print "[LocalIndexer.index_batch: log] Starting udpate {}".format(update_id)
        # Compute sha1
        sha1_images = [img+(get_SHA1_from_file(img[1]),) for img in batch]
        # Find new images
        new_files, new_uniques, new_fulls = self.get_new_unique_images(sha1_images)
        # Compute features
        features_filename,ins_num = self.feature_extractor.compute_features(new_files, update_id)
        # Compute hashcodes
        hashbits_filepath = self.hasher.compute_hashcodes(features_filename, ins_num, update_id)
        # Record current biggest ids
        umax = self.max_unique_id
        fmax = self.max_file_id
        # Insert new ids
        self.insert_new_uniques(new_uniques)
        self.insert_new_fulls(new_fulls)
        # Check that batch processing went well
        umax_new = self.max_unique_id
        fmax_new = self.max_file_id
        update_success = self.check_batch(umax, umax_new, len(new_uniques), fmax_new,fmax, len(sha1_images),
                                          hashbits_filepath, features_filename)
        if update_success:
            print "Update succesful!"
            # what should we do here? Save index? Basically just max_file_id?
        self.finalize_update(update_success, hashbits_filepath, features_filename, update_id)

    def get_sim_infos(self, nums):

        #'image_urls',sim[0] # Needed, actually local path
        #'cached_image_urls',sim[1] # Empty
        #'page_urls',sim[2] # Empty
        #'ht_ads_id',sim[3] # Empty
        #'ht_images_id',sim[4] # Empty
        #'sha1',sim[5] # Needed

        # should be something like get sim all infos? in local indexer.
        nb_sim = len(nums)
        out = [(None, None, None, None, None, None)]*nb_sim
        sha1_found = 0
        for image_id, location, sha1 in self.unique_images:
            try:
                nums_pos = nums.index(image_id)
                out[nums_pos] = (location, None, None, None, None, sha1)
                sha1_found += 1
                if nb_sim == sha1_found:
                    break
            except:
                pass
        if sha1_found != nb_sim:
            print "[get_sim_infos: warning] We do not find all sha1s: {} found out of {}".format(sha1_found,
                                                                                                      nb_sim)
        return out


