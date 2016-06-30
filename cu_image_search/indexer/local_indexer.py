import os
import sys
import time
#import MySQLdb
from generic_indexer import GenericIndexer
from image_downloader.file_downloader import FileDownloader
from memex_tools.sha1_tools import get_SHA1_from_file, get_SHA1_from_data

class LocalIndexer(GenericIndexer):
        
    def initialize_indexer_backend(self):
        """ Initialize backend.
        """
        self.image_downloader = None
        print "[LocalIndexer: log] initialized with values {},{},{},{}.".format(self.local_db_host,self.local_db_user,self.local_db_pwd,self.local_dbname)
        if self.image_downloader_type=="file_downloader":
            self.image_downloader = FileDownloader(self.global_conf_file)
        else:
            raise ValueError("[LocalIndexer.initialize_indexer_backend error] Unsupported image_downloader_type: {}.".format(self.image_downloader_type))
        # initialize db, hasher and feature_extractor too

    def is_indexed(self,sha1):
        # query index with single SHA1
        pass

    def are_indexed(self,sha1_list):
        # query index with list of SHA1
        pass

    def index_batch(self,batch):
        """ Index a batch in the form of a list of (id,url,other_data)
        """
        # Download images
        readable_images = self.image_downloader.download_images(batch)
        #print readable_images
        # Compute sha1
        sha1_images = [img+(get_SHA1_from_file(img[-1]),) for img in readable_images]
        print "sha1_images",sha1_images
        # Find new images
        # Compute features
        # Compute hashcodes

        
