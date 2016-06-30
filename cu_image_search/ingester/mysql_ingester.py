import os
import sys
import time
import MySQLdb
from .generic_ingester import GenericIngester

class MySQLIngester(GenericIngester):

    def initialize_source(self):
        """ Use information contained in `self.global_conf` to initialize MySQL config
        """
        self.host = self.global_conf['ist_db_host']
        self.user = self.global_conf['ist_db_user']
        self.pwd = self.global_conf['ist_db_pwd']
        self.db = self.global_conf['ist_db_dbname']
        print "[MySQLIngester: log] initialized with values {}, {}, {}, {}.".format(self.host,self.user,self.pwd,self.db)

    def get_batch(self):
        """ Should return a list of (id,url,other_data) querying for `batch_size` samples from `self.source` from `start`
        """
        if self.start is None or self.batch_size is None:
            raise ValueError("[MySQLIngester.get_batch: error] Parameters 'start' {} or 'batch_size' {} not set.".format(self.start,self.batch_size))
        self.source = MySQLdb.connect(host=self.host,user=self.user,passwd=self.pwd,db=self.db)
        c = self.source.cursor()
        sql = 'select id,location from images where id > '+str(self.start)+' and location is not null order by id asc limit '+str(self.batch_size)
        start_time = time.time()
        c.execute(sql)
        if self.verbose>0:
            print "[MySQLIngester.get_batch: log] Query database took: {}s.".format(time.time() - start_time)
        re = c.fetchall()
        self.source.close()
        if len(re)<self.batch_size and self.fail_less_than_batch:
            print "[MySQLIngester.get_batch: error] Not enough images ("+str(len(re))+")"
            return None
        return [(img[0],img[1],None) for img in re]
        
