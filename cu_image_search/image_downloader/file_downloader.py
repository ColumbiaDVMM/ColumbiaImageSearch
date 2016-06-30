import os
import time
import json
import shutil
import requests
import numpy as np
import multiprocessing
import subprocess as sub
from ..memex_tools.image_dl import mkpath,dlimage_args

class FileDownloader():

    def __init__(self,global_conf):
        self.global_conf = json.load(open(global_conf,'rt'))
        self.dl_pool_size = self.global_conf["FD_dl_pool_size"]
        self.dl_image_path = self.global_conf["FD_dl_image_path"]
        self.verbose = 0
        mkpath(self.dl_image_path)

    def download_images(self,batch,startid):
    	print "[FileDownloader.download_images: log] Will download {} images with {} workers.".format(len(batch),self.dl_pool_size)
        pool = multiprocessing.Pool(self.dl_pool_size)
        basepath = os.path.join(self.dl_image_path,str(startid))
        if not os.path.isdir(basepath):
            mkpath(basepath)
        # Parallel downloading
        download_arg = []
        for img_item in batch:
            url = img_item[1]
            #name = url.split('/')[-1]
            #filepath = os.path.join(update_image_cache,str(startid),name)
            download_arg.append([url,basepath])
        start_dl = time.time()
        # dlimage_basepath returns outpath if download succeeded, None otherwise
        download_indicator = pool.map(dlimage_args, download_arg)
        # Gather results
        downloaded = []
        for i,img_item in enumerate(batch):
            if download_indicator[i]:
                downloaded.append(img_item+(download_indicator[i],))
        print "[FileDownloader.download_images: log] Downloaded {} images in {:.2f}s.".format(len(downloaded),time.time()-start_dl)
        if not downloaded:
            return None
        # Image integrity check
        readable_images = []
        integrity_path = os.path.join(basepath,'integrity_check')
        if not os.path.exists(integrity_path):
            os.mkdir(integrity_path)
        integrity_filepath = os.path.join(integrity_path,str(startid)+'.txt')
        f = open(integrity_filepath,'w')
        ok_tag = '[OK]'
        error_tag = '[ERROR]'
        png_tag = '0x89 0x50'
        unsp_tag = 'Unsupported color conversion request'
        for img_item in downloaded:
            command = 'jpeginfo -c '+ img_item[-1]
            output, error = sub.Popen(command.split(' '), stdout=sub.PIPE, stderr=sub.PIPE).communicate()
            if output.find(ok_tag)<0:
                f.write(output)
            if output.find(error_tag)>=0 and output.find(png_tag)<0 and output.find(unsp_tag)<0:
                continue
            readable_images.append(img_item)
        f.close()
        print "[FileDownloader.download_images: log] We have {} readable images.".format(len(readable_images))
        if not readable_images:
            return None
        return readable_images
