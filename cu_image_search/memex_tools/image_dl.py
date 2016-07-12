import os
import requests
import shutil
import time
import numpy as np

imagedltimeout=2

def mkpath(outpath):
    pos_slash=[pos for pos,c in enumerate(outpath) if c=="/"]
    for pos in pos_slash:
        try:
            os.mkdir(outpath[:pos])
        except:
            pass

def dlimage_basepath(url,basepath,logf=None):
    if not url:
        return None
    pos_slash=[pos for pos,c in enumerate(url) if c=="/"]
    #pos_point=[pos for pos,c in enumerate(url) if c=="."]
    if not pos_slash:
        return None
    file_img=url[pos_slash[-1]+1:]
    # path with time and random to ensure unique names
    outpath=os.path.join(basepath,str(time.time())+'_'+str(np.int32(np.random.random()*(10e6)))+'_'+file_img)
    mkpath(outpath)
    #print "Downloading image from {} to {}.".format(url,outpath)
    try:
        r = requests.get(url, stream=True, timeout=imagedltimeout)
        if r.status_code == 200:
            with open(outpath, 'wb') as f:
                r.raw.decode_content = True
                shutil.copyfileobj(r.raw, f)
            return outpath
    except Exception as inst:
        if logf:
            logf.write("Download failed for img that should be saved at {} from url {}.\n".format(outpath,url))
        else:
            print "Download failed for img that should be saved at {} from url {}.".format(outpath,url)
        print inst 
        return None


def dlimage(url,logf=None):
    return dlimage_basepath(url,'./',logf)

def dlimage_args(args):
    if len(args)==2:
       print args[0],args[1]
       return dlimage_basepath(args[0],args[1])    
    else:
       print "[dl_image_args: warning] incorrect agruments: {}.".format(args)
       return None
