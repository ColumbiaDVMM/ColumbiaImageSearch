import requests
imagedltimeout=3

def mkpath(outpath):
    import os
    pos_slash=[pos for pos,c in enumerate(outpath) if c=="/"]
    for pos in pos_slash:
        try:
            os.mkdir(outpath[:pos])
        except:
            pass

def dlimage(url,verbose=False):
    import numpy as np
    import shutil
    import time
    import os
    pos_slash=[pos for pos,c in enumerate(url) if c=="/"]
    file_img=url[pos_slash[-1]+1:]
    # path with time and random to ensure unique names
    outpath=os.path.join('./'+str(time.time())+'_'+str(np.int32(np.random.random()*(10e6)))+'_'+file_img)
    mkpath(outpath)
    if verbose:
        print "Downloading image from {} to {}.".format(url,outpath)
    try:
        r = requests.get(url, stream=True, timeout=imagedltimeout)
        if r.status_code == 200:
            with open(outpath, 'wb') as f:
                r.raw.decode_content = True
                shutil.copyfileobj(r.raw, f)
            return outpath
    except Exception as inst:
        if verbose:
            print "Download failed for img that should be saved at {} from url {}.".format(outpath,url)
            print inst 
        return None

def get_SHA1_from_data(data):
    import hashlib
    sha1hash = None
    try:
        sha1 = hashlib.sha1()
        sha1.update(data)
        sha1hash = sha1.hexdigest().upper()
    except:
        print "Could not read data to compute SHA1."
    return sha1hash

def get_SHA1_from_URL_StringIO(url,verbose=0):
    from StringIO import StringIO
    if verbose>1:
        print "Downloading image from {}.".format(url)
    try:
        r = requests.get(url, timeout=imagedltimeout)
        if r.status_code == 200:
            r_sio = StringIO(r.content)
            if int(r.headers['content-length']) == 0:
                raise ValueError("Empty image.")
            else:
                data = r_sio.read()
                sha1hash = get_SHA1_from_data(data)
                return sha1hash
        else:
            raise ValueError("Incorrect status_code: {}.".format(r.status_code))
    except Exception as inst:
        if verbose>0:
            print "Download failed from url {}.".format(url)
            print inst 
    return None

def get_SHA1_from_URL(url,verbose=False):
    if verbose:
        print "Downloading image from {}.".format(url)
    try:
        r = requests.get(url, stream=True, timeout=imagedltimeout)
        if r.status_code == 200:
            sha1hash = get_SHA1_from_data(r.raw.data)
            return sha1hash
    except Exception as inst:
        if verbose:
            print "Download failed from url {}.".format(url)
            print inst 
        return None

def get_b64_from_data(data):
    import base64
    b64_from_data= None
    try:
        b64_from_data = base64.b64encode(data)
    except:
        print "Could not read data to compute base64 string."
    return b64_from_data

def get_b64_from_URL(url,verbose=False):
    if verbose:
        print "Downloading image from {}.".format(url)
    try:
        r = requests.get(url, stream=True, timeout=imagedltimeout)
        if r.status_code == 200:
            b64_from_data = get_b64_from_data(r.raw.data)
            return b64_from_data
    except Exception as inst:
        if verbose:
            print "Download failed from url {}.".format(url)
            print inst 
        return None

def get_b64_from_URL_StringIO(url,verbose=False):
    from StringIO import StringIO
    if verbose:
        print "Downloading image from {}.".format(url)
    try:
        r = requests.get(url, timeout=imagedltimeout)
        if r.status_code == 200:
            r_sio = StringIO(r.content)
            data = r_sio.read()
            b64_from_data = get_b64_from_data(r.raw.data)
            return b64_from_data
        else:
            print "Incorrect status_code {} for url {}".format(r.status_code,url)
    except Exception as inst:
        if verbose:
            print "Download failed from url {}.".format(url)
            print inst 
    return None

def get_SHA1_b64_from_URL(url,verbose=False):
    if verbose:
        print "Downloading image from {}.".format(url)
    try:
        r = requests.get(url, stream=True, timeout=imagedltimeout)
        if r.status_code == 200:
            sha1hash = get_SHA1_from_data(r.raw.data)
            b64_from_data = get_b64_from_data(r.raw.data)
            return sha1hash,b64_from_data
    except Exception as inst:
        if verbose:
            print "Download failed from url {}.".format(url)
            print inst 
        return None,None

if __name__ == "__main__":
    import profile
    profile.run('sha1 = get_SHA1_from_URL("https://s3.amazonaws.com/memex-images/full/581ed33d3e12498f12c86b44010306b172f4ad6a.jpg")')
    print sha1
    profile.run('sha1_sio = get_SHA1_from_URL_StringIO("https://s3.amazonaws.com/memex-images/full/581ed33d3e12498f12c86b44010306b172f4ad6a.jpg")')
    print sha1_sio
    sha1_sio = get_SHA1_from_URL_StringIO("https://s3.amazonaws.com/memex-images/full/581ed33d3e12498f12c86b44010306b172f4ad6a.jpg")
    print sha1_sio