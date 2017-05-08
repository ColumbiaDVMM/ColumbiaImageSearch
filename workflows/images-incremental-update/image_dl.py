import requests
imagedltimeout=3

class UnknownImageFormat(Exception):
    pass

def get_image_size_and_format(input):
    # adapted from https://github.com/scardine/image_size
    """
    Return (width, height, format) for a given img file content stream.
    No external dependencies except the struct modules from core.
    """
    import struct

    height = -1
    width = -1
    format = None
    data = input.read(25)

    if data[:6] in ('GIF87a', 'GIF89a'):
        # GIFs
        w, h = struct.unpack("<HH", data[6:10])
        width = int(w)
        height = int(h)
        format = 'GIF'
    elif data.startswith('\211PNG\r\n\032\n') and (data[12:16] == 'IHDR'):
        # PNGs
        w, h = struct.unpack(">LL", data[16:24])
        width = int(w)
        height = int(h)
        format = 'PNG'
    elif data.startswith('\211PNG\r\n\032\n'):
        # older PNGs?
        w, h = struct.unpack(">LL", data[8:16])
        width = int(w)
        height = int(h)
        format = 'PNG'
    elif data.startswith('\377\330'):
        # JPEG
        format = 'JPEG'
        msg = " raised while trying to decode as JPEG."
        input.seek(0)
        input.read(2)
        b = input.read(1)
        try:
            while (b and ord(b) != 0xDA):
                while (ord(b) != 0xFF): b = input.read(1)
                while (ord(b) == 0xFF): b = input.read(1)
                if (ord(b) >= 0xC0 and ord(b) <= 0xC3):
                    input.read(3)
                    h, w = struct.unpack(">HH", input.read(4))
                    break
                else:
                    input.read(int(struct.unpack(">H", input.read(2))[0])-2)
                b = input.read(1)
            width = int(w)
            height = int(h)
        except struct.error:
            raise UnknownImageFormat("StructError" + msg)
        except ValueError:
            raise UnknownImageFormat("ValueError" + msg)
        except Exception as e:
            raise UnknownImageFormat(e.__class__.__name__ + msg)
    else:
        raise UnknownImageFormat("Sorry, don't know how to get information from this file.")

    return width, height, format

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
    from cStringIO import StringIO
    import sys
    if verbose>1:
        print "Downloading image from {}.".format(url)
    try:
        r = requests.get(url, timeout=imagedltimeout)
        if r.status_code == 200:
            r_sio = StringIO(r.content)
            if int(r.headers['content-length']) == 0:
                del r
                raise ValueError("Empty image.")
            else:
                data = r_sio.read()
                sha1hash = get_SHA1_from_data(data)
                del r,r_sio,data
                return sha1hash
        else:
            raise ValueError("Incorrect status_code: {}.".format(r.status_code))
    except Exception as inst:
        print "Download failed from url {}. [{}]".format(url, inst)
    return None


def get_SHA1_imginfo_from_URL_StringIO_PIL(url,verbose=0):
    from cStringIO import StringIO
    import requests
    if verbose>1:
        print "Downloading image from {}.".format(url)
    try:
        r = requests.get(url, timeout=imagedltimeout)
        if r.status_code == 200:
            r_sio = StringIO(r.content)
            if int(r.headers['content-length']) == 0:
                del r
                raise ValueError("Empty image.")
            else:
                # with PIL, takes 1 second...
                #start = time.time()
                from PIL import Image
                img = Image.open(r_sio)
                w,h = img.size
                format = img.format
                del img
                #print "PIL get image size and format:",time.time()-start
                
                r_sio.seek(0)
                data = r_sio.read()
                # use a dict for img info so we can store any other info we may need
                img_info = dict()
                img_info['size'] = dict()
                
                img_info['size']['width'] = w
                img_info['size']['height'] = h
                img_info['format'] = format
                sha1hash = get_SHA1_from_data(data)
                del r,r_sio,data

                return sha1hash,img_info
        else:
            raise ValueError("Incorrect status_code: {}.".format(r.status_code))
    except Exception as inst:
        print "Download failed from url {}. [{}]".format(url, inst)
    return None

def get_SHA1_imginfo_from_URL_StringIO(url,verbose=0):
    from cStringIO import StringIO
    import requests
    if verbose>1:
        print "Downloading image from {}.".format(url)
    try:
        r = requests.get(url, timeout=imagedltimeout)
        if r.status_code == 200:
            r_sio = StringIO(r.content)
            if int(r.headers['content-length']) == 0:
                del r
                raise ValueError("Empty image.")
            else:
                # No PIL dependency, 10-5s.
                #start = time.time()
                w,h,format = get_image_size_and_format(r_sio)
                #print "get_image_size_and_format:",time.time()-start
                # Seek back to compute SHA1 on the whole binary content!
                r_sio.seek(0)
                data = r_sio.read()
                # use a dict for img info so we can store any other info we may need
                img_info = dict()
                img_info['size'] = dict()
                
                img_info['size']['width'] = w
                img_info['size']['height'] = h
                img_info['format'] = format
                sha1hash = get_SHA1_from_data(data)
                del r,r_sio,data

                return sha1hash,img_info
        else:
            raise ValueError("Incorrect status_code: {}.".format(r.status_code))
    except Exception as inst:
        print "Download failed from url {}. [{}]".format(url, inst)
    return None,None

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
    b64_from_data = None
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
            b64_from_data = get_b64_from_data(data)
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
    import time

    #profile.run('sha1 = get_SHA1_from_URL("https://s3.amazonaws.com/memex-images/full/581ed33d3e12498f12c86b44010306b172f4ad6a.jpg")')
    #profile.run('sha1_sio = get_SHA1_from_URL_StringIO("https://s3.amazonaws.com/memex-images/full/581ed33d3e12498f12c86b44010306b172f4ad6a.jpg")')
    #profile.run('sha1_sio, img_info = get_SHA1_imginfo_from_URL_StringIO("https://s3.amazonaws.com/memex-images/full/581ed33d3e12498f12c86b44010306b172f4ad6a.jpg")')
    start = time.time()    
    sha1 = get_SHA1_from_URL("https://s3.amazonaws.com/memex-images/full/581ed33d3e12498f12c86b44010306b172f4ad6a.jpg")
    print sha1,time.time()-start
    start = time.time()
    sha1_sio = get_SHA1_from_URL_StringIO("https://s3.amazonaws.com/memex-images/full/581ed33d3e12498f12c86b44010306b172f4ad6a.jpg")
    print sha1_sio,time.time()-start
    start = time.time()
    sha1_sio, img_info = get_SHA1_imginfo_from_URL_StringIO("https://s3.amazonaws.com/memex-images/full/581ed33d3e12498f12c86b44010306b172f4ad6a.jpg")
    print sha1_sio,img_info,time.time()-start
    start = time.time()
    sha1_sio, img_info = get_SHA1_imginfo_from_URL_StringIO_PIL("https://s3.amazonaws.com/memex-images/full/581ed33d3e12498f12c86b44010306b172f4ad6a.jpg")
    print sha1_sio,img_info,time.time()-start