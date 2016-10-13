imagedltimeout = 4


def get_image_from_URL(url,verbose=0):
    import requests
    from StringIO import StringIO
    data = None
    if verbose > 1:
        print("[get_image_from_URL: LOG] Downloading image from {}.".format(url))
    try:
        r = requests.get(url, timeout=imagedltimeout)
        if r.status_code == 200:
            r_sio = StringIO(r.content)
            if int(r.headers['content-length']) == 0:
                raise ValueError("Empty image.")
            else:
                data = r_sio.read()
        else:
            raise ValueError("[get_image_from_URL: ERROR] Incorrect status_code: {}.".format(r.status_code))
    except Exception as inst:
        if verbose>0:
            print("[get_image_from_URL: ERROR] Download failed from url {}.".format(url))
            print inst 
    return data


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


def get_b64_from_data(data):
    import base64
    b64_from_data = None
    try:
        b64_from_data = base64.b64encode(data)
    except:
        print "Could not read data to compute base64 string."
    return b64_from_data