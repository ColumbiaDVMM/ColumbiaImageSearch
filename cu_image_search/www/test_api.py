def get_b64(url, imagedltimeout = 2):
    import base64
    from cStringIO import StringIO
    import requests

    r = requests.get(url, timeout=imagedltimeout)
    b64_from_data= None
    if r.status_code == 200:
        r_sio = StringIO(r.content)
        if int(r.headers['content-length']) == 0:
            raise ValueError("Empty image.")
        else:
            data = r_sio.read()
            try:
                b64_from_data = base64.b64encode(data)
            except:
                raise ValueError("Could not read data to compute base64 string.")
    else:
        raise ValueError("Incorrect status_code: {}.".format(r.status_code))
    return b64_from_data

if __name__ == "__main__":
    import requests
    test_remote = True
    #test_remote = False
    if test_remote:
        import json
        auth_json = json.load(open('auth_token.json','rt'))
        #apiURL = "https://isi.memexproxy.com/ESCORTS/cu_image_search/byB64_nocache" # would need auth?
        apiURL = "http://10.3.2.124/cuimgsearch_escorts/cu_image_search/byB64_nocache" # would need auth?
        headers_auth = {'Authorization': 'Basic '+auth_json["auth_token"]}
    else:
        apiURL = "http://127.0.0.1:5000/cu_image_search/byB64_nocache"
        headers_auth = ''
    #print auth_json["auth_token"]
    sampleURLs = ['https://s3.amazonaws.com/roxyimages/939446e1543d2a7ebf73b438f6f21dbb6e71f04a.jpg','https://s3.amazonaws.com/roxyimages/f99f89526bdf335483c9776c73a059ead1f16d27.jpg']
    #sampleURLs = ['https://upload.wikimedia.org/wikipedia/commons/4/47/PNG_transparency_demonstration_1.png']
    query_b64 = [get_b64(one_url) for one_url in sampleURLs]

    #with open('bikini-001_png.b64','rt') as f:
    #    query_b64 = [line for line in f]   
    #query_b64 = [get_b64(sampleURL) for sampleURL in sampleURLs]
    #print query_b64
    
    print headers_auth

    res = requests.post(apiURL, data={"data":','.join(query_b64)}, headers=headers_auth)
    print res
    print res.content
    print res.json()
