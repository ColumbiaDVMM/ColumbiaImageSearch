import happybase
from StringIO import StringIO
imagedltimeout = 4

def encode_image(data):
    return StringIO(data).getvalue()


def get_image_from_URL(url,verbose=0):
    import requests
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


if __name__=="__main__":
    tab_image = 'escorts_images_sha1_infos_dev'
    col_image = dict()
    image_sha1s = ['000421227D83DA48DB4A417FCEFCA68272398B8E', '00078E0775B5C62813EE0058E5149BDDD2CB8D47', '000A33898C75D4093B85A75CD922D1EBE75C6302', 
'000A9EA182EBADC8362B683F244D2AF2F4DD2CEC']
    conn = happybase.Connection(host='10.1.94.57')
    table = conn.table(tab_image)
    for row in table.rows(image_sha1s):
        table.put(row[0], {'info:image': encode_image(get_image_from_URL(row[1]['info:s3_url'],1))})    
