import happybase
from StringIO import StringIO
from PIL import Image

def decode_image_PIL(binary_data):
    """ Returns PIL image from binary buffer.
    """
    f = StringIO(binary_data)
    img = Image.open(f)
    return img

if __name__=="__main__":
    tab_image = 'image_cache'    
    conn = happybase.Connection(host='10.1.94.57')
    image_sha1s = ['0000007031E3FA80C97940017253BEAB542EA334', '000001EC5DD154E58B72326EFC26A41A4C8E9586', '0000081A1D6D1A2023DAE07547C242ED3106E7FE']
    table = conn.table(tab_image)
    for row in table.rows(image_sha1s):
        binary_data = row[1]['image:binary']
        img = decode_image_PIL(binary_data)
        img.save(row[0]+'.jpeg',"JPEG")

