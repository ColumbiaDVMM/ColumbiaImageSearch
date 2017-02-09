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
    col_image = dict()
    col_image['image_cache'] = 'image:binary'
    conn = happybase.Connection(host='10.1.94.57')
    image_rows = dict()
    image_rows['image_cache'] = ['0000007031E3FA80C97940017253BEAB542EA334', '000001EC5DD154E58B72326EFC26A41A4C8E9586', 
'0000081A1D6D1A2023DAE07547C242ED3106E7FE']
    table = conn.table(tab_image)
    for row in table.rows(image_rows[tab_image]):
        binary_data = row[1][col_image[tab_image]]
        img = decode_image_PIL(binary_data)
        print("Saving image to: {}".format(row[0]+'.jpeg'))
        img.save(row[0]+'.jpeg',"JPEG")

