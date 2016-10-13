import happybase
from StringIO import StringIO

def encode_image(img_filename):
    # assuming file exist locally, e.g. with read_image_from_hbase script
    img_file = open(img+'.jpeg','rb')
    return StringIO(img_file.read()).getvalue()

if __name__=="__main__":
    tab_image = 'escorts_images_sha1_infos_incup'
    col_image = dict()
    col_image['escorts_images_sha1_infos_incup'] = 'info:image'
    image_sha1s = ['0000007031E3FA80C97940017253BEAB542EA334', '000001EC5DD154E58B72326EFC26A41A4C8E9586', 
'0000081A1D6D1A2023DAE07547C242ED3106E7FE']
    conn = happybase.Connection(host='10.1.94.57')
    table = conn.table(tab_image)
    for img in image_sha1s:
        table.put(img, {'info:imageStringIO': encode_image(img+'.jpeg')})    
