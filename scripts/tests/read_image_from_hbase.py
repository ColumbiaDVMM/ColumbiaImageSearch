import base64
import happybase
import numpy as np
from StringIO import StringIO
#import cv2 
from PIL import Image
from PIL.ImageFile import Parser

#def decode_image_opencv(binary_data):
#    """ Returns opencv image from binary buffer.
#    """
#    img_array = np.asarray(bytearray(data), dtype=np.uint8)
#    # see CV_LOAD_IMAGE_XXX flags in documentation of ``imread`` fucnction at:
#    # http://docs.opencv.org/2.4/modules/highgui/doc/reading_and_writing_images_and_video.html
#    return cv2.imdecode(img_array, 1)
    
def decode_image_PIL_parser(binary_data):
    """ Returns PIL image from binary buffer.
    """
    img_parser = Parser()
    img_parser.feed(data)
    return img_parser.close()

def decode_image_PIL(binary_data):
    """ Returns PIL image from binary buffer.
    """
    f = StringIO(binary_data)
    img = Image.open(f)
    return img

if __name__=="__main__":
    use_opencv = False
    tab_image = 'image_cache'    
    conn = happybase.Connection(host='10.1.94.57')
    image_sha1 = '0000007031E3FA80C97940017253BEAB542EA334'
    #image_sha1 = '000001EC5DD154E58B72326EFC26A41A4C8E9586'
    table = conn.table(tab_image)
    row = table.row(image_sha1)
    b64_data = row['image:binary']
    size = (int(row['image:width']),int(row['image:height']))
    # assuming all images are jpeg
    if use_opencv:
        img = decode_image_opencv(b64_data)
        cv2.imwrite(image_sha1+'.jpeg',img)
    else:
        img = decode_image_PIL(b64_data)
        img.save(image_sha1+'.jpeg',"JPEG")

