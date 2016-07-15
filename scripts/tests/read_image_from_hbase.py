import base64
import happybase
import numpy as np
import cv2 

def decode_image_b64(b64_data):
    """ Returns opencv image from b64 buffer.
    """
    data = base64.b64decode(b64_data)
    img_array = np.asarray(bytearray(data), dtype=np.uint8)
    # see CV_LOAD_IMAGE_XXX flags in documentation of ``imread`` fucnction at:
    # http://docs.opencv.org/2.4/modules/highgui/doc/reading_and_writing_images_and_video.html
    return cv2.imdecode(img_array, 1)

if __name__=="__main__":
    tab_image = 'image_cache'    
    conn = happybase.Connection(host='10.1.94.57')
    image_sha1 = '0000007031E3FA80C97940017253BEAB542EA334'
    table = conn.table(tab_image)
    row = table.row(image_sha1)
    img = decode_image_b64(row['image:binary'])
    cv2.imwrite(image_sha1+'.jpeg',img)
