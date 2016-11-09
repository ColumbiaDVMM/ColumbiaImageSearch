import base64
import requests
import json
from optparse import OptionParser

""" Test file for Columbia Image Search tool in MEMEX HT domain.
"""

__author__ = "Svebor Karaman"
__email__ = "svebor.karaman (at) columbia.edu"
# Please feel free to contact me if you have questions/suggestions

imagedltimeout = 4

def get_b64_from_data(data):
    """ Encode data to base64.
    """
    import base64
    b64_from_data = None
    try:
        b64_from_data = base64.b64encode(data)
    except:
        print("Could not read data to compute base64 string.")
    return b64_from_data


def get_b64_from_URL_StringIO(url,verbose=False):
    """ Encode the image at 'url' into a base64 string.
    """
    from StringIO import StringIO
    if verbose:
        print("Downloading image from {}.".format(url))
    try:
        r = requests.get(url, timeout=imagedltimeout)
        if r.status_code == 200:
            r_sio = StringIO(r.content)
            data = r_sio.read()
            return get_b64_from_data(data)
        else:
            print("Incorrect status_code {} for url {}".format(r.status_code, url))
    except Exception as inst:
        if verbose:
            print("Download failed from url {}. Error was {}".format(url, inst))
    return None


if __name__ == "__main__":
    # get options
    parser = OptionParser()
    parser.add_option("-u", "--user", dest="user")
    parser.add_option("-p", "--password", dest="password")
    parser.add_option("-i", "--image", dest="image_data", default="https://s3.amazonaws.com/roxyimages/5e68973d60c3f3f970159b66f8f3cc4bc64eef39.jpg")
    # should be one of the valid_service_urls:
    # - https://isi.memexproxy.com/ESCORTS/cu_image_search/byURL [default]
    # - https://isi.memexproxy.com/ESCORTS/cu_image_search/bySHA1
    # e.g. python test_columbia_imagesearch.py -s https://isi.memexproxy.com/ESCORTS/cu_image_search/bySHA1 -i 000002BAD9088A6971FA72CA19325C181370E3BF -u user -p password
    # - https://isi.memexproxy.com/ESCORTS/cu_image_search/byB64 [could induce a big -i parameter, so -i should still be the URL of the image to be encoded in base64]
    parser.add_option("-s", "--service", dest="service_url", default="https://isi.memexproxy.com/ESCORTS/cu_image_search/byURL")
    parser.add_option("-r", "--request_type", dest="request_type", default="GET")
    parser.add_option("-o", "--options", dest="options", default="")

    # validate options
    valid_request_types = ["GET", "POST"]
    valid_service_urls = ["https://isi.memexproxy.com/ESCORTS/cu_image_search/byURL", "https://isi.memexproxy.com/ESCORTS/cu_image_search/bySHA1", "https://isi.memexproxy.com/ESCORTS/cu_image_search/byB64"]
    (c_options, args) = parser.parse_args()
    print("Got options: {}".format(c_options))
    if not c_options.user or not c_options.password:
        print("You NEED to input a user and password with options -u and -p")
        exit()
    if c_options.request_type not in valid_request_types:
        print("Invalid request_type {}. Valid types are: {}".format(c_options.request_type, valid_request_types))
        exit()
    if c_options.service_url not in valid_service_urls:
        print("Invalid service_url {}. Valid URLs are: {}".format(c_options.service_url, valid_service_urls))
        exit()

    # prepare data
    userpasswd = base64.encodestring(c_options.user.strip()+':'+c_options.password.strip()).strip()
    headers = { 'Content-type' : 'application/x-www-form-urlencoded', 'Authorization' : 'Basic {}'.format(userpasswd)}
    if c_options.service_url.strip().endswith("byB64"):
        print("Encoding image to base64.")
        base64image = get_b64_from_URL_StringIO(c_options.image_data)
        payload = { 'data': base64image}
        if c_options.request_type == "GET":
            print("[WARNING] Beware using GET and byB64 could give a 414 error. Please use POST")
    else:
        payload = { 'data': c_options.image_data}

    # request
    print("Running {} request with headers:\n{}".format(c_options.request_type, headers))
    # This could print out a lot for base64
    #print("Running {} request with headers:\n{}\nand payload:\n{}".format(c_options.request_type, headers, payload))
    if c_options.request_type == "POST":
        r = requests.post(c_options.service_url, data=payload, headers=headers)
    if c_options.request_type == "GET":
        r = requests.get(c_options.service_url, params=payload, headers=headers)
    if r.status_code == 200:
        response = r.content
        result = json.loads(response)
        print(result)
    else:
        print("Got invalid status code {}.".format(r.status_code))
