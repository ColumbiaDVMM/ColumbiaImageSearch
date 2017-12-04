# API usage

This file details how the image search API can be used and is organized as follows.

- [Endpoints overview](#endpoints-overview)
- [Additional details](#additional-details)
- [Output format for a face search API](#output-format-for-a-face-search-api)
- [Output format for an image search API](#output-format-for-an-image-search-api)
- [Sample query code for a face search API](#sample-query-code-for-a-face-search-api)
- [Sample query code for an image search API](#sample-query-code-for-an-image-search-api)

## Endpoints overview


Here is a list of the API routes:

- `/status` (e.g. `localhost:80/cufacesearch/status`): 
    - Returns a JSON formatted list of information about the image similarity service status, namely the `API_start_time`, 
    `API_uptime`, `last_refresh_time` and number of indexed faces `nb_indexed` values. This is also use to refresh the index if it has not been 
    refreshed in the last 4 hours. 
    - No parameter.

- `/refresh`: to force a refresh of the index, i.e. checking for newly processed updates not yet indexed.

- `/byURL`
    - Returns a JSON formatted list of similar images of the query image(s) accessible at the provided URL(s).
    - Parameters:
        - `data` [required]: the URL(s) from which the query image(s) should be downloaded 

- `/byB64`
    - Returns a JSON formatted list of similar faces of the base64 encoded query image(s).
    - Parameters:
        - `data` [required]: the base64 encoded query image(s)

- `/bySHA1`
    - Returns a JSON formatted list of similar faces of the query image(s) identified by their SHA1. This is intended to 
    be used only for indexed images. If your query image is not indexed yet, you should fallback to calling the 
    `byURL` endpoint using the image S3 URL.
    - Parameters:
        - `data` [required]: the SHA1 checksum of the query image(s)

- `/view_similar_byX`: any of the previously listed endpoints `byX` can be prefixed by `view_similar_` to access a simple 
visualization of the results, i.e. `view_similar_byURL`, `view_similar_byB64` and `view_similar_bySHA1`. 
This is provided to give a quick idea of the type of results you can expect. Beware, no blurring applied on images here. 

## Additional details
All endpoints support an additional parameter `options` that should be JSON formatted and can contain the following values:

- `near_dup`: `boolean`, to specify if near duplicate search is active [default: 0, i.e. near duplicate search is not active]
- `near_dup_th`: `float`, corresponding to the distance threshold for near duplicate definition [reasonable values are in 0.1-0.5]
- `max_returned`: `int`, the maximum number of returned images

All query endpoints support both `GET`/`POST` methods. When using the `POST` method, parameters should be inputted as `form-data`.

All query endpoints support batch querying, i.e. more than one image can be given as query, the list of images should be 
separated by commas in the `data` field.

## Output format for a face search API

The output format is a JSON with the top-level fields:

- `NumberImages`: the number of images submitted.
- `NumberFaces`: the total number of faces detected in the images submitted.
- `NumberSimilarFaces`: the total number of similar faces found.
- `Timing`: the total time it took to process the query.
- `AllSimilarFaces`: an array containing the detected faces and similar faces information.

Each value in the `AllSimilarFaces` array has the fields:

- `QueryFace`: a JSON with fields “top”, “right”, “bottom” and “left” indicated the face bounding box in the image, e.g. `{"top": 101, "right": 141, "bottom": 153, "left": 89}`
- `QuerySha1`: the SHA1 checksum of the query image.
- `QueryURL`: the query image URL. Not present if query was made with B64 encoded image.
- `ImgInfo`: An array containing some information about the query image, as [format, width, height] e.g. ["JPEG", 200, 292]
- `SimilarFaces`: JSON containing the information about the similar faces.

Each `SimilarFaces` JSON has the fields:

- `NumberFaces`: the number of similar faces found for the query face.
- `Faces`: array of bounding boxes information of the similar faces.
- `Distances`: array of distances values between the query face and the similar faces.
- `ImageSha1s`: array of the image sha1s of the similar faces.
- `CachedImageURLs`: array of the cached URLs in S3 of the images containing the similar faces.
- `ImgInfo`: Array of information about the similar images. Actually empty for now.

### Sample query code for a face search API

Below is a sample python script than you can run to test the face search API.

```python
import json
import base64
import requests
from StringIO import StringIO
from optparse import OptionParser

 
""" Test file for the Columbia Face Search Service for MEMEX.
"""
__author__ = "Svebor Karaman"
__email__ = "svebor.karaman (at) columbia.edu"
# Please feel free to contact me if you have questions/suggestions
 
 
imagedltimeout = 4
base_search_url = "https://localhost:80/cufacesearch/"
 

def get_b64_from_data(data):
 """ Encode data to base64.
 """
 b64_from_data = None
 try:
    b64_from_data = base64.b64encode(data)
 except:
    print("Could not read data to compute base64 string.")
 return b64_from_data
 

def get_b64_from_URL_StringIO(url,verbose=False):
 """ Encode the image at 'url' into a base64 string.
 """
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
 
 # Get options
 parser = OptionParser()
 parser.add_option("-u", "--user", dest="user")
 parser.add_option("-p", "--password", dest="password")
 parser.add_option("-i", "--image", dest="image_data", required=True)
 # Should be one of the valid_service_urls:
 # - base_search_url+"byURL" [default]
 # - base_search_url+"bySHA1"
 # - base_search_url+"byB64" [could induce a big -i parameter, so -i should still be the URL of the image to be encoded in base64]
 parser.add_option("-s", "--service", dest="service_url", default=base_search_url+"byURL")
 parser.add_option("-r", "--request_type", dest="request_type", default="POST")
 parser.add_option("-o", "--options", dest="options", default="{}")
  
 # Validate options
 valid_request_types = ["GET", "POST"]
 valid_service_urls = [base_search_url+x for x in ["byURL", "bySHA1", "byB64"]]
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
 
 # Prepare data
 userpasswd = base64.encodestring(c_options.user.strip()+':'+c_options.password.strip()).strip()
 headers = { 'Content-type' : 'application/x-www-form-urlencoded', 'Authorization' : 'Basic {}'.format(userpasswd)}
 if c_options.service_url.strip().endswith("byB64"):
    print "Encoding image to base64."
    base64image = get_b64_from_URL_StringIO(c_options.image_data)
       payload = { 'data': base64image}
    if c_options.request_type == "GET":
        print("[WARNING] Beware using GET and byB64 could give a 414 error. Please use POST")
 else:
    payload = { 'data': c_options.image_data}
 
 
 payload['options'] = c_options.options
 # Request
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
```

### Output format for an image search API

The output format is a JSON with the top-level fields:

- `NumberImages`: the number of images submitted.
- `NumberSimilarImages`: the total number of similar images found.
- `Timing`: the total time it took to process the query.
- `AllSimilarImages`: an array containing the similar images information.

Each value in the `AllSimilarImages` array has the fields:

- `QuerySha1`: the SHA1 checksum of the query image.
- `QueryURL`: the query image URL. Not present if query was made with B64 encoded image.
- `ImgInfo`: An array containing some information about the query image, as [format, width, height] e.g. ["JPEG", 200, 292]
- `SimilarImages`: JSON containing the information about the similar images.

Each `SimilarImages` JSON has the fields:

- `NumberImages`: the number of similar images found for the query image.
- `Distances`: array of distances values between the query image and the similar images.
- `ImageSha1s`: array of the image sha1s of the similar images.
- `CachedImageURLs`: array of the cached URLs in S3 of the similar images.
- `ImgInfo`: Array of information about the similar images. Actually empty for now.

### Sample query code for an image search API

Below is a sample python script than you can run to test the image search API.

```python
import json
import base64
import requests
from StringIO import StringIO
from optparse import OptionParser

 
""" Test file for the Columbia Image Search Service for MEMEX.
"""
__author__ = "Svebor Karaman"
__email__ = "svebor.karaman (at) columbia.edu"
# Please feel free to contact me if you have questions/suggestions
 
 
imagedltimeout = 4
base_search_url = "https://localhost:80/cuimgsearch/"
 

def get_b64_from_data(data):
 """ Encode data to base64.
 """
 b64_from_data = None
 try:
    b64_from_data = base64.b64encode(data)
 except:
    print("Could not read data to compute base64 string.")
 return b64_from_data
 

def get_b64_from_URL_StringIO(url,verbose=False):
 """ Encode the image at 'url' into a base64 string.
 """
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
 
 # Get options
 parser = OptionParser()
 parser.add_option("-u", "--user", dest="user")
 parser.add_option("-p", "--password", dest="password")
 parser.add_option("-i", "--image", dest="image_data", required=True)
 # Should be one of the valid_service_urls:
 # - base_search_url+"byURL" [default]
 # - base_search_url+"bySHA1"
 # - base_search_url+"byB64" [could induce a big -i parameter, so -i should still be the URL of the image to be encoded in base64]
 parser.add_option("-s", "--service", dest="service_url", default=base_search_url+"byURL")
 parser.add_option("-r", "--request_type", dest="request_type", default="POST")
 parser.add_option("-o", "--options", dest="options", default="{}")
  
 # Validate options
 valid_request_types = ["GET", "POST"]
 valid_service_urls = [base_search_url+x for x in ["byURL", "bySHA1", "byB64"]]
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
 
 # Prepare data
 userpasswd = base64.encodestring(c_options.user.strip()+':'+c_options.password.strip()).strip()
 headers = { 'Content-type' : 'application/x-www-form-urlencoded', 'Authorization' : 'Basic {}'.format(userpasswd)}
 if c_options.service_url.strip().endswith("byB64"):
    print "Encoding image to base64."
    base64image = get_b64_from_URL_StringIO(c_options.image_data)
       payload = { 'data': base64image}
    if c_options.request_type == "GET":
        print("[WARNING] Beware using GET and byB64 could give a 414 error. Please use POST")
 else:
    payload = { 'data': c_options.image_data}
 
 
 payload['options'] = c_options.options
 # Request
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
```