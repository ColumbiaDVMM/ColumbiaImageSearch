import sys
import time
import json
from datetime import datetime

from flask import Markup, flash, request, render_template, make_response
from flask_restful import Resource

from ..imgio.imgio import ImageMIMETypes, get_SHA1_img_type_from_B64, get_SHA1_img_info_from_buffer, buffer_to_B64
from ..detector.utils import build_bbox_str_list


from socket import *
sock = socket()
sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

global_searcher = None
global_start_time = None
input_type = "image"

class APIResponder(Resource):

  def __init__(self):
    self.searcher = global_searcher
    self.start_time = global_start_time
    self.input_type = input_type
    # how to blur canvas images but keep the face clean?
    self.valid_options = ["near_dup", "near_dup_th", "no_blur", "detect_only", "max_height", "max_returned"]
    # TODO: should come from indexer.img_URL_column
    self.column_url = "info:s3_url"

  def get(self, mode):
    query = request.args.get('data')
    options = request.args.get('options')
    if query:
      print "[get] received parameters: {}".format(request.args.keys())
      try:
        print u'[get] received data: '+query.encode('utf-8').strip()
      except:
        print '[get] data contains unicode'
      print "[get] received options: {}".format(options)
      return self.process_query(mode, query, options)
    else:
      return self.process_mode(mode)

  def put(self, mode):
    return self.put_post(mode)

  def post(self, mode):
    return self.put_post(mode)

  def put_post(self, mode):
    print("[put/post] received parameters: {}".format(request.form.keys()))
    print("[put/post] received request: {}".format(request))
    query = request.form['data']
    try:
      options = request.form['options']
    except:
      options = None
    print("[put/post] received data of length: {}".format(len(query)))
    print("[put/post] received options: {}".format(options))
    if not query:
      return {'error': 'no data received'}
    else:
      return self.process_query(mode, query, options)

  def process_mode(self, mode):
    if mode == "status":
      return self.status()
    elif mode == "refresh":
      return self.refresh()
    else:
      return {'error': 'unknown_mode: '+str(mode)+'. Did you forget to give \'data\' parameter?'}

  def process_query(self, mode, query, options=None):
    start = time.time()
    if mode == "byURL":
      resp = self.search_byURL(query, options)
    elif mode == "bySHA1":
      resp = self.search_bySHA1(query, options)
    elif mode == "byPATH":
      resp = self.search_byPATH(query, options)
    elif mode == "byB64":
      resp = self.search_byB64(query, options)
    elif mode == "view_image_sha1":
      return self.view_image_sha1(query, options)
    elif mode == "view_similar_byURL":
      query_reponse = self.search_byURL(query, options)
      return self.view_similar_query_response('URL', query, query_reponse, options)
    elif mode == "view_similar_byB64":
      query_reponse = self.search_byB64(query, options)
      return self.view_similar_query_response('B64', query, query_reponse, options)
    elif mode == "view_similar_byPATH":
      query_reponse = self.search_byPATH(query, options)
      return self.view_similar_query_response('PATH', query, query_reponse, options)
    elif mode == "view_similar_bySHA1":
      query_reponse = self.search_bySHA1(query, options)
      return self.view_similar_query_response('SHA1', query, query_reponse, options)
    # elif mode == "byURL_nocache":
    #   resp = self.search_byURL_nocache(query, options)
    # elif mode == "bySHA1_nocache":
    #   resp = self.search_bySHA1_nocache(query, options)
    # elif mode == "byB64_nocache":
    #   resp = self.search_byB64_nocache(query, options)
    else:
      return {'error': 'unknown_mode: '+str(mode)}
    resp['Timing'] = time.time()-start
    return resp

  def get_options_dict(self, options):
    errors = []
    options_dict = dict()
    if options:
      try:
        options_dict = json.loads(options)
      except Exception as inst:
        err_msg = "[get_options: error] Could not load options from: {}. {}".format(options, inst)
        print(err_msg)
        errors.append(err_msg)
      for k in options_dict:
        if k not in self.valid_options:
          err_msg = "[get_options: error] Unknown option {}".format(k)
          print(err_msg)
          errors.append(err_msg)
    return options_dict, errors

  def append_errors(self, outp, errors=[]):
    if errors:
      e_d = dict()
      if 'errors' in outp:
        e_d = outp['errors']
      for i,e in enumerate(errors):
        e_d['error_{}'.format(i)] = e
      outp['errors'] = e_d
    return outp

  def search_byURL(self, query, options=None):
    query_urls = self.get_clean_urls_from_query(query)
    options_dict, errors = self.get_options_dict(options)
    #outp = self.searcher.search_image_list(query_urls, options_dict)
    outp = self.searcher.search_imageURL_list(query_urls, options_dict)
    outp_we = self.append_errors(outp, errors)
    sys.stdout.flush()
    return outp_we

  def search_byPATH(self, query, options=None):
    query_paths = query.split(',')
    options_dict, errors = self.get_options_dict(options)
    outp = self.searcher.search_image_path_list(query_paths, options_dict)
    outp_we = self.append_errors(outp, errors)
    sys.stdout.flush()
    return outp_we

  def search_bySHA1(self, query, options=None):
    query_sha1s = query.split(',')
    options_dict, errors = self.get_options_dict(options)
    # get the URLs from HBase and search
    # TODO: how to deal with ingestion from folder here?...
    rows_urls = self.searcher.indexer.get_columns_from_sha1_rows(query_sha1s, columns=[self.column_url])
    query_urls = [row[1][self.column_url] for row in rows_urls]
    #outp = self.searcher.search_image_list(query_urls, options_dict)
    outp = self.searcher.search_imageURL_list(query_urls, options_dict)
    outp_we = self.append_errors(outp, errors)
    sys.stdout.flush()
    return outp_we

  def search_byB64(self, query, options=None):
    query_b64s = [str(x) for x in query.split(',')]
    options_dict, errors = self.get_options_dict(options)
    outp = self.searcher.search_imageB64_list(query_b64s, options_dict)
    outp_we = self.append_errors(outp, errors)
    sys.stdout.flush()
    return outp_we

  def refresh(self):
    # Force check if new images are available in HBase
    # Could be called if data needs to be as up-to-date as it can be...
    if self.searcher:
      self.searcher.load_codes()
    return {'refresh': 'just run a new refresh'}

  def status(self):
    # prepare output
    status_dict = {'status': 'OK'}

    status_dict['API_start_time'] = self.start_time.isoformat(' ')
    status_dict['API_uptime'] = str(datetime.now()-self.start_time)

    # Try to refresh on status call but at most every hour
    if self.searcher.last_refresh:
      last_refresh_time = self.searcher.last_refresh
    else:
      last_refresh_time = self.searcher.indexer.last_refresh

    diff_time = datetime.now()-last_refresh_time
    if self.searcher and diff_time.total_seconds() > 3600*4:
      self.searcher.load_codes()
      last_refresh_time = self.searcher.last_refresh

    status_dict['last_refresh_time'] = last_refresh_time.isoformat(' ')
    status_dict['nb_indexed'] = str(self.searcher.searcher.get_nb_indexed())
    return status_dict

  #TODO: Deal with muliple query images with an array parameter request.form.getlist(key)
  @staticmethod
  def get_clean_urls_from_query(query):
    """ To deal with comma in URLs.
    """
    # TODO: fix issue with unicode in URL
    tmp_query_urls = ['http'+str(x) for x in query.split('http') if x]
    query_urls = []
    for x in tmp_query_urls:
      if x[-1] == ',':
        query_urls.append(x[:-1])
      else:
        query_urls.append(x)
    print "[get_clean_urls_from_query: info] {}".format(query_urls)
    return query_urls

  def get_image_str(self, row):
    return "<img src=\"{}\" title=\"{}\" class=\"img_blur\">".format(row[1]["info:s3_url"],row[0])

  def view_image_sha1(self, query, options=None):
    # Not really used anymore...
    query_sha1s = [str(x) for x in query.split(',')]
    rows = self.searcher.indexer.get_columns_from_sha1_rows(query_sha1s, ["info:s3_url"])
    images_str = ""
    # TODO: change this to actually just produce a list of images to fill a new template
    for row in rows:
      images_str += self.get_image_str(row)
    images = Markup(images_str)
    flash(images)
    headers = {'Content-Type': 'text/html'}
    return make_response(render_template('view_images.html'),200,headers)

  def view_similar_query_response(self, query_type, query, query_response, options=None):

    if query_type == 'B64':
      # get :
      # - sha1 to be able to map to query response
      # - image type to make sure the image is displayed properly
      # - embedded format for each b64 query
      # TODO: use array parameter
      query_list = query.split(',')
      query_b64_infos = [get_SHA1_img_type_from_B64(q) for q in query_list]
      query_urls_map = dict()
      for img_id, img_info in enumerate(query_b64_infos):
        query_urls_map[img_info[0]] = "data:"+ImageMIMETypes[img_info[1]]+";base64,"+str(query_list[img_id])
    elif query_type == "PATH":
      # Encode query in B64
      query_infos = []
      query_list = query.split(',')
      query_list_B64 = []
      for q in query_list:
        with open(q,'rb') as img_buffer:
          query_infos.append(get_SHA1_img_info_from_buffer(img_buffer))
          query_list_B64.append(buffer_to_B64(img_buffer))
      query_urls_map = dict()
      for img_id, img_info in enumerate(query_infos):
        query_urls_map[img_info[0]] = "data:" + ImageMIMETypes[img_info[1]] + ";base64," + str(query_list_B64[img_id])
    elif query_type == 'SHA1' or query_type == "URL":
      # URLs should already be in query response
      pass
    else:
      print "[view_similar_query_response: error] Unknown query_type: {}".format(query_type)
      return None

    # Get errors
    options_dict, errors_options = self.get_options_dict(options)

    # Parse similar faces response
    all_sim_faces = query_response[self.searcher.do.map['all_similar_'+self.input_type+'s']]
    search_results = []
    print "[view_similar_query_response: log] len(sim_images): {}".format(len(all_sim_faces))
    for i in range(len(all_sim_faces)):
      # Parse query face, and build face tuple (sha1, url/b64 img, face bounding box)
      query_face = all_sim_faces[i]
      #print "query_face [{}]: {}".format(query_face.keys(), query_face)
      sys.stdout.flush()
      query_sha1 = query_face[self.searcher.do.map['query_sha1']]
      if query_type == 'B64' or query_type == "PATH":
        query_face_img = query_urls_map[query_sha1]
      else:
        query_face_img = query_face[self.searcher.do.map['query_url']]
      if self.searcher.do.map['query_'+self.input_type] in query_face:
        query_face_bbox = query_face[self.searcher.do.map['query_'+self.input_type]]
        query_face_bbox_compstr = build_bbox_str_list(query_face_bbox)
      else:
        query_face_bbox_compstr = []
      img_size = None
      if self.searcher.do.map['img_info'] in query_face:
        img_size = query_face[self.searcher.do.map['img_info']][1:]
      out_query_face = (query_sha1, query_face_img, query_face_bbox_compstr, img_size)
      # Parse similar faces
      similar_faces = query_face[self.searcher.do.map['similar_'+self.input_type+'s']]
      #print similar_faces[self.searcher.do.map['number_faces']]
      out_similar_faces = []
      for j in range(similar_faces[self.searcher.do.map['number_'+self.input_type+'s']]):
        # build face tuple (sha1, url/b64 img, face bounding box, distance) for one similar face
        osface_sha1 = similar_faces[self.searcher.do.map['image_sha1s']][j]
        #if query_type == "PATH":
        if self.searcher.file_input:
          with open(similar_faces[self.searcher.do.map['cached_image_urls']][j], 'rb') as img_buffer:
            img_info = get_SHA1_img_info_from_buffer(img_buffer)
            img_B64 = buffer_to_B64(img_buffer)
          osface_url = "data:" + ImageMIMETypes[img_info[1]] + ";base64," + str(img_B64)
        else:
          osface_url = similar_faces[self.searcher.do.map['cached_image_urls']][j]
        osface_bbox_compstr = None
        if self.input_type != "image":
          osface_bbox = similar_faces[self.searcher.do.map[self.input_type+'s']][j]
          osface_bbox_compstr = build_bbox_str_list(osface_bbox)
        osface_img_size = None
        if self.searcher.do.map['img_info'] in similar_faces:
          osface_img_size = similar_faces[self.searcher.do.map['img_info']][j][1:]
        osface_dist = similar_faces[self.searcher.do.map['distances']][j]
        out_similar_faces.append((osface_sha1, osface_url, osface_bbox_compstr, osface_dist, osface_img_size))
      # build output
      search_results.append((out_query_face, [out_similar_faces]))

    # Prepare settings
    settings = dict()
    settings["no_blur"] = False
    if "no_blur" in options_dict:
      settings["no_blur"] = options_dict["no_blur"]
    if "max_height" in options_dict:
      settings["max_height"] = options_dict["max_height"]

    headers = {'Content-Type': 'text/html'}

    #print search_results
    sys.stdout.flush()
    if self.input_type != "image":
      return make_response(render_template('view_similar_faces_wbbox.html',
                                         settings=settings,
                                         search_results=search_results),
                         200, headers)
    else:
      # if query_type == "PATH":
      #   return make_response(render_template('view_similar_images_local.html',
      #                                        settings=settings,
      #                                        search_results=search_results),
      #                        200, headers)
      # else:
      return make_response(render_template('view_similar_images.html',
                                           settings=settings,
                                           search_results=search_results),
                             200, headers)

  # def search_byURL(self, query, options=None):
  #   return self.search_byURL_nocache(query, options)

  # def search_bySHA1(self, query, options=None):
  #   # could use precomputed faces detections and features here
  #   # could also have precomputed similarities and return them here
  #   return self.search_bySHA1_nocache(query, options)

  # def search_byB64(self, query, options=None):
  #   # we can implement a version that computes the sha1
  #   # and get preocmputed features from HBase
  #   # left for later as we consider queries with b64 are for out of index images
  #   return self.search_byB64_nocache(query, options)