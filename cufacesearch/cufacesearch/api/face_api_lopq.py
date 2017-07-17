import time
import json
from datetime import datetime

from flask import Markup, flash, request, render_template, make_response
from flask_restful import Resource

from ..imgio.imgio import ImageMIMETypes, get_SHA1_img_type_from_B64, build_bbox_str_list


from socket import *
sock = socket()
sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

global_searcher = None
global_start_time = None

# should be under creative commons licence
default_img = "https://c1.staticflickr.com/9/8542/8666789945_0077a6d060_z.jpg"


class APIResponder(Resource):


    def __init__(self):
        self.searcher = global_searcher
        self.start_time = global_start_time
        self.valid_options = ["near_dup", "near_dup_th", "no_blur", "detect_only", "max_height"]
        self.column_url = "info:s3_url"


    def get(self, mode):
        print("[get] received parameters: {}".format(request.args.keys()))
        query = request.args.get('data')
        options = request.args.get('options')
        print("[get] received data: {}".format(query))
        print("[get] received options: {}".format(options))
        if query:
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
        elif mode == "byURL_nocache":
            resp = self.search_byURL_nocache(query, options)
        elif mode == "bySHA1":
            resp = self.search_bySHA1(query, options)
        elif mode == "bySHA1_nocache":
            resp = self.search_bySHA1_nocache(query, options)
        elif mode == "byB64":
            resp = self.search_byB64(query, options)
        elif mode == "byB64_nocache":
            resp = self.search_byB64_nocache(query, options)
        elif mode == "view_similar_byURL":
            query_reponse = self.search_byURL(query, options)
            return self.view_similar_query_response('URL', query, query_reponse, options)
        elif mode == "view_image_sha1":
            return self.view_image_sha1(query, options)
        elif mode == "view_similar_byB64":
            query_reponse = self.search_byB64(query, options)
            return self.view_similar_query_response('B64', query, query_reponse, options)
        elif mode == "view_similar_bySHA1":
            query_reponse = self.search_bySHA1(query, options)
            return self.view_similar_query_response('SHA1', query, query_reponse, options)
        else:
            return {'error': 'unknown_mode: '+str(mode)}
        resp['timing'] = time.time()-start
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
        return self.search_byURL_nocache(query, options)


    def search_byURL_nocache(self, query, options=None):
        query_urls = self.get_clean_urls_from_query(query)
        options_dict, errors = self.get_options_dict(options)
        # TODO: how to deal with URLs that requires identification e.g. for the summer QPR.
        outp = self.searcher.search_image_list(query_urls, options_dict)
        outp_we = self.append_errors(outp, errors)
        return outp_we


    def search_bySHA1(self, query, options=None):
        # could use precomputed faces detections and features here
        # could also have precomputed similarities and return them here
        return self.search_bySHA1_nocache(query, options)


    def search_bySHA1_nocache(self, query, options=None):
        query_sha1s = query.split(',')
        options_dict, errors = self.get_options_dict(options)
        # get the URLs from HBase and search
        rows_urls = self.searcher.indexer.get_columns_from_sha1_rows(query_sha1s, columns=[self.column_url])
        query_urls = [row[1][self.column_url] for row in rows_urls]
        outp = self.searcher.search_image_list(query_urls, options_dict)
        outp_we = self.append_errors(outp, errors)
        return outp_we

    def search_byB64(self, query, options=None):
        # we can implement a version that computes the sha1
        # and get preocmputed features from HBase
        # left for later as we consider queries with b64 are for out of index images
        return self.search_byB64_nocache(query, options)


    def search_byB64_nocache(self, query, options=None):
        query_b64s = [str(x) for x in query.split(',')]
        options_dict, errors = self.get_options_dict(options)
        outp = self.searcher.search_imageB64_list(query_b64s, options_dict)
        outp_we = self.append_errors(outp, errors)
        return outp_we


    def refresh(self):
        # If new codes are avaible in HDFS?
        from ..searcher.searcher_lopqhbase import SearcherLOPQHBase
        new_searcher = SearcherLOPQHBase(self.searcher.global_conf_filename)
        self.searcher = new_searcher
        return {'refresh': 'just run a new refresh'}


    def status(self):
        status_dict = {'status': 'OK'}
        status_dict['API_start_time'] = self.start_time.isoformat(' ')
        status_dict['API_uptime'] = str(datetime.now()-self.start_time)
        if self.searcher.indexer.last_refresh:
            status_dict['last_refresh_time'] = self.searcher.indexer.last_refresh.isoformat(' ')
        else:
            status_dict['last_refresh_time'] = self.searcher.indexer.last_refresh
        status_dict['nb_indexed'] = str(self.searcher.searcher_lopq.nb_indexed)
        return status_dict

    @staticmethod
    def get_clean_urls_from_query(query):
        """ To deal with comma in URLs.
        """
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
        query_list = query.split(',')
        query_b64_infos = [get_SHA1_img_type_from_B64(q) for q in query_list]
        query_urls_map = dict()
        for img_id,img_info in enumerate(query_b64_infos):
          query_urls_map[img_info[0]] = "data:"+ImageMIMETypes[img_info[1]]+";base64,"+str(query_list[img_id])
      elif query_type == 'SHA1' or query_type == "URL":
        # URLs should already be in query response
        pass
      else:
        print "[view_similar_query_response: error] Unknown query_type: {}".format(query_type)
        return None

      # Get errors
      options_dict, errors_options = self.get_options_dict(options)

      # Parse similar faces response
      all_sim_faces = query_response[self.searcher.do.map['all_similar_faces']]
      search_results = []
      print "[view_similar_query_response: log] len(sim_faces): {}".format(len(all_sim_faces))
      for i in range(len(all_sim_faces)):
        # Parse query face, and build face tuple (sha1, url/b64 img, face bounding box)
        query_face = all_sim_faces[i]
        query_sha1 = query_face[self.searcher.do.map['query_sha1']]
        if query_type == 'B64':
          query_face_img = query_urls_map[query_sha1]
        else:
          query_face_img = query_face[self.searcher.do.map['query_url']]
        query_face_bbox = query_face[self.searcher.do.map['query_face']]
        query_face_bbox_compstr = build_bbox_str_list(query_face_bbox)
        # face_width = query_face_bbox['right'] - query_face_bbox['left']
        # face_height = query_face_bbox['bottom'] - query_face_bbox['top']
        # query_face_bbox_compstr = []
        # query_face_bbox_compstr.append(str(query_face_bbox['top']))
        # query_face_bbox_compstr.append(str(query_face_bbox['left']))
        # query_face_bbox_compstr.append(str(face_width))
        # query_face_bbox_compstr.append(str(face_height))
        img_size = query_face[self.searcher.do.map['img_info']][1:]
        out_query_face = (query_sha1, query_face_img, query_face_bbox_compstr, img_size)
        # Parse similar faces
        out_similar_faces = []
        similar_faces = query_face[self.searcher.do.map['similar_faces']]
        for j in range(similar_faces[self.searcher.do.map['number_faces']]):
          # build face tuple (sha1, url/b64 img, face bounding box, distance) for one similar face
          osface_sha1 = similar_faces[self.searcher.do.map['image_sha1s']][j]
          osface_url = similar_faces[self.searcher.do.map['cached_image_urls']][j]
          osface_bbox = similar_faces[self.searcher.do.map['faces']][j]
          osface_bbox_compstr = build_bbox_str_list(osface_bbox)
          osface_img_size = similar_faces[self.searcher.do.map['img_info']][j][1:]
          osface_dist = similar_faces[self.searcher.do.map['distances']][j]
          out_similar_faces.append((osface_sha1, osface_url, osface_bbox_compstr, osface_dist, osface_img_size))
        # build output
        search_results.append([out_query_face, out_similar_faces])

      # Prepare settings
      settings = dict()
      settings["no_blur"] = False
      if "no_blur" in options_dict:
        settings["no_blur"] = options_dict["no_blur"]
      if "max_height" in options_dict:
        settings["max_height"] = options_dict["max_height"]

      headers = {'Content-Type': 'text/html'}

      return make_response(render_template('view_similar_faces_wbbox.html',
                                           settings=settings,
                                           search_results=search_results),
                           200, headers)