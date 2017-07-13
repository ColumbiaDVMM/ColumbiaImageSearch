import time
import json
from datetime import datetime

from flask import Markup, flash, request, render_template, make_response
from flask_restful import Resource

from socket import *
sock = socket()
sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

global_searcher = None
global_start_time = None


class APIResponder(Resource):


    def __init__(self):

        self.searcher = global_searcher
        self.start_time = global_start_time
        self.valid_options = ["near_dup", "near_dup_th", "no_blur", "detect_only"]
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
                    err_msg = "[get_options: error] Unkown option {}".format(k)
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
        # could have precomputed similarities and return them here
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
        self.searcher.indexer.hasher.init_hasher()
        if not self.searcher.indexer.initializing:
            self.searcher.indexer.initialize_sha1_mapping()
            return {'refresh': 'just run a new refresh'}
        else:
            self.searcher.indexer.refresh_inqueue = True
            return {'refresh': 'pushed a refresh in queue.'}


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
        tmp_query_urls = ['http' + x for x in query.split('http') if x]
        query_urls = []
        for x in tmp_query_urls:
            if x[-1] == ',':
                query_urls.append(x[:-1])
            else:
                query_urls.append(x)
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
        if query_type == 'URL':
            query_urls = self.get_clean_urls_from_query(query)
        elif query_type == 'B64':
            # need to get embedded format for each b64 query
            # TODO: deal with different image encoding
            query_urls = ["data:image/jpeg;base64,"+str(q) for q in query.split(',')]
        elif query_type == 'SHA1':
            # need to get url for each sha1 query
            query_urls = []
            for one_query in query_response["images"]:
                query_image_row = self.searcher.indexer.get_columns_from_sha1_rows([str(one_query["query_sha1"])], ["info:s3_url"])
                query_urls.append(query_image_row[0][1]["info:s3_url"])
        else:
            print "[view_similar_query_response: error] Unknown query_type: {}".format(query_type)
            return None

        # Get errors
        options_dict, errors_options = self.get_options_dict(options)
        errors_search = None
        if "errors" in query_response:
            errors_search = query_response["errors"]

        # Parse similar images response
        sim_images = query_response["images"]
        similar_images_response = []
        print "[view_similar_query_response: log] len(query_urls): {}, len(sim_images): {}".format(len(query_urls), len(sim_images))
        for i in range(len(query_urls)):
            if sim_images and len(sim_images)>=i:
                one_res = [(query_urls[i], sim_images[i]["query_sha1"])]
                one_sims = []
                for j,sim_sha1 in enumerate(sim_images[i]["similar_images"]["sha1"]):
                    one_sims += ((sim_images[i]["similar_images"]["cached_image_urls"][j], sim_sha1, sim_images[i]["similar_images"]["distance"][j]),)
                one_res.append(one_sims)
            else:
                one_res = [(query_urls[i], ""), [('','','')]]
            similar_images_response.append(one_res)
        if not similar_images_response:
            similar_images_response.append([('','No results'),[('','','')]])
        flash_message = (False, similar_images_response)
        if "no_blur" in options_dict:
            flash_message = (options_dict["no_blur"],similar_images_response)
        flash(flash_message,'message')
        headers = {'Content-Type': 'text/html'}
        sys.stdout.flush()
        # TODO: pass named arguments instead of flash messages 
        return make_response(render_template('view_similar_images.html'),200,headers)



