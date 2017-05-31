from flask import Flask, Markup, flash, request, render_template, make_response
from flask_restful import Resource, Api

from socket import *

sock=socket()
sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

import os
import sys
import time
import json
from datetime import datetime
from collections import OrderedDict
import imghdr
from datetime import datetime
from argparse import ArgumentParser

from PIL import Image, ImageFile
#Image.LOAD_TRUNCATED_IMAGES = True
#ImageFile.LOAD_TRUNCATED_IMAGES = True
from StringIO import StringIO
import happybase
sys.path.append('../..')

import cu_image_search
from cu_image_search.search import searcher_hbaseremote

app = Flask(__name__)
app.secret_key = "secret_key"
app.config['SESSION_TYPE'] = 'filesystem'

api = Api(app)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024


def get_clean_urls_from_query(query):
    """ To deal with comma in URLs
    """
    tmp_query_urls = ['http'+x for x in query.split('http') if x]
    query_urls = []
    for x in tmp_query_urls:
        if x[-1]==',':
            query_urls.append(x[:-1])
        else:
            query_urls.append(x)
    return query_urls

@app.after_request
def after_request(response):
  #response.headers.add('Access-Control-Allow-Origin', '*')
  #response.headers.add('Access-Control-Allow-Credentials', 'true')
  response.headers.add('Access-Control-Allow-Headers', 'Keep-Alive,User-Agent,If-Modified-Since,Cache-Control,x-requested-with,Content-Type,origin,authorization,accept,client-security-token')
  response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
  return response

if cu_image_search.dev:
    global_conf_file = '../../conf/global_var_remotehbase_dev.json'
else:
    global_conf_file = '../../conf/global_var_remotehbase_release.json'

#global_conf_file = '../../conf/global_var_remotehbase.json'

global_searcher = None
global_start_time = None

class APIResponder(Resource):


    def __init__(self):
        #self.searcher = searcher_hbaseremote.Searcher(global_conf_file)
        self.searcher = global_searcher
        self.start_time = global_start_time
        # "no_diskout" not yet supported. Issue with managing SWIG output
        #self.valid_options = ["near_dup", "near_dup_th", "no_blur", "no_diskout"]
        self.valid_options = ["near_dup", "near_dup_th", "no_blur"]
        # dl_pool_size could be set to a big value for the update process, overwrite here
        self.searcher.indexer.image_downloader.dl_pool_size = 2


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
        # view modes
        elif mode == "view_similar_byURL":
            query_reponse = self.search_byURL(query, options)
            return self.view_similar_query_response('URL', query, query_reponse, options)
        elif mode == "view_image_sha1":
            return self.view_image_sha1(query, options)
        elif mode == "view_similar_bySHA1":
            query_reponse = self.search_bySHA1(query, options)
            return self.view_similar_query_response('SHA1', query, query_reponse, options)
            #return self.view_similar_images_sha1(query, options)
        else:
            return {'error': 'unknown_mode: '+str(mode)}
        # finalize resp
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
        query_urls = get_clean_urls_from_query(query)
        options_dict, errors = self.get_options_dict(options)
        
        # look for s3url in s3url sha1 mapping?
        # if not present, download and compute sha1
        # search for similar images by sha1 for those we could retrieve
        # search with 'search_image_list' for other images
        outp = self.searcher.search_image_list(query_urls, options_dict)
        outp_we = self.append_errors(outp, errors)
        return outp_we


    def search_byURL_nocache(self, query, options=None):
        query_urls = get_clean_urls_from_query(query)
        options_dict, errors = self.get_options_dict(options)
        return self.searcher.search_image_list(query_urls, options_dict)


    def search_bySHA1_nocache(self, query, options=None):
        query_sha1s = query.split(',')
        print("[search_bySHA1_nocache: log] query_sha1s is: {}".format(query_sha1s))
        feats, ok_ids = self.searcher.indexer.get_precomp_from_sha1(query_sha1s,["sentibank"])
        if len(ok_ids[0]) < len(query_sha1s):
            # fall back to URL query
            rows = self.searcher.indexer.get_columns_from_sha1_rows(query_sha1s,["info:s3_url"])
            print("[search_bySHA1_nocache: log] query_sha1s is: {}".format(query_sha1s))
            urls = [row[1]["info:s3_url"] for row in rows]
            # simulate query 
            return self.search_byURL_nocache(','.join(urls), options)
        corrupted = [i for i in range(len(query_sha1s)) if i not in ok_ids[0]]
        # featuresfile may require a full path
        featuresfile = "tmp"+str(time.time())
        with open(featuresfile,'wb') as out:
            for i,_ in enumerate(feats[0]):
                try:
                    tmp_feat = feats[0][i]
                    print("[search_bySHA1_nocache: info] {} tmp_feat.shape was: {}".format(i, tmp_feat.shape))
                    # TypeError: must be string or buffer, not list?
                    out.write(tmp_feat)
                except TypeError as inst:
                    print("[search_bySHA1_nocache: error] tmp_feat was {}. Error was: {}".format(tmp_feat, inst))
        options_dict, errors = self.get_options_dict(options)
        if "no_diskout" in options_dict:
            out_res = self.searcher.indexer.hasher.get_similar_images_from_featuresfile_nodiskout(featuresfile, self.searcher.ratio)
            outp = self.searcher.format_output_nodiskout(out_res, len(query_sha1s), corrupted, query_sha1s, options_dict)
        else:
            simname = self.searcher.indexer.hasher.get_similar_images_from_featuresfile(featuresfile, self.searcher.ratio)
            outp = self.searcher.format_output(simname, len(query_sha1s), corrupted, query_sha1s, options_dict)
        outp_we = self.append_errors(outp, errors)
        # cleanup
        #os.remove(simname)
        #os.remove(featuresfile)
        return outp_we
        

    def search_bySHA1(self, query, options=None):
        # cached sha1 search
        query_sha1s = [str(x) for x in query.split(',')]
        print("[search_bySHA1] query_sha1s {}".format(query_sha1s))
        # validate the sha1 here?
        # retrieve similar images from hbase table 'escorts_images_similar_row_from_ts'
        import numpy as np
        corrupted = []
        sim_rows = []
        dec = 0
        rows_sim = self.searcher.indexer.get_similar_images_from_sha1(query_sha1s)
        retrieved_sha1s = [x[0] for x in rows_sim]
        print("[search_bySHA1_cache] retrieved {}".format(retrieved_sha1s))
        for i,sha1 in enumerate(query_sha1s):
            if sha1 in retrieved_sha1s:
                tmp_row = rows_sim[i-dec]
                sha1s_sim = [x.split(':')[1] for x in tmp_row[1]]
                dists_sim = [np.float32(tmp_row[1][x]) for x in tmp_row[1]]
                # we should sort by distances
                sorted_pos = np.argsort(dists_sim)
                ids_sim = self.searcher.indexer.get_ids_from_sha1s_hbase(sha1s_sim)
                print("[search_bySHA1_cache] found similar images {} with distances {} and ids {} for {}.".format(sha1s_sim, dists_sim, ids_sim, sha1))
                #dict_ids = {}
                #for t_id in ids_sim:
                #    dict_ids[t_id[1]] = t_id[0]
                pos_ok = []
                sim_row = ""
                for pos in sorted_pos:
                    #if sha1s_sim[pos] in dict_ids:
                    pos_ok.append(pos)
                    if not sim_row:
                        sim_row += str(sha1s_sim[pos])
                    else:
                        sim_row += " "+str(sha1s_sim[pos])
                for pos in pos_ok:
                    sim_row += " "+str(dists_sim[pos])
                print("[search_bySHA1_cache] sim_row for {}: {}".format(sha1, sim_row))
                sim_rows.append(sim_row)
            else:
                sim_rows.append("")
                dec += 1
        simname = "sim"+str(time.time())
        with open(simname,'wb') as outsim:
            for row in sim_rows:
                outsim.write(row+"\n")
        options_dict, errors = self.get_options_dict(options)
        options_dict['sha1_sim'] = True
        outp = self.searcher.format_output(simname, len(query_sha1s), corrupted, query_sha1s, options_dict)
        outp_we = self.append_errors(outp, errors)
        # cleanup
        os.remove(simname)
        return outp_we
    

    def search_byB64(self, query, options=None):
        # we can implement a version that computes the sha1
        # and query for percomputed similar images using 
        # self.searcher.indexer.get_similar_images_from_sha1(query_sha1s)
        # left for later as we consider queries with b64 are for out of index images
        return self.search_byB64_nocache(query, options)


    def search_byB64_nocache(self, query, options=None):
        query_b64s = [str(x) for x in query.split(',')]
        options_dict, errors = self.get_options_dict(options)
        import shutil
        import base64
        errors = []
        search_id = "tmp"+str(time.time())
        list_imgs = []
        for i,one_b64 in enumerate(query_b64s):
            img_fn = search_id+'_'+str(i)+'.jpg'
            if one_b64.startswith('data:'):
                # this is the image header
                continue
            try:
                with open(img_fn, 'wb') as f:
                    print("[search_byB64_nocache] Processing base64 encoded image of length {} ending with: {}".format(len(one_b64), one_b64[-20:]))
                    img_byte = base64.b64decode(one_b64)
                    img_type = imghdr.what('', img_byte)
                    if img_type != 'jpeg':
                        #f = StringIO("data:image/png;base64,"+img_byte)
                        f = StringIO(img_byte)
                        try:
                            im = Image.open(f)
                            im.save(img_fn,"JPEG")
                        except IOError:
                            err_msg = "[search_byB64_nocache: error] Non-jpeg image conversion failed."
                            print(err_msg)
                            errors.append(err_msg)
                            os.remove(img_fn)
                    else:
                        f.write(img_byte)
                    list_imgs.append(img_fn)
            except:
                print("[search_byB64_nocache] Error when decoding image.")
                errors.append("[search_byB64_nocache] Error when decoding image with length {}.".format(len(one_b64)))
        outp = self.searcher.search_from_image_filenames_nocache(list_imgs, search_id, options_dict)
        outp_we = self.append_errors(outp, errors)
        # cleanup
        for f in list_imgs:
            try:
                os.remove(f)
            except:
                pass
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
        status_dict['indexed_images'] = len(self.searcher.indexer.sha1_featid_mapping)
        return status_dict


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


    def view_similar_images_sha1(self, query, options=None):
        options_dict, errors = self.get_options_dict(options)
        query_sha1s = [str(x) for x in query.split(',')]
        print("[view_similar_images_sha1] querying with {} sha1s: {}".format(len(query_sha1s), query_sha1s))
        sys.stdout.flush()
        rows_sim = self.searcher.indexer.get_similar_images_from_sha1(query_sha1s)
        similar_images_response = []
        print("[view_similar_images_sha1] found similar images for {} sha1s out of {}.".format(len(rows_sim), len(query_sha1s)))
        sys.stdout.flush()
        for i in range(len(rows_sim)):
            query_image_row = self.searcher.indexer.get_columns_from_sha1_rows([str(rows_sim[i][0])], ["info:s3_url"])
            print("[view_similar_images_sha1] query_image_row: {}".format(query_image_row))
            sys.stdout.flush()
            one_res = [(query_image_row[0][1]["info:s3_url"], query_image_row[0][0])]
            #similar_images.append("<h2>Query image:</h2>"+self.get_image_str(query_image_row[0])+"<h2>Query results:</h2>")
            sim_sha1s = [x.split(':')[1] for x in rows_sim[i][1]]
            sim_rows = self.searcher.indexer.get_columns_from_sha1_rows(sim_sha1s, ["info:s3_url"])
            print("[view_similar_images_sha1] found {} sim_rows.".format(len(sim_rows)))
            sys.stdout.flush()
            one_sims = []
            for row in sim_rows:
                # should add distance here as third element
                one_sims += ((row[1]["info:s3_url"], row[0]), '')
            one_res.append(one_sims)
            print("[view_similar_images_sha1] one_res: {}.".format(one_res))
            sys.stdout.flush()
            #similar_images[i] = Markup(similar_images[i]+"<br/><br/>")
            similar_images_response.append(one_res)
        if not similar_images_response:
            similar_images_response.append([('','No results'),[('','','')]])
        if "no_blur" in options_dict:
            flash((options_dict["no_blur"],similar_images_response))
        else:
            flash((False,similar_images_response))
        headers = {'Content-Type': 'text/html'}
        sys.stdout.flush()
        return make_response(render_template('view_similar_images.html'),200,headers)


    def view_similar_query_response(self, query_type, query, query_response, options=None):
        print "[view_similar_query_response: log] query_type: {}".format(query_type)
        if query_type == 'URL':
            query_urls = get_clean_urls_from_query(query)
        elif query_type == 'SHA1':
            # need to get url for each sha1 query
            query_urls = []
            for one_query in query_response["images"]:
                query_image_row = self.searcher.indexer.get_columns_from_sha1_rows([str(one_query["query_sha1"])], ["info:s3_url"])
                query_urls.append(query_image_row[0][1]["info:s3_url"])
        # TODO for base64 image query as no URL: prepend data:image/jpeg;base64 or whatever is the actually type of image
        else:
            return None
        options_dict, errors_options = self.get_options_dict(options)
        sim_images = query_response["images"]
        errors_search = None
        if "errors" in query_response:
            errors_search = query_response["errors"]
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


api.add_resource(APIResponder, '/cu_image_search/<string:mode>')


if __name__ == '__main__':

    parser = ArgumentParser()
    parser.add_argument("-c", "--conf", dest="conf_file", default=None)
    options = parser.parse_args()
    if options.conf_file is not None:
        print "Setting conf file to: {}".format(options.conf_file)
        global_conf_file = options.conf_file
 
    global_searcher = searcher_hbaseremote.Searcher(global_conf_file)
    global_start_time = datetime.now()
    
    ## This cannot recover from an 'IOError: [Errno 32] Broken pipe' error when client disconnect before response has been sent e.g. nginx timeout at memexproxy...
    #app.run(debug=True, host='0.0.0.0')
    #app.run(debug=False, host='0.0.0.0')

    from gevent.wsgi import WSGIServer
    http_server = WSGIServer(('', 5000), app)
    #http_server = WSGIServer(('', 5002), app)
    http_server.serve_forever()
