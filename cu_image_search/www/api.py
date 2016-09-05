from flask import Flask, Markup, flash, request, render_template, make_response
from flask_restful import Resource, Api

import os
import sys
import time
import happybase
sys.path.append('../..')

import cu_image_search
from cu_image_search.search import searcher_hbaseremote

app = Flask(__name__)
app.secret_key = "secret_key"
app.config['SESSION_TYPE'] = 'filesystem'
api = Api(app)

global_conf_file = '../../conf/global_var_remotehbase.json'
global_searcher = None

class Searcher(Resource):


    def __init__(self):
        #self.searcher = searcher_hbaseremote.Searcher(global_conf_file)
        self.searcher = global_searcher


    def get(self, mode):
        print("[get] received parameters: {}".format(request.args.keys()))
        query = request.args.get('data')
        print("[get] received data: {}".format(query))
        if query:
            return self.process_query(mode, query)
        else:
            return self.process_mode(mode)
    

    def process_mode(self, mode):
        if mode == "refresh":
            return self.refresh()
        else:
            return {'error': 'unknown_mode: '+str(mode)+'. Did you forget to give data parameter?'}


    def process_query(self, mode, query):
        if mode == "byURL":
            return self.search_byURL(query)
        elif mode == "byURL_nocache":
            return self.search_byURL_nocache(query)
        elif mode == "bySHA1":
            return self.search_bySHA1(query)
        elif mode == "bySHA1_nocache":
            return self.search_bySHA1_nocache(query)
        elif mode == "byB64":
            return self.search_byB64(query)
        elif mode == "byB64_nocache":
            return self.search_byB64_nocache(query)
        elif mode == "view_image_sha1":
            return self.view_image_sha1(query)
        elif mode == "view_similar_images_sha1":
            return self.view_similar_images_sha1(query)
        else:
            return {'error': 'unknown_mode: '+str(mode)}


    def put(self, mode):
        print("[put] received parameters: {}".format(request.form.keys()))
        query = request.form['data']
        print("[put] received data: {}".format(query))
        if not query:
            return {'error': 'no data received'}
        else:
            return self.process_query(mode, query)


    def post(self, mode):
        print("[post] received parameters: {}".format(request.form.keys()))
        query = request.form['data']
        print("[post] received data: {}".format(query))
        if not query:
            return {'error': 'no data received'}
        else:
            return self.process_query(mode, query)

    def search_byURL(self, query):
        query_urls = query.split(',')
        # look for s3url in s3url sha1 mapping?
        # if not present, download and compute sha1
        # search for similar images by sha1 for those we could retrieve
        # search with 'search_image_list' for other images
        return self.searcher.search_image_list(query_urls)


    def search_byURL_nocache(self, query):
        query_urls = query.split(',')
        return self.searcher.search_image_list(query_urls)

    def search_bySHA1_nocache(self, query):
        query_sha1s = query.split(',')
        feats,ok_ids = self.searcher.indexer.get_precomp_from_sha1(query_sha1s,["sentibank"])
        corrupted = [i for i in range(len(query_sha1s)) if i not in ok_ids]
        # featuresfile may require a full path
        featuresfile = "tmp"+str(time.time())
        with open(featuresfile,'wb') as out:
            for i,_ in enumerate(feats):
                tmp_feat = feats[i]
                out.write(tmp_feat)
        simname = self.searcher.indexer.hasher.get_similar_images_from_featuresfile(featuresfile, self.searcher.ratio)
        out = self.searcher.format_output(simname, len(query_sha1s), corrupted, query_sha1s)
        # cleanup
        os.remove(simname)
        os.remove(featuresfile)
        return out
        

    def search_bySHA1(self, query):
        import numpy as np
        out = {}
        dec = 0
        # cached sha1 search
        query_sha1s = [str(x) for x in query.split(',')]
        print("[search_bySHA1] query_sha1s {}".format(query_sha1s))
        # retrieve similar images from hbase table 'escorts_images_similar_row'
        corrupted = []
        sim_rows = []
        rows_sim = self.searcher.indexer.get_similar_images_from_sha1(query_sha1s)
        retrieved_sha1s = [x[0] for x in rows_sim]
        print("[search_bySHA1] retrieved {}".format(retrieved_sha1s))
        for i,sha1 in enumerate(query_sha1s):
            if sha1 in retrieved_sha1s:
                tmp_row = rows_sim[i-dec]
                sha1s_sim = [x.split(':')[1] for x in tmp_row[1]]
                dists_sim = [np.float32(tmp_row[1][x]) for x in tmp_row[1]]
                # we should sort by distances
                sorted_pos = np.argsort(dists_sim)
                ids_sim = self.searcher.indexer.get_ids_from_sha1s_hbase(sha1s_sim)
                print("[search_bySHA1] found similar images {} with distances {} and ids {} for {}.".format(sha1s_sim, dists_sim, ids_sim, sha1))
                dict_ids = {}
                for t_id in ids_sim:
                    dict_ids[t_id[1]] = t_id[0]
                pos_ok = []
                sim_row = ""
                for pos in sorted_pos:
                    if sha1s_sim[pos] in dict_ids:
                        pos_ok.append(pos)
                        if not sim_row:
                            sim_row += str(sha1s_sim[pos])
                        else:
                            sim_row += " "+str(sha1s_sim[pos])
                for pos in pos_ok:
                    sim_row += " "+str(dists_sim[pos])
                print("[search_bySHA1] sim_row for {}: {}".format(sha1, sim_row))
                sim_rows.append(sim_row)
            else:
                sim_rows.append("")
                dec += 1
        simname = "sim"+str(time.time())
        with open(simname,'wb') as outsim:
            for row in sim_rows:
                outsim.write(row+"\n")
        out = self.searcher.format_output(simname, len(query_sha1s), corrupted, query_sha1s, True)
        # cleanup
        os.remove(simname)
        return out
    

    def search_byB64(self, query):
        return {'query_by_b64': str(query)}


    def search_byB64_nocache(self, query):
        query_b64s = [str(x) for x in query.split(',')]
        import shutil
        import base64
        search_id = "tmp"+str(time.time())
        list_imgs = []
        for i,one_b64 in enumerate(query_b64s):
            img_fn = search_id+'_'+str(i)
            with open(img_fn, 'wb') as f:
                f.write(base64.b64decode(one_b64))
            list_imgs.append(img_fn)
        outp = self.searcher.search_from_image_filenames_nocache(list_imgs, search_id)
        # cleanup
        for f in list_imgs:
            os.remove(f)
        return outp


    def refresh(self):
        return {'refresh': 'should_refresh'}


    def get_image_str(self, row):
        return "<img src=\"{}\" title=\"{}\" class=\"img_blur\">".format(row[1]["info:s3_url"],row[0])

    def view_image_sha1(self, query):
        query_sha1s = [str(x) for x in query.split(',')]
        rows = self.searcher.indexer.get_columns_from_sha1_rows(query_sha1s, ["info:s3_url"])
        images_str = ""
        for row in rows:
            images_str += self.get_image_str(row)
        images = Markup(images_str)
        flash(images)
        headers = {'Content-Type': 'text/html'}
        return make_response(render_template('view_images.html'),200,headers)


    def view_similar_images_sha1(self, query):
        query_sha1s = [str(x) for x in query.split(',')]
        rows_sim = self.searcher.indexer.get_similar_images_from_sha1(query_sha1s)
        similar_images = []
        for i in range(len(rows_sim)):
            query_image_row = self.searcher.indexer.get_columns_from_sha1_rows([str(rows_sim[i][0])], ["info:s3_url"])
            print("[view_similar_images_sha1] query_image_row: {}".format(query_image_row))
            similar_images.append("<h2>Query image:</h2>"+self.get_image_str(query_image_row[0])+"<h2>Query results:</h2>")
            sim_sha1s = [x.split(':')[1] for x in rows_sim[i][1]]
            sim_rows = self.searcher.indexer.get_columns_from_sha1_rows(sim_sha1s, ["info:s3_url"])
            print("[view_similar_images_sha1] sim_rows: {}".format(sim_rows))
            for row in sim_rows:
                similar_images[i] += self.get_image_str(row)
            similar_images[i] = Markup(similar_images[i]+"<br/><br/>")
        flash(similar_images)
        headers = {'Content-Type': 'text/html'}
        return make_response(render_template('view_similar_images.html'),200,headers)

#api.add_resource(Searcher, '/cu_image_search/<string:mode>/<path:query>', '/cu_image_search/<string:mode>', methods=['GET', 'POST'])
api.add_resource(Searcher, '/cu_image_search/<string:mode>')

if __name__ == '__main__':
    global_searcher = searcher_hbaseremote.Searcher(global_conf_file)
    app.run(debug=True, host='0.0.0.0')
