from flask import Flask
from flask_restful import Resource, Api

import sys
sys.path.append('../..')

import cu_image_search
from cu_image_search.search import searcher_hbaseremote

app = Flask(__name__)
api = Api(app)

global_conf_file = '../../conf/global_var_remotehbase.json'


class Searcher(Resource):


    def __init__(self):
        self.searcher = searcher_hbaseremote.Searcher(global_conf_file)


    def get(self, mode, query=None):
        if query:
            if mode == "byURL":
                return self.search_byURL(query)
            elif mode == "bySHA1":
                return self.search_bySHA1(query)
            elif mode == "byB64":
                return self.search_byB64(query)
            else:
                return {'unknown_mode': str(mode)}
        if mode == "refresh":
            return self.refresh()
        

    def search_byURL(self, query):
        query_urls = query.split(',')
        return self.searcher.search_image_list(query_urls)

    def search_bySHA1(self, query):
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
        return self.searcher.format_output(simname, len(query_sha1s), corrupted, query_sha1s)
        

    def search_byB64(self, query):
        return {'query_by_b64': str(query)}


    def refresh(self):
        return {'refresh': 'should_refresh'}


api.add_resource(Searcher, '/cu_image_search/<string:mode>/<path:query>', '/cu_image_search/<string:mode>')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
