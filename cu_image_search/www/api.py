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
        return {'query_by_sha1': str(query)}

    def search_byB64(self, query):
        return {'query_by_b64': str(query)}


    def refresh(self):
        return {'refresh': 'should_refresh'}


api.add_resource(Searcher, '/cu_image_search/<string:mode>/<path:query>', '/cu_image_search/<string:mode>')

if __name__ == '__main__':
    app.run(debug=True)
