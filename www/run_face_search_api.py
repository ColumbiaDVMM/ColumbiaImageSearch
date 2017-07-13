from flask import Flask
from flask_restful import Api
from datetime import datetime
from argparse import ArgumentParser

from cu_face_search.api import face_api_lopq
from cu_face_search.searcher import searcher_lopqhbase

app = Flask(__name__)
app.secret_key = "secret_key"
app.config['SESSION_TYPE'] = 'filesystem'

api = Api(app)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024


@app.after_request
def after_request(response):
  response.headers.add('Access-Control-Allow-Headers', 'Keep-Alive,User-Agent,If-Modified-Since,Cache-Control,x-requested-with,Content-Type,origin,authorization,accept,client-security-token')
  response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
  return response

# default conf file
global_conf_file = '../../conf/global_var_facesearch.json'

api.add_resource(face_api_lopq.APIResponder, '/cu_image_search/<string:mode>')

if __name__ == '__main__':

  parser = ArgumentParser()
  parser.add_argument("-c", "--conf", dest="conf_file", default=None)
  parser.add_argument("-p", "--port", dest="port", type=int, default=5000)
  options = parser.parse_args()
  if options.conf_file is not None:
    print "Setting conf file to: {}".format(options.conf_file)
    global_conf_file = options.conf_file

  # Initialize all objects only once
  face_api_lopq.global_searcher = searcher_lopqhbase.SearcherLOPQHBase(global_conf_file)
  face_api_lopq.global_detector = dblib_detector.DLibFaceDetector()
  face_api_lopq.global_start_time = datetime.now()

  # Start API
  from gevent.wsgi import WSGIServer
  http_server = WSGIServer(('', options.port), app)
  http_server.serve_forever()