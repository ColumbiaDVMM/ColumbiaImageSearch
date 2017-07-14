from flask import Flask
from flask_restful import Api
from datetime import datetime
from argparse import ArgumentParser

from cufacesearch.api import face_api_lopq
from cufacesearch.searcher import searcher_lopqhbase

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
global_conf_file = '../conf/global_conf_facesearch.json'

api.add_resource(face_api_lopq.APIResponder, '/cufacesearch/<string:mode>')

if __name__ == '__main__':

  # Parse input parameters
  parser = ArgumentParser()
  parser.add_argument("-c", "--conf", dest="conf_file", default=None)
  parser.add_argument("-p", "--port", dest="port", type=int, default=5000)
  options = parser.parse_args()

  # Try to fallback to default conf
  if options.conf_file is not None:
    print "Setting conf file to: {}".format(options.conf_file)
    global_conf_file = options.conf_file

  # Initialize searcher object only once
  face_api_lopq.global_searcher = searcher_lopqhbase.SearcherLOPQHBase(global_conf_file)
  face_api_lopq.global_start_time = datetime.now()

  # Start API
  print 'Starting Face Search API on port {}'.format(options.port)
  from gevent.wsgi import WSGIServer
  http_server = WSGIServer(('', options.port), app)
  http_server.serve_forever()


