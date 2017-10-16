import sys
import json
import time
from flask import Flask
from flask_restful import Api
from datetime import datetime
from argparse import ArgumentParser

from cufacesearch.api import api
from cufacesearch.searcher import searcher_lopqhbase

app = Flask(__name__)
app.secret_key = "secret_key"
app.config['SESSION_TYPE'] = 'filesystem'

searchapi = Api(app)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024


@app.after_request
def after_request(response):
  response.headers.add('Access-Control-Allow-Headers', 'Keep-Alive,User-Agent,If-Modified-Since,Cache-Control,x-requested-with,Content-Type,origin,authorization,accept,client-security-token')
  response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
  return response

if __name__ == '__main__':

  # Parse input parameters
  parser = ArgumentParser()
  parser.add_argument("-c", "--conf", dest="conf_file", required=True)
  parser.add_argument("-p", "--port", dest="port", type=int, default=5000)
  parser.add_argument("-e", "--endpoint", dest="endpoint", type=str, default="cuimgsearch")
  options = parser.parse_args()

  print "Setting conf file to: {}".format(options.conf_file)
  global_conf = json.load(open(options.conf_file, 'rt'))


  # Initialize searcher object only once
  while True:
    try:
      api.global_searcher = searcher_lopqhbase.SearcherLOPQHBase(global_conf)
      break
    except Exception as inst:
      err_msg = "Failed to initialized searcher ({}): {}".format(type(inst), inst)
      from cufacesearch.common.error import full_trace_error
      full_trace_error(err_msg)
      time.sleep(60)
  api.global_start_time = datetime.now()
  api.input_type = api.global_searcher.input_type

  searchapi.add_resource(api.APIResponder, '/'+options.endpoint+'/<string:mode>')


  # Start API
  print 'Starting Face Search API on port {}'.format(options.port)
  sys.stdout.flush()

  from gevent.wsgi import WSGIServer
  http_server = WSGIServer(('', options.port), app)
  http_server.serve_forever()


