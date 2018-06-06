from __future__ import print_function

import os
import sys
import time
from datetime import datetime
from flask import Flask
from flask_restful import Api

from cufacesearch.api import api
from cufacesearch.searcher import searcher_lopqhbase

app = Flask(__name__)
GLOBAL_CONF = None
GLOBAL_PORT = 5000
GLOBAL_ENDPOINT = "cuimgsearch"

def setup_app(app):
  print("Setting up application...")
  app.secret_key = "secret_key"
  app.config['SESSION_TYPE'] = 'filesystem'

  searchapi = Api(app)
  app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024

  # Set parameters
  options = dict()
  # Get parameters from environment variables
  options["conf"] = os.environ['SEARCH_CONF_FILE']
  options["port"] = os.getenv('SEARCH_PORT', GLOBAL_PORT)
  options["endpoint"] = os.getenv('SEARCH_ENDPOINT', GLOBAL_ENDPOINT)

  api.global_start_time = datetime.now()
  # Set api.input_type too? How?

  # Initialize searcher object only once
  while True:
    try:
      api.global_searcher = searcher_lopqhbase.SearcherLOPQHBase(options["conf"])
      break
    except Exception as inst:
      err_msg = "Failed to initialized searcher ({}): {}".format(type(inst), inst)
      print(err_msg)
      sys.stdout.flush()
      time.sleep(60)

  # Setup API
  searchapi.add_resource(api.APIResponder, '/' + options["endpoint"] + '/<string:mode>')

  @app.after_request
  def after_request(response):
    """ Adds appropriate headers to the HTTP response.

    :type response: HTTP response object.
    """
    response.headers.add('Access-Control-Allow-Headers',
                         'Keep-Alive,User-Agent,If-Modified-Since,Cache-Control,x-requested-with,Content-Type,origin,authorization,accept,client-security-token')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
    return response


setup_app(app)


if __name__ == '__main__':
  app.run()
