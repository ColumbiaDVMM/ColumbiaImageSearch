import os
import shutil
import logging
import json
import types
import threading
import copy

from flask import Flask, render_template, Response
from flask import request, abort, redirect, url_for
from flask_cors import CORS, cross_origin
from flask_restful import Resource, Api

from config import config
from locker import Locker

# logger
logger = logging.getLogger('api-manager.log')
log_file = logging.FileHandler(config['logging']['file_path'])
logger.addHandler(log_file)
log_file.setFormatter(logging.Formatter(config['logging']['format']))
logger.setLevel(config['logging']['level'])

# flask app
app = Flask(__name__)
cors = CORS(app, resources={r"*": {"origins": "*"}})
api = Api(app)


def api_route(self, *args, **kwargs):
    def wrapper(cls):
        self.add_resource(cls, *args, **kwargs)
        return cls

    return wrapper


api.route = types.MethodType(api_route, api)

# in-memory data
# TODO: how to make it persistent i.e. deal with restart?
data = {}
# what we really care about is knowing for each domain:
# - what is the address for the image similarity service
# - what is the status of indexing (not indexed, indexing, indexed)
# - what is the time range that we have indexed (if not everything)
# - which project is using which domain?...


@app.after_request
def after_request(response):
  response.headers.add('Access-Control-Allow-Headers', 'Keep-Alive,User-Agent,If-Modified-Since,Cache-Control,x-requested-with,Content-Type,origin,authorization,accept,client-security-token')
  response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
  return response

# One locker for project and domain
project_lock = Locker()
domain_lock = Locker()


def _get_project_dir_path(project_name):
    return os.path.join(config['repo']['local_path'], project_name)


def _get_domain_dir_path(domain_name):
    return os.path.join(config['image']['base_domain_dir_path'], domain_name)


def _submit_worfklow(day_to_process, table_sha1, table_update, domain):
    payload = build_images_workflow_payload(day_to_process, table_sha1, table_update, domain)
    json_submit = submit_worfklow(payload)
    job_id = json_submit['id']
    # can use job_id to check status with: get_job_info(job_id)
    return job_id


def json_encode(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError


@api.route('/debug')
class Debug(Resource):

    def get(self):
        if not config['debug']:
            return abort(404)
        debug_info = {
            'data': json.loads(json.dumps(data, default=json_encode))
        }
        return debug_info


@api.route('/')
class Home(Resource):
    def get(self):
        return self.__dict__


@api.route('/projects')
class AllProjects(Resource):
    def post(self):
        input = request.get_json(force=True)
        project_name = input.get('project_name', '')
        if len(project_name) == 0 or len(project_name) >= 256:
            return rest.bad_request('Invalid project name.')
        if project_name in data:
            return rest.exists('Project name already exists.')
        project_sources = input.get('sources', [])
        if len(project_sources) == 0:
            return rest.bad_request('Invalid sources.')

        # create project data structure, folders & files
        project_dir_path = _get_project_dir_path(project_name)
        try:
            project_lock.acquire(project_name)
            if not os.path.exists(project_dir_path):
                os.makedirs(project_dir_path)
            data[project_name] = templates.get('project')
            data[project_name]['master_config'] = templates.get('master_config')
            data[project_name]['master_config']['sources'] = project_sources
            with open(os.path.join(project_dir_path, 'master_config.json'), 'w') as f:
                f.write(json.dumps(data[project_name]['master_config'], indent=4, default=json_encode))
            # we should try to create a service for domain "sources:type" 
            # (or update it if timerange defined by "sources:start_date" and "sources:end_date" is bigger than existing)
            # get domain lock
            # if folder is empty copy data from config['image']['sample_dir_path']
            # build "table_sha1" as images_DOMAIN_sha1_infos
            # build "table_updates" as images_DOMAIN_updates
            # edit config_file
            # we should change "ist_els_doc_type", "HBI_table_sha1infos", and "HBI_table_updatesinfos" from the global conf file (and "HBI_table_sim" ?)
            # _submit_worfklow(day_to_process, table_sha1, table_update, domain)
            # start docker
            # start update and api
            # we need to add the proxypass rule to Apache and restart Apache too...
            # once domain creation has been started how do we give back infos ? [TODO: check with Amandeep]
            # for now consider a predefined pattern based on domain?
            logger.info('project %s created.' % project_name)
            return rest.created()
        except Exception as e:
            logger.error('creating project %s: %s' % (project_name, e.message))
        finally:
            project_lock.release(project_name)

    def get(self):
        return data.keys()

    def delete(self):
        for project_name in data.keys():  # not iterkeys(), need to do del in iteration
            try:
                project_lock.acquire(project_name)
                del data[project_name]
                shutil.rmtree(os.path.join(_get_project_dir_path(project_name)))
            except Exception as e:
                logger.error('deleting project %s: %s' % (project_name, e.message))
                return rest.internal_error('deleting project %s error, halted.' % project_name)
            finally:
                project_lock.remove(project_name)

        return rest.deleted()


@api.route('/projects/<project_name>')
class Project(Resource):
    def post(self, project_name):
        if project_name not in data:
            return rest.not_found()
        input = request.get_json(force=True)
        project_sources = input.get('sources', [])
        if len(project_sources) == 0:
            return rest.bad_request('Invalid sources.')
        try:
            project_lock.acquire(project_name)
            data[project_name]['master_config'] = project_sources
            # this is an update. 
            # TODO: we may need to update the corresponding domain image similarity service
            return rest.created()
        except Exception as e:
            logger.error('Updating project %s: %s' % (project_name, e.message))
            return rest.internal_error('Updating project %s error, halted.' % project_name)
        finally:
            project_lock.release(project_name)

    def put(self, project_name):
        return self.post(project_name)

    def get(self, project_name):
        if project_name not in data:
            return rest.not_found()
        return data[project_name]

    def delete(self, project_name):
        if project_name not in data:
            return rest.not_found()
        try:
            project_lock.acquire(project_name)
            del data[project_name]
            # shutil.rmtree(os.path.join(_get_project_dir_path(project_name)))
            return rest.deleted()
        except Exception as e:
            logger.error('deleting project %s: %s' % (project_name, e.message))
            return rest.internal_error('deleting project %s error, halted.' % project_name)
        finally:
            project_lock.remove(project_name)



if __name__ == '__main__':

    from gevent.wsgi import WSGIServer
    http_server = WSGIServer(('', config['server']['port']), app)
    http_server.serve_forever()
