import os
import shutil
import logging
import json
import types
import threading
import subprocess as sub

from flask import Flask, render_template, Response
from flask import request, abort, redirect, url_for
from flask_cors import CORS, cross_origin
from flask_restful import Resource, Api

from config import config
from locker import Locker
import rest
from oozie_job_manager import build_images_workflow_payload_v2, submit_worfklow

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
max_ts = 9999999999999

def api_route(self, *args, **kwargs):
    def wrapper(cls):
        self.add_resource(cls, *args, **kwargs)
        return cls

    return wrapper


api.route = types.MethodType(api_route, api)

# in-memory data
# TODO: how to make it persistent i.e. deal with restart?
data = {}
data['domains'] = {}
data['projects'] = {}
# what we really care about is knowing for each domain:
# - what is the address for the image similarity service for one domain (actually one project)
# - what is the status of indexing (not indexed, indexing, indexed)
# - what is the time range that we have indexed (if not everything)
# what are the ports used on the host.


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


def _submit_worfklow(start_ts, end_ts, table_sha1, table_update, domain):
    payload = build_images_workflow_payload_v2(start_ts, end_ts, table_sha1, table_update, domain)
    json_submit = submit_worfklow(payload)
    job_id = json_submit['id']
    logger.info('[submit_worfklow: log] submitted workflow %s for domain %s.' % (job_id, domain))
    # can use job_id to check status with: get_job_info(job_id)
    return job_id


def parse_isodate_to_ts(input_date):
    import dateutil.parser
    import calendar
    parsed_date = dateutil.parser.parse(input_date)
    print "[parsed_date: {}]".format(parsed_date)
    return calendar.timegm(parsed_date.utctimetuple())*1000


def setup_service_url(domain_name):
    # attribute a port (how to make sure it is free? for now just assume it is)
    if 'ports' not in data:
        port = config['image']['first_port'] 
        data['ports'] = []
    else:
        port = max(data['ports'])+1
    data['ports'].append(port)
    # build the proxypass rule for Apache 
    endpt = "/cuimgsearch_{}".format(domain_name)
    lurl = "http://localhost:{}/".format(port)
    proxypass_template = "\nProxyPass {}/ {}\nProxyPassReverse {}/ {}\n<Location {}>\n\tRequire all granted\n</Location>\n"
    proxypass_filled = proxypass_template.format(endpt, lurl, endpt, lurl, endpt)
    logger.info("[setup_service_url: log] updating Apache conf with: {}".format(proxypass_filled))
    # read apache conf file up to '</VirtualHost>'
    outconf_str = ""
    with open(config['image']['apache_conf_file'], 'rt') as inconf:
        for line in inconf:
            # add the new rule before the end
            if line.strip()=='</VirtualHost>':
                outconf_str += proxypass_filled 
            outconf_str += line
    # overwrite conf file
    # this would fail if api is not running with sudo...
    try:
        with open(config['image']['apache_conf_file'], 'wt') as outconf:
            outconf.write(outconf_str)
    except Exception as inst:
        logger.info("[setup_service_url: log] Could not overwrite Apache conf file. {}".format(inst))
        raise IOError("Could not overwrite Apache conf file")

    # and restart Apache
    # this may require root privilege, and would it kill the current connection?
    command = 'sudo service apache2 restart'
    logger.info("[setup_service_url: log] restarting Apache...")
    output, error = sub.Popen(command.split(' '), stdout=sub.PIPE, stderr=sub.PIPE).communicate()
    logger.info("[setup_service_url: log] restarted Apache. out: {}, err: {}".format(output, error))
    service_url = config['image']['base_service_url']+endpt
    return port, service_url    


def check_domain_service(project_sources):
    #logger.info('[check_domain_service: log] project_sources: %s' % (project_sources))
    # why is project_sources a list actually? Assume we want the first entry? Or loop?
    one_source = project_sources[0]
    domain_name = one_source['type']
    try:
        start_ts = parse_isodate_to_ts(one_source['start_date'])
    except Exception as inst:
        logger.error("[check_domain_service: log] Could not parse 'start_date' (error was: {}). Assuming 0 as start_ts.".format(inst))
        start_ts = 0
    try:
        end_ts = parse_isodate_to_ts(one_source['end_date'])
    except:
        logger.error("[check_domain_service: log] Could not parse 'end_date' assuming {} as end_ts.".format(max_ts))
        start_ts = 0
    # get domain lock
    logger.info('[check_domain_service: log] domain_name: %s, start_ts: %s, end_ts: %s' % (domain_name, start_ts, end_ts))
    domain_dir_path = _get_domain_dir_path(domain_name)
    domain_lock.acquire(domain_name)
    if os.path.isdir(domain_dir_path):
        logger.info('[check_domain_service: log] service exists for domain_name: %s, check if we need to update.' % (domain_name))
        # Check conf to see if we need to update 
        config_file = os.path.join(domain_dir_path, config['image']['config_filepath'])
        config_json = json.load(open(config_file,'rt'))
        if 'start_ts' not in config_json or 'end_ts' not in config_json:
            err_msg = 'service exists for domain: %s, but creation seems incomplete.' % (domain_name)
            logger.error('[check_domain_service: error] '+err_msg)
            domain_lock.release(domain_name)
            return -1, err_msg
        new_start_ts = min(start_ts, config_json['start_ts'])
        new_end_ts = max(end_ts, config_json['end_ts'])
        if new_start_ts < config_json['start_ts'] or new_end_ts > config_json['end_ts']:
            # submit workflow with min(start_ts, stored_start_ts) and max(end_ts, stored_end_ts)
            job_id = _submit_worfklow(new_start_ts, new_end_ts, config_json['HBI_table_sha1infos'], config_json['HBI_table_updatesinfos'], domain_name)
            # add job_id to job_ids and save config
            config_json['job_ids'].append(job_id)
            # update data['domains']
            data['domains'][domain]['status'] = 'updating'
            data['domains'][domain]['job_ids'].append(job_id)
            # write out new config file
            with open(config_file, 'wt') as conf_out:
                conf_out.write(json.dumps(config_json))
            logger.info('[check_domain_service: log] updating domain %s' % (domain_name))
    else:
        # if folder is empty copy data from config['image']['sample_dir_path']
        logger.info('[check_domain_service: log] copying from %s to %s' % (config['image']['sample_dir_path'], domain_dir_path))
        try:
            if not os.path.isdir(config['image']['base_domain_dir_path']):
                os.makedirs(config['image']['base_domain_dir_path'])
            shutil.copytree(config['image']['sample_dir_path'], domain_dir_path)
        except shutil.Error as inst:
            raise ValueError('Could not copy from template directory {} to {}. {}'.format(config['image']['sample_dir_path'], domain_dir_path, inst))
        # edit config_file by replacing DOMAIN by the actual domain in :
        # "ist_els_doc_type", "HBI_table_sha1infos", and "HBI_table_updatesinfos" (and "HBI_table_sim" ?)
        config_file = os.path.join(domain_dir_path, config['image']['config_filepath'])
        logger.info('[check_domain_service: log] loading config_file from %s' % (config_file))
        config_json = json.load(open(config_file,'rt'))
        # - ist_els_doc_type
        config_json['ist_els_doc_type'] = domain_name
        # - HBI_table_sha1infos
        config_json['HBI_table_sha1infos'] = config_json['HBI_table_sha1infos'].replace('DOMAIN', domain_name)
        # - HBI_table_updatesinfos
        config_json['HBI_table_updatesinfos'] = config_json['HBI_table_updatesinfos'].replace('DOMAIN', domain_name)
        # - HBI_table_sim
        config_json['HBI_table_sim'] = config_json['HBI_table_sim'].replace('DOMAIN', domain_name)
        # - put start and end date too, so we can check if we need to update.
        # but use ts to actually call the workflow
        config_json['start_date'] = one_source['start_date']
        config_json['end_date'] = one_source['end_date']
        config_json['start_ts'] = start_ts
        config_json['end_ts'] = end_ts
        # submit workflow to get images data
        logger.info('[check_domain_service: log] submitting workflow with parameters: %s, %s, %s, %s, %s' % (start_ts, end_ts, config_json['HBI_table_sha1infos'], config_json['HBI_table_updatesinfos'], domain_name))
        job_id = _submit_worfklow(start_ts, end_ts, config_json['HBI_table_sha1infos'], config_json['HBI_table_updatesinfos'], domain_name)
        # save job id to be able to check status?
        config_json['job_ids'] = [job_id]
        # setup service
        port, service_url = setup_service_url(domain_name)
        data['domains'][domain_name] = {}
        data['domains'][domain_name]['service_url'] = service_url
        data['domains'][domain_name]['status'] = 'indexing'
        data['domains'][domain_name]['job_ids'] = [job_id]
        # write out new config file
        logger.info('[check_domain_service: log] updating config_file: %s' % config_file)
        with open(config_file, 'wt') as conf_out:
            conf_out.write(json.dumps(config_json))
        logger.info('[check_domain_service: log] wrote config_file: %s' % config_file)
        # call start_docker_columbia_image_search_qpr.sh with the right domain and port
        command = '{}/setup/search/start_docker_columbia_image_search_qpr.sh -p {} -d {}'.format(config['image']['host_repo_path'], port, domain_name)
        logger.info("[check_domain_service: log] Starting docker for domain {} with command: {}".format(domain_name, command))
        output, error = sub.Popen(command.split(' '), stdout=sub.PIPE, stderr=sub.PIPE).communicate()
        logger.info("[check_domain_service: log] Started docker for domain {}. out: {}, err: {}".format(domain_name, output, error))
    
    # once domain creation has been started how do we give back infos ? [TODO: check with Amandeep]
    # right back in project config, commit and push?
    # for now consider a predefined pattern based on domain?
    # release lock
    domain_lock.release(domain_name)
    # what should we return?
    return 0, None


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
        logger.info('/projects received: %s' % (input))
        project_name = input.get('project_name', '')
        if len(project_name) == 0 or len(project_name) >= 256:
            return rest.bad_request('Invalid project name.')
        if project_name in data['projects']:
            return rest.exists('Project name already exists.')
        project_sources = input.get('sources', [])
        if len(project_sources) == 0:
            return rest.bad_request('Invalid sources.')

        logger.info('/projects project_name: %s' % (project_name))
        logger.info('/projects project_sources: %s' % (project_sources))
        # create project data structure, folders & files
        project_dir_path = _get_project_dir_path(project_name)
        try:
            project_lock.acquire(project_name)
            if not os.path.exists(project_dir_path):
                logger.info('/projects creating directory: %s' % (project_dir_path))
                os.makedirs(project_dir_path)
            data['projects'][project_name] = {'sources': {}}
            data['projects'][project_name]['sources'] = project_sources
            with open(os.path.join(project_dir_path, 'project_config.json'), 'w') as f:
                f.write(json.dumps(data['projects'][project_name], indent=4, default=json_encode))
            # we should try to create a service for domain "sources:type" 
            # (or update it if timerange defined by "sources:start_date" and "sources:end_date" is bigger than existing)
            ret, err = check_domain_service(project_sources)
            if ret==0:
                logger.info('project %s created.' % project_name)
                return rest.created()
            else:
                logger.info('project %s creation failed. %s' % (project_name, err))
                return rest.internal_error(err)
        except Exception as e:
            logger.error('creating project %s: %s' % (project_name, e.message))
        finally:
            project_lock.release(project_name)

    def get(self):
        return data['projects'].keys()

    def delete(self):
        for project_name in data['projects'].keys():  # not iterkeys(), need to do del in iteration
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
        if project_name not in data['projects']:
            return rest.not_found()
        input = request.get_json(force=True)
        project_sources = input.get('sources', [])
        if len(project_sources) == 0:
            return rest.bad_request('Invalid sources.')
        try:
            project_lock.acquire(project_name)
            data['projects'][project_name]['master_config'] = project_sources
            # this is an update. 
            # TODO: we may need to update the corresponding domain image similarity service
            ret, err = check_domain_service(project_sources)
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
