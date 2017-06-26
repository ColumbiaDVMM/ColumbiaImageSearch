import os
import sys
import shutil
import logging
import json
import types
import threading
import happybase
import subprocess as sub

from flask import Flask, render_template, Response
from flask import request, abort, redirect, url_for
from flask_cors import CORS, cross_origin
from flask_restful import Resource, Api

from config import config
from locker import Locker
import rest
from oozie_job_manager import build_images_workflow_payload_v2, build_images_index_workflow_payload, submit_worfklow, get_job_info, rerun_job
import pymongo
from pymongo import MongoClient

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

# mongoDB
client = MongoClient()
db = client.api_manager_db
db_domains = db.domains
db_projects = db.projects

# in-memory data
# TODO: make it persistent i.e. deal with restart using mongoDB
data = {}
data['domains'] = {}
data['projects'] = {}
# ports ?
# what we really care about is knowing for each domain:
# - what is the address for the image similarity service for one domain (actually one project)
# - what is the status of indexing (not indexed, indexing, indexed)
# - what is the time range that we have indexed (if not everything)
# what are the ports used on the host.

# use before_first_request to try to load data from disk? Build docker image?
# use after_request for all functions that modify data to save data to disk? ~ http://flask.pocoo.org/snippets/53/

def initialize_data_fromdb():
    # try to read data stored in db from a previous session
    # fill projects, domains and ports
    for project in db_projects.find():
        logger.info('loading project from mongodb: {}'.format(project))
        data['projects'][project['project_name']] = dict()
        for key in project:
            # id is an object that is not JSON serializable
            if key != u'_id' and key != '_id':
                data['projects'][project['project_name']][key] = project[key]
    for domain in db_domains.find():
        logger.info('loading domain from mongodb: {}'.format(domain))
        data['domains'][domain['domain_name']] = dict()
        for key in domain:
            # id is an object that is not JSON serializable
            if key != u'_id' and key != '_id':
                data['domains'][domain['domain_name']][key] = domain[key]
            if key == 'port': 
                if 'ports' not in data:
                    data['ports'] = [domain[key]]
                else:
                    data['ports'].append(domain[key])
    # reset apache conf
    reset_apache_conf()
    restart_apache()
    # restart dockers
    for domain_name in data['domains']:
        start_docker(data['domains'][domain_name]['port'], domain_name)


@app.after_request
def after_request(response):
  response.headers.add('Access-Control-Allow-Headers', 'Keep-Alive,User-Agent,If-Modified-Since,Cache-Control,x-requested-with,Content-Type,origin,authorization,accept,client-security-token')
  response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
  return response


# One locker for project and domain
project_lock = Locker()
domain_lock = Locker()

def _copy_from_hdfs(hdfs_path, local_path):
    import subprocess
    subprocess.call(['hadoop', 'fs', '-copyToLocal', hdfs_path, local_path])    

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


def _submit_buildindex_worfklow(ingestion_id, table_sha1infos, pingback_url):
    payload = build_images_index_workflow_payload(ingestion_id, table_sha1infos, pingback_url)
    logger.info('[submit_worfklow: log] submitted payload for ingestion_id: {}'.format(payload))
    json_submit = submit_worfklow(payload)
    job_id = json_submit['id']
    logger.info('[submit_worfklow: log] submitted workflow for ingestion_id: %s.' % (ingestion_id))
    return job_id

def parse_isodate_to_ts(input_date):
    import dateutil.parser
    import calendar
    parsed_date = dateutil.parser.parse(input_date)
    print "[parsed_date: {}]".format(parsed_date)
    return calendar.timegm(parsed_date.utctimetuple())*1000


def reset_apache_conf():
    # for each domain create proxypass and add it to initial conf file
    # read initial apache conf file up to '</VirtualHost>'
    inconf_file = config['image']['in_apache_conf_file']
    outconf_str = ""
    with open(inconf_file, 'rt') as inconf:
        for line in inconf:
            outconf_str += line
    for domain_name in data['domains']:
        port = data['domains'][domain_name]['port']
        proxypass_filled, service_url = fill_proxypass(domain_name, port)
        logger.info("[setup_service_url: log] updating Apache conf with: {}".format(proxypass_filled))
        outconf_str = add_proxypass_to_conf(outconf_str.split('\n'), proxypass_filled)
    write_out_apache_conf(outconf_str)

def write_out_apache_conf(outconf_str):
    try:
        with open(config['image']['out_apache_conf_file'], 'wt') as outconf:
            outconf.write(outconf_str)
    except Exception as inst:
        logger.info("[setup_service_url: log] Could not overwrite Apache conf file. {}".format(inst))
        raise IOError("Could not overwrite Apache conf file")


def fill_proxypass(domain_name, port):
    endpt = "/cuimgsearch_{}".format(domain_name)
    service_url = config['image']['base_service_url']+endpt
    lurl = "http://localhost:{}/".format(port)
    proxypass_template = "\nProxyPass {}/ {}\nProxyPassReverse {}/ {}\n<Location {}>\n\tRequire all granted\n</Location>\n"
    proxypass_filled = proxypass_template.format(endpt, lurl, endpt, lurl, endpt)
    return proxypass_filled, service_url

def add_proxypass_to_conf(inconf, proxypass_filled):
    outconf_str = ""
    for line in inconf:
        # add the new rule before the end
        if line.strip()=='</VirtualHost>':
            outconf_str += proxypass_filled 
        outconf_str += line+'\n'
    return outconf_str

def setup_service_url(domain_name):
    # attribute a port (how to make sure it is free? for now just assume it is)
    if 'ports' not in data:
        port = config['image']['first_port'] 
        data['ports'] = []
    else:
        port = max(data['ports'])+1
    data['ports'].append(port)
    # build the proxypass rule for Apache 
    # TODO: should have all this predefined for domain1-4
    proxypass_filled, service_url = fill_proxypass(domain_name, port)
    logger.info("[setup_service_url: log] updating Apache conf with: {}".format(proxypass_filled))
    # read apache conf file up to '</VirtualHost>'
    inconf_file = config['image']['in_apache_conf_file']
    # check if we already setup one domain...
    if os.path.isfile(config['image']['out_apache_conf_file']): 
        # start from there
        inconf_file = config['image']['out_apache_conf_file']
    with open(inconf_file, 'rt') as inconf:
        outconf_str = add_proxypass_to_conf(inconf, proxypass_filled)
    # overwrite conf file
    write_out_apache_conf(outconf_str)

    return port, service_url    


def restart_apache():
    # this requires root privilege

    # v2. dirty but seems to work
    command_shell = 'sleep 3; sudo service apache2 restart'
    logger.info("[setup_service_url: log] restarting Apache in 3 seconds...")
    subproc = sub.Popen(command_shell, shell=True)


def get_start_end_ts(one_source):
    '''Parse start and end timestamp from `start_date` and `end_date` in the provided source'''
    
    try:
        start_ts = parse_isodate_to_ts(one_source['start_date'])
    except Exception as inst:
        err_msg = "Could not parse 'start_date' (error was: {}).".format(inst)
        logger.error("[get_start_end_ts: log] "+err_msg)
        raise ValueError(err_msg)
    try:
        end_ts = parse_isodate_to_ts(one_source['end_date'])
    except Exception as inst:
        err_msg = "Could not parse 'end_date' (error was: {}).".format(inst)
        logger.error("[get_start_end_ts: log] "+err_msg)
        raise ValueError(err_msg)
    return start_ts, end_ts

def check_project_indexing_finished(project_name):
    """Check if we can find lopq_model and lopd_codes.
    """
    if data['projects'][project_name]['status'] == 'indexing' or data['projects'][project_name]['status'] == 'rerunning':
        # look for columns lopq_model and lopd_codes in hbase update table row of this ingestion
        ingestion_id = data['projects'][project_name]['ingestion_id']
        logger.info('[check_project_indexing_finished: log] checking if ingestion %s has completed.' % (ingestion_id))
        try:
            from happybase.connection import Connection
            conn = Connection(config['image']['hbase_host'])
            table = conn.table(config['image']['hbase_table_updates'])
            columns=[config['image']['lopq_model_column'], config['image']['lopq_codes_column']]
            row = table.row(ingestion_id, columns=columns)
            # if found, copy to domain data folder
            if len(row)==len(columns):
                logger.info('[check_project_indexing_finished: log] ingestion %s looks completed' % (ingestion_id))
                # copy codes first
                local_codes_path = os.path.join(_get_domain_dir_path(data['projects'][project_name]['domain']), config['image']['lopq_codes_local_suffix'])
                _copy_from_hdfs(row[config['image']['lopq_codes_column']], local_codes_path)
                local_model_path = os.path.join(_get_domain_dir_path(data['projects'][project_name]['domain']), config['image']['lopq_model_local_suffix'])
                _copy_from_hdfs(row[config['image']['lopq_model_column']], local_model_path)
                if os.path.exists(local_codes_path) and os.path.exists(local_model_path):
                    data['projects'][project_name]['status'] == 'ready'
                else:
                    logger.info('[check_project_indexing_finished: log] ingestion %s has completed but local copy failed...' % (ingestion_id))
                    # for debugging store infos: row[config['image']['lopq_codes_column']], row[config['image']['lopq_model_column']]
            else: # else, 
                # check the job is still running
                job_id = data['projects'][project_name]['job_id']
                output = get_job_info(job_id)
                if output['status'] == 'RUNNING':
                    pass # we just have to wait for the job to end
                else:
                    # if it is not, the job failed... what should we do?
                    # mark project as failed?
                    if data['projects'][project_name]['status'] == 'indexing':
                        # try to rerun once
                        logger.info('[check_project_indexing_finished: log] rerunning ingestion %s who has failed once...' % (ingestion_id))
                        rerun_output = rerun_job(job_id)
                        logger.info('[check_project_indexing_finished: log] resubmission output was: {}'.format(rerun_output))
                    elif data['projects'][project_name]['status'] == 'rerunning':
                        logger.info('[check_project_indexing_finished: log] ingestion %s has failed twice...' % (ingestion_id))
                        logger.info('[check_project_indexing_finished: log] job info output was: {}'.format(output))
                        data['projects'][project_name]['status'] = 'failed'
                        # for debugging store info: output
        except Exception as inst:
            logger.error('[check_project_indexing_finished: error] {}'.format(inst))
                

def start_docker(port, domain_name):
    # call start_docker_columbia_image_search_qpr.sh with the right domain and port
    # this can take a while if the docker image was not build yet... 
    command = '{}{} -p {} -d {}'.format(config['image']['host_repo_path'], config['image']['setup_docker_path'], port, domain_name)
    logger.info("[check_domain_service: log] Starting docker for domain {} with command: {}".format(domain_name, command))
    docker_proc = sub.Popen(command.split(' '), stdout=sub.PIPE, stderr=sub.PIPE)


def check_domain_service(project_sources, project_name):
    #logger.info('[check_domain_service: log] project_sources: %s' % (project_sources))
    # why is project_sources a list actually? Assume we want the first entry? Or loop?
    one_source = project_sources[0]
    domain_name = one_source['type']
    start_ts, end_ts = get_start_end_ts(one_source)
    
    logger.info('[check_domain_service: log] domain_name: %s, start_ts: %s, end_ts: %s' % (domain_name, start_ts, end_ts))
    # should we check domain_name is valid e.g. exists in CDR?

    domain_dir_path = _get_domain_dir_path(domain_name)

    # get domain lock
    if os.path.isdir(domain_dir_path): # or test 'domain_name' in data['domains']?
        logger.info('[check_domain_service: log] service exists for domain_name: %s, check if we need to update.' % (domain_name))
        # Check conf to see if we need to update 
        # Do we actually want to allow that?
        config_file = os.path.join(domain_dir_path, config['image']['config_filepath'])
        config_json = json.load(open(config_file,'rt'))
        if 'start_ts' not in config_json or 'end_ts' not in config_json:
            err_msg = 'service exists for domain: %s, but creation seems incomplete.' % (domain_name)
            logger.error('[check_domain_service: error] '+err_msg)
            domain_lock.release(domain_name)
            return -1, domain_name, err_msg
        new_start_ts = min(start_ts, config_json['start_ts'])
        new_end_ts = max(end_ts, config_json['end_ts'])
        if new_start_ts < config_json['start_ts'] or new_end_ts > config_json['end_ts']:
            domain_lock.acquire(domain_name)
            # save update infos in domain data
            ingestion_id = '-'.join([domain_name, str(start_ts), str(end_ts)])
            data['domains'][domain_name]['ingestion_id'].append(ingestion_id)
            # submit workflow with min(start_ts, stored_start_ts) and max(end_ts, stored_end_ts)
            endpt = "/cu_imgsearch_manager/projects/{}".format(project_name)
            pingback_url = config['image']['base_service_url']+endpt
            job_id = _submit_buildindex_worfklow(ingestion_id, data['domains'][domain_name]['table_sha1infos'], pingback_url)
            # add job_id to job_ids and save config
            config_json['job_ids'].append(job_id)
            data['domains'][domain_name]['job_ids'].append(job_id)
            # write out new config file
            with open(config_file, 'wt') as conf_out:
                conf_out.write(json.dumps(config_json))
            msg = 'updating domain %s' % (domain_name)
            logger.info('[check_domain_service: log] '+msg)
            # update in mongodb too
            db_domains.find_one_and_replace({'domain_name':domain_name}, data['domains'][domain_name])
            domain_lock.release(domain_name)
            return 1, domain_name, ingestion_id, job_id, msg
        else:
            msg = 'no need to update for domain %s' % (domain_name)
            logger.info('[check_domain_service: log] '+msg)
            return 1, domain_name, None, None, msg
    else:
        domain_lock.acquire(domain_name)
        # if folder is empty copy data from config['image']['sample_dir_path']
        # copy the whole folder
        logger.info('[check_domain_service: log] copying from %s to %s' % (config['image']['sample_dir_path'], domain_dir_path))
        try:
            if not os.path.isdir(config['image']['base_domain_dir_path']):
                os.makedirs(config['image']['base_domain_dir_path'])
            shutil.copytree(config['image']['sample_dir_path'], domain_dir_path)
        except shutil.Error as inst:
            raise ValueError('Could not copy from template directory {} to {}. {}'.format(config['image']['sample_dir_path'], domain_dir_path, inst))
        # then copy sample config file
        source_conf = os.path.join(config['image']['host_repo_path'],config['image']['config_sample_filepath'])
        config_file = os.path.join(domain_dir_path,config['image']['config_filepath'])
        logger.info('[check_domain_service: log] copying config file from %s to %s' % (source_conf, config_file))
        shutil.copy(source_conf, config_file)
        # edit config_file by replacing DOMAIN by the actual domain in :
        # "ist_els_doc_type", "HBI_table_sha1infos", and "HBI_table_updatesinfos" (and "HBI_table_sim" ?)
        logger.info('[check_domain_service: log] loading config_file from %s' % (config_file))
        config_json = json.load(open(config_file,'rt'))
        # - HBI_table_sha1infos
        config_json['HBI_table_sha1infos'] = config_json['HBI_table_sha1infos'].replace('DOMAIN', domain_name)
        ingestion_id = '-'.join([domain_name, str(start_ts), str(end_ts)])
        # save that in project and domain infos too
        config_json['ingestion_id'] = ingestion_id
        # setup service
        port, service_url = setup_service_url(domain_name)
        endpt = "/cu_imgsearch_manager/projects/{}".format(project_name)
        pingback_url = config['image']['base_service_url']+endpt
        # submit workflow to get images data
        logger.info('[check_domain_service: log] submitting workflow with parameters: %s, %s, %s' % (ingestion_id, config_json['HBI_table_sha1infos'], pingback_url))
        job_id = _submit_buildindex_worfklow(ingestion_id, config_json['HBI_table_sha1infos'], pingback_url)
        # save job id to be able to check status?
        config_json['job_ids'] = job_id
        # write out new config file
        logger.info('[check_domain_service: log] updating config_file: %s' % config_file)
        with open(config_file, 'wt') as conf_out:
            conf_out.write(json.dumps(config_json))
        logger.info('[check_domain_service: log] wrote config_file: %s' % config_file)
        start_docker(port, domain_name)
        # store all infos of that domain
        data['domains'][domain_name] = {}
        data['domains'][domain_name]['domain_name'] = domain_name
        data['domains'][domain_name]['port'] = port
        data['domains'][domain_name]['table_sha1infos'] = config_json['HBI_table_sha1infos']
        data['domains'][domain_name]['service_url'] = service_url
        data['domains'][domain_name]['ingestion_id'] = [ingestion_id] # to allow for updates?
        data['domains'][domain_name]['job_ids'] = [job_id]
        data['domains'][domain_name]['docker_name'] = 'columbia_university_search_similar_images_'+domain_name
        # insert in mongoDB
        db_domains.insert_one(data['domains'][domain_name])
        domain_lock.release(domain_name)
        
        # we will restart apache AFTER returning
        return 0, domain_name, ingestion_id, job_id, None


def json_encode(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError


@api.route('/debug')
class Debug(Resource):

    def get(self):
        try:
            if not config['debug']:
                return abort(404)
            debug_info = {
                'data': json.loads(json.dumps(data, default=json_encode))
            }
            return debug_info
        except Exception as e:
            logger.error('debug: {}. {}'.format(e, sys.exc_info()[0]))



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
            msg = 'You cannot post an existing project to the /projects endpoint. For updates, post to projects/{your_project_name}'
            return rest.bad_request(msg)
        project_sources = input.get('sources', [])
        if len(project_sources) == 0:
            return rest.bad_request('Invalid sources.')

        logger.info('/projects project_name: %s' % (project_name))
        logger.info('/projects project_sources: %s' % (project_sources))            

        try:
            # create project data structure, folders & files
            project_dir_path = _get_project_dir_path(project_name)
        
            project_lock.acquire(project_name)
            logger.info('/projects creating directory: %s' % (project_dir_path))
            os.makedirs(project_dir_path)
            data['projects'][project_name] = {'sources': {}}
            data['projects'][project_name]['project_name'] = project_name
            data['projects'][project_name]['sources'] = project_sources
            with open(os.path.join(project_dir_path, 'project_config.json'), 'w') as f:
                f.write(json.dumps(data['projects'][project_name], indent=4, default=json_encode))
            # we should try to create a service for domain "sources:type" 
            # (or update it if timerange defined by "sources:start_date" and "sources:end_date" is bigger than existing)
            ret, domain_name, ingestion_id, job_id, err = check_domain_service(project_sources, project_name)
            data['projects'][project_name]['domain'] = domain_name
            if ret==0:
                msg = 'project %s created.' % project_name
                logger.info(msg)
                # store job infos
                data['projects'][project_name]['ingestion_id'] = ingestion_id
                data['projects'][project_name]['job_id'] = job_id
                data['projects'][project_name]['status'] = 'indexing'
                # insert into mongoDB
                db_projects.insert_one(data['projects'][project_name])
                try:
                    return rest.created(msg)
                finally: # still executed before returning...
                    restart_apache()
            elif ret==1:
                msg = 'domain for project %s was already previously created. %s' % (project_name, err)
                logger.info(msg)
                # what should we return in this case
                return rest.ok(msg) 
            else:
                # we should remove project_name
                del data['projects'][project_name]
                msg = 'project %s creation failed while creating search service: %s' % (project_name, err)
                logger.info(msg)
                return rest.internal_error(msg)
        except Exception as e:
            # try to remove project_name
            try:
                del data['projects'][project_name]
            except:
                pass
            # try to remove data files too
            try:
                shutil.rmtree(os.path.join(_get_project_dir_path(project_name)))
            except:
                pass
            msg = 'project {} creation failed: {} {}'.format(project_name, e, sys.exc_info()[0])
            logger.error(msg)
            return rest.internal_error(msg)
        finally:
            project_lock.release(project_name)


    def get(self):
        return data['projects'].keys()


    def delete(self):
        # redundant with projects/project_name/delete
        msg = 'cannot delete from projects endpoint. you should call projects/{your_project_name}'
        return rest.bad_request(msg)


@api.route('/projects/<project_name>')
class Project(Resource):
    def post(self, project_name):
        # for updates?
        if project_name not in data['projects']:
            return rest.not_found()
        input = request.get_json(force=True)
        project_sources = input.get('sources', [])
        if len(project_sources) == 0:
            return rest.bad_request('Invalid sources.')
        try:
            project_lock.acquire(project_name)
            data['projects'][project_name]['master_config'] = project_sources
            # This would mean an update, we need to update the corresponding domain image similarity service
            ret, domain_name, ingestion_id, job_id, err = check_domain_service(project_sources, project_name)
            return rest.created()
        except Exception as e:
            logger.error('Updating project %s: %s' % (project_name, e.message))
            return rest.internal_error('Updating project %s error, halted.' % project_name)
        finally:
            project_lock.release(project_name)


    def get(self, project_name):
        if project_name not in data['projects']:
            return rest.not_found()
        check_project_indexing_finished(project_name)
        logger.info('Getting project %s, dict keys are %s' % (project_name, data['projects'][project_name].keys()))
        return data['projects'][project_name]


    def delete(self, project_name):
        if project_name not in data['projects']:
            return rest.not_found()
        try:
            project_lock.acquire(project_name)
            # - get corresponding domain
            domain_name = data['projects'][project_name]['domain']
            # remove project:
            # - from current data dict
            del data['projects'][project_name]
            # - files associated with project
            shutil.rmtree(os.path.join(_get_project_dir_path(project_name)))
            # - from mongodb
            db_projects.delete_one({'project_name':project_name})
            msg = 'project {} has been deleted'.format(project_name)
            logger.info(msg)
            # if it's the last project from a domain, shoud we remove the domain?
            # for now assume one project per domain and delete too
            # stop and remove docker container
            docker_name = data['domains'][domain_name]['docker_name']
            subproc = sub.Popen("sudo docker stop {}; sudo docker rm {}".format(docker_name, docker_name), shell=True)
            # cleanup ports list
            data['ports'].remove(data['domains'][domain_name]['port'])
            # remove domain:
            # - from current data dict
            del data['domains'][domain_name]
            # - files associated with project
            shutil.rmtree(os.path.join(_get_domain_dir_path(domain_name)))
            # - from mongodb
            db_domains.delete_one({'domain_name':domain_name})
            # should we also clean up things in HDFS?...
            msg2 = 'domain {} has been deleted'.format(domain_name)
            logger.info(msg2)
            # regenerate apache conf from scratch for domains that are still active.
            reset_apache_conf()

            return rest.deleted(msg+' '+msg2)
        except Exception as e:
            logger.error('deleting project %s: %s' % (project_name, e.message))
            return rest.internal_error('deleting project %s error, halted.' % project_name)
        finally:
            project_lock.remove(project_name)


@api.route('/domains')
class AllDomains(Resource):

    def post(self):
        return rest.bad_request('You cannot post to this endpoint. Domains are created from projects.')


    def get(self):
        return data['domains'].keys()


@api.route('/domains/<domain_name>')
class Domain(Resource):

    def post(self, domain_name):
        return rest.bad_request('You cannot post a domain, you should post a project using a domain.')

    def put(self, domain_name):
        return self.post(domain_name)

    def get(self, domain_name):
        if domain_name not in data['domains']:
            return rest.not_found()
        return data['domains'][domain_name]

    def delete(self, domain_name):
        # Should we allow it?
        return rest.bad_request('Deleting a domain is not allowed.')


if __name__ == '__main__':

    initialize_data_fromdb()
    # we should also check if dockers are running?
    # api services within each docker?

    from gevent.wsgi import WSGIServer
    http_server = WSGIServer(('', config['server']['port']), app)
    http_server.serve_forever()
