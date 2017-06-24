import urllib2
import requests
import json

oozie_url_v1 = 'http://memex-utils:11000/oozie/v1/'
oozie_url_v2 = 'http://memex-utils:11000/oozie/v2/'
# path for get-images-domain worfklow
get_images_domain_path_v1 = "hdfs://memex:8020/user/hue/oozie/workspaces/hue-oozie-1494982632.42/"
get_images_domain_path_v2 = "hdfs://memex:8020/user/hue/oozie/workspaces/hue-oozie-1496437517.17/"
build_images_index_workflow_path = "hdfs://memex:8020/user/hue/oozie/workspaces/hue-oozie-1498176362.6/"


# # check workflows
# req = urllib2.Request(oozie_url_v2+'jobs?jobtype=wf')
# response = urllib2.urlopen(req)
# output = response.read()
# print json.dumps(json.loads(output), indent=4, separators=(',', ': '))

def append_property_toXML(XML, name, value):
	XML += "<property><name>{}</name><value>{}</value></property>".format(name, value)
	return XML


def build_images_workflow_payload_v1(day_to_process, table_sha1, table_update, domain, workflow_path=get_images_domain_path_v1):
    payload = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><configuration>"
    payload = append_property_toXML(payload, "user.name", "skaraman")
    payload = append_property_toXML(payload, "oozie.wf.application.path", workflow_path)
    payload = append_property_toXML(payload, "jobTracker", "memex-rm.xdata.data-tactics-corp.com:8032")
    payload = append_property_toXML(payload, "nameNode", "hdfs://memex")
    # TODO: later on we may have a start_date and end_date instead of a day
    payload = append_property_toXML(payload, "DAYTOPROCESS", day_to_process)
    payload = append_property_toXML(payload, "TABLE_SHA1", table_sha1)
    payload = append_property_toXML(payload, "TABLE_UPDATE", table_update)
    payload = append_property_toXML(payload, "ES_DOMAIN", domain)
    payload += "</configuration>"
    return payload

def build_images_workflow_payload_v2(start_ts, end_ts, table_sha1, table_update, domain, workflow_path=get_images_domain_path_v2):
    payload = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><configuration>"
    payload = append_property_toXML(payload, "user.name", "skaraman")
    payload = append_property_toXML(payload, "oozie.wf.application.path", workflow_path)
    payload = append_property_toXML(payload, "jobTracker", "memex-rm.xdata.data-tactics-corp.com:8032")
    payload = append_property_toXML(payload, "nameNode", "hdfs://memex")
    payload = append_property_toXML(payload, "START_TS", start_ts)
    payload = append_property_toXML(payload, "END_TS", end_ts)
    payload = append_property_toXML(payload, "TABLE_SHA1", table_sha1)
    payload = append_property_toXML(payload, "TABLE_UPDATE", table_update)
    payload = append_property_toXML(payload, "ES_DOMAIN", domain)
    payload += "</configuration>"
    return payload

def build_images_index_workflow_payload(ingestion_id, table_sha1, pingback_url, workflow_path=build_images_index_workflow_path):
    payload = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><configuration>"
    payload = append_property_toXML(payload, "user.name", "skaraman")
    payload = append_property_toXML(payload, "oozie.wf.application.path", workflow_path)
    payload = append_property_toXML(payload, "jobTracker", "memex-rm.xdata.data-tactics-corp.com:8032")
    payload = append_property_toXML(payload, "nameNode", "hdfs://memex")
    payload = append_property_toXML(payload, "INGESTION_ID", ingestion_id)
    payload = append_property_toXML(payload, "TABLE_SHA1", table_sha1)
    payload = append_property_toXML(payload, "PINGBACK_URL", pingback_url)
    payload += "</configuration>"
    return payload

# # submit a workflow
def submit_worfklow(payload):
    headers = {'Content-Type': 'application/xml'}
    req = requests.post(oozie_url_v1+'jobs?action=start', data=payload, headers=headers)
    #print req
    output = req.json()
    #print output
    return output


def get_job_info(job_id):
    get_job_info_str = "/job/{}?show=info"
    req = requests.get(oozie_url_v1+get_job_info_str.format(job_id))
    #print req
    output = req.json()
    #print output
    #print json.dumps(output, indent=4, separators=(',', ': '))
    return output

if __name__ == "__main__":
    day_to_process = "2017-04-21"
    table_sha1 = "escorts_images_sha1_infos_ext_dev"
    table_update = "escorts_images_updates_dev"
    domain = "escorts"
    payload = build_images_workflow_payload_v1(day_to_process, table_sha1, table_update, domain)
    json_submit = submit_worfklow(payload)
    #job_id = "0000427-170307035803271-oozie-oozi-W"
    job_id = json_submit['id']
    json_info = get_job_info(job_id)

