import urllib2
import requests
import json

oozie_url_v1 = 'http://memex-utils:11000/oozie/v1/'
oozie_url_v2 = 'http://memex-utils:11000/oozie/v2/'

# # check workflows
# req = urllib2.Request(oozie_url_v2+'jobs?jobtype=wf')
# response = urllib2.urlopen(req)
# output = response.read()
# print json.dumps(json.loads(output), indent=4, separators=(',', ': '))

def append_property_toXML(XML, name, value):
	XML += "<property><name>{}</name><value>{}</value></property>".format(name, value)
	return XML

# # submit a workflow
def submit_worfklow(workflow_path):
    payload = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><configuration>"
    payload = append_property_toXML(payload, "user.name", "skaraman")
    payload = append_property_toXML(payload, "oozie.wf.application.path", workflow_path)
    payload = append_property_toXML(payload, "jobTracker", "memex-rm.xdata.data-tactics-corp.com:8032")
    payload = append_property_toXML(payload, "nameNode", "hdfs://memex")
    payload = append_property_toXML(payload, "DAYTOPROCESS", "2017-04-02")
    payload = append_property_toXML(payload, "TABLE_SHA1", "escorts_images_sha1_infos_ext_dev")
    payload = append_property_toXML(payload, "TABLE_UPDATE", "escorts_images_updates_dev")
    payload = append_property_toXML(payload, "ES_DOMAIN", "escorts")
    payload +="</configuration>"
    headers = {'Content-Type': 'application/xml'}
    print payload
    req = requests.post(oozie_url_v1+'jobs?action=start', data=payload, headers=headers)
    print req
    output = req.json()
    print output
    print json.dumps(output, indent=4, separators=(',', ': '))
    return output


def get_job_info(job_id):
    get_job_info_str = "/job/{}?show=info"
    req = requests.get(oozie_url_v1+get_job_info_str.format(job_id))
    print req
    output = req.json()
    #print output
    print json.dumps(output, indent=4, separators=(',', ': '))
    return output

# path for get-images-domain worfklow
get_images_domain_path = "hdfs://memex:8020/user/hue/oozie/workspaces/hue-oozie-1494982632.42/"
json_submit = submit_worfklow(get_images_domain_path)
#job_id = "0000427-170307035803271-oozie-oozi-W"
job_id = json_submit['id']
json_info = get_job_info(job_id)



