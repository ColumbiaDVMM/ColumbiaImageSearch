import logging

# we could store here: 
# - path to default data folder
# - path to base domain folder
# - path to script to run when a new project is created

# Local
#'sample_dir_path': '/Users/svebor/Documents/Workspace/CodeColumbia/MEMEX/data_sample/',
#'base_domain_dir_path': '/Users/svebor/Documents/Workspace/CodeColumbia/MEMEX/data_domains/',
#'apache_conf_file': '/Users/svebor/Documents/Workspace/CodeColumbia/MEMEX/fake-apache.conf',
#'host_repo_path': '/Users/svebor/Documents/Workspace/CodeColumbia/MEMEX/ColumbiaImageSearch',
        
# OpenStack
# 'sample_dir_path': '/home/ubuntu/data_sample',
# 'base_domain_dir_path': '/home/ubuntu/data_domains/',
# 'apache_conf_file': '/etc/apache2/sites-enabled/000-default.conf',
# 'host_repo_path': '/home/ubuntu/ColumbiaImageSearch',

config = {
    'debug': True,
    'server': {
        'host': '127.0.0.1',
        'port': 5555,
    },
    'repo': {
        'local_path': '/home/ubuntu/mydig-projects-test',
    },
    'logging': {
        'file_path': 'log.log',
        'format': '%(asctime)s %(levelname)s %(message)s',
        'level': logging.INFO
    },
    'image': {
        'sample_dir_path': '/home/ubuntu/data_sample',
        'base_domain_dir_path': '/home/ubuntu/data_domains/',
        'in_apache_conf_file': '/etc/apache2/sites-enabled/000-default.conf.init',
        'out_apache_conf_file': '/etc/apache2/sites-enabled/000-default.conf',
        'host_repo_path': '/home/ubuntu/ColumbiaImageSearch',
        'config_filepath': 'data/global_var_remotehbase_release.json',
        'setup_script_path': 'setup_image_search.sh',
        'base_service_url': 'http://10.3.2.135',
        'first_port': 5000
    }
}
