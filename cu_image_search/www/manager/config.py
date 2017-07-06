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
        'config_sample_filepath': 'conf/global_var_sample_summerqpr2017.json',
        'config_filepath': 'data/global_var_summerqpr2017.json',
        'setup_docker_path': '/setup/search/start_docker_columbia_image_search_qpr.sh',
        'setup_script_path': 'setup_image_search.sh',
        #'base_service_url': 'http://10.3.2.124',
        'base_service_url': 'https://imgsearchqpr.memexproxy.com',
        'hbase_host': '10.1.94.57',
        'lopq_model_column': 'info:lopq_model_pkl',
        'lopq_codes_column': 'info:lopq_codes_path',
        'lopq_model_local_suffix': 'data/lopq_model',
        'lopq_codes_local_suffix': 'data/lopq_codes',
        'hbase_table_updates': 'images_summerqpr2017_updates',
        'first_port': 5000
    }
}
