import logging

# we could store here: 
# - path to default data folder
# - path to base domain folder
# - path to script to run when a new project is created

config = {
    'debug': True,
    'server': {
        'host': '127.0.0.1',
        'port': 5555,
    },
    'logging': {
        'file_path': 'log.log',
        'format': '%(asctime)s %(levelname)s %(message)s',
        'level': logging.INFO
    },
    'image': {
        'sample_dir_path': '/home/ubuntu/data_sample',
        'base_domain_dir_path': '/home/ubuntu/data_domains/',
        'config_filename': 'global_var_remotehbase_release.json',
        'setup_script_path': 'setup_image_search.sh'
    }
}