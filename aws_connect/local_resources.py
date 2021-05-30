"""
This method provides utility functions to load local resources.
"""


import os.path
import yaml


# Set current directory path
current_dir = os.path.abspath(os.path.dirname(__file__))

app_config_path = os.path.abspath(current_dir + '/../' + 'application.ynl')
app_config = open(app_config_path)
config = yaml.load(app_config, Loader=yaml.FullLoader)

app_secrets_path = os.path.abspath(current_dir + '/../' + '.secrets')
app_secrets = open(app_secrets_path)
secret = yaml.load(app_secrets, Loader=yaml.FullLoader)


# get configuration from application.yml
def load_app_conf():
    return config


# get app secrets from .secrets
def load_secrets_conf():
    return secret
