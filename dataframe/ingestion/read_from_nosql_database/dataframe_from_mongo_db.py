"""
This application demonstrates how to connect and ingest data from NoSQL database e.g. MongoDB.
"""
import yaml
from pyspark.sql import SparkSession
import os.path


if __name__ =='__main__':
    print('\n************************ Read data from MongoDB ************************\n')

    sparkSession = SparkSession\
        .builder\
        .appName('Ingest data from MongoDB')\
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel('ERROR')

    # Start - Read configuration and credentials
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_cofig_file = os.path.abspath(current_dir + '/../../../..' + '/application.yml')
    app_secrets_file = os.path.abspath(current_dir + '/../../../..' + '/.secrets')

    app_config = yaml.load(open(app_cofig_file), Loader=yaml.FullLoader)
    app_secrets = yaml.load(open(app_secrets_file), Loader=yaml.FullLoader)
    # End - Read configuration and credentials

    print('******** App Config ' + app_config)