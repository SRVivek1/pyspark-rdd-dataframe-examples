"""
This program demonstrates how to read data from SFTP servers.
"""


from pyspark.sql import SparkSession
import os.path
import yaml


if __name__ == '__main__':
    print('\n******************************* Read data from SFTP *******************************')

    sparkSession = SparkSession\
        .builder\
        .appName('Read data from SFTP server')\
        .config('spark.jars.packages', 'com.springml:spark-sftp_2.11:1.1.1')\
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel('ERROR')

    # Start : Read configuration
    current_dir = os.path.abspath(os.path.dirname(__file__))
    project_root_dir = os.path.abspath(current_dir + '../../../..')

    app_config_file = os.path.abspath(project_root_dir + '/application.yml')
    app_secrets_file = os.path.abspath(project_root_dir + '/.secrets')

    config_file = open(app_config_file)
    app_config = yaml.load(config_file, Loader=yaml.FullLoader)

    secrets_file = open(app_secrets_file)
    app_secrets = yaml.load(secrets_file, Loader=yaml.FullLoader)
    # End : Read configuration

    # Start : SFTP configuration
    sftp_connection_config = {
        'host': app_secrets['sftp_conf']['hostname'],
        'port': app_secrets['sftp_conf']['port'],
        'username': app_secrets['sftp_conf']['username'],
        'pem': os.path.abspath(project_root_dir + app_secrets['sftp_conf']['pem'])
    }
    # End : SFTP configuration

    # Start : File information
    sftp_file_details = {
        'filetype': app_config['sftp_conf']['filetype'],
        'delimiter': app_config['sftp_conf']['delimiter']
    }
    # End : File information

    # Start : Read data from SFTP
    receipts_df = sparkSession.read\
        .format('com.springml.spark.sftp')\
        .options(**sftp_connection_config)\
        .options(**sftp_file_details)\
        .load(app_config['sftp_conf']['directory'] + '/receipts_delta_GBR_14_10_2017.csv')
    # End : Read data from SFTP

    # Show data
    print('\n********************** Sample data read from SFTP : receipts_df.show(10, truncate=False)')
    receipts_df.show(10, truncate=False)

