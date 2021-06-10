"""
This program demonstrates to ingest data from MySQL Database.
"""


from pyspark.sql import SparkSession
import os.path
import yaml


if __name__ == '__main__':
    print('\n***************** Ingest data from MySQL Database*****************')

    # Configure command line args
    os.environ['PYSPARK_SUBMIT_ARGS'] = (
        '--package "mysql:mysql-connector-java:8.0.15" pyspark-shell'
    )

    # Create Spark session
    sparSession = SparkSession\
        .builder\
        .appName('Ingest data from MySQL')\
        .getOrCreate()

    sparSession.sparkContext.setLogLevel('ERROR')

    # Start : Load application config
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_file = os.path.abspath(current_dir + '../../..' + '/application.yml')
    app_secrets_file = os.path.abspath(current_dir + '../../..' + '/.secrets')

    conf = open(app_config_file)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)

    secrets = open(app_secrets_file)
    app_secrets = yaml.load(secrets, Loader=yaml.FullLoader)
    # End : Load application config

    # Start : JDBC Connection configuration
    jdbc_params = {
        'url': '',
        'lowerBound': '1',
        'upperBound': '100',
        'dbtable': app_conf['mysql_conf']['dbtable'],
        'numPartitions': '2',
        'partitionColumn': app_conf['mysql_conf']['partition_column'],
        'user': app_secrets['mysql_conf']['username'],
        'password': app_secrets['mysql_conf']['password']
    }
    # End : JDBC Connection configuration

    # Start : Read data from Database
    txn_df = sparSession.read\
        .format('jdbc')\
        .option('driver', 'com.mysql.cj.jdbc.Driver')\
        .options(**jdbc_params)\
        .load()
    # End : Read data from Database

    # Print schema
    print('\n********************* txn_df.printSchema()')
    txn_df.printSchema()

    print('\n******************** txn_df.show(10, truncate=False)')
    txn_df.show(10, truncate=False)

    # Shutdown spark context
    sparSession.stop()

# Command
# --------------------------
# spark-submit dataframe/ingestion/read_from_databases/dataframe_from_mysql_db.py
