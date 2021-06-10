"""
This program demonstrates to ingest data from MySQL Database.
"""


from pyspark.sql import SparkSession
import os.path
import yaml


def get_mysql_jdbc_url(db_config: dict) -> str:
    """
    Generate Database JDBC URL.
    :param db_config:
    :return: mysql jdbc url
    """
    host = db_config['mysql_conf']['hostname']
    port = db_config['mysql_conf']['port']
    database = db_config['mysql_conf']['database']
    jdbc_url_template = 'jdbc:mysql://{}:{}/{}?autoReconnect=true&useSSL=false'

    return jdbc_url_template.format(host, port, database)


if __name__ == '__main__':
    print('\n***************** Ingest data from MySQL Database*****************')

    # Configure command line args - Not working
    '''os.environ['PYSPARK_SUBMIT_ARGS'] = (
        '--packages "mysql:mysql-connector-java:8.0.15" pyspark-shell'
    )'''

    # Create Spark session
    sparSession = SparkSession\
        .builder\
        .appName('Ingest data from MySQL')\
        .getOrCreate()

    sparSession.sparkContext.setLogLevel('ERROR')

    # Start : Load application config
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_file = os.path.abspath(current_dir + '../../../..' + '/application.yml')
    app_secrets_file = os.path.abspath(current_dir + '../../../..' + '/.secrets')

    conf = open(app_config_file)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)

    secrets = open(app_secrets_file)
    app_secrets = yaml.load(secrets, Loader=yaml.FullLoader)
    # End : Load application config

    # Start : JDBC Connection configuration - using table name
    jdbc_params = {
        'url': get_mysql_jdbc_url(app_secrets),
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

    # Read data using query instead of table name
    print('\n******************** Read data using query instead of table name.')

    jdbc_params_query = {
        'url': get_mysql_jdbc_url(app_secrets),
        'lowerBound': '1',
        'upperBound': '100',
        'dbtable': app_conf['mysql_conf']['query'],
        'numPartitions': '2',
        '[partitionColumn': app_conf['mysql_conf']['partition_column'],
        'user': app_conf['mysql_conf']['username'],
        'password': app_conf['mysql_conf']['password']
    }

    txn_df_2 = sparSession\
        .read\
        .format('jdbc')\
        .option('driver', 'com.mysql.cj.jdbc.Driver')\
        .options(**jdbc_params)\
        .load()

    print('\n******************** txn_df_2.show(10, truncate=False)')
    txn_df_2.show(10, truncate=False)

    # Shutdown spark context
    sparSession.stop()

# Command
# --------------------------
# spark-submit --packages "mysql:mysql-connector-java:8.0.15" dataframe/ingestion/read_from_databases/dataframe_from_mysql_db.py
#
# Output
# -----------------
# ********************* txn_df.printSchema()
# root
#  |-- App_Transaction_Id: long (nullable = true)
#  |-- Internal_Member_Id: string (nullable = true)
#  |-- Location_External_Reference: string (nullable = true)
#  |-- Transaction_Type_Id: long (nullable = true)
#  |-- Transaction_Timestamp: string (nullable = true)
#  |-- Activity_Timestamp: string (nullable = true)
#  |-- Activity_App_Date: string (nullable = true)
#  |-- Transaction_Retail_Value: string (nullable = true)
#  |-- Transaction_Profit_Value: string (nullable = true)
#  |-- Transaction_Base_Point_Value: string (nullable = true)
#  |-- Transaction_Point_Value: string (nullable = true)
#  |-- Transaction_Won_Value: string (nullable = true)
#  |-- Event_Name: string (nullable = true)
#  |-- Issue_Audit_User: string (nullable = true)
#  |-- Transaction_External_Reference: string (nullable = true)
#  |-- Cancel_Timestamp: string (nullable = true)
#  |-- Cancel_Audit: string (nullable = true)
#  |-- Cancel_Audit_User: string (nullable = true)
#
#
# ******************** txn_df.show(10, truncate=False)
# +------------------+------------------+---------------------------+-------------------+-----------------------------+-----------------------------+-----------------+------------------------+------------------------+----------------------------+-----------------------+---------------------+----------+----------------+------------------------------+----------------+------------+-----------------+
# |App_Transaction_Id|Internal_Member_Id|Location_External_Reference|Transaction_Type_Id|Transaction_Timestamp        |Activity_Timestamp           |Activity_App_Date|Transaction_Retail_Value|Transaction_Profit_Value|Transaction_Base_Point_Value|Transaction_Point_Value|Transaction_Won_Value|Event_Name|Issue_Audit_User|Transaction_External_Reference|Cancel_Timestamp|Cancel_Audit|Cancel_Audit_User|
# +------------------+------------------+---------------------------+-------------------+-----------------------------+-----------------------------+-----------------+------------------------+------------------------+----------------------------+-----------------------+---------------------+----------+----------------+------------------------------+----------------+------------+-----------------+
# |53481455          |PC7135361         |MOBILEAPP                  |17                 |2017-07-22 00:49:08.503000000|2017-07-22 00:49:08.487000000|2017-07-21       |0.00                    |0.00                    |20.00                       |20.00                  |0.00                 |          |InfoSys         |                              |                |null        |                 |
# |53481542          |PC7135361         |WEB                        |17                 |2017-07-22 00:50:17.540000000|2017-07-22 00:50:17.523000000|2017-07-21       |0.00                    |0.00                    |54.00                       |54.00                  |0.00                 |          |Proximity       |                              |                |null        |                 |
# |53481518          |PC7135361         |MOBILE                     |17                 |2017-07-22 00:49:55.600000000|2017-07-22 00:49:55.600000000|2017-07-21       |0.00                    |0.00                    |10.00                       |10.00                  |0.00                 |          |InfoSys         |                              |                |null        |                 |
# |53481522          |PC7135361         |MOBILEAPP                  |17                 |2017-07-22 00:49:58.413000000|2017-07-22 00:49:58.400000000|2017-07-21       |0.00                    |0.00                    |36.00                       |36.00                  |0.00                 |          |InfoSys         |                              |                |null        |                 |
# |53481588          |PC7135361         |MOBILE                     |17                 |2017-07-22 00:50:44.637000000|2017-07-22 00:50:44.607000000|2017-07-21       |0.00                    |0.00                    |36.00                       |36.00                  |0.00                 |          |InfoSys         |                              |                |null        |                 |
# |53481441          |PC7135361         |MOBILE                     |17                 |2017-07-22 00:48:59.987000000|2017-07-22 00:48:59.970000000|2017-07-21       |0.00                    |0.00                    |36.00                       |36.00                  |0.00                 |          |InfoSys         |                              |                |null        |                 |
# |53481440          |PC7135361         |MOBILEAPP                  |21                 |2017-07-22 00:48:59.987000000|2017-07-22 04:48:59          |2017-07-22       |0.00                    |0.00                    |10.00                       |60.00                  |0.00                 |          |InfoSys         |W9GPTTCNWYCF4FC               |                |null        |                 |
# |53481564          |PC6890261         |MOBILEAPP                  |17                 |2017-07-22 00:50:32.793000000|2017-07-22 00:50:32.760000000|2017-07-21       |0.00                    |0.00                    |10.00                       |10.00                  |0.00                 |          |InfoSys         |                              |                |null        |                 |
# |53481437          |PC6890261         |MOBILEAPP                  |17                 |2017-07-22 00:48:54.930000000|2017-07-22 00:48:54.913000000|2017-07-21       |0.00                    |0.00                    |36.00                       |36.00                  |0.00                 |          |InfoSys         |                              |                |null        |                 |
# |53481572          |PC6890261         |MOBILEAPP                  |21                 |2017-07-22 00:50:35.010000000|2017-07-22 04:50:34          |2017-07-22       |0.00                    |0.00                    |40.00                       |90.00                  |0.00                 |          |InfoSys         |JPRCWR3PPPC3FJ3               |                |null        |                 |
# +------------------+------------------+---------------------------+-------------------+-----------------------------+-----------------------------+-----------------+------------------------+------------------------+----------------------------+-----------------------+---------------------+----------+----------------+------------------------------+----------------+------------+-----------------+
# only showing top 10 rows
