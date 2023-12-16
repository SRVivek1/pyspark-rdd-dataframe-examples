"""
This program demonstrates how to read data from SFTP servers.
"""


from pyspark.sql import SparkSession
import os.path
import yaml


if __name__ == '__main__':
    print('\n******************************* Read data from AWS EC2 SFTP Server *******************************')

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
        'pem': os.path.abspath(project_root_dir + '/' + app_secrets['sftp_conf']['pem'])
    }
    # End : SFTP configuration

    # Start : File information
    sftp_file_details = {
        'filetype': app_config['sftp_conf']['filetype'],
        'delimiter': app_config['sftp_conf']['delimiter']
    }
    # End : File information

    # Start : Read data from SFTP - Error: Delimiter can't be empty
    '''receipts_df = sparkSession.read\
        .format('com.springml.spark.sftp')\
        .options(**sftp_connection_config)\
        .options(**sftp_file_details)\
        .load(app_config['sftp_conf']['directory'] + '/receipts_delta_GBR_14_10_2017.csv')'''

    receipts_df = sparkSession.read\
        .format("com.springml.spark.sftp")\
        .option("host", 'ec2-34-247-32-96.eu-west-1.compute.amazonaws.com')\
        .option("port", '22')\
        .option("username", 'ubuntu')\
        .option("pem", '/home/hadoop/rdd-dataframe-examples/ec2-pem-1.pem')\
        .option("fileType", "csv")\
        .option("delimiter", "|")\
        .load('/home/ubuntu/sftp/receipts_delta_GBR_14_10_2017.csv')
    # End : Read data from SFTP

    # Show data
    print('\n********************** Sample data read from SFTP : receipts_df.show(10, truncate=False)')
    receipts_df.show(10, truncate=False)

# Command
# ---------------
# Not working - spark-submit --packages "com.springml:spark-sftp_2.11:1.1.1" dataframe/ingestion/read_from_sftp/dataframe_from_sftp_server.py
#
# Working
# --------
# spark-submit --packages "com.springml:spark-sftp_2.11:1.1.1,org.scala-lang:scala-library:2.11.11,org.scala-lang:scala-compiler:2.11.11,org.scala-lang:scala-reflect:2.11.11" dataframe/ingestion/read_from_sftp/dataframe_from_sftp_server.py
#
#
# Output
# ------------------
# ********************** Sample data read from SFTP : receipts_df.show(10, truncate=False)
# +-----------+----------+----------+---------+--------------+-------------+------------+-------------+--------------------------------------------------------------------------------------------------------------------------+-----------+------------+------+--------+----------+------------+-------------------+----------------+----------------+-------------------+------------+--------------------+----------------+------------------+
# |loyalty_id |process_id|mobile_uid|mobile_os|receipt_status|receipt_total|receipt_date|points_earned|products                                                                                                                  |store      |process_flag|locale|msg_code|program_id|country_code|r_cre_time         |doublesided_flag|base_point_value|sblp_transaction_id|receipt_type|r_last_modified_time|acp_id          |ereceipt_file_type|
# +-----------+----------+----------+---------+--------------+-------------+------------+-------------+--------------------------------------------------------------------------------------------------------------------------+-----------+------------+------+--------+----------+------------+-------------------+----------------+----------------+-------------------+------------+--------------------+----------------+------------------+
# |I034867789V|03TRKVhTO |NULL      |iOS      |15            |17.57        |02/19/2018  |63           |Active pants-6.30                                                                                                         |30000000071|0           |en_GB |30      |U-D3224   |gb          |2018-02-19 12:04:07|0               |63              |703035             |normal      |2018-02-19 12:09:15 |yqqImamjIxfrvKGU|NULL              |
# |67215200   |05fZGU0rA |NULL      |android  |15            |89.74        |02/19/2018  |70           |PAMPERS-7.00                                                                                                              |30000000076|0           |en_GB |30      |U-D3224   |gb          |2018-02-19 13:30:12|0               |70              |703205             |normal      |2018-02-19 13:32:03 |inILfSpcrR57bq8J|NULL              |
# |I028362656R|0aiMBx2eb |NULL      |iOS      |15            |17.85        |02/19/2018  |160          |Pampers Baby Dry Taped Size 4 Essential Pack 44 Nappies-8.00, Pampers Baby Dry Taped Size 4 Essential Pack 44 Nappies-8.00|30000000070|0           |en_GB |30      |U-D3224   |gb          |2018-02-19 20:06:19|0               |160             |703971             |normal      |2018-02-19 20:07:29 |0               |NULL              |
# |I036378500P|0EVVZybdq |NULL      |iOS      |22            |NULL         |NULL        |NULL         |NULL                                                                                                                      |NULL       |0           |en_GB |21      |U-D3224   |gb          |2018-02-19 14:05:58|0               |NULL            |NULL               |NULL        |2018-02-19 14:06:18 |9IXyeaJMCGdnBdAy|NULL              |
# |I035113531V|0ExCeCzmp |NULL      |iOS      |15            |38.28        |02/19/2018  |145          |Pampers baby dry pants-14.45                                                                                              |30000000070|0           |en_GB |30      |U-D3224   |gb          |2018-02-19 10:12:16|0               |145             |702850             |normal      |2018-02-19 10:13:42 |3QPnx1WNSBv6iDhs|NULL              |
# |I033956922L|0GE0w7hMk |NULL      |android  |15            |23.52        |02/19/2018  |80           |PAMPERS NAPPIES-8.00                                                                                                      |30000000068|0           |en_GB |30      |U-D3224   |gb          |2018-02-19 16:13:55|0               |80              |703543             |normal      |2018-02-19 16:22:11 |hDDNrC9vWrbXyCTe|NULL              |
# |I033147742E|0h7xOUYMS |NULL      |iOS      |15            |10.25        |02/19/2018  |40           |Nappies-4.00                                                                                                              |30000000071|0           |en_GB |30      |U-D3224   |gb          |2018-02-19 15:15:12|0               |40              |703391             |normal      |2018-02-19 15:17:33 |0               |NULL              |
# |I034871772G|0hzx38IgK |NULL      |iOS      |15            |52.09        |02/19/2018  |40           |Nappies-4.00                                                                                                              |30000000071|0           |en_GB |30      |U-D3224   |gb          |2018-02-19 12:24:51|0               |40              |703079             |normal      |2018-02-19 12:26:30 |42neNSiQ1ErvamcI|NULL              |
# |I031819596S|0iHEFMOnK |NULL      |android  |15            |20.1         |02/19/2018  |300          |Nappies-12.00, PAMPERS NAPPIES-8.00                                                                                       |30000000066|0           |en_GB |30      |U-D3224   |gb          |2018-02-19 16:27:40|0               |200             |703563             |normal      |2018-02-19 16:29:15 |kF7HjcxJIAQiOO6r|NULL              |
# +-----------+----------+----------+---------+--------------+-------------+------------+-------------+--------------------------------------------------------------------------------------------------------------------------+-----------+------------+------+--------+----------+------------+-------------------+----------------+----------------+-------------------+------------+--------------------+----------------+------------------+
