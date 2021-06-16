"""
This program demonstrates different transformation examples on DataFrame instance.
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os.path
import yaml

if __name__ == "__main__":
    sparkSession = SparkSession\
        .builder\
        .appName('Data curation using DSL')\
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('ERROR')

    print("\n***************************** Data curation using DSL *****************************\n")

    # Load App Configs
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_file = os.path.abspath(current_dir + '../../../..' + '/application.yml')
    app_secret_file = os.path.abspath(current_dir + '../../../..' + '/.secrets')

    app_config = yaml.load(open(app_config_file), Loader=yaml.FullLoader)
    app_secret = yaml.load(open(app_secret_file), Loader=yaml.FullLoader)

    # Load data from AWS S3
    hadoop_conf = sparkSession.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set('fs.s3a.access.key', app_secret['s3_conf']['access_key'])
    hadoop_conf.set('fs.s3a.secret.key', app_secret['s3_conf']['secret_access_key'])

    # File path of AWS S3
    file_path = 's3a://' + app_config["s3_conf"]["s3_bucket"] + '/finances-small'
    print('\n************************ S3 URL : ' + file_path)

    # Read data in Data frame
    finances_df = sparkSession.read.parquet(file_path)

    # Print schema and sample records
    finances_df.printSchema()
    print('\n******************* Sample records : finances_df.show(5, truncate=False)\n')
    finances_df.show(5, truncate=False)

    # Sort the data using 'Amount' column
    print('\n******************* Sample records : finances_df.orderBy(col(\'Amount\')).show(5, truncate=False)\n')
    finances_df.orderBy(col('Amount')).show(5, truncate=False)

# Command
# --------------------
# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.8.4,com.amazonaws:aws-java-sdk:1.11.95,com.amazonaws:aws-java-sdk-core:1.11.95,com.amazonaws:aws-java-sdk-s3:1.11.95,com.amazonaws:aws-java-sdk-kms:1.11.95" dataframe/curation/dsl/finance_data_analysis_dsl_aws_s3.py
#
# Output
# --------------------
# ***************************** Data curation using DSL *****************************
#
#
# ************************ S3 URL : s3a://dataeng-test-1/finances-small
# root
#  |-- AccountNumber: string (nullable = true)
#  |-- Amount: double (nullable = true)
#  |-- Date: string (nullable = true)
#  |-- Description: string (nullable = true)
#
#
# ******************* Sample records : finances_df.show(5, truncate=False)
#
# +-------------+------+---------+-------------+
# |AccountNumber|Amount|Date     |Description  |
# +-------------+------+---------+-------------+
# |123-ABC-789  |1.23  |1/1/2015 |Drug Store   |
# |456-DEF-456  |200.0 |1/3/2015 |Electronics  |
# |333-XYZ-999  |106.0 |1/4/2015 |Gas          |
# |123-ABC-789  |2.36  |1/9/2015 |Grocery Store|
# |456-DEF-456  |23.16 |1/11/2015|Unknown      |
# +-------------+------+---------+-------------+
# only showing top 5 rows
#
#
# ******************* Sample records : finances_df.orderBy(col('Amount')).show(5, truncate=False)
#
# +-------------+------+---------+-------------+
# |AccountNumber|Amount|Date     |Description  |
# +-------------+------+---------+-------------+
# |123-ABC-789  |0.0   |2/14/2015|Unknown      |
# |123-ABC-789  |1.23  |1/1/2015 |Drug Store   |
# |987-CBA-321  |2.29  |1/31/2015|Drug Store   |
# |123-ABC-789  |2.36  |1/9/2015 |Grocery Store|
# |456-DEF-456  |6.78  |2/23/2015|Drug Store   |
# +-------------+------+---------+-------------+
# only showing top 5 rows
