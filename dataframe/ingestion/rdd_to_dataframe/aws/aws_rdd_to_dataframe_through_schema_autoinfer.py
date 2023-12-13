"""
    Problem:
    -------------
        --> Create DataFrame from RDD without schema definition.

    Platform:
    -------------
        --> AWS EMR, Cloud

"""
from pyspark import SparkConf
from pyspark.sql import SparkSession
import os
import yaml


if __name__ == '__main__':

    # Create session
    spark = SparkSession.builder\
        .appName('RDD to DataFrame POC')\
        .config('spark.jars.packages','org.apache.hadoop:hadoop-aws:2.7.4')\
        .master('local[*]')\
        .getOrCreate()

    sc = spark.sparkContext

    # Log
    sc.setLogLevel('ERROR')

    # Read config
    pw_dir = os.path.abspath(os.path.dirname(__file__))
    app_conf_file = os.path.abspath(pw_dir + '../../../../../' + 'application.yml')
    secrets_file = os.path.abspath(pw_dir + '../../../../../../' + '.secrets')

    app_conf = yaml.load(open(app_conf_file), Loader=yaml.FullLoader)
    secrets = yaml.load(open(secrets_file), Loader=yaml.FullLoader)

    # AWS s3 connection access keys
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set('fs.s3a.access.key', secrets['s3_conf']['access_key'])
    hadoop_conf.set('fs.s3a.secret.key', secrets['s3_conf']['secret_access_key'])
    #hadoop_conf.set('spark.hadoop.fs.s3a.multipart.size', '104857600')
    hadoop_conf.set('fs.s3a.multipart.size', '104857600')

    txn_fct_rdd = sc.textFile('s3a://' + app_conf['s3_conf']['s3_bucket'] + '/txn_fct.csv')

    print('********************* RAW Data from csv file ')
    txn_fct_rdd.foreach(print)

    # data cleaning
    txn_fct_rdd = txn_fct_rdd\
        .filter(lambda record: record.find('txn_id|create_time|'))\
        .map(lambda record: record.split('|'))\
        .map(lambda rec: (int(rec[0]), rec[1], float(rec[2]), rec[3], rec[4], rec[5], rec[6]))

    # print records
    print("************** Transformed RDD")
    txn_fct_rdd.foreach(print)

# Submit command
# spark-submit --packages 'org.apache.hadoop:hadoop-aws:2.7.4' dataframe/ingestion/rdd_to_dataframe/aws/aws_rdd_to_dataframe_through_schema_autoinfer.py
#
