"""
This program demonstrates how to connect to AWS resources such as S3
"""

from pyspark.sql import SparkSession
import os.path
import yaml


def create_spark_session() -> SparkSession:
    session = SparkSession\
        .builder\
        .appName('local-server-app')\
        .getOrCreate()
    return session


if __name__ == '__main__':
    sparkSession = create_spark_session()
    sparkContext = sparkSession.sparkContext

    # Set logging level to Debug
    sparkContext.setLogLevel('DEBUG')

    # Load configuration and secrets
    current_dir = os.path.abspath(os.path.dirname(__file__))

    app_config_path = os.path.abspath(current_dir + '/../' + 'application.yml')
    app_config = open(app_config_path)
    app_conf = yaml.load(app_config, Loader=yaml.FullLoader)

    app_secrets_path = os.path.abspath(current_dir + '/../' + '.secrets')
    app_secrets = open(app_secrets_path)
    app_secret = yaml.load(app_secrets, Loader=yaml.FullLoader)

    # Setup hadoop configuration to use AWS S3
    hadoop_conf = sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    demographics_rdd = sparkContext.textFile("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/demographic.csv")

    # Print all Spark configurations
    print('********** Spark Configuration : {0}'.format(sparkContext.getConf().getAll()))

    print('********** Rdd print : {0}'.format(demographics_rdd))
    print('********** First 10 records : {0}'.format(demographics_rdd.take(10)))
    print('********** Partitions : {0}'.format(demographics_rdd.getNumPartitions()))


# Spark Submit command to run the application
# Command-1 :
#   spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" aws_connect/load_data_from_s3.py