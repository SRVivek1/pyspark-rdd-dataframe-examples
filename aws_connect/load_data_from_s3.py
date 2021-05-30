"""
This program demonstrates how to connect to AWS resources such as S3
"""

from pyspark.sql import SparkSession
import aws_connect.local_resources as res_mgr


def create_spark_session() -> SparkSession:
    session = SparkSession\
        .builder\
        .master('local')\
        .appName('local-server-app')\
        .getOrCreate()
    return session


if __name__ == '__main__':
    sparkSession = create_spark_session()
    sContext = sparkSession.sparkContext

    # Load configuration and secrets
    app_conf = res_mgr.load_app_conf()
    app_secret = res_mgr.load_secrets_conf()

    # Setup hadoop configuration to use AWS S3
    hadoop_conf = sContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    # Fix for NumberFormatError 100M - Impacting getNumPartitions()
    hadoop_conf.set('spark.shuffle.service.index.cache.size', '104857600')
    # hadoop_conf.set("fs.s3a.multipart.size", "104857600")
    # hadoop_conf.set("spark.hadoop.mapred.output.compress", "true")
    # hadoop_conf.set("spark.hadoop.mapred.output.compression.codec", "true")
    # hadoop_conf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
    # hadoop_conf.set("spark.hadoop.mapred.output.compression.`type", "BLOCK")
    # hadoop_conf.set("spark.speculation", "false")

    demographics_rdd = sContext.textFile("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/demographic.csv")

    print('********** Rdd print : {0}'.format(demographics_rdd))
    print('********** First 10 records : {0}'.format(demographics_rdd.take(10)))
    print('********** Partitions : {0}'.format(demographics_rdd.getNumPartitions()))
