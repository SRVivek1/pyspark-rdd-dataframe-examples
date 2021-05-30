"""
This file demonstrates load file from local system.

*** Note:  Configured for Ubuntu-VM setup.
"""

from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    return SparkSession\
        .builder\
        .appName('local-file-read-app')\
        .getOrCreate()


if __name__ == '__main__':
    sparkSession = get_spark_session();
    sparkContext = sparkSession.sparkContext

    # Read local file as RDD
    txn_rdd = sparkContext.textFile('/home/viveksingh/project-data/sidharth/data/txn_fct.csv')

    print('RDD instance : {0}'.format(txn_rdd))
    print('RDD partitions : {0}'.format(txn_rdd.getNumPartitions()))

    print('First 15 records : \n{0}'.format(txn_rdd.take(15)))


# Spark Submit command to run the application
# Command-1 :
#   spark-submit rdd/rdd_load_loca_file.py
