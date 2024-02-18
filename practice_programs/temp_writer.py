"""
	Read data from My SQL
"""

from pyspark.sql import SparkSession


if __name__ == '__main__':

    spark = SparkSession.builder \
        .appName('read-from-db-mysql') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    jdbc_params = {
        'driver': 'com.mysql.cj.jdbc.Driver',
        'lowerBound': 1,
        'upperBound': 100,
        'numPartitions': 2,
        'partitionColumn': 'App_Transaction_Id',
        'user': 'vsingh',
        'password': 'c0nn3ct0r'
    }

    df = spark.read \
        .options(**jdbc_params) \
        .jdbc(url='jdbc:mysql://localhost:32769/devdb?useSSL=false', table='TransactionSync')

    df.printSchema()
    df.show(10)