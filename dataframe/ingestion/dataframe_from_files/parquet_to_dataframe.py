"""
This application demonstrates how to read/write Apache Parquet files in spark.
"""


from pyspark.sql import SparkSession
from constants import app_constants as appConstants


if __name__ == '__main__':
    print('\n************************** Spark - Read/Write Parquet files **************************')

    sparkSession = SparkSession.builder.appName('parquet-to-dataframe').getOrCreate()
    sparkSession.sparkContext.setLogLevel('ERROR')

    # Read Parquet file
    nyc_omo_df = sparkSession.read.parquet(appConstants.NYC_OMO_PARQUET)

    print('\n************* # of partitions : ' + nyc_omo_df.rdd.getNumPartitions())
    print('\n************* # of records : ' + str(nyc_omo_df.count()))

    print('\n************* nyc_omo_df.printSchema()')
    nyc_omo_df.printSchema()

    print('\n************* nyc_omo_df.show(nyc_omo_df.count(), False)')
    nyc_omo_df.show(nyc_omo_df.count(), False)

    # Repartition
    print('\n************* nyc_omo_df = nyc_omo_df.repartition(5)')
    nyc_omo_df = nyc_omo_df.repartition(5)

    print('\n************* # of partitions : ' + nyc_omo_df.rdd.getNumPartitions())
    print('\n************* # of records : ' + str(nyc_omo_df.count()))

    print('\n************* nyc_omo_df.printSchema()')
    nyc_omo_df.printSchema()

    print('\n************* nyc_omo_df.show(nyc_omo_df.count(), False)')
    nyc_omo_df.show(nyc_omo_df.count(), False)