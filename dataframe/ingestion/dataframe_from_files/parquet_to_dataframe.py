"""
This application demonstrates how to read/write Apache Parquet files in spark.
"""


from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as sqlFuntions
from constants import app_constants as appConstants


if __name__ == '__main__':
    print('\n************************** Spark - Read/Write Parquet files **************************')

    # Using legacy version of Parquet file.
    sparkSession = SparkSession.builder \
        .config('spark.sql.legacy.parquet.int96RebaseModeInRead', 'CORRECTED')\
        .appName('parquet-to-dataframe')\
        .getOrCreate()

    # sparkSession.conf.set('spark.sql.legacy.parquet.int96RebaseModeInRead', 'CORRECTED')

    sparkSession.sparkContext.setLogLevel('ERROR')

    # Read Parquet file
    nyc_omo_df = sparkSession.read.parquet(appConstants.NYC_OMO_PARQUET)

    print('\n************* # of partitions : ' + str(nyc_omo_df.rdd.getNumPartitions()))
    print('\n************* # of records : ' + str(nyc_omo_df.count()))

    print('\n************* nyc_omo_df.printSchema()')
    nyc_omo_df.printSchema()

    print('\n************* nyc_omo_df.show(5, False)')
    nyc_omo_df.show(5, False)

    # Repartition
    print('\n************* nyc_omo_df = nyc_omo_df.repartition(5)')
    nyc_omo_df = nyc_omo_df.repartition(5)

    print('\n************* # of partitions : ' + str(nyc_omo_df.rdd.getNumPartitions()))
    print('\n************* # of records : ' + str(nyc_omo_df.count()))

    print('\n************* nyc_omo_df.printSchema()')
    nyc_omo_df.printSchema()

    print('\n************* nyc_omo_df.show(5, False)')
    nyc_omo_df.show(5, False)

    print('\n************* Summery of NYC Open Market Order (OMO) charges dataset : nyc_omo_df.describe().show()')
    nyc_omo_df.describe().show()

    print('\n************* Distinct Boroughs in record : nyc_omo_df.select(col(\'Boro\')).distinct().show(100)')
    nyc_omo_df.select(sqlFuntions.col('Boro')).distinct().show(100)

    print('\n************* OMO frequency distribution of different Boroughs')
    nyc_omo_df \
        .groupBy('Boro') \
        .agg({'Boro': 'count'}) \
        .withColumnRenamed('count(Boro)', 'frequency_distribution') \
        .show()

    print('\n************* OMO ZIP and Boro List')
    boro_zip_df = nyc_omo_df \
        .groupBy('Boro') \
        .agg({'Zip': 'collect_set'}) \
        .withColumnRenamed("collect_set(Zip)", "ZipList") \
        .withColumn('ZipCount', sqlFuntions.size(sqlFuntions.col('ZipList')))\

    boro_zip_df\
        .select('Boro', 'ZipList', 'ZipCount')\
        .show()

    # Windows function
    window_specs = Window.partitionBy('OMOCreateDate')
    omo_daily_frequency = nyc_omo_df\
        .withColumn('DailyFrequency', sqlFuntions.count('OMOID').over(window_specs))

    print('\n**************** # of partitions in windowed OMO dataframe : ' + str(omo_daily_frequency.rdd.getNumPartitions()))

    print('\n**************** Sample records in windowed OMO dataframe : ')
    omo_daily_frequency.show(10)

    # Write windowed data to filesystem
    write_path = appConstants.file_write_path + 'nyc_omo_data_parquet'
    print('\n**************** Write windowed data to : ' + write_path)

    # Setting legacy mode
    sparkSession.conf.set('spark.sql.legacy.parquet.int96RebaseModeInWrite', 'CORRECTED')
    omo_daily_frequency\
        .repartition(5)\
        .write\
        .mode('overwrite')\
        .parquet(write_path)

    # Stop Spark service
    sparkSession.stop()

# Command
# -----------------
#   spark-submit dataframe/ingestion/dataframe_from_files/parquet_to_dataframe.py
#
# Output
# -----------------
#
