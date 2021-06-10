"""
This application demonstrates how to read/write Apache Parquet files in spark.
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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
    nyc_omo_df.select(col('Boro')).distinct().show(100)

    print('\n************* OMO frequency distribution of different Boroughs')
    nyc_omo_df \
        .groupBy('Boro') \
        .agg({'Boro': 'count'}) \
        .show()


# Command
# -----------------
#   spark-submit dataframe/ingestion/dataframe_from_files/parquet_to_dataframe.py
#
# Output
# -----------------
#
