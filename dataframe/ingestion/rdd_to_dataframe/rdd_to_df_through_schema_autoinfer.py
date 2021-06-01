"""
This program demonstrates how to create DataFrame from RDD without Schema definition.
"""


from pyspark.sql import SparkSession
import constants.app_constants as appConstants


if __name__ == '__main__':

    sparkSession = SparkSession \
        .builder \
        .appName('rdd-to-dataframe') \
        .master('') \
        .getOrCreate()

    sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("ERROR")

    # Load data from local
    txn_fct_rdd = sparkContext.textFile(appConstants.txn_fct_csv_file)

    print('\n***************** Raw data snippet : ')
    txn_fct_rdd.take(5).foreach(print)

