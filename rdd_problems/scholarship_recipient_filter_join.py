"""
This program find students which are from 'Switzerland' has debt and financial dependents.
"""


from pyspark.sql import SparkSession
import constants.app_constants as app_const

if __name__ == '__main__':
    sparkSession = SparkSession \
        .builder \
        .appName('rdd-filter-join-app') \
        .getOrCreate()

    sparkContext = sparkSession.sparkContext

    # Set logging level to ERROR
    sparkContext.setLogLevel("ERROR")

    # Load demographic file data
    demographic_rdd = sparkContext.textFile(app_const.demographic_csv_file)
    finances_rdd = sparkContext.textFile(app_const.finances_csv_file)

    demographic_rdd.foreach(print)
    finances_rdd.foreach(print)
