"""
This program find students which are from 'Switzerland' has debt and financial dependents.
"""


from pyspark.sql import SparkSession
import constants.app_constants as app_const
from distutils.util import strtobool


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

    print('\n********** Demographic data : ')
    demographic_rdd.foreach(print)

    print('\n********** finances data : ')
    finances_rdd.foreach(print)

    # Create pair RDD and Filter for Switzerland
    demographic_pair_rdd = demographic_rdd \
        .map(lambda lines: lines.split(',')) \
        .map(lambda lst: (int(lst[0]), (int(lst[1]),
                                        strtobool(lst[2]), lst[3], lst[4],
                                        strtobool(lst[5]), strtobool(lst[6]), int(lst[7])))) \
        .filter(lambda rec: rec[1][3] == 'Switzerland')

