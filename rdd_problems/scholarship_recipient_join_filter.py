"""
This program demonstrates how to do SQL kind of join in RDDs
"""

from pyspark.sql import SparkSession
from distutils.util import strtobool


def get_spark_session() -> SparkSession:
    return SparkSession\
        .builder\
        .appName('spark-join-demo-app')\
        .getOrCreate()


if __name__ == '__main__':

    sparkSession = get_spark_session()
    sparkContecxt = sparkSession.sparkContext

    demographic_csv_file = '/home/viveksingh/project-data/sidharth/data/demographic.csv'
    finances_csv_file = '/home/viveksingh/project-data/sidharth/data/finances.csv'

    print('\n************* Demographic File : {0}'.format(demographic_csv_file))
    print('\n************* Finances File : {0}'.format(finances_csv_file))

    demographic_rdd = sparkContecxt.textFile(demographic_csv_file)
    finances_rdd = sparkContecxt.textFile(finances_csv_file)

    # process demographic data
    demographic_pair_rdd = demographic_rdd \
        .map(lambda line: line.split(',')) \
        .map(lambda lst: (int(lst[0]), int(lst[1]), strtobool(lst[2]), lst[3], lst[4], strtobool(lst[5]), strtobool(lst[6]), int(lst[7])))

    print('******** demographic_pair_rdd : {0}'.format(demographic_pair_rdd.take(10)))
