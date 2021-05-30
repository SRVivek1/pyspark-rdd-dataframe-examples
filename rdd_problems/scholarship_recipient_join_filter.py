"""
This program demonstrates how to do SQL kind of join in RDDs
"""

from pyspark.sql import SparkSession

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
    print(demographic_rdd.map(lambda line: line.split(',')).take(10))
