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
    sparkContext = sparkSession.sparkContext

    # Set logging level to ERROR to compress INFO logs
    sparkContext.setLogLevel("ERROR")

    demographic_csv_file = '/home/viveksingh/project-data/sidharth/data/demographic.csv'
    finances_csv_file = '/home/viveksingh/project-data/sidharth/data/finances.csv'

    print('\n************* Demographic File : {0}'.format(demographic_csv_file))
    print('\n************* Finances File : {0}'.format(finances_csv_file))

    demographic_rdd = sparkContext.textFile(demographic_csv_file)
    finances_rdd = sparkContext.textFile(finances_csv_file)

    # Transforming data into tuples
    demographic_pair_rdd = demographic_rdd \
        .map(lambda line: line.split(',')) \
        .map(lambda lst: (int(lst[0]), (int(lst[1]), strtobool(lst[2]), lst[3], lst[4], strtobool(lst[5]), strtobool(lst[6]), int(lst[7]))))

    print('\n******** type(demographic_pair_rdd) : {0}'.format(type(demographic_pair_rdd)))
    print('\n******** demographic_pair_rdd : {0}'.format(demographic_pair_rdd.take(10)))

    finances_pair_rdd = finances_rdd \
        .map(lambda line: line.split(',')) \
        .map(lambda lst: (int(lst[0]), (strtobool(lst[1]), strtobool(lst[2]), strtobool(lst[3]), int(lst[4]))))

    print('\n******** type(finances_pair_rdd) : {0}'.format(type(finances_pair_rdd)))
    print('\n******** finances_pair_rdd : {0}'.format(finances_pair_rdd.take(10)))

    # Join data of demographic rdd and finances rdd
    joined_rdd = demographic_pair_rdd \
        .join(finances_pair_rdd)
    print('\n******** type(joined_rdd) : {0}'.format(type(joined_rdd)))
    print('\n********* demographic_pair_rdd.join(finances_pair_rdd)')
    joined_rdd.foreach(print)

    # Find matches where city is 'Switzerland' and having debts and financial dependents
    result_rdd = joined_rdd \
        .filter(lambda rec: (rec[1][0][2] == "Switzerland") and (rec[1][1][0] == 1) and (rec[1][1][1] == 1))

    print('\n********* Applicants eligible for scholarship :')
    result_rdd.foreach(print)


# Spark command
#   spark-submit rdd_problems/scholarship_recipient_join_filter.py


# Output
# ************* Demographic File : /home/viveksingh/project-data/sidharth/data/demographic.csv
#
# ************* Finances File : /home/viveksingh/project-data/sidharth/data/finances.csv
#
# ******** type(demographic_pair_rdd) : <class 'pyspark.rdd.PipelinedRDD'>
# ******** demographic_pair_rdd : [(101, (18, 1, 'Switzerland', 'M', 1, 1, 1)), (102, (19, 1, 'Switzerland', 'F', 1, 0, 1)), (103, (22, 1, 'Switzerland', 'M', 1, 0, 2)), (104, (18, 1, 'Switzerland', 'F', 1, 0, 2))]
#
# ******** type(finances_pair_rdd) : <class 'pyspark.rdd.PipelinedRDD'>
# ******** finances_pair_rdd : [(101, (0, 1, 1, 60000)), (102, (1, 1, 0, 50000)), (103, (1, 1, 0, 55000)), (104, (1, 0, 0, 65000))]
#
# ******** type(joined_rdd) : <class 'pyspark.rdd.PipelinedRDD'>
# ********* demographic_pair_rdd.join(finances_pair_rdd)
# (104, ((18, 1, 'Switzerland', 'F', 1, 0, 2), (1, 0, 0, 65000)))
# (101, ((18, 1, 'Switzerland', 'M', 1, 1, 1), (0, 1, 1, 60000)))
# (103, ((22, 1, 'Switzerland', 'M', 1, 0, 2), (1, 1, 0, 55000)))
# (102, ((19, 1, 'Switzerland', 'F', 1, 0, 1), (1, 1, 0, 50000)))
#
# ********* Applicants eligible for scholarship :
# (102, ((19, 1, 'Switzerland', 'F', 1, 0, 1), (1, 1, 0, 50000)))
# (103, ((22, 1, 'Switzerland', 'M', 1, 0, 2), (1, 1, 0, 55000)))
# viveksingh@ubuntu-pc:~/spark-projects/rdd-dataframe-examples$
