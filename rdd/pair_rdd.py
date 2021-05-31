"""
This example show RDD vs Pair RDD creation.
"""


from pyspark.sql import SparkSession


if __name__ == '__main__':
    sparkSession = SparkSession \
        .builder \
        .appName('paid-rdd-demo') \
        .getOrCreate()

    rdd_int_list = sparkSession.sparkContext.parallelize(range(1, 1000, 4))
    rdd_list_pair = sparkSession.sparkContext.parallelize([('a', 65), ('b', 66), ('c', 67), ('d', 68)])

    print('********* sc.parallelize(range(1, 1000, 4)) : {0}'.format(type(rdd_int_list)))
    print("********* sc.parallelize([('a', 65), ('b', 66), ('c', 67), ('d', 68)]) : {0}".format(type(rdd_list_pair)))
