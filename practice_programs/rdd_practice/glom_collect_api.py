"""
This program demonstrates the use of glom and collect APIs.
"""

from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName('glom-collect-test') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    print('************************************ App logs **********************************')
    # Parallelize a collection
    rdd = spark.sparkContext.parallelize(range(1, 21))

    # get the partitions
    print(f'Default Parallelism : {spark.sparkContext.defaultParallelism}')

    print(f'rdd.getNumPartitions() : {rdd.getNumPartitions()}')
    print(f'rdd.takeSample(..) : {rdd.takeSample(withReplacement=False, num=20)}')

    # glom function
    temp = rdd.glom()
    print(f'rdd.glom() : {temp}')
    print(f'temp.getNumPartitions() : {temp.getNumPartitions()}')
    print(f'temp.getStorageLevel() : {temp.getStorageLevel()}')

    # This is printing all the data because glom has
    # coalesced all records of each partition into a list.
    # Returning RDD[List]
    print(f'temp.take(20) : {temp.take(5)}')
    print(f'temp.take(2) : {temp.take(2)}')
    print(f'temp.take(4) : {temp.take(4)}')

    # collect function - return all the data from partitions as List.
    print(f'temp.collect() : {temp.collect()}')


# spark-submit --master 'local[*]' glom_collect_api.py
#
# Output
# ------------
# ************************************ App logs **********************************
# Default Parallelism : 4
# rdd.getNumPartitions() : 4
# rdd.takeSample(..) : [11, 5, 2, 10, 1, 7, 12, 15, 13, 4, 8, 9, 6, 19, 18, 17, 20, 3, 16, 14]
# rdd.glom() : PythonRDD[3] at RDD at PythonRDD.scala:53
# temp.getNumPartitions() : 4
# temp.getStorageLevel() : Serialized 1x Replicated
# temp.take(20) : [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10], [11, 12, 13, 14, 15], [16, 17, 18, 19, 20]]
# temp.take(2) : [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]]
# rdd.glom().take(4) : [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10], [11, 12, 13, 14, 15], [16, 17, 18, 19, 20]]
# temp.collect() : [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10], [11, 12, 13, 14, 15], [16, 17, 18, 19, 20]]
#
#
#
# #