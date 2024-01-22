"""
    This program demonstrates the use of mapPartition(..) API.
"""

from pyspark.sql import SparkSession
import sys

try:
    import testdata.test_files as testdata
except ImportError:
    sys.path.append('/home/viveksingh/wrkspace/pyspark-rdd-dataframe-examples/')
    try:
        import testdata.test_files as testdata
    finally:
        print('finally')

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('map-partition') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    print('************************************ App logs **********************************')

    # Read CSV with textFile(...) API
    rdd = spark.sparkContext.textFile(testdata.CRED_TXN_CSV)

    print(f'rdd.take(10) : {rdd.take(10)}')
    print(f'rdd.getNumPartitions() : {rdd.getNumPartitions()}')
    print(f'Records : {rdd.count()}')

    # Transform data into tuples
    def str_split(partition):
        print('Inside - str_split(partition) function')
        print(type(partition))

        rec_itr = []
        # TODO - Business logic
        return rec_itr

    # Transform using mapPartition API
    # Note:
    # Check the output even though we have 50 records to filter
    # still the  str_split method is called 2 times because
    # the rdd has 2 partitions
    rdd.filter(lambda rec: 'AccNum~Amount~Date~Category' not in rec).mapPartitions(str_split).take(10)

    print('Repartitioning the rdd in 4 partitions')
    # change partition to 4
    rdd_4 = rdd.repartition(4)
    rdd_4.filter(lambda rec: 'AccNum~Amount~Date~Category' not in rec).mapPartitions(str_split).take(10)

# Command
# -------------------
#  spark-submit --master 'local[*]' map_partition_api.py
#
# Output
# -------------------
# ************************************ App logs **********************************
# rdd.take(10) : ['AccNum~Amount~Date~Category', '123-ABC-789~1.23~01/01/2015~Drug Store', '456-DEF-456~200.0~01/03/2015~Electronics', '333-XYZ-999~106.0~01/04/2015~Gas', '123-ABC-789~2.36~01/09/2015~Grocery Store', '456-DEF-456~23.16~01/11/2015~Unknown', '123-ABC-789~42.12~01/12/2015~Park', '456-DEF-456~20.0~01/12/2015~Electronics', '333-XYZ-999~52.13~01/17/2015~Gas', '333-XYZ-999~41.67~01/19/2015~Some Totally Fake Long Description']
# rdd.getNumPartitions() : 2
# Records : 50
# Inside - str_split(partition) function
# <class 'filter'>
# Inside - str_split(partition) function
# <class 'filter'>
#
# Repartitioning the rdd in 4 partitions
# Inside - str_split(partition) function
# <class 'filter'>
# Inside - str_split(partition) function
# <class 'filter'>
# Inside - str_split(partition) function
# <class 'filter'>
# Inside - str_split(partition) function
# <class 'filter'>
#
#
#
# #
