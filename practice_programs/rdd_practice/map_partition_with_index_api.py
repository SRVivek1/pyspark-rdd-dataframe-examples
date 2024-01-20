"""
    This program demonstrates the use of mapPartitionWithIndex(..) API.
"""

from pyspark.sql import SparkSession
import sys

try:
    import testdata.test_files as testdata
except ImportError:
    sys.path.append('/home/viveksingh/wrkspace/pyspark-sidharth/pyspark-rdd-dataframe-examples/')
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

    # change partition to 4
    rdd_4 = rdd = spark.sparkContext.textFile(testdata.CRED_TXN_CSV, minPartitions=4)
    print(f'rdd_4.take(10) : {rdd_4.take(10)}')
    print(f'rdd_4.getNumPartitions() : {rdd_4.getNumPartitions()}')
    print(f'Records : {rdd_4.count()}')

    print('***************** Transformed data:')
    # Transform data into tuples
    def str_split(index, partition):
        print('Inside - str_split(index, partition) function')
        print(type(partition))

        # remove header from 1st partition
        if index == 0:
            partition = list(partition)[1:]

        # Do nothing
        return partition

    # Transform using mapPartition API
    # Note:
    # Check the output even though we have 50 records to filter
    # still the  str_split method is called 2 times because
    # the rdd has 2 partitions
    print(f'rdd.mapPartitionsWithIndex(str_split).take(10) : {rdd.mapPartitionsWithIndex(str_split).take(10)}')

    print('RDD with 4 partitions')
    print(f'rdd_4.mapPartitionsWithIndex(str_split).take(10): {rdd_4.mapPartitionsWithIndex(str_split).take(10)}')

    # debug rdd_4
    print("****************** debug")
    print(f'rdd_4.glom().collect(): {rdd_4.glom().collect()}')

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
# rdd_4.take(10) : ['AccNum~Amount~Date~Category', '123-ABC-789~1.23~01/01/2015~Drug Store', '456-DEF-456~200.0~01/03/2015~Electronics', '333-XYZ-999~106.0~01/04/2015~Gas', '123-ABC-789~2.36~01/09/2015~Grocery Store', '456-DEF-456~23.16~01/11/2015~Unknown', '123-ABC-789~42.12~01/12/2015~Park', '456-DEF-456~20.0~01/12/2015~Electronics', '333-XYZ-999~52.13~01/17/2015~Gas', '333-XYZ-999~41.67~01/19/2015~Some Totally Fake Long Description']
# rdd_4.getNumPartitions() : 4
# Records : 50
# ***************** Transformed data:
# Inside - str_split(index, partition) function
# <class 'generator'>
# rdd.mapPartitionsWithIndex(str_split).take(10) : ['123-ABC-789~1.23~01/01/2015~Drug Store', '456-DEF-456~200.0~01/03/2015~Electronics', '333-XYZ-999~106.0~01/04/2015~Gas', '123-ABC-789~2.36~01/09/2015~Grocery Store', '456-DEF-456~23.16~01/11/2015~Unknown', '123-ABC-789~42.12~01/12/2015~Park', '456-DEF-456~20.0~01/12/2015~Electronics', '333-XYZ-999~52.13~01/17/2015~Gas', '333-XYZ-999~41.67~01/19/2015~Some Totally Fake Long Description', '333-XYZ-999~56.37~01/21/2015~Gas']
# RDD with 4 partitions
# Inside - str_split(index, partition) function
# <class 'generator'>
# rdd_4.mapPartitionsWithIndex(str_split).take(10): ['123-ABC-789~1.23~01/01/2015~Drug Store', '456-DEF-456~200.0~01/03/2015~Electronics', '333-XYZ-999~106.0~01/04/2015~Gas', '123-ABC-789~2.36~01/09/2015~Grocery Store', '456-DEF-456~23.16~01/11/2015~Unknown', '123-ABC-789~42.12~01/12/2015~Park', '456-DEF-456~20.0~01/12/2015~Electronics', '333-XYZ-999~52.13~01/17/2015~Gas', '333-XYZ-999~41.67~01/19/2015~Some Totally Fake Long Description', '333-XYZ-999~56.37~01/21/2015~Gas']
# ****************** debug
# rdd_4.glom().collect(): [['AccNum~Amount~Date~Category', '123-ABC-789~1.23~01/01/2015~Drug Store', '456-DEF-456~200.0~01/03/2015~Electronics', '333-XYZ-999~106.0~01/04/2015~Gas', '123-ABC-789~2.36~01/09/2015~Grocery Store', '456-DEF-456~23.16~01/11/2015~Unknown', '123-ABC-789~42.12~01/12/2015~Park', '456-DEF-456~20.0~01/12/2015~Electronics', '333-XYZ-999~52.13~01/17/2015~Gas', '333-XYZ-999~41.67~01/19/2015~Some Totally Fake Long Description', '333-XYZ-999~56.37~01/21/2015~Gas', '987-CBA-321~63.84~01/23/2015~Grocery Store', '123-ABC-789~160.91~01/24/2015~Electronics'], ['456-DEF-456~78.77~01/24/2015~Grocery Store', '333-XYZ-999~86.24~01/29/2015~Movies', '456-DEF-456~93.71~01/31/2015~Grocery Store', '987-CBA-321~2.29~01/31/2015~Drug Store', '456-DEF-456~108.64~01/31/2015~Park', '456-DEF-456~116.11~01/31/2015~Books', '123-ABC-789~27.19~02/10/2015~Grocery Store', '333-XYZ-999~131.04~02/11/2015~Electronics', '456-DEF-456~18.99~02/12/2015~Drug Store', '123-ABC-789~145.98~02/13/2015~Grocery Store', '123-ABC-789~153.44~02/14/2015~Grocery Store', '123-ABC-789~0.0~02/14/2015~Unknown'], ['987-CBA-321~168.38~02/14/2015~Electronics', '333-XYZ-999~175.84~02/14/2015~Gas', '333-XYZ-999~43.12~02/18/2015~Grocery Store', '987-CBA-321~101.18~02/19/2015~Park', '456-DEF-456~215.67~02/20/2015~Electronics', '123-ABC-789~9.17~02/20/2015~Movies', '456-DEF-456~71.31~02/22/2015~Gas', '456-DEF-456~6.78~02/23/2015~Drug Store', '123-ABC-789~138.51~02/24/2015~Grocery Store', '333-XYZ-999~43.12~02/26/2015~Gas', '123-ABC-789~123.58~02/26/2015~Grocery Store', '987-CBA-321~150.33~02/27/2015~Grocery Store', '987-CBA-321~63.12~02/28/2015~Gas'], ['123-ABC-789~22.34~03/01/2015~Movies', '456-DEF-456~189.53~03/02/2015~Books', '123-ABC-789~4000.0~03/02/2015~Electronics', '333-XYZ-999~43.12~03/02/2015~Books', '333-XYZ-999~228.73~03/03/2015~Grocery Store', '333-XYZ-999~241.8~03/04/2015~Electronics', '123-ABC-789~254.87~03/04/2015~Grocery Store', '987-CBA-321~12.42~03/04/2015~Movies', '456-DEF-456~281.0~03/09/2015~Electronics', '987-CBA-321~42.42~03/10/2015~Books', '987-CBA-321~267.93~03/11/2015~Grocery Store', '456-DEF-456~42.85~03/12/2015~Gas']]
#
#
#
# #
