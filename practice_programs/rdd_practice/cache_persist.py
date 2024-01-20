"""
    This program demonstrates the functionalities of cache() and persist(..) APIs
"""

from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('glom-collect-test') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    print('************************************ App logs **********************************')
    rdd = spark.sparkContext.parallelize(range(1, 201, 5))
    print(f'RDD data count : {rdd.count()}')
    print(f'RDD data : {rdd.take(20)}')

    # take rdd of odd numbers
    odd_rdd = rdd.filter(lambda rec: rec % 2 != 0)
    print(f'ODD RDD count : {odd_rdd.count()}')
    print(f'ODD RDD data : {odd_rdd.take(20)}')

    print(f'odd_rdd.is_cached : {odd_rdd.is_cached}')
    print(f'odd_rdd.getStorageLevel() : {odd_rdd.getStorageLevel()}')

    # rdd.cache() the odd rdd
    odd_rdd.cache()
    print('odd_rdd.cache()')
    print(f'odd_rdd.is_cached : {odd_rdd.is_cached}')
    print(f'odd_rdd.getStorageLevel() : {odd_rdd.getStorageLevel()}')

    # RDD.persist(...)
    even_rdd = rdd.filter(lambda rec: rec % 2 == 0)
    print(f'even_rdd.count() : {even_rdd.count()}')
    print(f'even_rdd.take(20) : {even_rdd.take(20)}')

    print(f'even_rdd.is_cached : {even_rdd.is_cached}')
    print(f'even_rdd.getStorageLevel() : {even_rdd.getStorageLevel()}')

    # persist(...)
    even_rdd.persist()
    print('even_rdd.persist()')
    print(f'even_rdd.is_cached : {even_rdd.is_cached}')
    print(f'even_rdd.getStorageLevel() : {even_rdd.getStorageLevel()}')

    # Otherwise it will throw exception
    # pyspark.errors.exceptions.captured.UnsupportedOperationException:
    # Cannot change storage level of an RDD after it was already assigned a level
    even_rdd.unpersist()

    # persist(DISK_ONLY)
    even_rdd.persist(StorageLevel.DISK_ONLY)
    print('even_rdd.persist(StorageLevel.DISK_ONLY)')
    print(f'even_rdd.is_cached : {even_rdd.is_cached}')
    print(f'even_rdd.getStorageLevel() : {even_rdd.getStorageLevel()}')
#
# Command
# --------------------
# park-submit --master 'local[*]' cache_persist.py
#
# Output
# ----------------------
# ************************************ App logs **********************************
# RDD data count : 40
# RDD data : [1, 6, 11, 16, 21, 26, 31, 36, 41, 46, 51, 56, 61, 66, 71, 76, 81, 86, 91, 96]
# ODD RDD count : 20
# ODD RDD data : [1, 11, 21, 31, 41, 51, 61, 71, 81, 91, 101, 111, 121, 131, 141, 151, 161, 171, 181, 191]
# odd_rdd.is_cached : False
# odd_rdd.getStorageLevel() : Serialized 1x Replicated
# odd_rdd.cache()
# odd_rdd.is_cached : True
# odd_rdd.getStorageLevel() : Memory Serialized 1x Replicated
# even_rdd.count() : 20
# even_rdd.take(20) : [6, 16, 26, 36, 46, 56, 66, 76, 86, 96, 106, 116, 126, 136, 146, 156, 166, 176, 186, 196]
# even_rdd.is_cached : False
# even_rdd.getStorageLevel() : Serialized 1x Replicated
# even_rdd.persist()
# even_rdd.is_cached : True
# even_rdd.getStorageLevel() : Memory Serialized 1x Replicated
# even_rdd.persist(StorageLevel.DISK_ONLY)
# even_rdd.is_cached : True
# even_rdd.getStorageLevel() : Disk Serialized 1x Replicated
#
# #