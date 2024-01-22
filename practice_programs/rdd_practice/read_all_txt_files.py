"""
    Read all files and do basic data gathering.
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

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('load all files') \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    rdd = sc.wholeTextFiles(testdata.TEST_DATA)

    rdd_files_read = rdd.map(lambda rec: rec[0])
    print(f'RDD : \n {rdd_files_read.take(10)}')

# Command
# -------------------
# spark-submit --master 'local[*]' read_all_txt_files.py
#
# Output
# -------------------
# RDD :
#  ['file:/home/viveksingh/wrkspace/pyspark-sidharth/pyspark-rdd-dataframe-examples/testdata/rdd-examples-data/demographic.csv', 'file:/home/viveksingh/wrkspace/pyspark-sidharth/pyspark-rdd-dataframe-examples/testdata/rdd-examples-data/finances.csv', 'file:/home/viveksingh/wrkspace/pyspark-sidharth/pyspark-rdd-dataframe-examples/testdata/rdd-examples-data/finances_3.csv', 'file:/home/viveksingh/wrkspace/pyspark-sidharth/pyspark-rdd-dataframe-examples/testdata/rdd-examples-data/course_2.csv', 'file:/home/viveksingh/wrkspace/pyspark-sidharth/pyspark-rdd-dataframe-examples/testdata/rdd-examples-data/user_info.csv', 'file:/home/viveksingh/wrkspace/pyspark-sidharth/pyspark-rdd-dataframe-examples/testdata/rdd-examples-data/employee_2_20180123.json', 'file:/home/viveksingh/wrkspace/pyspark-sidharth/pyspark-rdd-dataframe-examples/testdata/rdd-examples-data/demographic_fixedlength.txt', 'file:/home/viveksingh/wrkspace/pyspark-sidharth/pyspark-rdd-dataframe-examples/testdata/rdd-examples-data/demographic_2.csv', 'file:/home/viveksingh/wrkspace/pyspark-sidharth/pyspark-rdd-dataframe-examples/testdata/rdd-examples-data/registration.csv', 'file:/home/viveksingh/wrkspace/pyspark-sidharth/pyspark-rdd-dataframe-examples/testdata/rdd-examples-data/listing.csv']
#
#
# #