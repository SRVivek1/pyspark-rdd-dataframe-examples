"""
    Read CSV file and do basic data gathering.
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

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('load csv') \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    rdd = sc.textFile(testdata.CRED_TXN_CSV)
    print(rdd.take(20))

    def tuple_mapping(rec: str) -> (str, float, str, str):
        arr = rec.split('~')
        return arr[0], float(arr[1]), arr[2], arr[3]

    rdd = rdd \
        .filter(lambda rec: 'AccNum~Amount~Date~Category' not in rec) \
        .map(tuple_mapping)

    # cache the transformed rdd
    rdd.cache()

    res_rdd = rdd \
        .map(lambda rec: (rec[0], rec[1])) \
        .reduceByKey(lambda a, b: a + b)

    print(res_rdd.take(20))

    # total transaction amount
    total_amount = rdd.map(lambda rec: rec[1]).reduce(lambda a,b: a + b)
    print(f'Total amt : {total_amount}')
#
# Command
# --------------------
# spark-submit --master 'local[*]' load_csv.py
#
# Output
# --------------------
# ['AccNum~Amount~Date~Category', '123-ABC-789~1.23~01/01/2015~Drug Store', '456-DEF-456~200.0~01/03/2015~Electronics', '333-XYZ-999~106.0~01/04/2015~Gas', '123-ABC-789~2.36~01/09/2015~Grocery Store', '456-DEF-456~23.16~01/11/2015~Unknown', '123-ABC-789~42.12~01/12/2015~Park', '456-DEF-456~20.0~01/12/2015~Electronics', '333-XYZ-999~52.13~01/17/2015~Gas', '333-XYZ-999~41.67~01/19/2015~Some Totally Fake Long Description', '333-XYZ-999~56.37~01/21/2015~Gas', '987-CBA-321~63.84~01/23/2015~Grocery Store', '123-ABC-789~160.91~01/24/2015~Electronics', '456-DEF-456~78.77~01/24/2015~Grocery Store', '333-XYZ-999~86.24~01/29/2015~Movies', '456-DEF-456~93.71~01/31/2015~Grocery Store', '987-CBA-321~2.29~01/31/2015~Drug Store', '456-DEF-456~108.64~01/31/2015~Park', '456-DEF-456~116.11~01/31/2015~Books', '123-ABC-789~27.19~02/10/2015~Grocery Store']
# [('333-XYZ-999', 1249.18), ('987-CBA-321', 871.91), ('123-ABC-789', 5081.700000000001), ('456-DEF-456', 1466.52)]
# Total amt : 8669.31
#
# #
