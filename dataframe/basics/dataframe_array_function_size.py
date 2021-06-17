"""
This program demonstrates the array size(...) function.
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == '__main__':
    """
    Driver program.
    """

    sparkSession = SparkSession\
        .builder\
        .appName('demo-array-fun-size')\
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('ERROR')

    # DataFrame with 3 elements
    df = sparkSession.createDataFrame([([1, 2, 3],), ([1],), ([],)], ['data'])

    print('\n***************** DataFrame data :')
    df.collect()

    # Find size of elements in each record
    print('\n***************** df.select(size(df.data)).collect()')
    df.select(size(df.data)).collect()
