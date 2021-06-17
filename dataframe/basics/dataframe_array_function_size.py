"""
This program demonstrates the array size(...) function.
"""


from pyspark.sql import SparkSession


if __name__ == '__main__':
    """
    Driver program.
    """

    sparkSession = SparkSession\
        .builder\
        .appName('demo-array-fun-size')\
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('ERROR')

    df = sparkSession.createDataFrame([([1, 2, 3],), ([1],), ([],)], ['data'])
