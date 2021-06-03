"""
This program demonstrates the of select functions.
"""


from pyspark.sql import SparkSession


if __name__ == '__main__':
    sparkSession = SparkSession \
        .builder \
        .appName('dataframe-select-functions') \
        .getOrCreate()

    sparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel('ERROR')

    print('\n************* Dataframe select functions\n')
    time_df = sparkSession.createDataFrame([{'name': 'Vivek', 'age': 30}])

    time_df.printSchema()
    time_df.show(5)
