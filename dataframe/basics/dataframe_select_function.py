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
    time_df = sparkSession \
        .createDataFrame([
            {'name': 'Vivek', 'age': 30},
            {'name': 'Ravi', 'age': 31},
            {'name': 'Rohit', 'age': 20}])

    time_df.printSchema()
    time_df.show(5)

    # Select all records
    print("*************** time_df.select('*')\n")
    print(time_df.select('*').collect())

    print("*************** time_df.select('name', 'age')\n")
    print(time_df.select('name', 'age').collect())

    print("*************** time_df.select('name')\n")
    print(time_df.select('name').collect())

    print("*************** time_df.select(time_df.name, (time_df.age + 10).alias('age+10'))\n")
    print(time_df.select(time_df.name, (time_df.age + 10).alias('age+10')).collect())

