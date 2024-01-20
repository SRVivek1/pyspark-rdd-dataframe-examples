"""
    This program demonstrates updating default configurations.
"""


from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName('glom-collect-test') \
        .config('spark.default.parallelism', '10') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    print('************************************ App logs **********************************')

    # Default on my machine is configured as 4, which I overwrite to 10
    print(f'spark.sparkContext.defaultParallelism : {spark.sparkContext.defaultParallelism}')

    # TODO
    print(f'spark.sparkContext.defaultMinPartitions : {spark.sparkContext.defaultMinPartitions}')

#
# spark-submit --master 'local[*]' spark-defaults.py 
# Output
# -------------------
# ************************************ App logs **********************************
# spark.sparkContext.defaultParallelism : 10
# spark.sparkContext.defaultMinPartitions : 2
#
#
#
# #