"""
Hello World RDD program.
"""


from pyspark.sql import SparkSession
import os.path


# Create or load existing session object.
def create_spark_session() -> SparkSession:
    return SparkSession\
        .builder\
        .appName('HelloWorldSpark')\
        .master('local[*]')\
        .getOrCreate()


if __name__ == '__main__':

    # update OS path -- Not needed non-aws apps
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--package "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Get/Load Spark session
    session = create_spark_session()

    # Set log level to ERROR
    session.sparkContext.setLogLevel('ERROR')

    # read spark context
    sparkContext = session.sparkContext

    # Create an RDD from python collection
    rdd = sparkContext.parallelize(range(1, 1000, 4))

    print('RDD instance : {0}'.format(rdd))
    print('RDD partitions : {0}'.format(rdd.getNumPartitions()))

    print('First 15 records : \n{0}'.format(rdd.take(15)))

    rdd_part_8 = rdd.repartition(8)
    print('RDD new partitions : {0}'.format(rdd_part_8.getNumPartitions()))

    print('First 15 records : \n{0}'.format(rdd_part_8.take(15)))

# Spark Submit command to run the application
# Command-1 :
#   spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" rdd/rdd_hello.py
# Command-2 :
#   spark-submit rdd/rdd_hello.py
