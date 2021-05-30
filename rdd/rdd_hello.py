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

    # update OS path
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--package "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Get/Load Spark session
    session = create_spark_session()

    # Set log level to ERROR
    session.sparkContext.setLogLevel('ERROR')

    # read spark context
    sContext = session.sparkContext

    # Create an RDD from python collection
    rdd = sContext.parallelize(range(1, 1000, 5))

    print('RDD instance : {0}'.format(rdd))
    print('RDD partitions : {0}'.format(rdd.getNumPartitions()))

    print('First 15 records : \n{0}'.format(rdd.take(15)))

# Spark Submit command to run the application
# Command-1 :
#   spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" rdd/rdd_hello.py
# Command-2 :
#   spark-submit rdd/rdd_hello.py
