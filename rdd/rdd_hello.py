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

    # Load application config and secrets
    # current_dir = os.path.abspath(os.path.dirname(__file__))
    # app_conf_path = os.path.abspath(current_dir + '/../' + 'application.yml')
    # app_secrets_path = os.path.abspath(current_dir + '/../' + '.secrets')

    # config = open(app_conf_path)
    # app_conf = yaml.load(config, Loader=yaml.FullLoader)
    # secret = open(app_secrets_path)

    # read spark context
    sContext = session.sparkContext

    # Create an RDD from python collection
    rdd = sContext.parallelize(range(1, 1000, 5))

    print('RDD instance : {0}'.format(rdd))
    print('RDD partitions : {0}'.format(rdd.getNumPartitions()))

    print('First 15 records : \n{0}'.format(rdd.take(15)))
