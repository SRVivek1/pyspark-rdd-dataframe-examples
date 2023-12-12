"""
    Problem Statement
    -------------------
        > Find students from Switzerland having loan (debt) and have dependents.

"""

from pyspark.sql import SparkSession
from distutils.util import strtobool
import os
import yaml


if __name__ == '__main__':

    # Pyspark configuration
    os.environ['PYSPARK_SUBMIT_ARGS'] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Create session
    spark = SparkSession.builder \
        .appName('Spark AWS Program') \
        .master('local[*]') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    # read configuration files
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + '../../../../' + 'application.yml')
    app_secrets_path = os.path.abspath(current_dir + '../../../../../' + '.secrets')

    # Open config files and read them using yaml API.
    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)

    secrets = open(app_secrets_path)
    secrets_conf = yaml.load(secrets, Loader=yaml.FullLoader)

    # configure access keys to connect to s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set('fs.s3a.access.key', secrets_conf['s3_conf']['access_key'])
    hadoop_conf.set('fs.s3a.secret.key', secrets_conf['s3_conf']['secret_access_key'])

    # read the files from s3 bucket
    demographics_rdd = spark.sparkContext.textFile('s3a://' + app_conf['s3_conf']['s3_bucket'] + '/demographic.csv')
    finances_rdd = spark.sparkContext.textFile("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/finances.csv")

    demographics_pair_rdd = demographics_rdd \
        .map(lambda line: line.split(",")) \
        .map(lambda lst: (
    int(lst[0]), (int(lst[1]), strtobool(lst[2]), lst[3], lst[4], strtobool(lst[5]), strtobool(lst[6]), int(lst[7]))))

    finances_pair_rdd = finances_rdd \
        .map(lambda line: line.split(",")) \
        .map(lambda lst: (int(lst[0]), (strtobool(lst[1]), strtobool(lst[2]), strtobool(lst[3]), int(lst[4]))))

    print('Participants belongs to \'Switzerland\', having debts and financial dependents,')
    join_pair_rdd = demographics_pair_rdd\
        .join(finances_pair_rdd)\
        .filter(lambda rec: (rec[1][0][2] == "Switzerland") and (rec[1][1][0] == 1) and (rec[1][1][1] == 1))

    join_pair_rdd.foreach(print)

# Command to submit this application
#
# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" rdd_problems/scholarship/aws/scholarship_recipient_join_filter.py