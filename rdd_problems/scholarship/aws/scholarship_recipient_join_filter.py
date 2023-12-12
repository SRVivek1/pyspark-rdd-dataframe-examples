"""
    Problem Statement
    -------------------
        > Find students from Switzerland having loan (debt) and have dependents.

"""

from pyspark.sql import SparkSession
import os
import yaml

if __name__ == '__main__':

    # Pyspark configuration
    os.environ['PYSPARK_SUBMIT_ARGS'] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Create session
    session = SparkSession.builder \
        .appName('Spark AWS Program') \
        .master('local[*]') \
        .getOrCreate()

    #sc = session.sparkContext
    session.sparkContext.setLogLevel('ERROR')

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
    hadoop_conf = session.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set('fs.s3a.access.key', secrets_conf['s3_conf']['access_key'])
    hadoop_conf.set('fs.s3a.secret.key', secrets_conf['s3_conf']['secret_access_key'])

    # verify below conf.
    #hadoop_conf.set('fs.s3a.endpoint.region', 's3.us-east-2.amazonaws.com')
    #hadoop_conf.set('s3a.endpoint.region', 's3.us-east-2.amazonaws.com')

    # read the files from s3 bucket
    #rdd = session.sparkContext.textFile('s3a://' + app_conf['s3_conf']['s3_bucket'] + '/demographic.csv')
    rdd = session.sparkContext.textFile('s3a://vsingh-dev.spark.test.data/demographic.csv')
    rdd.take(5)

    #pair_rdd = rdd.map(lambda rec : (int(rec[0]), (rec[1]), rec[2], rec[3], rec[4], rec[5], rec[6], rec[7]))
    # pair_rdd = pair_rdd.filter(lambda rec: rec[1][2] == 'Switzerland')

    #pair_rdd.foreach(print)


# Command to submit this application
# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" rdd_problems/scholarship/aws/scholaship_recipient_join_filter.py
