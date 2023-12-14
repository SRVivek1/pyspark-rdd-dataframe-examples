"""
    Requirement
    ---------------
        -> This program demonstrates how to write data to AWS s3.
        -> Partitioning data based on column before writing
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, LongType, BooleanType, DoubleType
import os
import yaml

if __name__ == '__main__':

    spark = SparkSession.builder \
        .appName('DataFrame POC - write csv to s3') \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    # Read configuration
    # Note:
    # --> Validate/Fix file location before running the app.
    pwd = os.path.abspath(os.path.dirname(__file__))
    app_conf = yaml.load(open(pwd + '../../../../../' + '/application.yml'), Loader=yaml.FullLoader)
    secrets = yaml.load(open(os.path.abspath(pwd + '../../../../../../' + '.secrets')), Loader=yaml.FullLoader)

    # Update AWS auth
    hdp_conf = sc._jsc.hadoopConfiguration()
    hdp_conf.set('fs.s3a.access.key', secrets['s3_conf']['access_key'])
    hdp_conf.set('fs.s3a.secret.key', secrets['s3_conf']['secret_access_key'])

    # finances schema
    finances_schema = StructType() \
        .add('id', LongType(), True) \
        .add('has_debt', BooleanType(), True) \
        .add('has_financial_dependents', BooleanType(), True) \
        .add('has_student_loan', BooleanType(), True) \
        .add('income', DoubleType(), True)

    # read csv
    finances_df = spark.read \
        .option('header', True) \
        .option('delimeter', ',') \
        .schema(finances_schema) \
        .csv('s3a://' + app_conf['s3_conf']['s3_bucket'] + '/finances_3.csv')

    # Add a new column
    finances_df = finances_df \
        .withColumn('loan_amount', finances_df['income'] * 5)

    # print data
    print('********************* finances_3.csv with new column loan_amount')
    finances_df.printSchema()
    finances_df.show()

    # Repartition data based on - has_student_loan
    # and then store them in AWS s3
    write_dir = 's3a://' + app_conf['s3_conf']['s3_write_bucket'] + '/finances_3/has_student_loan/'
    finances_df \
        .repartition(2) \
        .write \
        .partitionBy('has_student_loan') \
        .mode('overwrite') \
        .option('header', True) \
        .option('delimeter', ',') \
        .csv(write_dir)

    print(f'*************** Data written to AWS s3 : {write_dir}')


#
# Command
#
# spark-submit --packages 'org.apache.hadoop:hadoop-aws:2.7.4' --master yarn ./program.py
#
#
