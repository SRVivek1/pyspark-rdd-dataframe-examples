"""
    Requirement
    ---------------
        Write a spark program to read csv file from amazon s3 as DataFrame.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, LongType, BooleanType, DoubleType
import os.path
import yaml


if __name__ == '__main__':

    spark = SparkSession.builder \
        .appName('DataFrame POC - reac csv file') \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    # Read configuration
    # Note:
    # --> Update file path location before execution
    cur_dir = os.path.dirname(__file__)
    app_conf = yaml.load(open(os.path.abspath(cur_dir + '../../../../../' + 'application.yml')), Loader=yaml.FullLoader)
    secrets = yaml.load(open(os.path.abspath(cur_dir + '../../../../../../' + '.secrets')), Loader=yaml.FullLoader)

    # AWS authentication
    hdp_conf = sc._jsc.hadoopConfiguration()
    hdp_conf.set('fs.s3a.access.key', secrets['s3_conf']['access_key'])
    hdp_conf.set('fs.s3a.secret.key', secrets['s3_conf']['secret_access_key'])

    # Create schema for finances_3.csv
    # [id,has_debt,has_financial_dependents,has_student_loans,income]
    finances_schema = StructType() \
        .add('id', LongType(), True) \
        .add('has_debt', BooleanType(), True) \
        .add('has_financial_dependents', BooleanType(), True) \
        .add('has_student_loans', BooleanType(), True) \
        .add('income', DoubleType(), True)

    # Create DataFrame
    # load file from AWS S3
    # load(..) API
    finances_df = spark.read \
        .option('header', True) \
        .option('delimeter', ',') \
        .format('csv') \
        .schema(finances_schema) \
        .load('s3a://' + app_conf['s3_conf']['s3_bucket'] + '/finances_3.csv')

    print('****************** DataFrame from Finances_3.csv')
    print('****************** DataFrame# load(..) API')
    finances_df.printSchema()
    finances_df.show()

    # Create DataFrame from csv
    # csv(...) API
    finances_df_1 = spark.read \
        .option('header', True) \
        .option('delimeter', ',') \
        .schema(finances_schema) \
        .csv(path='s3a://' + app_conf['s3_conf']['s3_bucket'] + '/finances_3.csv')

    print('****************** DataFrame from Finances_3.csv')
    print('****************** DataFrame# csv(..) API')
    finances_df_1.printSchema()
    finances_df_1.show()

    # Create DataFrame from csv
    # csv(...) API
    finances_df_2 = spark.read \
        .csv(
        path='s3a://' + app_conf['s3_conf']['s3_bucket'] + '/finances_3.csv',
        schema=finances_schema,
        header=True,
        sep=','
    )
    print('****************** DataFrame from Finances_3.csv')
    print('****************** DataFrame# csv(..) API')
    finances_df_2.printSchema()
    finances_df_2.show()

    # Rename column names
    print('******************* DataFrame with new column names')
    print('******************* toDF() API.')
    finances_df_2 = finances_df_2 \
        .toDF('id', 'has_debt_', 'has_financial_dependents_', 'has_student_loan_', 'income_')

    finances_df_2.printSchema()
    finances_df_2.show()

# Command
# spark-submit --packages 'org.apache.hadoop:hadoop-aws:2.7.4' --master yarn ./program.py
#
