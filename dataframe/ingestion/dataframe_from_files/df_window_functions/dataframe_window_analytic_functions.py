"""
    Requirement
    ----------------
        This is a POC application to demonstrate use of DataFrame Window analytic function.

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, rank, dense_rank, percent_rank, ntile
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql.window import Window, WindowSpec
import os
import yaml

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('POC - AWS S3 read write parquet files') \
        .config('spark.sql.legacy.parquet.int96RebaseModeInRead', 'CORRECTED') \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    # READ Config
    cur_dir = os.path.abspath(os.path.abspath(__file__))
    app_conf = yaml.load(open(cur_dir + '../../../../../' + 'application.yml'), Loader=yaml.FullLoader)
    secrets = yaml.load(open(cur_dir + '../../../../../../' + '.secrets'), Loader=yaml.FullLoader)

    # AWS authentication
    hdp_cnf = sc._jsc.hadoopConfiguration()
    hdp_cnf.set('fs.s3a.access.key', secrets['s3_conf']['access_key'])
    hdp_cnf.set('fs.s3a.secret.key', secrets['s3_conf']['secret_access_key'])

    # Note -this DF object is not used as it has complex dataset
    # Read parquet files
    nyc_omo_df = spark.read \
        .parquet('s3a://' + app_conf['s3_conf']['s3_bucket'] + '/NYC_OMO/')

    # Using employee data from collection
    employees = [("James", "Sales", 3000),
           ("Michael", "Sales", 4600),
           ("Robert", "Sales", 4100),
           ("Maria", "Finance", 3000),
           ("James", "Sales", 3000),
           ("Scott", "Finance", 3300),
           ("Jen", "Finance", 3900),
           ("Jeff", "Marketing", 3000),
           ("Kumar", "Marketing", 2000),
           ("Saif", "Sales", 4100)]

    employees_rdd = sc.parallelize(employees) \
        .map(lambda rec: (rec[0], rec[1], float(rec[2])))

    for rec in employees_rdd.take(10):
        print(rec)

    # Schema
    employees_schema = StructType() \
        .add('name', StringType(), False) \
        .add('department', StringType(), True) \
        .add('salary', DoubleType(), True)

    print('****************** employees_df cretaed')
    employee_df = spark.createDataFrame(employees_rdd, schema=employees_schema)
    employee_df.printSchema()
    employee_df.show(10)

    # TODO Window analytic functions
    
#
# command
# ------------
# spark-submit --packages 'org.apache.hadoop:hadoop-aws:2.7.4' --master yarn ./program.py
#
# Execution cluster - DataBricks Community
#
#
#
