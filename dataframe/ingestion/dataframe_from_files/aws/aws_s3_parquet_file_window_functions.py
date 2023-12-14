"""
    Requirement
    -------------
        --> This program demonstrates window function usages with DataFrame created using parquet files.
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size
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

    # TODO - write dataframe window functions
