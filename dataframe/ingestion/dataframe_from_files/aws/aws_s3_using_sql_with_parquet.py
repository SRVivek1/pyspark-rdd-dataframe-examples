"""
    Requirement
    ---------------
        >> Using SQL queries directly to load parquet files.
"""

from pyspark.sql import SparkSession
import os
import yaml

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('POC - Using SQL queries to load parquet files') \
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

    # parquet files location
    parquet_loc = 's3a://' + app_conf['s3_conf']['s3_bucket'] + '/finances-small/'
    print('**************** Parquet location : {}'.format(parquet_loc))

    # Prepare a select query
    sql_query = f'select * from parquet.`{parquet_loc}`'
    print(f'***************** SQL Query to load parquet data : \n {sql_query}')

    # Run SQL query to load data
    finances_small_df = spark.sql(sql_query)
    finances_small_df.printSchema()
    finances_small_df.show()

#
# Command
# ---------------
# spark-submit --packages 'org.apache.org.hadoop-aws:2.7.4' --master yarn ./program.py
#
# Execution Env: DataBricks CE cluster
#
# **************** Parquet location : s3a://vsingh-spark-test-data/finances-small/
# ***************** SQL Query to load parquet data :
#  select * from parquet.`s3a://vsingh-spark-test-data/finances-small/`
# root
#  |-- AccountNumber: string (nullable = true)
#  |-- Amount: double (nullable = true)
#  |-- Date: string (nullable = true)
#  |-- Description: string (nullable = true)
#
# +-------------+------+---------+--------------------+
# |AccountNumber|Amount|     Date|         Description|
# +-------------+------+---------+--------------------+
# |  123-ABC-789|  1.23| 1/1/2015|          Drug Store|
# |  456-DEF-456| 200.0| 1/3/2015|         Electronics|
# |  333-XYZ-999| 106.0| 1/4/2015|                 Gas|
# |  123-ABC-789|  2.36| 1/9/2015|       Grocery Store|
# |  456-DEF-456| 23.16|1/11/2015|             Unknown|
# |  123-ABC-789| 42.12|1/12/2015|                Park|
# |  456-DEF-456|  20.0|1/12/2015|         Electronics|
# |  333-XYZ-999| 52.13|1/17/2015|                 Gas|
# |  333-XYZ-999| 41.67|1/19/2015|Some Totally Fake...|
# |  333-XYZ-999| 56.37|1/21/2015|                 Gas|
# |  987-CBA-321| 63.84|1/23/2015|       Grocery Store|
# |  123-ABC-789|160.91|1/24/2015|         Electronics|
# |  456-DEF-456| 78.77|1/24/2015|       Grocery Store|
# |  333-XYZ-999| 86.24|1/29/2015|              Movies|
# |  456-DEF-456| 93.71|1/31/2015|       Grocery Store|
# |  987-CBA-321|  2.29|1/31/2015|          Drug Store|
# |  456-DEF-456|108.64|1/31/2015|                Park|
# |  456-DEF-456|116.11|1/31/2015|               Books|
# |  123-ABC-789| 27.19|2/10/2015|       Grocery Store|
# |  333-XYZ-999|131.04|2/11/2015|         Electronics|
# +-------------+------+---------+--------------------+
#
#
