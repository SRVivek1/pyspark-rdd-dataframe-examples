"""
    Requirement
    ----------------
        This is a POC application to demonstrate use of DataFrame Window analytic function.

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import cume_dist, lag, lead
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

    # Window Analytic functions

    # WindowSpec
    window_spec = Window.partitionBy('department').orderBy('salary')

    # cume_dist & lag
    tempdf = employee_df \
        .withColumn('cume_dist', cume_dist().over(window_spec)) \
        .withColumn('lag', lag(col='salary', offset=2).over(window_spec))
    tempdf.printSchema()
    tempdf.show()

    # cume_dist & lead
    tempdf = employee_df \
        .withColumn('cume_dist', cume_dist().over(window_spec)) \
        .withColumn('lead', lead('salary', 2).over(window_spec))
    tempdf.printSchema()
    tempdf.show()

    # cume_dist, lag & lead
    tempdf = employee_df \
        .withColumn('cume_dist', cume_dist().over(window_spec)) \
        .withColumn('lag', lag(col='salary', offset=2).over(window_spec)) \
        .withColumn('lead', lead('salary', 2).over(window_spec))
    tempdf.printSchema()
    tempdf.show()

#
# command
# ------------
# spark-submit --packages 'org.apache.hadoop:hadoop-aws:2.7.4' --master yarn ./program.py
#
# Execution cluster - DataBricks Community
#
# ('James', 'Sales', 3000.0)
# ('Michael', 'Sales', 4600.0)
# ('Robert', 'Sales', 4100.0)
# ('Maria', 'Finance', 3000.0)
# ('James', 'Sales', 3000.0)
# ('Scott', 'Finance', 3300.0)
# ('Jen', 'Finance', 3900.0)
# ('Jeff', 'Marketing', 3000.0)
# ('Kumar', 'Marketing', 2000.0)
# ('Saif', 'Sales', 4100.0)
# ****************** employees_df cretaed
# root
#  |-- name: string (nullable = false)
#  |-- department: string (nullable = true)
#  |-- salary: double (nullable = true)
#
# +-------+----------+------+
# |   name|department|salary|
# +-------+----------+------+
# |  James|     Sales|3000.0|
# |Michael|     Sales|4600.0|
# | Robert|     Sales|4100.0|
# |  Maria|   Finance|3000.0|
# |  James|     Sales|3000.0|
# |  Scott|   Finance|3300.0|
# |    Jen|   Finance|3900.0|
# |   Jeff| Marketing|3000.0|
# |  Kumar| Marketing|2000.0|
# |   Saif|     Sales|4100.0|
# +-------+----------+------+
#
# root
#  |-- name: string (nullable = false)
#  |-- department: string (nullable = true)
#  |-- salary: double (nullable = true)
#  |-- cume_dist: double (nullable = false)
#  |-- lag: double (nullable = true)
#
# +-------+----------+------+------------------+------+
# |   name|department|salary|         cume_dist|   lag|
# +-------+----------+------+------------------+------+
# |  Maria|   Finance|3000.0|0.3333333333333333|  null|
# |  Scott|   Finance|3300.0|0.6666666666666666|  null|
# |    Jen|   Finance|3900.0|               1.0|3000.0|
# |  Kumar| Marketing|2000.0|               0.5|  null|
# |   Jeff| Marketing|3000.0|               1.0|  null|
# |  James|     Sales|3000.0|               0.4|  null|
# |  James|     Sales|3000.0|               0.4|  null|
# | Robert|     Sales|4100.0|               0.8|3000.0|
# |   Saif|     Sales|4100.0|               0.8|3000.0|
# |Michael|     Sales|4600.0|               1.0|4100.0|
# +-------+----------+------+------------------+------+
#
# root
#  |-- name: string (nullable = false)
#  |-- department: string (nullable = true)
#  |-- salary: double (nullable = true)
#  |-- cume_dist: double (nullable = false)
#  |-- lead: double (nullable = true)
#
# +-------+----------+------+------------------+------+
# |   name|department|salary|         cume_dist|  lead|
# +-------+----------+------+------------------+------+
# |  Maria|   Finance|3000.0|0.3333333333333333|3900.0|
# |  Scott|   Finance|3300.0|0.6666666666666666|  null|
# |    Jen|   Finance|3900.0|               1.0|  null|
# |  Kumar| Marketing|2000.0|               0.5|  null|
# |   Jeff| Marketing|3000.0|               1.0|  null|
# |  James|     Sales|3000.0|               0.4|4100.0|
# |  James|     Sales|3000.0|               0.4|4100.0|
# | Robert|     Sales|4100.0|               0.8|4600.0|
# |   Saif|     Sales|4100.0|               0.8|  null|
# |Michael|     Sales|4600.0|               1.0|  null|
# +-------+----------+------+------------------+------+
#
# root
#  |-- name: string (nullable = false)
#  |-- department: string (nullable = true)
#  |-- salary: double (nullable = true)
#  |-- cume_dist: double (nullable = false)
#  |-- lag: double (nullable = true)
#  |-- lead: double (nullable = true)
#
# +-------+----------+------+------------------+------+------+
# |   name|department|salary|         cume_dist|   lag|  lead|
# +-------+----------+------+------------------+------+------+
# |  Maria|   Finance|3000.0|0.3333333333333333|  null|3900.0|
# |  Scott|   Finance|3300.0|0.6666666666666666|  null|  null|
# |    Jen|   Finance|3900.0|               1.0|3000.0|  null|
# |  Kumar| Marketing|2000.0|               0.5|  null|  null|
# |   Jeff| Marketing|3000.0|               1.0|  null|  null|
# |  James|     Sales|3000.0|               0.4|  null|4100.0|
# |  James|     Sales|3000.0|               0.4|  null|4100.0|
# | Robert|     Sales|4100.0|               0.8|3000.0|4600.0|
# |   Saif|     Sales|4100.0|               0.8|3000.0|  null|
# |Michael|     Sales|4600.0|               1.0|4100.0|  null|
# +-------+----------+------+------------------+------+------+
#
