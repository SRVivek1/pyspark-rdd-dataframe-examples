"""
    Requirement
    ---------------
        This program demonstrates the use of Spark Window aggregate functions.
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, avg, sum, min, max, col
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

    # Window Aggregate functions

    # WindowSpec
    window_spec = Window.partitionBy('department').orderBy('salary')
    window_spec_agg = Window.partitionBy('department')

    # Aggregate functions
    tempdf = employee_df \
        .withColumn('row_number', row_number().over(window_spec)) \
        .withColumn('avg', avg('salary').over(window_spec_agg)) \
        .withColumn('sum', sum('salary').over(window_spec_agg)) \
        .withColumn('min', min('salary').over(window_spec_agg)) \
        .withColumn('max', max('salary').over(window_spec_agg))

    tempdf.printSchema()
    tempdf.show()

    # where condition API
    print('************* .where(tempdf.row_number == 1)')
    tempdf \
        .where(tempdf.row_number == 1) \
        .select("row_number", "department", "avg", "sum", "min", "max") \
        .show()

    print('*************** .where(col(\'avg\') >= 3000')
    tempdf \
        .where(col('avg') >= 3000) \
        .select("row_number", "department", "avg", "sum", "min", "max") \
        .show()
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
#  |-- row_number: integer (nullable = false)
#  |-- avg: double (nullable = true)
#  |-- sum: double (nullable = true)
#  |-- min: double (nullable = true)
#  |-- max: double (nullable = true)
#
# +-------+----------+------+----------+------+-------+------+------+
# |   name|department|salary|row_number|   avg|    sum|   min|   max|
# +-------+----------+------+----------+------+-------+------+------+
# |  Maria|   Finance|3000.0|         1|3400.0|10200.0|3000.0|3900.0|
# |  Scott|   Finance|3300.0|         2|3400.0|10200.0|3000.0|3900.0|
# |    Jen|   Finance|3900.0|         3|3400.0|10200.0|3000.0|3900.0|
# |  Kumar| Marketing|2000.0|         1|2500.0| 5000.0|2000.0|3000.0|
# |   Jeff| Marketing|3000.0|         2|2500.0| 5000.0|2000.0|3000.0|
# |  James|     Sales|3000.0|         1|3760.0|18800.0|3000.0|4600.0|
# |  James|     Sales|3000.0|         2|3760.0|18800.0|3000.0|4600.0|
# | Robert|     Sales|4100.0|         3|3760.0|18800.0|3000.0|4600.0|
# |   Saif|     Sales|4100.0|         4|3760.0|18800.0|3000.0|4600.0|
# |Michael|     Sales|4600.0|         5|3760.0|18800.0|3000.0|4600.0|
# +-------+----------+------+----------+------+-------+------+------+
#
# ************* .where(tempdf.row_number == 1)
# +----------+----------+------+-------+------+------+
# |row_number|department|   avg|    sum|   min|   max|
# +----------+----------+------+-------+------+------+
# |         1|   Finance|3400.0|10200.0|3000.0|3900.0|
# |         1| Marketing|2500.0| 5000.0|2000.0|3000.0|
# |         1|     Sales|3760.0|18800.0|3000.0|4600.0|
# +----------+----------+------+-------+------+------+
#
# *************** .where(col(\'avg\') >= 3000
# +----------+----------+------+-------+------+------+
# |row_number|department|   avg|    sum|   min|   max|
# +----------+----------+------+-------+------+------+
# |         1|   Finance|3400.0|10200.0|3000.0|3900.0|
# |         2|   Finance|3400.0|10200.0|3000.0|3900.0|
# |         3|   Finance|3400.0|10200.0|3000.0|3900.0|
# |         1|     Sales|3760.0|18800.0|3000.0|4600.0|
# |         2|     Sales|3760.0|18800.0|3000.0|4600.0|
# |         3|     Sales|3760.0|18800.0|3000.0|4600.0|
# |         4|     Sales|3760.0|18800.0|3000.0|4600.0|
# |         5|     Sales|3760.0|18800.0|3000.0|4600.0|
# +----------+----------+------+-------+------+------+
#
