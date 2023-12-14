"""
    Requirement
    -----------------
        > This program demonstrates how to read and write json files in aws s3.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
import os
import yaml


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('POC - Read-write json in aws s3') \
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

    # Read json file
    companies_df = spark.read \
        .json('s3a://' + app_conf['s3_conf']['s3_bucket'] + '/company.json')

    print('*********************** Print schema and data of companies.json')
    companies_df.printSchema()
    companies_df.show(100, truncate=False)

    # Start: Selecting column from a DataFrame
    # Print company names col from data frame
    print('********************** Print company column data')
    print('********************** select(col(\'company\')).show()')
    companies_df.select(col('company')).show()

    # using str column name
    print('********************** select(\'company\').show()')
    companies_df.select('company').show()

    print('********************** select(companies_df[\'company\'])')
    companies_df.select(companies_df['company']).show()

    print('********************** select(companies_df.company).show()')
    companies_df.select(companies_df.company).show()

    # End: Selecting column from a DataFrame

    # Start : Flatten records using explode(..)
    flattened_companies_df = companies_df \
        .select(
            col('company'),
            explode(col('employees')).alias('employee'))

    print('*********************** explode(col(\'employees\')).alias(\'employee\')')
    flattened_companies_df.printSchema()
    flattened_companies_df.show(100, truncate=False)
    # End : Flatten records using explode(..)

    # Create new column for firstName and LastName
    flattened_companies_df = flattened_companies_df.select(
        col('company'),
        col('employee'),
        col('employee.firstName').alias('firstName'),
        col('employee.lastName').alias('lastName')
    )

    print('*********************** col(\'employee.firstName\').alias(\'firstName\')')
    flattened_companies_df.printSchema()
    flattened_companies_df.show(100, truncate=False)

    # Start : Write DF to s3 in json
    flattened_companies_df.repartition(3)\
        .write \
        .partitionBy('company') \
        .mode('overwrite') \
        .json('s3a://' + app_conf['s3_conf']['s3_write_bucket'] + '/comapny/partitionby_company/')
    # End : Write DF to s3 in json
#
# command
# spark-submit --packages 'org.apache.hadoop:hadoop-aws:2.7.4' --master yarn ./program.py
#
# Execution environment : DataBrick park server
#
# *********************** Print schema and data of companies.json
# root
#  |-- company: string (nullable = true)
#  |-- employees: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- firstName: string (nullable = true)
#  |    |    |-- lastName: string (nullable = true)
#
# +--------+-------------------------------------+
# |company |employees                            |
# +--------+-------------------------------------+
# |NewCo   |[{Sidhartha, Ray}, {Pratik, Solanki}]|
# |FamilyCo|[{Jiten, Gupta}, {Pallavi, Gupta}]   |
# |OldCo   |[{Vivek, Garg}, {Nitin, Gupta}]      |
# |ClosedCo|[]                                   |
# +--------+-------------------------------------+
#
# ********************** Print column data
# ********************** select(col('company')).show()
# +--------+
# | company|
# +--------+
# |   NewCo|
# |FamilyCo|
# |   OldCo|
# |ClosedCo|
# +--------+
#
# ********************** select('company').show()
# +--------+
# | company|
# +--------+
# |   NewCo|
# |FamilyCo|
# |   OldCo|
# |ClosedCo|
# +--------+
#
# ********************** select(companies_df['company'])
# +--------+
# | company|
# +--------+
# |   NewCo|
# |FamilyCo|
# |   OldCo|
# |ClosedCo|
# +--------+
#
# ********************** select(companies_df.company).show()
# +--------+
# |company |
# +--------+
# |NewCo   |
# |FamilyCo|
# |OldCo   |
# |ClosedCo|
# +--------+
#
# *********************** explode(col('employees')).alias('employee')
# root
#  |-- company: string (nullable = true)
#  |-- employee: struct (nullable = true)
#  |    |-- firstName: string (nullable = true)
#  |    |-- lastName: string (nullable = true)
#
# +--------+-----------------+
# |company |employee         |
# +--------+-----------------+
# |NewCo   |{Sidhartha, Ray} |
# |NewCo   |{Pratik, Solanki}|
# |FamilyCo|{Jiten, Gupta}   |
# |FamilyCo|{Pallavi, Gupta} |
# |OldCo   |{Vivek, Garg}    |
# |OldCo   |{Nitin, Gupta}   |
# +--------+-----------------+
#
# *********************** col('employee.firstName').alias('firstName')
# root
#  |-- company: string (nullable = true)
#  |-- employee: struct (nullable = true)
#  |    |-- firstName: string (nullable = true)
#  |    |-- lastName: string (nullable = true)
#  |-- firstName: string (nullable = true)
#  |-- lastName: string (nullable = true)
#
# +--------+-----------------+---------+--------+
# |company |employee         |firstName|lastName|
# +--------+-----------------+---------+--------+
# |NewCo   |{Sidhartha, Ray} |Sidhartha|Ray     |
# |NewCo   |{Pratik, Solanki}|Pratik   |Solanki |
# |FamilyCo|{Jiten, Gupta}   |Jiten    |Gupta   |
# |FamilyCo|{Pallavi, Gupta} |Pallavi  |Gupta   |
# |OldCo   |{Vivek, Garg}    |Vivek    |Garg    |
# |OldCo   |{Nitin, Gupta}   |Nitin    |Gupta   |
# +--------+-----------------+---------+--------+
#

