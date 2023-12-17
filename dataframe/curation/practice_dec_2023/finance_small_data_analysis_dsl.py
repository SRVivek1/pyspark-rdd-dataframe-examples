"""
        Requirement
        -----------------
            --> Read finance-small parquet file.
            --> Demonstrate use of DSL data curation APIs
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, asc, concat_ws, avg, sum, count, min, max, collect_set, collect_list, size, sort_array, array_contains
import os
import yaml

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('Practice - Data curation') \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    # read config
    cur_dir = os.path.abspath(os.path.dirname(__file__))
    app_conf = yaml.load(open(cur_dir + '../../../../../' + 'application.yml'), Loader=yaml.FullLoader)
    secrets = yaml.load(open(cur_dir + '../../../../../../' + '.secrets'), Loader=yaml.FullLoader)

    # AWS Config
    hdp_conf = sc._jsc.hadoopConfiguration()
    hdp_conf.set('fs.s3a.access.key', secrets['s3_conf']['access_key'])
    hdp_conf.set('fs.s3a.secret.key', secrets['s3_conf']['secret_access_key'])

    # Load parquet
    finance_small_df = spark.read.parquet('s3a://' + app_conf['s3_conf']['s3_bucket'] + '/finances-small')

    finance_small_df.printSchema()
    finance_small_df.show(10)

    # Start : Sorting API
    # Sort data using orderBy(..) API
    # Ascending
    print("**************** df.orderBy('amount', ascending=True).show(10, truncate=False)")
    finance_small_df.orderBy('amount', ascending=True).show(10, truncate=False)

    # Descending
    print("**************** df.orderBy(col('amount'), ascending=False).show(10, truncate=False)")
    finance_small_df.orderBy(col('amount'), ascending=False).show(10, truncate=False)

    # using desc(..) API
    print("**************** df.orderBy(desc('amount')).show(10, truncate=False)")
    finance_small_df.orderBy(desc('amount')).show(10, truncate=False)

    # Sort on multiple columns asc(..) & desc(..) API
    print("**************** df.orderBy(asc('date'), desc('amount')).show(15)")
    finance_small_df.orderBy(asc('date'), desc('amount')).show(15)

    print("**************** df.orderBy(desc('date'), asc('amount')).show(15)")
    finance_small_df.orderBy(desc('date'), asc('amount')).show(15)

    # End : Sorting API

    # Start : Creating new columns

    # select API
    # concat_ws add columns data in new column with seperation
    print("***************** df.select(concat_ws(' ~ ', col('AccountNumber'), col('Description'))) ")
    finance_small_df.select(concat_ws(' ~ ', col('AccountNumber'), col('Description'))).show(10, truncate=False)

    # Providing custom name to new column
    # concat_ws(..) & alias(..) APIs
    print(
        "***************** df.select(concat_ws(' ~ ', col('AccountNumber'), col('Description')).alias('AccountNumber ~ Description')) ")
    finance_small_df \
        .select(concat_ws(' ~ ', col('AccountNumber'), col('Description'))
                .alias('AccountNumber ~ Description')).show(10, truncate=False)

    # selecting other columns from DataFrame
    print(
        "**************** *********** df.select('AccountNumber', 'Amount', 'Date', 'Description', concat_ws(' ~ ', 'AccountNumber', 'Description'))... ")
    finance_small_df \
        .select('AccountNumber', 'Amount', 'Date', 'Description', concat_ws(' ~ ', 'AccountNumber', 'Description')
                .alias('AccountNumber ~ Description')) \
        .show(10, truncate=False)

    # withColumn(...) API
    finance_small_df \
        .withColumn('AccountNumber ~~ Description', concat_ws(' ~~ ', 'AccountNumber', 'Description')) \
        .show(10, False)

    # End : Creating new columns

    # Start : Aggregate functions
    agg_finance_df = finance_small_df \
        .groupBy('AccountNumber') \
        .agg(
            avg('Amount').alias('avg_trans_amount'),
            sum('Amount').alias('total_trans_amount'),
            count('Amount').alias('number_of_trans'),
            min('Amount').alias('min_amt_trans'),
            max('Amount').alias('max_amt_trans'),
            collect_set('Description').alias('unique_trans_desc'),
            collect_list('description').alias('all_trans_desc')
        )

    print("************ Aggregate functions  - including collect_set & collect_list functions")
    agg_finance_df.show(10, False)

    # End : Aggregate functions

    # Start : Array, list, set functions
    # Derive new columns based on 'unique_trans_desc' column.
    # Calculate - count of unique trans, order unique trans alphabetically & did customer went for movies.
    agg_finance_df.select('AccountNumber', 'unique_trans_desc',
                          size('unique_trans_desc').alias('count_of_unique_trans'),
                          sort_array('unique_trans_desc').alias('ordered_unique_trans'),
                          array_contains('unique_trans_desc', 'Movies').alias('went_to_movie')) \
        .show(10, False)

    # End : Array, list, set functions

#
#
#
