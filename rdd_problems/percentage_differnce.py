"""
    Requirement
    ----------------
        >> Find percentage difference between current and previous date revenue.

        Formular - ((Today earning - previous day earning) / previous day earning) * 100

"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType

if __name__ == '__main__':

    spark = SparkSession.builder \
        .appName('POC - Find nth highest salary') \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    # prepare data set
    daily_sales = [('product-1', '1/12/2023', 100), ('product-1', '2/12/2023', 133),
                   ('product-1', '3/12/2023', 157), ('product-1', '4/12/2023', 189),
                   ('product-1', '5/12/2023', 217), ('product-1', '6/12/2023', 249),
                   ('product-2', '1/12/2023', 120), ('product-2', '2/12/2023', 163),
                   ('product-2', '3/12/2023', 181), ('product-2', '4/12/2023', 197),
                   ('product-2', '5/12/2023', 227), ('product-2', '6/12/2023', 254),
                   ]

    # create DF
    sales_df = sc.parallelize(daily_sales).toDF(['product', 'date', 'amount'])
    sales_df.printSchema()
    sales_df.orderBy('product', 'date').show()

    # Find percentage difference with previous day earning
    win_spec = Window.partitionBy('product').orderBy(col('date'))

    # Show previous day sales in a new column
    print('***************** Create a new colum having previous day sales amount')
    print("***************** .withColumn('prev_sale', lag('amount', 1, None).over(win_spec))")
    result_df = sales_df \
        .withColumn('prev_sale', lag('amount', 1, None).over(win_spec))
    result_df.show()

    # calculate percentage difference with previous day sales
    print("**************** Percent difference of sales with previous day.")
    print("**************** .withColumn('percent_diff', bround((((result_df.amount - result_df.prev_sale)/result_df.prev_sale)*100), 2))")
    result_df = result_df \
        .withColumn('percent_diff', bround((((result_df.amount - result_df.prev_sale)/result_df.prev_sale)*100), 2))
    result_df.show()

    # Support legacy date time format
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    # inline example
    # Read previous day sales amount in a column
    # parse str date to Date type
    # calculate percent difference
    # drop records having null in percent diff column
    # select specific row to display
    print("*************** Percent difference of sales with previous day.")
    print("*************** Inline transformation to get percent difference of sales")
    result_df = sales_df \
        .withColumn('prev_sale', lag('amount', 1, None).over(win_spec)) \
        .withColumn('date', unix_timestamp('date', 'dd/MM/yyyy').cast(TimestampType())) \
        .withColumn('percent_diff', bround((((sales_df.amount - col('prev_sale')) / col('prev_sale')) * 100), 2)) \
        .select('product', 'date', 'amount', 'percent_diff')

    result_df.show()

    print("*************** Drops records having any null value")
    print("*************** df.dropna(how='any', subset=['percent_diff'])")
    result_df \
        .dropna(how='any', subset=['percent_diff']) \
        .show()

#
# Output
# ----------------
# root
#  |-- product: string (nullable = true)
#  |-- date: string (nullable = true)
#  |-- amount: long (nullable = true)
#
# +---------+---------+------+
# |  product|     date|amount|
# +---------+---------+------+
# |product-1|1/12/2023|   100|
# |product-1|2/12/2023|   133|
# |product-1|3/12/2023|   157|
# |product-1|4/12/2023|   189|
# |product-1|5/12/2023|   217|
# |product-1|6/12/2023|   249|
# |product-2|1/12/2023|   120|
# |product-2|2/12/2023|   163|
# |product-2|3/12/2023|   181|
# |product-2|4/12/2023|   197|
# |product-2|5/12/2023|   227|
# |product-2|6/12/2023|   254|
# +---------+---------+------+
#
# ***************** Create a new colum having previous day sales amount
# ***************** .withColumn('prev_sale', lag('amount', 1, None).over(win_spec))
# +---------+---------+------+---------+
# |  product|     date|amount|prev_sale|
# +---------+---------+------+---------+
# |product-1|1/12/2023|   100|     null|
# |product-1|2/12/2023|   133|      100|
# |product-1|3/12/2023|   157|      133|
# |product-1|4/12/2023|   189|      157|
# |product-1|5/12/2023|   217|      189|
# |product-1|6/12/2023|   249|      217|
# |product-2|1/12/2023|   120|     null|
# |product-2|2/12/2023|   163|      120|
# |product-2|3/12/2023|   181|      163|
# |product-2|4/12/2023|   197|      181|
# |product-2|5/12/2023|   227|      197|
# |product-2|6/12/2023|   254|      227|
# +---------+---------+------+---------+
#
# **************** Percent difference of sales with previous day.
# **************** .withColumn('percent_diff', bround((((result_df.amount - result_df.prev_sale)/result_df.prev_sale)*100), 2))
# +---------+---------+------+---------+------------+
# |  product|     date|amount|prev_sale|percent_diff|
# +---------+---------+------+---------+------------+
# |product-1|1/12/2023|   100|     null|        null|
# |product-1|2/12/2023|   133|      100|        33.0|
# |product-1|3/12/2023|   157|      133|       18.05|
# |product-1|4/12/2023|   189|      157|       20.38|
# |product-1|5/12/2023|   217|      189|       14.81|
# |product-1|6/12/2023|   249|      217|       14.75|
# |product-2|1/12/2023|   120|     null|        null|
# |product-2|2/12/2023|   163|      120|       35.83|
# |product-2|3/12/2023|   181|      163|       11.04|
# |product-2|4/12/2023|   197|      181|        8.84|
# |product-2|5/12/2023|   227|      197|       15.23|
# |product-2|6/12/2023|   254|      227|       11.89|
# +---------+---------+------+---------+------------+
#
# *************** Percent difference of sales with previous day.
# *************** Inline transformation to get percent difference of sales
# +---------+-------------------+------+------------+
# |  product|               date|amount|percent_diff|
# +---------+-------------------+------+------------+
# |product-1|2023-12-01 00:00:00|   100|        null|
# |product-1|2023-12-02 00:00:00|   133|        33.0|
# |product-1|2023-12-03 00:00:00|   157|       18.05|
# |product-1|2023-12-04 00:00:00|   189|       20.38|
# |product-1|2023-12-05 00:00:00|   217|       14.81|
# |product-1|2023-12-06 00:00:00|   249|       14.75|
# |product-2|2023-12-01 00:00:00|   120|        null|
# |product-2|2023-12-02 00:00:00|   163|       35.83|
# |product-2|2023-12-03 00:00:00|   181|       11.04|
# |product-2|2023-12-04 00:00:00|   197|        8.84|
# |product-2|2023-12-05 00:00:00|   227|       15.23|
# |product-2|2023-12-06 00:00:00|   254|       11.89|
# +---------+-------------------+------+------------+
#
# *************** Drops records having any null value
# *************** df.dropna(how='any', subset=['percent_diff'])
# +---------+-------------------+------+------------+
# |  product|               date|amount|percent_diff|
# +---------+-------------------+------+------------+
# |product-1|2023-12-02 00:00:00|   133|        33.0|
# |product-1|2023-12-03 00:00:00|   157|       18.05|
# |product-1|2023-12-04 00:00:00|   189|       20.38|
# |product-1|2023-12-05 00:00:00|   217|       14.81|
# |product-1|2023-12-06 00:00:00|   249|       14.75|
# |product-2|2023-12-02 00:00:00|   163|       35.83|
# |product-2|2023-12-03 00:00:00|   181|       11.04|
# |product-2|2023-12-04 00:00:00|   197|        8.84|
# |product-2|2023-12-05 00:00:00|   227|       15.23|
# |product-2|2023-12-06 00:00:00|   254|       11.89|
# +---------+-------------------+------+------------+
#
