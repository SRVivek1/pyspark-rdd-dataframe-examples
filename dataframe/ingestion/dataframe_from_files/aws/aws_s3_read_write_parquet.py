"""
    Requirement
    -----------------
        --> This program demonstrates the use case to read and write parquet data to aws s3.

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

    # Read parquet file
    nyc_omo_df = spark.read \
        .parquet('s3a://' + app_conf['s3_conf']['s3_bucket'] + '/NYC_OMO/')

    print(f'************* # of partitions : {nyc_omo_df.rdd.getNumPartitions()}')
    print(f'************* # of records : {nyc_omo_df.count()}')

    print('\n************* nyc_omo_df.printSchema()')
    nyc_omo_df.printSchema()

    print('\n************* nyc_omo_df.show(5, False)')
    nyc_omo_df.show(5, False)

    # Repartition
    print('************** nyc_omo_df.repartition(5)')
    temp_df = nyc_omo_df.repartition(5)
    print(f'************* # of partitions : {temp_df.rdd.getNumPartitions()}')

    print('************** nyc_omo_df.repartition(7)')
    temp_df = nyc_omo_df.repartition(7)
    print(f'************* # of partitions : {temp_df.rdd.getNumPartitions()}')

    # describe() -> Computes basic statistics for numeric and string columns.
    print('************** describe() -> Computes basic statistics for numeric and string columns.')
    print('************** nyc_omo_df.describe().show()')
    nyc_omo_df.describe().show()

    # Read distinct
    print('\n************* Distinct Boroughs in record : nyc_omo_df.select(col(\'Boro\')).distinct().show(100)')
    nyc_omo_df.select(col('Boro')).distinct().show(100, False)

    # Group by - using expression
    print('\n************* OMO frequency distribution of different Boroughs')
    tempdf = nyc_omo_df \
        .groupBy(col('Boro')) \
        .agg({'Boro': 'count'}) \
        .withColumnRenamed('count(Boro)', 'frequency_distribution')

    # Show new transformed data
    tempdf.show()

    # Group by - agg - collect_set by zip code
    print('\n************* OMO ZIP and Boro List')
    tempdf = nyc_omo_df \
        .groupBy(col('Boro')) \
        .agg({'Zip': 'collect_set'}) \
        .withColumnRenamed('collect_set(Zip)', 'ZipList') \
        .withColumn('ZipCount', size(col('ZipList')))

    # Show 3 colums
    tempdf \
        .select('Boro', 'ZipList', 'ZipCount') \
        .show(100, False)

    # Write back to s3
    nyc_omo_df \
        .repartition(5) \
        .write \
        .mode('overwrite') \
        .parquet('s3a://' + app_conf['s3_conf']['s3_write_bucket'] + '/parquet/nyc_omo_write/2023-12-15')

#
# command
# ---------------
# spark-submit --packages 'org.apache.hadoop-aws:2.7.4' --master yarn ./program.py
#
# Execution cluster - Databricks Community Edition
#
# ************* # of partitions : 5
# ************* # of records : 406023
#
# ************* nyc_omo_df.printSchema()
# root
#  |-- OMOID: integer (nullable = true)
#  |-- OMONumber: string (nullable = true)
#  |-- BuildingID: integer (nullable = true)
#  |-- BoroID: integer (nullable = true)
#  |-- Boro: string (nullable = true)
#  |-- HouseNumber: string (nullable = true)
#  |-- StreetName: string (nullable = true)
#  |-- Apartment: string (nullable = true)
#  |-- Zip: double (nullable = true)
#  |-- Block: integer (nullable = true)
#  |-- Lot: integer (nullable = true)
#  |-- LifeCycle: string (nullable = true)
#  |-- WorkTypeGeneral: string (nullable = true)
#  |-- OMOStatusReason: string (nullable = true)
#  |-- OMOAwardAmount: double (nullable = true)
#  |-- OMOCreateDate: timestamp (nullable = true)
#  |-- NetChangeOrders: integer (nullable = true)
#  |-- OMOAwardDate: timestamp (nullable = true)
#  |-- IsAEP: string (nullable = true)
#  |-- IsCommercialDemolition: string (nullable = true)
#  |-- ServiceChargeFlag: boolean (nullable = true)
#  |-- FEMAEventID: integer (nullable = true)
#  |-- FEMAEvent: string (nullable = true)
#  |-- OMODescription: string (nullable = true)
#
#
# ************* nyc_omo_df.show(5, False)
# +-------+---------+----------+------+---------+-----------+----------------------------+---------+-------+-----+---+----------+---------------+--------------------+--------------+-------------------+---------------+-------------------+-----+----------------------+-----------------+-----------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------+
# |OMOID  |OMONumber|BuildingID|BoroID|Boro     |HouseNumber|StreetName                  |Apartment|Zip    |Block|Lot|LifeCycle |WorkTypeGeneral|OMOStatusReason     |OMOAwardAmount|OMOCreateDate      |NetChangeOrders|OMOAwardDate       |IsAEP|IsCommercialDemolition|ServiceChargeFlag|FEMAEventID|FEMAEvent|OMODescription                                                                                                                                        |
# +-------+---------+----------+------+---------+-----------+----------------------------+---------+-------+-----+---+----------+---------------+--------------------+--------------+-------------------+---------------+-------------------+-----+----------------------+-----------------+-----------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------+
# |1669250|E812754  |876444    |4     |Queens   |14-09      |PINSON STREET               |1        |11691.0|15652|3  |Building  |DELEAD         |OMO Completed       |10.5          |2007-11-19 18:30:00|0              |2007-11-27 18:30:00|null |null                  |false            |null       |null     |as per rc #20080000875 (i.a.t.l.), perform total lead analysis of 02 dust wipe sample(s) via environmental protection agency (epa) sw845-3050-7420 me |
# |4378989|EH18656  |56780     |2     |Bronx    |1046       |CLAY AVENUE                 |2A       |10456.0|2425 |5  |Building  |DELEAD         |OMO Completed       |36.0          |2017-06-05 18:30:00|0              |2017-06-06 18:30:00|null |null                  |false            |null       |null     |perform total lead analysis of 09 dust wipe sample(s) via environmental protection agency (epa) sw8453050-7000b method utilizing flame atomic absorpti|
# |1608449|D700139  |95709     |2     |Bronx    |1784       |MERRILL STREET              |null     |10460.0|3898 |80 |Building  |ASBEST         |OMO Completed       |425.0         |2006-11-30 18:30:00|0              |2006-12-03 18:30:00|null |null                  |false            |null       |null     |carry out asbestos investigation for future demolition of structure. supply written report to hpd.                                                    |
# |2373767|DA00691  |633990    |4     |Queens   |142        |BEACH 96 STREET             |null     |11693.0|16168|17 |Demolished|GC             |OMO Completed       |390.0         |2010-04-06 18:30:00|0              |2010-04-08 18:30:00|null |null                  |false            |null       |null     | 1- repair opening in 8' fence at front 2- re-install top rail ''e'' list                                                                             |
# |4723551|EJ10844  |4168      |1     |Manhattan|2816       |FREDERICK DOUGLASS BOULEVARD|1B       |10039.0|2035 |1  |Building  |GC             |Owner Refused Access|1000.0        |2019-01-14 18:30:00|0              |2019-01-28 18:30:00|null |null                  |true             |null       |null     |apt 1b; pvt hall: replace the defective parkay floor at pvt hall. square off floor and underlay with plywood board approx 20 square ft with si        |
# +-------+---------+----------+------+---------+-----------+----------------------------+---------+-------+-----+---+----------+---------------+--------------------+--------------+-------------------+---------------+-------------------+-----+----------------------+-----------------+-----------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------+
# only showing top 5 rows
#
# ************** nyc_omo_df.repartition(5)
# ************* # of partitions : 5
# ************** nyc_omo_df.repartition(7)
# ************* # of partitions : 7
# ************** describe() -> Computes basic statistics for numeric and string columns.
# ************** nyc_omo_df.describe().show()
# +-------+-----------------+---------+------------------+------------------+-------------+------------------+--------------+--------------------+------------------+------------------+------------------+---------------+---------------+--------------------+------------------+--------------------+-----+----------------------+------------------+---------------+--------------------+
# |summary|            OMOID|OMONumber|        BuildingID|            BoroID|         Boro|       HouseNumber|    StreetName|           Apartment|               Zip|             Block|               Lot|      LifeCycle|WorkTypeGeneral|     OMOStatusReason|    OMOAwardAmount|     NetChangeOrders|IsAEP|IsCommercialDemolition|       FEMAEventID|      FEMAEvent|      OMODescription|
# +-------+-----------------+---------+------------------+------------------+-------------+------------------+--------------+--------------------+------------------+------------------+------------------+---------------+---------------+--------------------+------------------+--------------------+-----+----------------------+------------------+---------------+--------------------+
# |  count|           406023|   406023|            406023|            406023|       406023|            406023|        406023|              309911|            405956|            406023|            406023|         406023|         406023|              402081|            406023|              406023|24190|                   704|              1183|           1069|              406021|
# |   mean|  2283145.1962278|     null|254493.45723518127|2.5287853151176165|         null|1056.3171764501242|          null|6.471340052741421E98|10816.503759028072|3443.0685700071176|121.48553899655931|           null|           null|                null|1612.8404196806964|0.013314516665312064| null|                  null|330.73034657650044|           null|                 1.0|
# | stddev|1213798.155918892|     null| 222068.8958767314|0.9435178601177154|         null|1164.6251654741593|          null|2.275318009024909...|502.09756259954145|2623.8266035984716| 758.1596606927003|           null|           null|                null|18894.063675983325|   7.739198149613596| null|                  null|108.04912471059545|           null|                null|
# |    min|               28|  D000001|                 1|                 1|        Bronx|                 0|      1 AVENUE|                    |               0.0|                 0|                 0|       Building|           7AFA|         Apt. Vacant|               0.0|                   0|  AEP|            COMM DEMOL|                 0|Hurricane Sandy|                    |
# |    max|          4758368|  EJ15477|            989058|                 5|Staten Island|               999|ZULETTE AVENUE|                rubb|           11697.0|             16350|              9100|UnderConstructi|           UTIL|landlord Restored...|         4150000.0|                4906|  AEP|            COMM DEMOL|               366|Hurricane Sandy|â€œsandyâ€ damag...|
# +-------+-----------------+---------+------------------+------------------+-------------+------------------+--------------+--------------------+------------------+------------------+------------------+---------------+---------------+--------------------+------------------+--------------------+-----+----------------------+------------------+---------------+--------------------+
#
#
# ************* Distinct Boroughs in record : nyc_omo_df.select(col('Boro')).distinct().show(100)
# +-------------+
# |Boro         |
# +-------------+
# |Queens       |
# |Brooklyn     |
# |Staten Island|
# |Manhattan    |
# |Bronx        |
# +-------------+
#
#
# ************* OMO frequency distribution of different Boroughs
# +-------------+----------------------+
# |         Boro|frequency_distribution|
# +-------------+----------------------+
# |       Queens|                 39678|
# |     Brooklyn|                180356|
# |Staten Island|                  7575|
# |    Manhattan|                 67738|
# |        Bronx|                110676|
# +-------------+----------------------+
#
#
# ************* OMO ZIP and Boro List
# +-------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
# |Boro         |ZipList                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |ZipCount|
# +-------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
# |Queens       |[11418.0, 11372.0, 11040.0, 11105.0, 11004.0, 11355.0, 11429.0, 11374.0, 11412.0, 11366.0, 11368.0, 11433.0, 11101.0, 11692.0, 11370.0, 11694.0, 11362.0, 11427.0, 11364.0, 11421.0, 11385.0, 11358.0, 10435.0, 11423.0, 11377.0, 11360.0, 11379.0, 11417.0, 11436.0, 11354.0, 11419.0, 11373.0, 11106.0, 11356.0, 11375.0, 11413.0, 11432.0, 11415.0, 11369.0, 11434.0, 11102.0, 11001.0, 11104.0, 11428.0, 11697.0, 11411.0, 11365.0, 11367.0, 11691.0, 11693.0, 11361.0, 11426.0, 11363.0, 11420.0, 11357.0, 11422.0, 11109.0, 11414.0, 11378.0, 11416.0, 11435.0, 11103.0]|62      |
# |Brooklyn     |[11233.0, 11206.0, 11211.0, 11225.0, 11230.0, 11249.0, 11203.0, 11208.0, 11205.0, 11224.0, 11207.0, 11221.0, 11226.0, 11204.0, 11223.0, 11201.0, 11220.0, 11239.0, 11217.0, 11222.0, 11236.0, 11214.0, 11219.0, 11238.0, 11216.0, 11235.0, 11213.0, 11218.0, 11232.0, 11237.0, 11210.0, 11215.0, 11229.0, 11234.0, 11212.0, 11231.0, 11416.0, 11209.0, 11228.0]                                                                                                                                                                                                               |39      |
# |Staten Island|[10309.0, 10314.0, 10303.0, 10302.0, 10307.0, 10306.0, 10310.0, 10304.0, 10308.0, 10312.0, 10301.0, 10305.0]                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |12      |
# |Manhattan    |[10019.0, 0.0, 10038.0, 10016.0, 10035.0, 10040.0, 10280.0, 10013.0, 10018.0, 10032.0, 10037.0, 10010.0, 10075.0, 10034.0, 10012.0, 10031.0, 10009.0, 10014.0, 10028.0, 10033.0, 10463.0, 10006.0, 10011.0, 10030.0, 10128.0, 10027.0, 10065.0, 10005.0, 10024.0, 10029.0, 10002.0, 10007.0, 10021.0, 10026.0, 10004.0, 10129.0, 10023.0, 10001.0, 10025.0, 10039.0, 10044.0, 10003.0, 10017.0, 10022.0, 10036.0]                                                                                                                                                             |45      |
# |Bronx        |[10460.0, 10468.0, 10457.0, 10451.0, 10465.0, 10470.0, 10454.0, 10459.0, 10473.0, 10462.0, 1901.0, 10467.0, 10456.0, 10475.0, 10464.0, 10453.0, 10472.0, 10461.0, 10466.0, 10455.0, 10469.0, 10474.0, 10458.0, 10463.0, 10452.0, 10471.0]                                                                                                                                                                                                                                                                                                                                     |26      |
# +-------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
#
