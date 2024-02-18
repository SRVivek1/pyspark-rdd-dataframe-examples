"""
This application demonstrates how to read/write Apache Parquet files in spark.
"""


from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as sql_function
from constants import app_constants as app_constants


if __name__ == '__main__':
    print('\n************************** Spark - Read/Write Parquet files **************************')

    # Using legacy version of Parquet file.
    sparkSession = SparkSession.builder \
        .config('spark.sql.legacy.parquet.int96RebaseModeInRead', 'CORRECTED')\
        .appName('parquet-to-dataframe')\
        .getOrCreate()

    # sparkSession.conf.set('spark.sql.legacy.parquet.int96RebaseModeInRead', 'CORRECTED')

    sparkSession.sparkContext.setLogLevel('ERROR')

    # Read Parquet file
    nyc_omo_df = sparkSession.read.parquet(app_constants.NYC_OMO_PARQUET)

    print('\n************* # of partitions : ' + str(nyc_omo_df.rdd.getNumPartitions()))
    print('\n************* # of records : ' + str(nyc_omo_df.count()))

    print('\n************* nyc_omo_df.printSchema()')
    nyc_omo_df.printSchema()

    print('\n************* nyc_omo_df.show(5, False)')
    nyc_omo_df.show(5, False)

    # Repartition
    print('\n************* nyc_omo_df = nyc_omo_df.repartition(5)')
    nyc_omo_df = nyc_omo_df.repartition(5)

    print('\n************* # of partitions : ' + str(nyc_omo_df.rdd.getNumPartitions()))
    print('\n************* # of records : ' + str(nyc_omo_df.count()))

    print('\n************* nyc_omo_df.printSchema()')
    nyc_omo_df.printSchema()

    print('\n************* nyc_omo_df.show(5, False)')
    nyc_omo_df.show(5, False)

    print('\n************* Summery of NYC Open Market Order (OMO) charges dataset : nyc_omo_df.describe().show()')
    # TODO: Analyze error
    #  Broke while processing - No error on console
    # nyc_omo_df.describe().show()

    print('\n************* Summery of NYC Open Market Order (OMO) Lot column : nyc_omo_df.describe([\'Lot\']).show()')
    nyc_omo_df.describe(['Lot']).show()

    print('\n************* Distinct Boroughs in record : nyc_omo_df.select(col(\'Boro\')).distinct().show(100)')
    nyc_omo_df.select(sql_function.col('Boro')).distinct().show(100)

    print('\n************* OMO frequency distribution of different Boroughs')
    nyc_omo_df \
        .groupBy('Boro') \
        .agg({'Boro': 'count'}) \
        .withColumnRenamed('count(Boro)', 'frequency_distribution') \
        .show()

    print('\n************* OMO ZIP and Boro List')
    boro_zip_df = nyc_omo_df \
        .groupBy('Boro') \
        .agg({'Zip': 'collect_set'}) \
        .withColumnRenamed("collect_set(Zip)", "ZipList") \
        .withColumn('ZipCount', sql_function.size(sql_function.col('ZipList')))\

    boro_zip_df\
        .select('Boro', 'ZipList', 'ZipCount')\
        .show()

    # Windows function
    window_specs = Window.partitionBy('OMOCreateDate')
    omo_daily_frequency = nyc_omo_df\
        .withColumn('DailyFrequency', sql_function.count('OMOID').over(window_specs))

    print('\n**************** # of partitions in windowed OMO dataframe : ' + str(omo_daily_frequency.rdd.getNumPartitions()))

    print('\n**************** Sample records in windowed OMO dataframe : ')
    omo_daily_frequency.show(10)

    # Write windowed data to filesystem
    write_path = app_constants.file_write_path + '/nyc_omo_data_parquet'
    print('\n**************** Write windowed data to : ' + write_path)

    # Setting legacy mode for older version of parquet file
    sparkSession.conf.set('spark.sql.legacy.parquet.int96RebaseModeInWrite', 'CORRECTED')

    omo_daily_frequency\
        .repartition(5)\
        .write\
        .mode('overwrite')\
        .parquet(write_path)

    # Stop Spark service
    sparkSession.stop()

# Command
# -----------------
#   export PYTHONPATH=$PYTHONPATH:.
#   spark-submit --master 'local[*]' dataframe/ingestion/dataframe_from_files/parquet_to_dataframe.py
#
# Output
# -----------------
# ************* # of partitions : 5
#
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
# |1669250|E812754  |876444    |4     |Queens   |14-09      |PINSON STREET               |1        |11691.0|15652|3  |Building  |DELEAD         |OMO Completed       |10.5          |2007-11-20 00:00:00|0              |2007-11-28 00:00:00|NULL |NULL                  |false            |NULL       |NULL     |as per rc #20080000875 (i.a.t.l.), perform total lead analysis of 02 dust wipe sample(s) via environmental protection agency (epa) sw845-3050-7420 me |
# |4378989|EH18656  |56780     |2     |Bronx    |1046       |CLAY AVENUE                 |2A       |10456.0|2425 |5  |Building  |DELEAD         |OMO Completed       |36.0          |2017-06-06 00:00:00|0              |2017-06-07 00:00:00|NULL |NULL                  |false            |NULL       |NULL     |perform total lead analysis of 09 dust wipe sample(s) via environmental protection agency (epa) sw8453050-7000b method utilizing flame atomic absorpti|
# |1608449|D700139  |95709     |2     |Bronx    |1784       |MERRILL STREET              |NULL     |10460.0|3898 |80 |Building  |ASBEST         |OMO Completed       |425.0         |2006-12-01 00:00:00|0              |2006-12-04 00:00:00|NULL |NULL                  |false            |NULL       |NULL     |carry out asbestos investigation for future demolition of structure. supply written report to hpd.                                                    |
# |2373767|DA00691  |633990    |4     |Queens   |142        |BEACH 96 STREET             |NULL     |11693.0|16168|17 |Demolished|GC             |OMO Completed       |390.0         |2010-04-07 00:00:00|0              |2010-04-09 00:00:00|NULL |NULL                  |false            |NULL       |NULL     | 1- repair opening in 8' fence at front 2- re-install top rail ''e'' list                                                                             |
# |4723551|EJ10844  |4168      |1     |Manhattan|2816       |FREDERICK DOUGLASS BOULEVARD|1B       |10039.0|2035 |1  |Building  |GC             |Owner Refused Access|1000.0        |2019-01-15 00:00:00|0              |2019-01-29 00:00:00|NULL |NULL                  |true             |NULL       |NULL     |apt 1b; pvt hall: replace the defective parkay floor at pvt hall. square off floor and underlay with plywood board approx 20 square ft with si        |
# +-------+---------+----------+------+---------+-----------+----------------------------+---------+-------+-----+---+----------+---------------+--------------------+--------------+-------------------+---------------+-------------------+-----+----------------------+-----------------+-----------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------+
# only showing top 5 rows
#
#
# ************* nyc_omo_df = nyc_omo_df.repartition(5)
#
# ************* # of partitions : 5
#
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
# +-------+---------+----------+------+-------------+-----------+--------------------+---------+-------+-----+---+---------+---------------+--------------------------------+--------------+-------------------+---------------+-------------------+-----+----------------------+-----------------+-----------+---------+-----------------------------------------------------------------------------------------------------------------------------------------------------+
# |OMOID  |OMONumber|BuildingID|BoroID|Boro         |HouseNumber|StreetName          |Apartment|Zip    |Block|Lot|LifeCycle|WorkTypeGeneral|OMOStatusReason                 |OMOAwardAmount|OMOCreateDate      |NetChangeOrders|OMOAwardDate       |IsAEP|IsCommercialDemolition|ServiceChargeFlag|FEMAEventID|FEMAEvent|OMODescription                                                                                                                                       |
# +-------+---------+----------+------+-------------+-----------+--------------------+---------+-------+-----+---+---------+---------------+--------------------------------+--------------+-------------------+---------------+-------------------+-----+----------------------+-----------------+-----------+---------+-----------------------------------------------------------------------------------------------------------------------------------------------------+
# |2225187|EA20186  |793607    |5     |Staten Island|148        |WESTERVELT AVENUE   |PH       |10301.0|50   |87 |Building |UTIL           |Utility Account Picked Up By ESB|1.0           |2009-12-21 00:00:00|0              |2009-12-22 00:00:00|NULL |NULL                  |false            |NULL       |NULL     |national grid provide gas to boiler / hot water heater.                                                                                              |
# |876640 |E310351  |315821    |3     |Brooklyn     |262        |IRVING AVENUE       |3-R      |11237.0|3309 |26 |Building |PLUMB          |OMO Completed                   |390.0         |2002-11-29 00:00:00|0              |2002-12-16 00:00:00|NULL |NULL                  |false            |NULL       |NULL     |apartment #3r: replace water closet and accessories and hardware. remove all work related debris. note: contractor must contact hpd @ (718) 636-     |
# |1473040|E516597  |321207    |3     |Brooklyn     |244        |KNICKERBOCKER AVENUE|NULL     |11237.0|3197 |29 |Building |UTIL           |Fuel Delivered                  |762.87        |2004-12-29 00:00:00|0              |2005-01-03 00:00:00|NULL |NULL                  |false            |NULL       |NULL     |provide automatic fuel delivery to the above building until further notice. #2 fuel. no prime and start necessary. 212-863-8779                      |
# |1499975|E525510  |6483      |1     |Manhattan    |44         |AVENUE B            |4A       |10009.0|399  |35 |Building |DELEAD         |OMO Completed                   |1745.0        |2005-03-21 00:00:00|0              |2005-03-23 00:00:00|NULL |NULL                  |false            |NULL       |NULL     |local law #1 violation : as per rc #20050021342 (pdg). and attached scope of work thoroughly remove all lead violations as per new york city administ|
# |1236460|E416112  |40287     |1     |Manhattan    |157        |WEST 131 STREET     |BSMT     |10027.0|1916 |9  |Building |PLUMB          |OMO Completed                   |1499.0        |2004-02-09 00:00:00|0              |2004-03-08 00:00:00|NULL |NULL                  |false            |NULL       |NULL     |bsmt. apartment: replace broken waste line pipe at kitchen sink. restore water to the same line. note: main valve shut-off at basement. remove al    |
# +-------+---------+----------+------+-------------+-----------+--------------------+---------+-------+-----+---+---------+---------------+--------------------------------+--------------+-------------------+---------------+-------------------+-----+----------------------+-----------------+-----------+---------+-----------------------------------------------------------------------------------------------------------------------------------------------------+
# only showing top 5 rows
#
#
# ************* Summery of NYC Open Market Order (OMO) charges dataset : nyc_omo_df.describe().show()
#
# ************* Summery of NYC Open Market Order (OMO) Lot column : nyc_omo_df.describe(['Lot']).show()
# +-------+------------------+
# |summary|               Lot|
# +-------+------------------+
# |  count|            406023|
# |   mean|121.48553899655931|
# | stddev| 758.1596606926987|
# |    min|                 0|
# |    max|              9100|
# +-------+------------------+
#
#
# ************* Distinct Boroughs in record : nyc_omo_df.select(col('Boro')).distinct().show(100)
# +-------------+
# |         Boro|
# +-------------+
# |       Queens|
# |     Brooklyn|
# |Staten Island|
# |    Manhattan|
# |        Bronx|
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
# +-------------+--------------------+--------+
# |         Boro|             ZipList|ZipCount|
# +-------------+--------------------+--------+
# |       Queens|[11418.0, 11372.0...|      62|
# |     Brooklyn|[11233.0, 11206.0...|      39|
# |Staten Island|[10309.0, 10314.0...|      12|
# |    Manhattan|[10019.0, 0.0, 10...|      45|
# |        Bronx|[10460.0, 10468.0...|      26|
# +-------------+--------------------+--------+
#
#
# **************** # of partitions in windowed OMO dataframe : 4
#
# **************** Sample records in windowed OMO dataframe :
# +-----+---------+----------+------+--------+-----------+----------------+---------+-------+-----+---+---------+---------------+--------------------+--------------+-------------------+---------------+-------------------+-----+----------------------+-----------------+-----------+---------+--------------------+--------------+
# |OMOID|OMONumber|BuildingID|BoroID|    Boro|HouseNumber|      StreetName|Apartment|    Zip|Block|Lot|LifeCycle|WorkTypeGeneral|     OMOStatusReason|OMOAwardAmount|      OMOCreateDate|NetChangeOrders|       OMOAwardDate|IsAEP|IsCommercialDemolition|ServiceChargeFlag|FEMAEventID|FEMAEvent|      OMODescription|DailyFrequency|
# +-----+---------+----------+------+--------+-----------+----------------+---------+-------+-----+---+---------+---------------+--------------------+--------------+-------------------+---------------+-------------------+-----+----------------------+-----------------+-----------+---------+--------------------+--------------+
# |  266|  E000296|    377641|     3|Brooklyn|        234|STOCKHOLM STREET|     NULL|11237.0| 3258| 17| Building|             GC|       OMO Completed|        1095.0|1998-08-15 00:00:00|              0|1998-08-27 00:00:00| NULL|                  NULL|            false|       NULL|     NULL|reseal tota l of ...|             4|
# |  275|  E000319|    311470|     3|Brooklyn|         36| HERKIMER STREET|     NULL|11216.0| 1865| 28| Building|           UTIL|       OMO Completed|        9990.0|1998-08-15 00:00:00|              0|1998-09-02 00:00:00| NULL|                  NULL|            false|       NULL|     NULL|dismantle and rem...|             4|
# |  280|  E000324|    327689|     3|Brooklyn|        283|   LINDEN STREET|      1-R|11237.0| 3326| 49| Building|           UTIL|       OMO Completed|        9990.0|1998-08-15 00:00:00|              0|1998-09-04 00:00:00| NULL|                  NULL|            false|       NULL|     NULL|obtain all necess...|             4|
# |  268|  E000299|    133486|     3|Brooklyn|        564|        7 AVENUE|     NULL|11215.0|  886| 44| Building|             GC|       OMO Completed|         485.0|1998-08-15 00:00:00|              0|1998-09-10 00:00:00| NULL|                  NULL|            false|       NULL|     NULL|hanging scaffoldi...|             4|
# |  330|  E000391|    382481|     3|Brooklyn|        261|     TROY AVENUE|     NULL|11213.0| 1383|  4| Building|           UTIL|Utility Account P...|           1.0|1998-08-19 00:00:00|              0|1998-08-20 00:00:00| NULL|                  NULL|            false|       NULL|     NULL|con-ed provide el...|             8|
# |  327|  E000388|    312369|     3|Brooklyn|        475|    HICKS STREET|     NULL|11231.0|  321| 27| Building|             GC|       OMO Completed|         845.0|1998-08-19 00:00:00|              0|1998-09-02 00:00:00| NULL|                  NULL|            false|       NULL|     NULL|remove 108 sqft. ...|             8|
# |  339|  E000401|    349551|     3|Brooklyn|       1356|  PACIFIC STREET|     NULL|11216.0| 1208| 27| Building|             GC|       Duplicate OMO|           0.0|1998-08-19 00:00:00|              0|               NULL| NULL|                  NULL|            false|       NULL|     NULL|demo 80 sqft of c...|             8|
# |  340|  E000402|    808955|     3|Brooklyn|       1360| NEW YORK AVENUE|     NULL|11210.0| 4964| 40| Building|             GC|      Refused Access|         490.0|1998-08-19 00:00:00|              0|1998-11-04 00:00:00| NULL|                  NULL|             true|       NULL|     NULL|supply & install ...|             8|
# |  334|  E000396|    287550|     3|Brooklyn|       1196| EASTERN PARKWAY|     NULL|11213.0| 1397| 35| Building|             GC|      Refused Access|        1997.0|1998-08-19 00:00:00|              0|1998-11-04 00:00:00| NULL|                  NULL|             true|       NULL|     NULL|fabracate new mar...|             8|
# |  338|  E000400|    349551|     3|Brooklyn|       1356|  PACIFIC STREET|     NULL|11216.0| 1208| 27| Building|             GC|       OMO Completed|         499.0|1998-08-19 00:00:00|              0|1998-09-01 00:00:00| NULL|                  NULL|            false|       NULL|     NULL|demo 80 sqft of c...|             8|
# +-----+---------+----------+------+--------+-----------+----------------+---------+-------+-----+---+---------+---------------+--------------------+--------------+-------------------+---------------+-------------------+-----+----------------------+-----------------+-----------+---------+--------------------+--------------+
# only showing top 10 rows
#
#
# **************** Write windowed data to : testdata/app-generated/nyc_omo_data_parquet
#
