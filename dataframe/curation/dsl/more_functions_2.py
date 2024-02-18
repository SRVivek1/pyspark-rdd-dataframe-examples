"""
    Transformation functions
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import DoubleType
from constants import app_constants

import pyspark.sql.functions as sqf

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('df-more-functions-2') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    df = spark.read.csv(path=app_constants.employee_details_min_dup_csv, sep=',', header=True)
    df.show(10)

    # remove dollar sign from salary colum
    df = df.withColumn('salary', sqf.regexp_replace('salary', '[$]', '').cast(DoubleType()))
    df.printSchema()
    df.show(10)

    # Window Ranking functions
    window_spec = Window.partitionBy('department').orderBy('salary')

    tdf = df \
        .withColumn('row_number', sqf.row_number().over(window_spec)) \
        .withColumn('rank', sqf.rank().over(window_spec)) \
        .withColumn('dense_rank', sqf.dense_rank().over(window_spec)) \
        .withColumn('ntile', sqf.ntile(3).over(window_spec)) \
        .withColumn('percent_rank', sqf.percent_rank().over(window_spec))

    tdf.printSchema()
    tdf.show(100)

# Command
# export PYTHONPATH=$PYTHONPATH:.
# spark-submit --master 'local[*]' dataframe/curation/dsl/more_functions_2.py
# #
# Output
# ---------------------
# +----------+---------+--------------+----------+---------+
# |first_name|last_name|      location|department|   salary|
# +----------+---------+--------------+----------+---------+
# |    Angela|   Mangam|     Żółkiewka|Accounting|$35416.88|
# |Willabella|  Sweeten|       Aborlan|Accounting|$21114.83|
# |  Ashleigh| Chezelle|         Jinhe|Accounting|$21114.83|
# |      Iain|  Dunnico|       Komenda|Accounting|$20268.08|
# |  Jeanette| Gallaway|     Bailizhou|Accounting| $3551.12|
# |    Michal|  Hampton| Dos Quebradas|Accounting|$29618.68|
# |  Sinclare|  Jillitt|   Ban Chalong|Accounting|$29618.68|
# |    Portia| Jephcote|    Kobiernice|Accounting|$14571.30|
# |      Zeke|   Testin|Telêmaco Borba|Accounting| $3582.71|
# |   Willard|Checketts|         Tantu|Accounting|$29997.88|
# +----------+---------+--------------+----------+---------+
# only showing top 10 rows
#
# root
#  |-- first_name: string (nullable = true)
#  |-- last_name: string (nullable = true)
#  |-- location: string (nullable = true)
#  |-- department: string (nullable = true)
#  |-- salary: double (nullable = true)
#
# +----------+---------+--------------+----------+--------+
# |first_name|last_name|      location|department|  salary|
# +----------+---------+--------------+----------+--------+
# |    Angela|   Mangam|     Żółkiewka|Accounting|35416.88|
# |Willabella|  Sweeten|       Aborlan|Accounting|21114.83|
# |  Ashleigh| Chezelle|         Jinhe|Accounting|21114.83|
# |      Iain|  Dunnico|       Komenda|Accounting|20268.08|
# |  Jeanette| Gallaway|     Bailizhou|Accounting| 3551.12|
# |    Michal|  Hampton| Dos Quebradas|Accounting|29618.68|
# |  Sinclare|  Jillitt|   Ban Chalong|Accounting|29618.68|
# |    Portia| Jephcote|    Kobiernice|Accounting| 14571.3|
# |      Zeke|   Testin|Telêmaco Borba|Accounting| 3582.71|
# |   Willard|Checketts|         Tantu|Accounting|29997.88|
# +----------+---------+--------------+----------+--------+
# only showing top 10 rows
#
# root
#  |-- first_name: string (nullable = true)
#  |-- last_name: string (nullable = true)
#  |-- location: string (nullable = true)
#  |-- department: string (nullable = true)
#  |-- salary: double (nullable = true)
#  |-- row_number: integer (nullable = false)
#  |-- rank: integer (nullable = false)
#  |-- dense_rank: integer (nullable = false)
#  |-- ntile: integer (nullable = false)
#  |-- percent_rank: double (nullable = false)
#
# +----------+--------------+--------------------+--------------------+--------+----------+----+----------+-----+-------------------+
# |first_name|     last_name|            location|          department|  salary|row_number|rank|dense_rank|ntile|       percent_rank|
# +----------+--------------+--------------------+--------------------+--------+----------+----+----------+-----+-------------------+
# |  Jeanette|      Gallaway|           Bailizhou|          Accounting| 3551.12|         1|   1|         1|    1|                0.0|
# |      Zeke|        Testin|      Telêmaco Borba|          Accounting| 3582.71|         2|   2|         2|    1|0.07692307692307693|
# |     Tonie|    Blomefield|                Oslo|          Accounting| 3582.71|         3|   2|         2|    1|0.07692307692307693|
# |    Portia|      Jephcote|          Kobiernice|          Accounting| 14571.3|         4|   4|         3|    1|0.23076923076923078|
# |      Iain|       Dunnico|             Komenda|          Accounting|20268.08|         5|   5|         4|    1| 0.3076923076923077|
# |Willabella|       Sweeten|             Aborlan|          Accounting|21114.83|         6|   6|         5|    2|0.38461538461538464|
# |  Ashleigh|      Chezelle|               Jinhe|          Accounting|21114.83|         7|   6|         5|    2|0.38461538461538464|
# |   Yoshiko|        Wilcot|    Vyerkhnyadzvinsk|          Accounting|26892.91|         8|   8|         6|    2| 0.5384615384615384|
# |     Yurik| Van der Kruis|       Oklahoma City|          Accounting|26892.91|         9|   8|         6|    2| 0.5384615384615384|
# |    Normie|        Fisbey|           Ta’ Xbiex|          Accounting|27338.53|        10|  10|         7|    2| 0.6923076923076923|
# |    Michal|       Hampton|       Dos Quebradas|          Accounting|29618.68|        11|  11|         8|    3| 0.7692307692307693|
# |  Sinclare|       Jillitt|         Ban Chalong|          Accounting|29618.68|        12|  11|         8|    3| 0.7692307692307693|
# |   Willard|     Checketts|               Tantu|          Accounting|29997.88|        13|  13|         9|    3| 0.9230769230769231|
# |    Angela|        Mangam|           Żółkiewka|          Accounting|35416.88|        14|  14|        10|    3|                1.0|
# |     Becka|         Beart|              Sakule|Business Development| 6182.17|         1|   1|         1|    1|                0.0|
# |    Coleen|         Sagar|       Wādī as Salqā|Business Development| 6182.17|         2|   1|         1|    1|                0.0|
# |   Madelyn|      Reynalds|             Shuikou|Business Development| 7312.87|         3|   3|         2|    1|0.14285714285714285|
# |   Bronnie|     Churchley|           Putrajawa|Business Development| 7312.87|         4|   3|         2|    1|0.14285714285714285|
# |    Cassie|        Kelcey|            Al ‘Awjā|Business Development|13843.28|         5|   5|         3|    1| 0.2857142857142857|
# |  Teresita|       Matasov|             Muliang|Business Development|13843.28|         6|   5|         3|    2| 0.2857142857142857|
# |    Mikael|        Loynes|        Yupiltepeque|Business Development|19058.65|         7|   7|         4|    2|0.42857142857142855|
# |   Queenie|      Tumbelty|             Guararé|Business Development|22211.81|         8|   8|         5|    2|                0.5|
# |   Sanford|         Rolfs|              Jelbuk|Business Development|23750.14|         9|   9|         6|    2| 0.5714285714285714|
# |    Cherey|      Jurewicz|              Xianyi|Business Development|24685.87|        10|  10|         7|    2| 0.6428571428571429|
# |  Benedict|          Huge|                Asan|Business Development|30932.48|        11|  11|         8|    3| 0.7142857142857143|
# |    Camala|        Seares|                Rano|Business Development|30932.48|        12|  11|         8|    3| 0.7142857142857143|
# |     Ariel|       Rihosek|             Dapitan|Business Development|35238.12|        13|  13|         9|    3| 0.8571428571428571|
# |     Jorey|          Nias|             Jaciara|Business Development|37837.85|        14|  14|        10|    3| 0.9285714285714286|
# |     Jorie|      Maclaine|            Zongzhai|Business Development| 39904.3|        15|  15|        11|    3|                1.0|
# |     Kermy|        Bygott|           Penamacor|         Engineering| 8501.81|         1|   1|         1|    1|                0.0|
# |     Gypsy|    Kattenhorn|        Mount Ayliff|         Engineering|14043.79|         2|   2|         2|    1|0.07142857142857142|
# |      Bert|         Spacy|             Isahaya|         Engineering|17834.44|         3|   3|         3|    1|0.14285714285714285|
# |       Ame|         Niven|           Nangerang|         Engineering|18118.85|         4|   4|         4|    1|0.21428571428571427|
# |    Penrod|        Maylor|       San Francisco|         Engineering|21516.82|         5|   5|         5|    1| 0.2857142857142857|
# |   Desirae|    Mazonowicz|           Brignoles|         Engineering|24848.24|         6|   6|         6|    2|0.35714285714285715|
# |    Cullie|           Caw|            Göteborg|         Engineering|24848.24|         7|   6|         6|    2|0.35714285714285715|
# |    Sallie|       Gettens|               Borås|         Engineering|24848.24|         8|   6|         6|    2|0.35714285714285715|
# |    Darrel|    Boerderman|         Even Yehuda|         Engineering|24848.24|         9|   6|         6|    2|0.35714285714285715|
# |    Harmon|         Tyght|         Boshkengash|         Engineering|24848.24|        10|   6|         6|    2|0.35714285714285715|
# |     Elihu|        Booler|               Hetou|         Engineering|27502.62|        11|  11|         7|    3| 0.7142857142857143|
# |      Duff|        Patten|             Longcun|         Engineering|30336.08|        12|  12|         8|    3| 0.7857142857142857|
# |     Emera|       Dungate|            Sumurber|         Engineering| 31730.8|        13|  13|         9|    3| 0.8571428571428571|
# |    Esdras|        Snoden|    Monte de Fralães|         Engineering|34574.13|        14|  14|        10|    3| 0.9285714285714286|
# |    Sonnie|     Courtliff|          Cristalina|         Engineering|37157.84|        15|  15|        11|    3|                1.0|
# |   Letitia|      Wolledge|             Tucuruí|     Human Resources|  1195.4|         1|   1|         1|    1|                0.0|
# |  Jennilee|     Painswick|     Aldeia do Bispo|     Human Resources| 7058.35|         2|   2|         2|    1|0.07142857142857142|
# |    Merrie|     Randerson|       Pärnu-Jaagupi|     Human Resources| 7058.35|         3|   2|         2|    1|0.07142857142857142|
# |     Olwen|      Proschke|Markaz Bilād aţ Ţ...|     Human Resources| 7058.35|         4|   2|         2|    1|0.07142857142857142|
# |     Lynde|        Allawy|              Estaca|     Human Resources|  7205.3|         5|   5|         3|    1| 0.2857142857142857|
# |   Stanley|       Purkess|          Talcahuano|     Human Resources| 25003.6|         6|   6|         4|    2|0.35714285714285715|
# |    Hubert|      Puleston|               Lubao|     Human Resources| 25618.1|         7|   7|         5|    2|0.42857142857142855|
# |    Joelie|        Burnel|              Carepa|     Human Resources|31721.91|         8|   8|         6|    2|                0.5|
# |    Daffie|Van der Velden|          Prislonica|     Human Resources| 32284.3|         9|   9|         7|    2| 0.5714285714285714|
# |       Ive|     LaBastida|             Hecheng|     Human Resources|34385.67|        10|  10|         8|    2| 0.6428571428571429|
# |    Cherri|      Hucknall|Al Mawşil al Jadīdah|     Human Resources|35539.85|        11|  11|         9|    3| 0.7142857142857143|
# |     Dulcy|       Kimmons|               Tanay|     Human Resources|35539.85|        12|  11|         9|    3| 0.7142857142857143|
# |       Man|      Montrose|        Kuala Lumpur|     Human Resources|35539.85|        13|  11|         9|    3| 0.7142857142857143|
# |    Briana|        Sivyer|             Чегране|     Human Resources|35539.85|        14|  11|         9|    3| 0.7142857142857143|
# |    Shalne|        Kaindl|          Hallstavik|     Human Resources|37703.45|        15|  15|        10|    3|                1.0|
# +----------+--------------+--------------------+--------------------+--------+----------+----+----------+-----+-------------------+
#
#
