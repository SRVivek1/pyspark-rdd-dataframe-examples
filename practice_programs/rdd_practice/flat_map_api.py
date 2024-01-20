"""
    This program demonstrates the use of flat map API.
"""

from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('map-partition') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    print('************************************ App logs **********************************')

    # Parallelize collection
    rdd = spark.sparkContext.parallelize(range(1, 20), 4)
    print(f'rdd.getNumPartitions() : {rdd.getNumPartitions()}')
    print(f'rdd.collect() : {rdd.collect()}')

    # Find divisibles
    def num_divs(n):
        result = []

        for i in range(1, n+1):
            if n % i == 0:
                result.append(i)

        return result

    fm_res = rdd.flatMap(num_divs).collect()
    print(f'type(rdd.flatMap(num_divs).collect()) : {type(fm_res)}')
    print(f'rdd.flatMap(num_divs).collect() : {fm_res}')

    print('***************** Using map()')
    map_res = rdd.map(num_divs).collect()
    print(f'type(rdd.map(num_divs).collect()) : {type(map_res)}')
    print(f'rdd.map(num_divs).collect() : {map_res}')

#
# command
# -----------------------
# spark-submit --master 'local[*]' flat_map_api.py
#
# Output
# ------------------------
# ************************************ App logs **********************************
# rdd.getNumPartitions() : 4
# rdd.collect() : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
# type(rdd.flatMap(num_divs).collect()) : <class 'list'>
# rdd.flatMap(num_divs).collect() : [1, 1, 2, 1, 3, 1, 2, 4, 1, 5, 1, 2, 3, 6, 1, 7, 1, 2, 4, 8, 1, 3, 9, 1, 2, 5, 10, 1, 11, 1, 2, 3, 4, 6, 12, 1, 13, 1, 2, 7, 14, 1, 3, 5, 15, 1, 2, 4, 8, 16, 1, 17, 1, 2, 3, 6, 9, 18, 1, 19]
# ***************** Using map()
# type(rdd.map(num_divs).collect()) : <class 'list'>
# rdd.map(num_divs).collect() : [[1], [1, 2], [1, 3], [1, 2, 4], [1, 5], [1, 2, 3, 6], [1, 7], [1, 2, 4, 8], [1, 3, 9], [1, 2, 5, 10], [1, 11], [1, 2, 3, 4, 6, 12], [1, 13], [1, 2, 7, 14], [1, 3, 5, 15], [1, 2, 4, 8, 16], [1, 17], [1, 2, 3, 6, 9, 18], [1, 19]]
#
#
