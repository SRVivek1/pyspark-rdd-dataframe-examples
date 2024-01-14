"""
	Find transaction amout for earch acc num.
"""

from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName('collection - test.').getOrCreate()

sc = spark.sparkContext
sc.setLogLevel('ERROR')

data = ['accnum~amount~date~category', '123~200.0~01/01/2015~Drug store', '456~400.0~01/01/2015~Grocery',
        '123~136.0~02/01/2015~Gas', '456~79.0~02/01/2015~Hotel', '789~341.5~02/01/2015~Travel']

# Create RDD
rdd = sc.parallelize(data)

print(f'Data: \n {rdd.coalesce(1).glom().collect()}')

rdd = rdd \
    .filter(lambda rec: 'accnum~amount~date~category' not in rec) \
    .map(lambda rec: (rec.split('~')[0], float(rec.split('~')[1]))) \
    .reduceByKey(lambda a, b: a + b)

print(rdd.take(10))
