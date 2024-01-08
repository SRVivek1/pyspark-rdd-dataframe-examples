"""
Consider a tuple of transaction ids with amount.
Get total transaction amount, per txnid amount, erc.
"""

from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder \
        .appName('collection: test program') \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    txn_data = [('txn001', 1000), ('txn002', 1400), ('txn003', 2000),
                ('txn001', 3000), ('txn002', 2500), ('txn003', 3500), ('txn004', 3100)]

    rdd = sc.parallelize(txn_data)
    print(f'Txn Data: {rdd.take(10)}')

    # total amount spend
    total_amt = rdd.reduce(lambda a, b: a + b)
    print(f'Total amount spent: {total_amt}')

    # Per id transaction
    rdd_txn = rdd.reduceByKey(lambda a, b: a + b)
    print(f'Per txn amount: {rdd_txn.take(10)}')

    # other info
    print(f'Total partitions: {rdd_txn.getNumPartitions()}')
    print(f'Data per partition: {rdd_txn.glom().collect()}')
    print(f'Reduce data to two partition: {rdd_txn.coalesce(2).glom().collect()}')

#
# Command
# --------------
# spark-submit --master 'local[*] ./app.py
#
# Output [Local machine] [Skipping Info logs]
# ----------------------------------------------------
# Txn Data: [('txn001', 1000), ('txn002', 1400), ('txn003', 2000), ('txn001', 3000), ('txn002', 2500), ('txn003', 3500), ('txn004', 3100)]
# Total amount spent: ('txn001', 1000, 'txn002', 1400, 'txn003', 2000, 'txn001', 3000, 'txn002', 2500, 'txn003', 3500, 'txn004', 3100)
# Per txn amount: [('txn001', 4000), ('txn002', 3900), ('txn003', 5500), ('txn004', 3100)]
# Total partitions: 4
# Data per partition: [[], [('txn001', 4000), ('txn002', 3900)], [], [('txn003', 5500), ('txn004', 3100)]]
# Reduce data to two partition: [[('txn001', 4000), ('txn002', 3900)], [('txn003', 5500), ('txn004', 3100)]]
#
