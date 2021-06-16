"""
This program demonstrates different transformation examples on DataFrame instance.
"""


from pyspark.sql import SparkSession
import os.path
import yaml

if __name__ == "__main__":
    print("\n*****************************Data curation using DSL *****************************\n")

    sparkSession = SparkSession\
        .builder\
        .appName('Data curation using DSL')\
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('ERROR')

    # Load App Configs
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_file = os.path.abspath(current_dir + '../../../..' + 'application.yml')
    app_secret_file = os.path.abspath(current_dir + '../../../..' + '.secrets')

    app_config = yaml.load(open(app_config_file), Loader=yaml.FullLoader)
    app_secret = yaml.load(open(app_secret_file), Loader=yaml.FullLoader)

    # Load data from AWS S3
    hadoop_conf = sparkSession.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set('fs.s3a.access.key', app_secret['s3_conf']['access_key'])
    hadoop_conf.set('fs.s3a.secret.key', app_secret['s3_conf']['secret_access_key'])

    # File path of AWS S3
    file_path = 's3a://' + app_config['s3_conf']['s3_bucket'] + './finances-small'

    # Read data in Data frame
    finances_df = sparkSession.read.parquet(file_path)

    # Print schema and sample records
    finances_df.printSchema()
    print('\n******************* Sample records : \n')
    finances_df.show(5, truncate=False)
