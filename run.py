import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf


def get_unit(unit):
    if unit == 'Ton':
        return 'Kgs'
    return unit

def get_quantity(quantity, unit):
    if unit == 'Ton':
        return quantity * 1000
    return quantity

def run(filename):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    files_path = os.path.join(dir_path, "files")
    file_path = os.path.join(files_path, filename)
    if not os.path.isfile(file_path):
        exit(-1)
    file_parts = filename.split('_')
    trade_type = file_parts[1]
    year = int(file_parts[2])

    spark = SparkSession \
        .builder \
        .appName("Python Spark - Import export") \
        .getOrCreate()

    # read in file using csv format
    df = spark.read.load(file_path,
                         format='csv',
                         header='true',
                         inferSchema='true')
 
    df = df.withColumn('trade_type', lit(trade_type))
    df = df.withColumn('year', lit(year))

    get_unit_udf = udf(get_unit, StringType())
    get_quantity_udf = udf(get_quantity, StringType())

    df = df.withColumn('unit_old', get_unit_udf(df['unit']))
    df = df.withColumn('unit', get_unit_udf(df['unit']))
    df = df.withColumn('quantity', get_quantity_udf(df['quantity'], df['unit']))
    df = df.withColumn('quantity', df['quantity'].cast("double"))

    df.createOrReplaceTempView("trade")

    # overall
    sqlDF = spark.sql(
        "SELECT year, trade_type, country_code, country_name, pc_code, pc_description, unit, unit_old, "
        "       count(*) AS nr_products, "
        "       SUM(quantity) AS total_quantity "
        "FROM trade "
        "WHERE quantity IS NOT NULL "
        "AND country_name != 'Unspecified' "        
        "GROUP BY year, trade_type, country_code, country_name, pc_code, pc_description, unit, unit_old "
        "ORDER BY  year, trade_type, country_code, country_name, pc_code, pc_description, unit, unit_old "
    )
    sqlDF = sqlDF.coalesce(4)

    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", "<AWS KEY ID>")
    sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", ""<AWS KEY SECRET>")
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3-<REGIOIN>.amazonaws.com")

    # Now comes writing to Redshift
    sqlDF.write \
        .format("com.databricks.spark.redshift") \
        .option("temporary_aws_access_key_id", "<AWS KEY ID>") \
        .option("temporary_aws_secret_access_key", "<AWS KEY SECRET>") \
        .option("url", "jdbc:postgresql://<HOST>:<PORT>/<DB>?user=<DB_USER>&password=<DB_USER_PWD>") \
        .option("dbtable", "trade") \
        .option("tempdir", "s3a://<BUCKET/PATH/TO/DIR>") \
        .option("aws_iam_role", "<IAM ROLE FOR REDSHIFT>") \
        .mode("append") \
        .save()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print('Filename not provided')
        exit(-1)
    run(sys.argv[1])
