from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

def make_spark():
    spark = SparkSession.builder\
    .master("yarn")\
    .config("spark.driver.cores", 1)\
    .config("spark.executor.instances", 10)\
    .config("spark.executor.memory", '5g')\
    .appName("demo_spark")\
    .getOrCreate()

    return spark

def main():
    spark = make_spark()

    # read in data
    df = spark.read.csv('data/csv/Subset_reviews.csv', header='True')
   
    # filter data for proper scores
    df = df.where(F.col('Score').isin([0,1,2,3,4,5]))

    # restructure as faster parquet
    df.write.mode('overwrite').parquet('data/parquet/')

    # read parquet file back in
    df = spark.read.parquet('data/parquet')

    df.groupBy('Score').count().sort(F.col('count').desc()).show()

if __name__ == '__main__':
    main()
