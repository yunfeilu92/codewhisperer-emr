## write a pyspark to count the total number of records in csv file using spark

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *


# method to read s3 csv file and store it in df
def read_s3_csv_file(bucket_name, file_name):
    # create a spark session
    spark = SparkSession.builder.appName("Reading s3 csv file").getOrCreate()
    # read the csv file
    df = spark.read.csv("s3://{}/{}".format(bucket_name, file_name), header=True, inferSchema=True)
    # stop the spark session
    spark.stop()
    return df

# method to store df as parquet file
def store_df_as_parquet(df, bucket_name, file_name):
    # write the parquet file
    df.write.parquet("s3://{}/{}".format(bucket_name, file_name))

# method to count the total number of records in df
def count_records_df(df):
    # count the number of records
    print(df.count())

# method to print schema of df
def print_schema_df(df):
    # print the schema
    df.printSchema()


# method to convert field name to lowercase
def convert_field_name_to_lowercase_df(df):
    # convert the original field name to lowercase
    df = df.toDF(*[col.lower() for col in df.columns])
    return df



# main method to start spark and call read_s3_csv_file, store_df_as_parquet, count_records_df, print_schema_df, convert_field_name_to_lowercase_df methods
if __name__ == "__main__":
    # create a spark session
    spark = SparkSession.builder.appName("Spark ETL").getOrCreate()
    # read the csv file
    df = read_s3_csv_file("yunfeilu-codewhisperer", "emr/data/tripdata.csv")
    # store the df as parquet file
    store_df_as_parquet(df, "yunfeilu-codewhisperer", "emr/data/parquet/tripdata.parquet")
    # count the total number of records
    count_records_df(df)
    # print the schema
    print_schema_df(df)
    # convert the original field name to lowercase
    df = convert_field_name_to_lowercase_df(df)
    # stop the spark session
    spark.stop()





