from pyspark.sql import SparkSession
from pyspark.sql.types import *
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("NycAirbnb").getOrCreate()

user_schema = StructType([StructField('id', IntegerType(), False), StructField('name', StringType(), True), StructField('host_id', IntegerType(), True), StructField('host_name', StringType(), True),
                         StructField('neighbourhood_group', StringType(), True), StructField('neighbourhood', StringType(), True), StructField('latitude', FloatType(), True), StructField('longitude', FloatType(), True),
                         StructField('room_type', StringType(), True), StructField('price', IntegerType(), True), StructField('minimum_nights', IntegerType(), True), StructField('number_of_reviews', IntegerType(), True),
                         StructField('last_review', StringType(), True), StructField('reviews_per_month', FloatType(), True), StructField('calculated_host_listings_count', IntegerType(), True), StructField('availability_365', IntegerType(), True)])


input_directory = "/Volumes/borys/default/ab_nyc/"
checkpoint_path = f"/tmp/borys/_checkpoint/ab_nyc"
DeltaTable.createIfNotExists(spark).tableName("bronze_table").addColumn("id", "INT").addColumn("name", "STRING").addColumn("host_id", "STRING").addColumn("host_name", "STRING").addColumn("neighbourhood_group", "STRING").addColumn("neighbourhood", "STRING").addColumn("latitude", "FLOAT").addColumn("longitude", "FLOAT").addColumn("room_type", "STRING").addColumn("price", "INT").addColumn("minimum_nights", "INT").addColumn("number_of_reviews", "INT").addColumn("last_review", "DATE").addColumn("reviews_per_month", "FLOAT").addColumn("calculated_host_listings_count", "INT").addColumn("availability_365", "INT").execute()


df = (spark.readStream
      .format("csv").schema(user_schema)
      .option("header", "true").option("checkpointLocation", checkpoint_path)
      .load(input_directory))

query = (df.writeStream
          .format("delta").option("checkpointLocation", checkpoint_path)
          .outputMode("append") 
          .table("bronze_table")
          .start())

query.awaitTermination()

delta_table_df = spark.read.table("bronze_table")

def modify_df(delta_table_df):
    filtered_zeros_df = delta_table_df.filter("price >= 0")
    to_date_df = filtered_zeros_df.withColumn("last_review", to_date(col("last_review"), "yyyy-mm-dd"))
    min_date = to_date_df.select(spark_min(col('last_review'))).collect()[0][0]
    filled_nulls_df = to_date_df.fillna({'last_review': min_date, 'reviews_per_month': 0})
    df_clean = filled_nulls_df.filter(col('latitude').isNotNull() & col('longitude').isNotNull())
    return df_clean

silver_table_df = modify_df(delta_table_df)

%sql
USE default
CREATE TABLE IF NOT EXISTS silver_table (
    id STRING,                        
    name STRING,                     
    host_id STRING,                   
    host_name STRING,               
    neighbourhood_group STRING,       
    neighbourhood STRING,             
    latitude FLOAT,                  
    longitude FLOAT,                  
    room_type STRING,                
    price INT,                       
    minimum_nights INT,               
    number_of_reviews INT,            
    last_review DATE,                 
    reviews_per_month FLOAT,          
    availability_365 INT
);

%python
silver_table_df.write.mode("overwrite").saveAsTable("silver_table")
