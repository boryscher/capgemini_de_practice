from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import to_date, col, min as spark_min, when
import os
import shutil

spark = SparkSession.builder.appName('PySpark_task').getOrCreate()

static_csv_path = "/home/borys/Desktop/processed/AB_NYC_2019_ETL.csv"
temp_csv_file = "/home/borys/Desktop/processed/AB_NYC_2019_ETL_temp.csv"
initial_streaming_dir = "/home/borys/Desktop/raw"
output_path = "/home/borys/Desktop/processed/temp"
checkpoint_initial = "/home/borys/Desktop/processed/checkpoint_dir"
archived_dir = "/home/borys/Desktop/processed/archive"
output_path_parquet = "/home/borys/Desktop/processed/AB_NYC_2019.parquet"



user_schema = StructType([StructField('id', IntegerType(), False), StructField('name', StringType(), True), StructField('host_id', IntegerType(), True), StructField('host_name', StringType(), True),
                         StructField('neighbourhood_group', StringType(), True), StructField('neighbourhood', StringType(), True), StructField('latitude', FloatType(), True), StructField('longitude', FloatType(), True),
                         StructField('room_type', StringType(), True), StructField('price', IntegerType(), True), StructField('minimum_nights', IntegerType(), True), StructField('number_of_reviews', IntegerType(), True),
                         StructField('last_review', StringType(), True), StructField('reviews_per_month', FloatType(), True), StructField('calculated_host_listings_count', IntegerType(), True), StructField('availability_365', IntegerType(), True)])

streaming_csv_file = spark.readStream.schema(user_schema).format('csv').option('header', True).option('cleanSource', 'archive').option('sourceArchiveDir', archived_dir).load(initial_streaming_dir)
query = streaming_csv_file.writeStream.format('csv').outputMode('append').option('path', output_path).option('checkpointLocation', checkpoint_initial).trigger(processingTime='10 seconds').start()
query.awaitTermination()
run_data_df = spark.read.csv(output_path, header=True, inferSchema=True)

if os.path.exists(static_csv_path):
    existing_data_df = spark.read.format('csv').option('header', True).schema(user_schema).load(static_csv_path)
    columns = streaming_csv_file.columns
    join_conditions = [run_data_df[col_name] == existing_data_df[col_name] for col_name in columns]
    combined_df = run_data_df.join(existing_data_df, join_conditions, "left")
else:
    combined_df = run_data_df

combined_df.write.mode("overwrite").option("header", "true").csv(temp_csv_file)

def delete_directory(directory_path):
    if os.path.exists(directory_path):
        shutil.rmtree(directory_path)



def replace_file(temp_path, final_path):
    if os.path.exists(final_path):
        os.remove(final_path)
    for filename in os.listdir(temp_path):
        temp_file_path = os.path.join(temp_path, filename)
        shutil.move(temp_file_path, final_path)

replace_file(temp_csv_file, static_csv_path)
delete_directory(output_path)


def modify_df(combined_df):
    filtered_zeros_df = combined_df.filter("price >= 0")
    to_date_df = filtered_zeros_df.withColumn("last_review", to_date(col("last_review"), "yyyy-mm-dd"))
    min_date = to_date_df.select(spark_min(col('last_review'))).collect()[0][0]
    filled_nulls_df = to_date_df.fillna({'last_review': min_date, 'reviews_per_month': 0})
    df_clean = filled_nulls_df.filter(col('latitude').isNotNull() & col('longitude').isNotNull())
    return df_clean

cleaned_data = modify_df(combined_df)

cleaned_data = cleaned_data.withColumn(
    "price_range",
    when(col("price") < 200, "Low")
    .when((col("price") >= 200) & (col("price") < 500), "Medium")
    .when((col("price") >= 500) & (col("price") < 1500), "High").when((col("price") >= 1500) & (col("price") < 6000), 'Luxury')
    .otherwise("Abnormaly high")
)

cleaned_data = cleaned_data.withColumn(
    "price_per_review",
    col("price") / col("number_of_reviews")
)

cleaned_data.createOrReplaceTempView("pyspark_cleaned_data")

sql_query = """
SELECT neighbourhood_group, COUNT(id) AS num_listings
FROM pyspark_cleaned_data
GROUP BY neighbourhood_group
ORDER BY num_listings DESC
"""

list_by_neigh_group = spark.sql(sql_query)


sql_query = """
SELECT name, price
FROM pyspark_cleaned_data
ORDER BY price DESC
LIMIT 10
"""

top_10_listings = spark.sql(sql_query)

sql_query = """
SELECT neighbourhood_group, room_type, AVG(price) AS avg_price
FROM pyspark_cleaned_data
GROUP BY neighbourhood_group, room_type
ORDER BY neighbourhood_group, room_type
"""

avg_price_by_room_type = spark.sql(sql_query)

cleaned_data_repartitioned = cleaned_data.repartition("neighbourhood_group")

cleaned_data_repartitioned.write.mode("overwrite").partitionBy("neighbourhood_group").parquet(output_path_parquet)
