### Rahul Kumar Nalubandhu
## Final Project
import pyspark
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import json
from pyspark.sql.types import StructType, FloatType, LongType, StringType, StructField, BooleanType
from pyspark.sql.functions import col, count, mean
from google.cloud import storage

sc = pyspark.SparkContext()

bucket = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
project = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
input_directory = 'gs://{}/hadoop/tmp/bigquerry/pyspark_input'.format(bucket)
output_directory = 'gs://{}/pyspark_demo_output'.format(bucket)

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('Yelp') \
  .getOrCreate()

#update with your project specific settings
conf={
    'mapred.bq.project.id':project,
    'mapred.bq.gcs.bucket':bucket,
    'mapred.bq.temp.gcs.path':input_directory,
    'mapred.bq.input.project.id': 'cs512-project-379819',
    'mapred.bq.input.dataset.id': 'yelp_data',
    'mapred.bq.input.table.id': 'yelp_academic_dataset_business',
}

## pull table from big query
table_data = sc.newAPIHadoopRDD(
    'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf = conf)

## convert table to a json like object, turn PosTime and Fseen back into numbers
vals = table_data.values()
# pprint.pprint(vals.take(5))  #added to help debug whether table was loaded
vals = vals.map(lambda line: json.loads(line))
vals = vals.map(lambda x: {**x, 'review_count': int(x['review_count'])})
vals = vals.map(lambda x: {**x, 'stars': float(x['stars'])})

##schema 
schema = StructType([
   StructField('business_id', StringType(), True),
   StructField('name', StringType(), True),
   StructField('address', StringType(), True),
   StructField('city', StringType(), True),
   StructField('state', StringType(), True),
   StructField('postal_code', StringType(), True),
   StructField('stars', FloatType(), True),
   StructField('review_count', LongType(), True),
   StructField('RestaurantsDelivery', BooleanType(), True),
   StructField('RestaurantsTakeOut', BooleanType(), True),
   StructField('BusinessAcceptsCreditCards', BooleanType(), True),
])

## create a dataframe object
df = spark.createDataFrame(vals, schema= schema)
df.show(10)

# Filter the data // this is for 2nd question
filtered_data = df.filter((col("RestaurantsDelivery") == True) & (col("RestaurantsTakeOut") == True) & (col("review_count") >= 50))
# Group the filtered data by city and calculate the average star rating
city_group = filtered_data.groupBy("city").agg(mean("stars").alias("avg_stars"), count("business_id").alias("restaurant_count"))
# Sort the grouped data by the average star rating in descending order and select the top 50 cities
top_cities = city_group.sort(col("avg_stars").desc()).limit(50)
# Calculate the proportion of highly-rated restaurants that accept card payments
card_accepting = filtered_data.filter(col("BusinessAcceptsCreditCards") == True).groupBy("city").agg(count("business_id").alias("card_accepting_count"))
top_cities = top_cities.join(card_accepting, on="city")
top_cities = top_cities.withColumn("card_accepting_proportion", col("card_accepting_count") / col("restaurant_count"))
# Display the results
top_cities.select("city", "avg_stars", "restaurant_count", "card_accepting_proportion").show()

# Convert Spark DataFrame to Pandas DataFrame
top_cities_pd = top_cities.toPandas()

# Calculate the correlation coefficient between 'avg_stars' and 'card_accepting_proportion'
correlation_coefficient = top_cities_pd['avg_stars'].corr(top_cities_pd['card_accepting_proportion'], method='pearson')
print(f"Correlation coefficient: {correlation_coefficient}")

# Save the plots to image files
plt.figure(figsize=(20, 8))
plt.bar(top_cities_pd["city"], top_cities_pd["avg_stars"])
plt.xlabel("City")
plt.ylabel("Average Star Rating")
plt.title("Average Star Ratings of Top Cities")
plt.xticks(rotation=90)
plt.subplots_adjust(bottom=0.25)
plt.savefig("average_star_ratings.png")  # Save the plot to a file

# Create a scatter plot
plt.figure(figsize=(20, 8))
plt.scatter(top_cities_pd["city"], top_cities_pd["card_accepting_proportion"])
plt.xlabel("City")
plt.ylabel("Proportion of Card Accepting Restaurants")
plt.title("Relationship Between City and Card Accepting Restaurants Proportion in Top Cities")
plt.xticks(rotation=90)
plt.subplots_adjust(bottom=0.25)
plt.savefig("card_accepting_proportion.png")  # Save the plot to a file

# Upload the plots to Google
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

bucket_name = "yelp_data_proj"

source_file_name1 = "average_star_ratings.png"
destination_blob_name1 = f"images/average_star_ratings.png"

source_file_name2 = "card_accepting_proportion.png"
destination_blob_name2 = f"images/card_accepting_proportion.png"

upload_to_gcs(bucket_name, source_file_name1, destination_blob_name1)
upload_to_gcs(bucket_name, source_file_name2, destination_blob_name2)

## deletes the temporary files
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)

