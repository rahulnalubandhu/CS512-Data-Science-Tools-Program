### Rahul Kumar Nalubandhu
## Final Project

import pyspark
from pyspark.sql import SparkSession
import pprint
import matplotlib.pyplot as plt
import json
from pyspark.sql.types import StructType, FloatType, LongType, StringType, StructField, BooleanType, IntegerType
from pyspark.sql.functions import col, count, mean
from google.cloud import storage
from scipy.stats import f_oneway


from scipy import stats


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

# this is for 3rd question of intrest
# filtering the data to create two separate DataFrames for businesses offering delivery services and those offering takeout services:

delivery_services = df.filter((col("RestaurantsDelivery") == True) & (col("RestaurantsTakeOut") == False))
takeout_services = df.filter((col("RestaurantsDelivery") == False) & (col("RestaurantsTakeOut") == True))

# Both delivery and takeout services are offered (True, True)
both_services = df.filter((col("RestaurantsDelivery") == True) & (col("RestaurantsTakeOut") == True))

# Neither delivery nor takeout services are offered (False, False)
no_services = df.filter((col("RestaurantsDelivery") == False) & (col("RestaurantsTakeOut") == False))

# Calculate the average star rating of the businesses offering delivery services
delivery_avg_rating = delivery_services.agg(mean("stars").alias("avg_delivery_stars")).collect()[0]["avg_delivery_stars"]

# Calculate the average star rating of the businesses offering takeout services
takeout_avg_rating = takeout_services.agg(mean("stars").alias("avg_takeout_stars")).collect()[0]["avg_takeout_stars"]

# Calculate the average star rating of the businesses offering Both services
both_avg_rating = both_services.agg(mean("stars").alias("avg_both_stars")).collect()[0]["avg_both_stars"]

# Calculate the average star rating of the businesses offering neither services
no_avg_rating = no_services.agg(mean("stars").alias("avg_no_stars")).collect()[0]["avg_no_stars"]

# Convert Spark DataFrame to Pandas DataFrame
top_cities_pd = top_cities.toPandas()

# Convert the 'stars' column of each group's DataFrame to Pandas Series to prepare for the t-test:
delivery_ratings_pd = delivery_services.select("stars").toPandas()["stars"]
takeout_ratings_pd = takeout_services.select("stars").toPandas()["stars"]
both_ratings_pd = both_services.select("stars").toPandas()["stars"]
no_services_pd = no_services.select("stars").toPandas()["stars"]

# Perform the one-way ANOVA test
F_statistic, p_value = f_oneway(delivery_ratings_pd, takeout_ratings_pd, both_ratings_pd, no_services_pd)

# Display the results
print(f"F-statistic for all 4 categories: {F_statistic}, P-value: {p_value}")

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
# Rotate the x-axis city labels to 45 degrees
plt.xticks(rotation=90)
# Adjust the margins to accommodate the rotated labels
plt.subplots_adjust(bottom=0.25)
plt.savefig("card_accepting_proportion.png")  # Save the plot to a file

# This code creates a boxplot showing the distribution of ratings for businesses offering 
# delivery services only, takeout services only, both services, and neither service.
plt.figure(figsize=(20, 8))
plt.boxplot([delivery_ratings_pd, takeout_ratings_pd, both_ratings_pd, no_services_pd],
            labels=["Delivery Services", "Takeout Services", "Both Services", "No Services"])
plt.ylabel("Stars")
plt.title("Distribution of Ratings for Delivery, Takeout, Both, and No Services")
plt.savefig("ratings_distribution_anova.png")



# Print summary statistics for each group
print("Summary statistics for delivery_services:")
delivery_services.describe("stars").show()
print("\nSummary statistics for takeout_services:")
takeout_services.describe("stars").show()
print("\nSummary statistics for both_services:")
both_services.describe("stars").show()
print("\nSummary statistics for no_services:")
no_services.describe("stars").show()



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

# source_file_name3 = "ratings_distribution.png"
# destination_blob_name3 = f"images/ratings_distribution.png"

source_file_name4 = "ratings_distribution_anova.png"
destination_blob_name4 = f"images/ratings_distribution_anova.png"

upload_to_gcs(bucket_name, source_file_name1, destination_blob_name1)
upload_to_gcs(bucket_name, source_file_name2, destination_blob_name2)
# upload_to_gcs(bucket_name, source_file_name3, destination_blob_name3)
upload_to_gcs(bucket_name, source_file_name4, destination_blob_name4)





## deletes the temporary files
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)

