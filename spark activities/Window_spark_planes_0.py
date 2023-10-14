### Starter code for CS 512 Spark Planes Distance - part 1
### This code comes with no promises! 
### It was verfied by the instructors, but things change often in Google Cloud Computing
### It should be a suitable starting point for most students to get this assignment started
### This is not the only way to solve this problem either, so don't feel like you have to use this code 
import pyspark
from pyspark.sql import SparkSession
import pprint
import json
from pyspark.sql.types import StructType, FloatType, LongType, StringType, StructField
from pyspark.sql import Window
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.functions import lead, udf, struct, col

### haversine distance
def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return float(c * r)

def To_numb(x):
  x['PosTime'] = int(x['PosTime'])
  x['FSeen'] = int(x['FSeen'])
  x['Lat'] = float(x['Lat'])
  x['Long'] = float(x['Long'])
  return x

sc = pyspark.SparkContext()

#PACKAGE_EXTENSIONS= ('gs://hadoop-lib/bigquery/bigquery-connector-hadoop2-latest.jar')

bucket = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
project = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
input_directory = 'gs://{}/hadoop/tmp/bigquerry/pyspark_input'.format(bucket)
output_directory = 'gs://{}/pyspark_demo_output'.format(bucket)

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('flights') \
  .getOrCreate()

#update with your project specific settings
conf={
    'mapred.bq.project.id':project,
    'mapred.bq.gcs.bucket':bucket,
    'mapred.bq.temp.gcs.path':input_directory,
    'mapred.bq.input.project.id': 'cs512-project-379819',
    'mapred.bq.input.dataset.id': 'aircraft_data',
    'mapred.bq.input.table.id': 'plane_data_20230308_013230', 
}

## pull table from big query
table_data = sc.newAPIHadoopRDD(
    'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf = conf)

## convert table to a json like object, turn PosTime and Fseen back into numbers
vals = table_data.values()
pprint.pprint(vals.take(5))  #added to help debug whether table was loaded
vals = vals.map(lambda line: json.loads(line))
vals = vals.map(To_numb)

##schema 
schema = StructType([
   StructField('FSeen', LongType(), True),
   StructField("Icao", StringType(), True),
   StructField("Lat", FloatType(), True),
   StructField("Long", FloatType(), True),
   StructField("PosTime", LongType(), True)])

## create a dataframe object
df1 = spark.createDataFrame(vals, schema= schema)


df1.repartition(6) 
pprint.pprint(vals.take(5))


## deletes the temporary files
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)