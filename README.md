#Να υλοποιηθεί το Query 3 χρησιμοποιώντας α) τo DataFrame API και β) το SQL API. Πειραματιστείτε κάνοντας την εισαγωγή των δεδομένων με χρήση αρχείων csv και parquet και σχολιάστε πώς επηρεάζεται η εκτέλεση
#Παρατηρούμε οτι με parquet είναι δέκα λεπτά πιο γρήγορο

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Parquet Analysis") \
    .getOrCreate()
df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/spetroutsatos/data/parquet/yellow_tripdata_2024")
df_zone = spark.read.parquet("hdfs://hdfs-namenode:9000/user/spetroutsatos/data/parquet/taxi_zone_lookup")


df_pickup = df_zone.alias("pickup_zone")
df_dropoff = df_zone.alias("dropoff_zone")

df_joined = df \
    .join(df_pickup, df["PULocationID"] == col("pickup_zone.LocationID"), "inner") \
    .join(df_dropoff, df["DOLocationID"] == col("dropoff_zone.LocationID"), "inner")

df_same_borough = df_joined.filter(
    col("pickup_zone.Borough") == col("dropoff_zone.Borough")
)

df_result = df_same_borough.groupBy(col("pickup_zone.Borough").alias("Borough")) \
    .count() \
    .orderBy("count", ascending=False)

df_result.show(5)
df.explain(True)


##### ME SQL #####
df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/spetroutsatos/data/parquet/yellow_tripdata_2024")
df_zone = spark.read.parquet("hdfs://hdfs-namenode:9000/user/spetroutsatos/data/parquet/taxi_zone_lookup")

df.createOrReplaceTempView("yellow_tripdata_2024")
df_zone.createOrReplaceTempView("taxi_zone_lookup")

result = spark.sql("""
        SELECT z.Borough, COUNT(*) as total_trips
        FROM yellow_tripdata_2024 t
        JOIN taxi_zone_lookup z ON t.PULocationID = z.LocationID
        JOIN taxi_zone_lookup l ON t.DOLocationID = l.LocationID
        WHERE z.Borough = l.Borough
        GROUP BY z.Borough
        ORDER BY total_trips DESC
""")
result.show(5)


#####################################################################
######### ME CSV ####################################################

df = spark.read.csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv", header=True, inferSchema=True)
df_zone = spark.read.csv("hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv", header=True, inferSchema=True)

df_pickup = df_zone.alias("pickup_zone")
df_dropoff = df_zone.alias("dropoff_zone")

df_joined = df \
    .join(df_pickup, df["PULocationID"] == col("pickup_zone.LocationID"), "inner") \
    .join(df_dropoff, df["DOLocationID"] == col("dropoff_zone.LocationID"), "inner")

df_same_borough = df_joined.filter(
    col("pickup_zone.Borough") == col("dropoff_zone.Borough")
)

df_result = df_same_borough.groupBy(col("pickup_zone.Borough").alias("Borough")) \
    .count() \
    .orderBy("count", ascending=False)

df_result.show(5)

##### ME SQL #####
df = spark.read.csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv", header=True, inferSchema=True)
df_zone = spark.read.csv("hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv", header=True, inferSchema=True)

df.createOrReplaceTempView("yellow_tripdata_2024")
df_zone.createOrReplaceTempView("taxi_zone_lookup")

result = spark.sql("""
        SELECT z.Borough, COUNT(*) as total_trips
        FROM yellow_tripdata_2024 t
        JOIN taxi_zone_lookup z ON t.PULocationID = z.LocationID
        JOIN taxi_zone_lookup l ON t.DOLocationID = l.LocationID
        WHERE z.Borough = l.Borough
        GROUP BY z.Borough
        ORDER BY total_trips DESC
""")

result.show(5)







