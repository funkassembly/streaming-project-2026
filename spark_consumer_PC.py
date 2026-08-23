from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count, min, max, floor
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# Μπορειτε να χρησιμοποιειτε παρενθέσεις ( ... ) γύρω από μπλοκ κώδικα για να επιτρέψετε αλλαγή γραμμής (multi-line statement) χωρίς να χρειάζεται να χρησιμοποιείτε τον χαρακτήρα backslash \


# 1. Ορισμός του Schema
## vehicles_to_pandas returns a pd.DataFrame:
#             A DataFrame containing the travel logs of vehicles, with the columns:
#
#             - 'name': the name of the vehicle (platoon).
#             - 'dn': the platoon size.
#             - 'orig': the origin node of the vehicle's trip.
#             - 'dest': the destination node of the vehicle's trip.
#             - 't': the timestep.
#             - 'link': the link the vehicle is on (or relevant status).
#             - 'x': the position of the vehicle on the link.
#             - 's': the spacing of the vehicle.
#             - 'v': the speed of the vehicle.
schema = StructType() \
    .add("name", StringType()) \
    .add("dn", IntegerType()) \
    .add("orig", StringType()) \
    .add("dest", StringType()) \
    .add("t", DoubleType()) \
    .add("link", StringType()) \
    .add("x", DoubleType()) \
    .add("s", DoubleType()) \
    .add("v", DoubleType())


# 2. Δημιουργία Spark Session
spark = (
    SparkSession.builder
    .appName("UXSIM-Consumer")
    .master("spark://spark-master:7077")
    .config("spark.mongodb.write.connection.uri", "mongodb://mongo:27017")
    .getOrCreate()
)

# 3. Σύνδεση στον Redpanda (Kafka-compatible)
# Μπορείτε να αντικαταστήσετε το 'uxsim' με όνομα του topic της επιλογής σας
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "redpanda:9092")
    .option("subscribe", "vehicle_positions")
    .option("startingOffsets", "latest")
    .load()
)

# 4. Parsing του JSON και Μετασχηματισμός
# Χρησιμοποιείστε "cast" για να μετατρεψετε τις raw binary τιμες σε ευαναγνωστο JSON string
# Εφαρμοστε το schema στο string, δημιουργώντας ενα μοναδικό struct με την ονομασία data
# Καθε key το JSON (name, orig, dest, etc.) μετατρεπεται σε ξεχωριστη στήλη στο DataFrame.

parsed = (df.select(from_json(col("value").cast("string"),schema).alias("data")).select("data.*"))

# 5. Υπολογισμός Στατιστικών ανά Ακμή (link) και Χρόνο (t)
# t = time από την εξομοίωση, v = ταχύτητα οχήματος
# Στον παρακάτω κώδικα χρησιμοποιούμε το πεδίο t της UXsim ως "time". Αν η εξομοίωση στέλνει δεδομένα πολύ γρήγορα, το Spark θα τα ομαδοποιεί σωστά ανά simulation step.
stats = (
	parsed
	.groupBy("t", "link")
	.agg(
		count("*").alias("vcount"),
		avg("v").alias("vspeed"),
		min("v").alias("min_speed"),
		max("v").alias("max_speed")
	)
)

#bonus 
windowed_stats = (
    parsed
    .withColumn(
        "window_start",
        floor(col("t") / 30) * 30
    )
    .groupBy("window_start", "link")
    .agg(
        count("*").alias("vcount"),
        avg("v").alias("vspeed"),
        min("v").alias("min_speed"),
        max("v").alias("max_speed")
    )
)

# 6α. Αποθήκευση στη MongoDB των αρχικών δεδομενων
def write_raw_to_mongo(batch_df, batch_id):
	(
	    batch_df.write
	    .format("mongodb")
	    .mode("append")
	    .option("database", "traffic")
	    .option("collection", "raw_data")
	    .save()
	)

query_raw = (
	parsed.writeStream
	.foreachBatch(write_raw_to_mongo)
	.outputMode("append")
	.option("checkpointLocation", "/tmp/checkpoints/raw_data")
	.start()
	)


# 6β. Αποθήκευση στη MongoDB των επεξεργασμενων δεδομενων
# Το outputMode("append") στο streaming απαιτεί τη χρήση Watermarks (χρονικά όρια) για να ξέρει το Spark πότε "έκλεισε" ένα group και μπορεί να το γράψει.
# Διαφορετικά, χρησιμοποιούμε το .outputMode("update"), το οποίο γράφει στη MongoDB μόνο τα links που άλλαξαν οι τιμές τους.
def write_stats_to_mongo(batch_df, batch_id):
	(
		batch_df.write
		.format("mongodb")
		.mode("append")
		.option("database", "traffic")
		.option("collection", "stats")
		.save()
	)

query_stats = (
	stats.writeStream
	.foreachBatch(write_stats_to_mongo)
	.outputMode("update")
	.option("checkpointLocation", "/tmp/checkpoints/stats")
	.start()
	)

#bonus mongodb stats
def write_windowed_to_mongo(batch_df, batch_id):

    (
        batch_df.write
        .format("mongodb")
        .mode("append")
        .option("database", "traffic")
        .option("collection", "window_stats")
        .save()
    )


query_windowed = (
    windowed_stats.writeStream
    .foreachBatch(write_windowed_to_mongo)
    .outputMode("update")
    .option(
        "checkpointLocation",
        "/tmp/checkpoints/window_stats"
    )
    .start()
)

# 7. Προβολή στην κονσόλα για debugging (προαιρετικά)
query_console = (
    stats.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/checkpoints/console")
    .start()
)


# Αναμονή για τον τερματισμό όλων των queries
#spark.streams.awaitAnyTermination() # query_mongo.awaitTermination()
query_console.awaitTermination()
