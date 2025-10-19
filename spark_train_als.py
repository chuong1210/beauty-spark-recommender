# spark_train_als.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as spark_max
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.ml.evaluation import RegressionEvaluator
import json
import os

# --- Config ---
MONGO_URI = "mongodb://127.0.0.1/amazon.reviews"   # db.collection
MONGO_DB = "amazon"
MONGO_COLL = "reviews"
SAMPLE_SIZE = 500000   # giảm nếu máy yếu
MODEL_PATH = "/tmp/als_model"         # lưu model
USER_MAP_PATH = "/tmp/user_map.json"  # lưu mapping
ITEM_MAP_PATH = "/tmp/item_map.json"
ITEM_VECTORS_PATH = "/tmp/item_vectors.jsonl"  # tạm lưu trước khi index ES
RANK = 50
MAX_ITER = 10
REG = 0.1

# --- Spark session ---
spark = SparkSession.builder \
    .appName("ALS_Train") \
    .master("local[*]") \
    .config("spark.driver.memory", "12g") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

# 1) Read sample from MongoDB
print(">>> Đọc dữ liệu từ MongoDB (sample)...")
df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
    .option("uri", f"mongodb://127.0.0.1/{MONGO_DB}.{MONGO_COLL}") \
    .option("pipeline", f"[{{'$sample': {{'size': {SAMPLE_SIZE}}}}}]") \
    .load()

print("Count sample:", df.count())
df.printSchema()
df.show(3, truncate=120)

# 2) Select + clean essential columns
# NOTE: adjust field names to your jsonl (your review file uses rating,user_id,asin,timestamp,...)
df = df.select(
    col("user_id").alias("user_id"),
    col("asin").alias("asin"),
    col("rating").alias("rating"),
    col("timestamp").alias("timestamp")
)

df = df.dropna(subset=["user_id", "asin", "rating"])
df = df.withColumn("rating", col("rating").cast(DoubleType()))

# 3) Aggregate for duplicates (user-item) -> take avg rating and latest timestamp
print(">>> Aggregate duplicates (avg rating, latest timestamp)...")
agg = df.groupBy("user_id", "asin").agg(
    avg("rating").alias("rating"),
    spark_max("timestamp").alias("latest_ts")
)

print("After agg:")
agg.show(5, truncate=120)

# 4) Indexing (StringIndexer) - create numeric user_idx and item_idx
print(">>> Fitting StringIndexer for users and items...")
user_indexer = StringIndexer(inputCol="user_id", outputCol="user_idx", handleInvalid='keep').fit(agg)
agg = user_indexer.transform(agg)
item_indexer = StringIndexer(inputCol="asin", outputCol="item_idx", handleInvalid='keep').fit(agg)
agg = item_indexer.transform(agg)

# cast index to integer
agg = agg.withColumn("user_idx", col("user_idx").cast(IntegerType())) \
         .withColumn("item_idx", col("item_idx").cast(IntegerType()))

agg = agg.select("user_id", "asin", "user_idx", "item_idx", "rating", "latest_ts")
agg.show(5, truncate=120)

# 5) Save mapping (label -> index) as JSON for serving
print(">>> Lưu mapping user/item ra JSON...")
user_labels = user_indexer.labels  # list
item_labels = item_indexer.labels

# build mapping dict (label->idx)
user_map = {label: idx for idx, label in enumerate(user_labels)}
item_map = {label: idx for idx, label in enumerate(item_labels)}

with open(USER_MAP_PATH, "w", encoding="utf-8") as f:
    json.dump(user_map, f)

with open(ITEM_MAP_PATH, "w", encoding="utf-8") as f:
    json.dump(item_map, f)

print("Saved user_map to", USER_MAP_PATH)
print("Saved item_map to", ITEM_MAP_PATH)

# 6) Prepare final ratings DataFrame for ALS
ratings = agg.select(col("user_idx").alias("user"), col("item_idx").alias("item"), col("rating").alias("rating"))
print("ratings sample:")
ratings.show(5)

# 7) Train-test split
train, test = ratings.randomSplit([0.8, 0.2], seed=42)
print("Train count:", train.count(), "Test count:", test.count())

# 8) Train ALS
print(">>> Training ALS model...")
als = ALS(
    maxIter=MAX_ITER,
    regParam=REG,
    rank=RANK,
    userCol="user",
    itemCol="item",
    ratingCol="rating",
    coldStartStrategy="drop",
    nonnegative=False
)
model = als.fit(train)

# 9) Evaluate
predictions = model.transform(test).na.drop()
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("RMSE =", rmse)

# 10) Save model (ALSModel)
print(">>> Saving ALS model...")
if os.path.exists(MODEL_PATH):
    import shutil
    shutil.rmtree(MODEL_PATH)
model.save(MODEL_PATH)
print("Model saved to", MODEL_PATH)

# 11) Export item factors (for ES)
print(">>> Export item factors...")
item_factors = model.itemFactors  # columns: id, features
# id is item index (int) matching item_idx
item_factors = item_factors.withColumnRenamed("id", "item_idx")
# create spark mapping df (item_idx -> asin)
mapping_rows = [(int(idx), label) for idx, label in enumerate(item_labels)]
mapping_df = spark.createDataFrame(mapping_rows, schema=["item_idx", "asin"])
item_with_asin = item_factors.join(mapping_df, on="item_idx", how="left")
# write to local jsonl for bulk index to ES
item_pd = item_with_asin.select("asin", "features").toPandas()
item_pd.to_json(ITEM_VECTORS_PATH, orient="records", lines=True)
print("Item vectors saved to", ITEM_VECTORS_PATH)

print(">>> Done.")
spark.stop()
