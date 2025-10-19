from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType, ArrayType
from pyspark.sql.functions import col, lit, unix_timestamp, current_timestamp
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
import os

class DataProcessor:
    def __init__(self):
        self.spark = SparkSession \
            .builder \
            .appName("BeautyRecommendation") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
            .config("spark.mongodb.input.uri", "mongodb://root:admin123@localhost:27017/?authSource=admin") \
            .config("spark.mongodb.output.uri", "mongodb://root:admin123@localhost:27017/?authSource=admin") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .getOrCreate()
    
    def load_review_data(self, file_path):
        """Load review data from JSONL file"""
        review_schema = StructType([
            StructField("rating", DoubleType(), True),
            StructField("title", StringType(), True),
            StructField("text", StringType(), True),
            StructField("images", ArrayType(StringType()), True),
            StructField("asin", StringType(), True),
            StructField("parent_asin", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("timestamp", LongType(), True),
            StructField("helpful_vote", LongType(), True),
            StructField("verified_purchase", BooleanType(), True)
        ])
        
        df = self.spark.read.json(file_path, schema=review_schema)
        return df
    
    def load_meta_data(self, file_path):
        """Load product metadata from JSONL file"""
        meta_schema = StructType([
            StructField("main_category", StringType(), True),
            StructField("title", StringType(), True),
            StructField("average_rating", DoubleType(), True),
            StructField("rating_number", LongType(), True),
            StructField("features", ArrayType(StringType()), True),
            StructField("description", ArrayType(StringType()), True),
            StructField("price", StringType(), True),
            StructField("images", ArrayType(StructType([
                StructField("thumb", StringType(), True),
                StructField("large", StringType(), True),
                StructField("variant", StringType(), True),
                StructField("hi_res", StringType(), True)
            ])), True),
            StructField("store", StringType(), True),
            StructField("categories", ArrayType(StringType()), True),
            StructField("details", StringType(), True),
            StructField("parent_asin", StringType(), True),
            StructField("bought_together", StringType(), True)
        ])
        
        df = self.spark.read.json(file_path, schema=meta_schema)
        return df
    
    def save_to_mongodb(self, df, database, collection):
            """Save DataFrame to MongoDB using format 'mongodb'"""
            df.write \
                .format("com.mongodb.spark.sql.DefaultSource") \
                .option("database", database) \
                .option("collection", collection) \
                .mode("overwrite") \
                .save()
            print(f"Saved {df.count()} records to {database}.{collection}")
    
    def prepare_rating_data(self, review_df):
        """Prepare rating data for collaborative filtering"""
        # Select relevant columns and rename
        rating_df = review_df.select(
            col("asin"),
            col("user_id").alias("reviewerId"),
            col("rating")
        ).dropDuplicates(["asin", "reviewerId"])
        
        # Remove null values
        rating_df = rating_df.na.drop()
        
        return rating_df
    def encode_features(self, rating_df):
        """Encode string features to indices"""
        indexers = [
            StringIndexer(inputCol="reviewerId", outputCol="reviewerId_index", handleInvalid='keep'),
            StringIndexer(inputCol="asin", outputCol="asin_index", handleInvalid='keep')
        ]
        
        pipeline = Pipeline(stages=indexers)
        model = pipeline.fit(rating_df)
        transformed_df = model.transform(rating_df)
        
        # Comment out to avoid Windows native IO error
        # model.write().overwrite().save("models/indexer_model")
        model.write().overwrite().save(r"C:\Users\chuon\Downloads\DoAn_BigDaTa\BigData\models\indexer_model")
        return transformed_df, model
    
    def balance_dataset(self, df):
        """Balance dataset using upsampling (optional)"""
        from pyspark.sql.functions import col
        
        # Count ratings distribution
        df.groupBy("rating").count().orderBy(col("count").desc()).show()
        
        # Optional: implement upsampling if needed
        # For now, return original df
        return df
    

    def process_all(self, review_path, meta_path):
        """Complete data processing pipeline"""
        # Convert to absolute paths for reliability
        review_abs_path = os.path.abspath(review_path)
        meta_abs_path = os.path.abspath(meta_path)
        
        # Validate files exist
        if not os.path.exists(review_abs_path):
            raise FileNotFoundError(f"Review file not found: {review_abs_path}. Download from https://jmcauley.ucsd.edu/data/amazon/")
        if not os.path.exists(meta_abs_path):
            raise FileNotFoundError(f"Metadata file not found: {meta_abs_path}. Download from https://jmcauley.ucsd.edu/data/amazon/")
        
        print("Loading review data...")
        review_df = self.load_review_data(review_abs_path)
        print(f"Loaded {review_df.count()} reviews")
        
        print("Loading metadata...")
        meta_df = self.load_meta_data(meta_abs_path)
        print(f"Loaded {meta_df.count()} products")
        
        print("Saving to MongoDB...")
        self.save_to_mongodb(review_df, "beauty_db", "reviews")
        self.save_to_mongodb(meta_df, "beauty_db", "products")
        
        print("Preparing rating data...")
        rating_df = self.prepare_rating_data(review_df)
        
        print("Encoding features...")
        encoded_df, indexer_model = self.encode_features(rating_df)
        
        print("Saving processed data...")
        self.save_to_mongodb(encoded_df, "beauty_db", "ratings_encoded")
        
        print("Data processing complete!")
        return encoded_df, meta_df, indexer_model
    
    def stop(self):
        self.spark.stop()


if __name__ == "__main__":
    processor = DataProcessor()
    
    # Process data
    review_file = "data/All_Beauty.jsonl"
    meta_file = "data/meta_All_Beauty.jsonl"
    
    encoded_df, meta_df, indexer = processor.process_all(review_file, meta_file)
    
    processor.stop()