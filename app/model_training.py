import os  # Thêm import này
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.functions import col, lit, unix_timestamp, current_timestamp
from pyspark.ml.feature import IndexToString, StringIndexer  # Thêm StringIndexer cho fallback
import json

class ModelTrainer:
    def __init__(self):
        # Tạo folder models nếu chưa có
        os.makedirs("models", exist_ok=True)
        
        # Set env cho temp dir để tránh lỗi cleanup trên Windows
        os.environ['SPARK_LOCAL_DIRS'] = 'C:/tmp/spark'  # Tạo folder C:/tmp/spark trước nếu cần
        
        self.spark = SparkSession \
            .builder \
            .appName("ModelTraining") \
            .master("local[*]") \
            .config("spark.mongodb.input.uri", "mongodb://root:admin123@localhost:27017/?authSource=admin") \
            .config("spark.mongodb.output.uri", "mongodb://root:admin123@localhost:27017/?authSource=admin") \
            .config("spark.sql.warehouse.dir", "file:///C:/tmp/hive") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
            .getOrCreate()
    
    def load_data_from_mongodb(self, database, collection):
        """Load data from MongoDB"""
        df = self.spark.read \
            .format("com.mongodb.spark.sql.DefaultSource") \
            .option("uri", f"mongodb://root:admin123@localhost:27017/{database}.{collection}?authSource=admin") \
            .load()
        return df
    
    def train_als_model(self, rating_df, max_iter=10, reg_param=0.01, rank=25):
        """Train ALS collaborative filtering model"""
        print("Splitting data...")
        train_data, test_data = rating_df.randomSplit([0.8, 0.2], seed=42)
        
        print(f"Training set: {train_data.count()} records")
        print(f"Test set: {test_data.count()} records")
        
        print("Training ALS model...")
        als = ALS(
            maxIter=max_iter,
            regParam=reg_param,
            rank=rank,
            userCol="reviewerId_index",
            itemCol="asin_index",
            ratingCol="rating",
            coldStartStrategy="drop",
            nonnegative=True
        )
        
        model = als.fit(train_data)
        
        print("Evaluating model...")
        predictions = model.transform(test_data)
        predictions = predictions.na.drop()
        
        evaluator_rmse = RegressionEvaluator(
            metricName="rmse",
            labelCol="rating",
            predictionCol="prediction"
        )
        rmse = evaluator_rmse.evaluate(predictions)
        
        evaluator_mae = RegressionEvaluator(
            metricName="mae",
            labelCol="rating",
            predictionCol="prediction"
        )
        mae = evaluator_mae.evaluate(predictions)
        
        print(f"Root Mean Squared Error (RMSE): {rmse}")
        print(f"Mean Absolute Error (MAE): {mae}")
        
        # Calculate baseline metrics
        target_stats = train_data.selectExpr(
            'percentile(rating, 0.25) as q1',
            'percentile(rating, 0.75) as q3',
            'avg(rating) as mean',
            'stddev(rating) as std_dev'
        ).collect()[0]
        
        iqr = target_stats.q3 - target_stats.q1
        mean = target_stats.mean
        std_dev = target_stats.std_dev
        
        print(f"Interquartile Range (IQR): {iqr}")
        print(f"Mean Rating: {mean}")
        print(f"Standard Deviation: {std_dev}")
        
        return model, rmse, mae
    
    def save_model(self, model, path):
        """Save trained model"""
        model.write().overwrite().save(path)
        print(f"Model saved to {path}")
    
    def extract_item_factors(self, model):
        """Extract item factor vectors from ALS model"""
        ver = model.uid
        ts = unix_timestamp(current_timestamp())
        
        item_factors = model.itemFactors.select(
            col("id"),  # Sửa: dùng "id" đúng (không phải "id" lặp)
            col("features").alias("model_factor"),
            lit(ver).alias("model_version"),
            ts.alias("model_timestamp")
        )
        
        return item_factors
    
    def fit_asin_indexer_fallback(self, rating_df):
        """Fallback: Fit StringIndexer nếu không load được model"""
        print("Fallback: Fitting ASIN indexer on-the-fly...")
        asin_indexer = StringIndexer(
            inputCol="asin", 
            outputCol="asin_index_temp", 
            handleInvalid='keep'
        )
        asin_model = asin_indexer.fit(rating_df.select("asin").distinct())
        return asin_model.labels
    
    def map_indices_to_asins(self, item_factors, asin_labels):
        """Map item indices back to ASINs using labels (sửa để dùng labels trực tiếp)"""
        # Convert indices to ASINs
        converter = IndexToString(
            inputCol="id",
            outputCol="asin",
            labels=asin_labels
        )
        
        result = converter.transform(item_factors)
        result = result.drop("id")
        
        return result
    
    def save_item_vectors_to_mongodb(self, item_vectors, database, collection):
        """Save item factor vectors to MongoDB"""
        item_vectors.write \
            .format("com.mongodb.spark.sql.DefaultSource") \
            .option("uri", f"mongodb://root:admin123@localhost:27017/{database}.{collection}?authSource=admin") \
            .mode("overwrite") \
            .save()
        print(f"Saved {item_vectors.count()} item vectors to {database}.{collection}")  # Sửa: đúng format print
    
    def train_and_save(self, max_iter=10, reg_param=0.01, rank=25):
        """Complete training pipeline"""
        print("Loading rating data from MongoDB...")
        rating_df = self.load_data_from_mongodb("beauty_db", "ratings_encoded")  # Sửa: đúng tên collection (ratings -> ratings_encoded)
        
        print("Training model...")
        model, rmse, mae = self.train_als_model(rating_df, max_iter, reg_param, rank)
        
        print("Saving model...")
        self.save_model(model, "models/als_model")
        
        print("Extracting item factors...")
        item_factors = self.extract_item_factors(model)
        
        # Sửa: Thử load indexer, fallback nếu lỗi
        try:
            print("Loading indexer model...")
            indexer_model = PipelineModel.load("models/indexer_model")
            asin_indexer = indexer_model.stages[1]  # asin indexer
            asin_labels = asin_indexer.labels
        except Exception as e:
            print(f"Error loading indexer: {e}. Using fallback...")
            asin_labels = self.fit_asin_indexer_fallback(rating_df)
        
        print("Mapping indices to ASINs...")
        item_vectors = self.map_indices_to_asins(item_factors, asin_labels)
        
        print("Saving item vectors to MongoDB...")
        self.save_item_vectors_to_mongodb(item_vectors, "beauty_db", "item_vectors")
        
        # Save metrics
        metrics = {
            "rmse": float(rmse),
            "mae": float(mae),
            "model_version": model.uid,
            "rank": rank,
            "max_iter": max_iter,
            "reg_param": reg_param
        }
        
        with open("models/metrics.json", "w") as f:
            json.dump(metrics, f, indent=2)
        
        print("Training complete!")
        return model, metrics
    
    def stop(self):
        self.spark.stop()


if __name__ == "__main__":
    trainer = ModelTrainer()
    model, metrics = trainer.train_and_save(max_iter=10, reg_param=0.01, rank=25)
    trainer.stop()