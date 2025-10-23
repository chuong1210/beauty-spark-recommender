from elasticsearch import Elasticsearch
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.recommendation import ALSModel
import numpy as np
import os
from datetime import datetime  # Fix: Thêm import datetime
import time
class HybridRecommender:
    def __init__(self, es_host='localhost', es_port=9200,
                 mongo_host='localhost', mongo_port=27017):
        self.es = Elasticsearch([f'http://{es_host}:{es_port}'])
        
        # Sử dụng env vars cho flexibility, nhưng force 'localhost' nếu Docker không chạy
        mongo_host = os.getenv("MONGO_HOST", mongo_host)
        if mongo_host == 'mongodb':  # Fix: Detect Docker name, fallback local
            mongo_host = 'localhost'
            print("Detected Docker MONGO_HOST='mongodb' – switched to 'localhost' for local run")
        mongo_port = int(os.getenv("MONGO_PORT", str(mongo_port)))  # Fix: str() cho env
        mongo_user = os.getenv("MONGO_USER", "root")
        mongo_password = os.getenv("MONGO_PASSWORD", "admin123")
        mongo_db = os.getenv("MONGO_DATABASE", "beauty_db")

        # MongoDB URI (Fix: Đảm bảo localhost và authSource cuối)
        mongo_uri = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/{mongo_db}?authSource=admin"
        print(f"Using Mongo URI: {mongo_uri}")  # Debug: In URI để check
        
        # MongoDB connection với retry
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)  # Timeout ngắn cho retry
                self.db = self.mongo_client[mongo_db]
                self.db.command('ping')  # Test ping
                print("MongoDB connected successfully")
                break
            except Exception as e:
                print(f"MongoDB connection attempt {attempt+1} failed: {e}")
                if attempt == max_retries - 1:
                    print("All retries failed – using dummy mode for Mongo")
                    self.db = None  # Fallback empty db
                else:
                    time.sleep(2)  # Wait 2s retry
        
        # Initialize Spark
        self.spark = SparkSession \
            .builder \
            .appName("RecommendationEngine") \
            .master("local[*]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
            .getOrCreate()
        
        # Load models với try-except
        try:
            self.als_model = ALSModel.load("models/als_model")
            self.indexer_model = PipelineModel.load("models/indexer_model")
            print("Models loaded successfully")
        except Exception as e:
            print(f"Error loading models: {e}. Using dummy mode.")
            self.als_model = None
            self.indexer_model = None
    
    def _get_popular_fallback(self, num=10):
        """Fallback: Top popular items from ES (sort by rating_number)"""
        try:
            body = {
                "query": {"match_all": {}},
                "sort": [{"rating_number": {"order": "desc"}}],
                "size": num,
                "_source": ["asin", "title", "average_rating", "rating_number"]
            }
            results = self.es.search(index='beauty_products', body=body)
            
            fallback_recs = []
            for hit in results['hits']['hits']:
                fallback_recs.append({
                    'asin': hit['_id'],
                    'score': hit['_source'].get('rating_number', 0) / 1000.0,
                    'product': hit['_source']
                })
            print(f"Fallback: {len(fallback_recs)} popular items for cold-start")
            return fallback_recs
        except Exception as e:
            print(f"Fallback error: {e}")
            return []
    
    def vector_similarity_query(self, query_vector, category=None, 
                                size=10, cosine=True):
        if cosine:
            score_fn = "doc['model_factor'].size() == 0 ? 0 : cosineSimilarity(params.query_vector, 'model_factor') + 1.0"
        else:
            score_fn = "doc['model_factor'].size() == 0 ? 0 : sigmoid(1, Math.E, -dotProduct(params.query_vector, 'model_factor'))"
        
        base_query = {"match_all": {}}
        if category:
            base_query = {
                "bool": {
                    "must": [
                        {"term": {"main_category": category}}
                    ]
                }
            }
        
        query_body = {
            "query": {
                "script_score": {
                    "query": base_query,
                    "script": {
                        "source": score_fn,
                        "params": {
                            "query_vector": query_vector
                        }
                    }
                }
            },
            "size": size
        }
        
        return query_body
    
    def get_similar_items_by_vector(self, asin, num=10):
        try:
            response = self.es.get(index='beauty_products', id=asin)
            source = response['_source']
            
            if 'model_factor' not in source or not source['model_factor']:
                print(f"No vector for ASIN {asin}")
                return None, []
            
            query_vector = source['model_factor']
            category = source.get('main_category')
            
            query = self.vector_similarity_query(query_vector, category, size=num+1, cosine=True)
            results = self.es.search(index='beauty_products', body=query)
            
            hits = [hit for hit in results['hits']['hits'] if hit['_id'] != asin][:num]
            
            return source, hits
        
        except Exception as e:
            print(f"Error getting similar items: {e}")
            return None, []
    
    def get_collaborative_recommendations(self, user_id, num=10):
        """Fix: Remove duplicate if, add fallback"""
        if not self.als_model or not self.indexer_model:
            print("Models not loaded, skipping CF")
            return self._get_popular_fallback(num)
        
        try:
            user_indexer = self.indexer_model.stages[0]
            user_labels = user_indexer.labels
            
            if user_id not in user_labels:
                print(f"User {user_id} not found in training data – using popular fallback")
                return self._get_popular_fallback(num)  # Fix: Fallback nếu cold-start
            
            user_idx = user_labels.index(user_id)
            
            from pyspark.sql import Row
            user_df = self.spark.createDataFrame([Row(reviewerId_index=float(user_idx))])
            
            recs = self.als_model.recommendForUserSubset(user_df, num)
            
            asin_indexer = self.indexer_model.stages[1]
            asin_labels = asin_indexer.labels
            
            recommendations = []
            for row in recs.collect():
                for rec in row.recommendations:
                    asin_idx = int(rec.asin_index)
                    if asin_idx < len(asin_labels):
                        asin = asin_labels[asin_idx]
                        score = float(rec.rating)
                        recommendations.append({
                            'asin': asin,
                            'score': score
                        })
            
            return recommendations[:num]
        
        except Exception as e:
            print(f"Error getting collaborative recommendations: {e}")
            return self._get_popular_fallback(num)  # Fix: Fallback nếu error
    
    def get_hybrid_recommendations(self, user_id, num=10, 
                                   cf_weight=0.6, cb_weight=0.4):
        cf_recs = self.get_collaborative_recommendations(user_id, num*2)
        
        if not cf_recs:
            print(f"Full cold-start for {user_id} – using enhanced fallback")
            popular = self._get_popular_fallback(num // 2)
            # Giả sử top category 'All Beauty'
            hot_cats = self._get_category_hot_items("All Beauty", num // 2)
            fallback = popular + hot_cats
            return [
                {'asin': item['asin'], 'score': item['score'], 'product': item.get('product', {})}
                for item in fallback[:num]
            ]
        
        hybrid_scores = {}
        
        for rec in cf_recs:
            asin = rec['asin']
            cf_score = rec['score']
            
            try:
                response = self.es.get(index='beauty_products', id=asin)
                product = response['_source']
                
                hybrid_score = cf_weight * cf_score
                if 'average_rating' in product and product['average_rating']:
                    rating_boost = product['average_rating'] / 5.0
                    hybrid_score += cb_weight * rating_boost
                
                hybrid_scores[asin] = {
                    'asin': asin,
                    'score': hybrid_score,
                    'product': product
                }
            
            except Exception as e:
                print(f"Could not get product {asin}: {e}")
                continue
        
        sorted_recs = sorted(hybrid_scores.values(), key=lambda x: x['score'], reverse=True)[:num]
        
        # Pad nếu ít
        if len(sorted_recs) < num:
            extra = self._get_popular_fallback(num - len(sorted_recs))
            sorted_recs += [
                {'asin': item['asin'], 'score': item['score'], 'product': item.get('product', {})}
                for item in extra[:num - len(sorted_recs)]
            ]
        
        return sorted_recs[:num]
    
    def _get_category_hot_items(self, category, num=5):
        """Helper: Hot items from category (ES query)"""
        try:
            body = {
                "query": {"term": {"main_category": category}},
                "sort": [{"rating_number": {"order": "desc"}}],
                "size": num
            }
            results = self.es.search(index='beauty_products', body=body)
            return [
                {'asin': hit['_id'], 'score': hit['_source'].get('rating_number', 0) / 1000.0, 'product': hit['_source']}
                for hit in results['hits']['hits']
            ]
        except:
            return []
    
    def search_and_recommend(self, query, num=10):
        try:
            if not self.es.indices.exists(index='beauty_products'):
                raise Exception("Index 'beauty_products' not found. Run elasticsearch_indexer.py first.")
            
            search_body = {
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": ["title^3", "description", "features^2", "main_category^2", "store"],
                        "type": "best_fields",
                        "fuzziness": "AUTO"
                    }
                },
                "size": num,
                "sort": [{"_score": {"order": "desc"}}]
            }
            
            results = self.es.search(index='beauty_products', body=search_body)
            
            recommendations = []
            for hit in results['hits']['hits']:
                recommendations.append({
                    'asin': hit['_id'],
                    'score': hit['_score'],
                    'product': hit['_source']
                })
            
            return recommendations
        except Exception as e:
            print(f"Search error: {e}")
            # Fallback Mongo regex search
            try:
                from bson.regex import Regex
                regex_query = Regex.from_native(query, flags='i')
                products = list(self.db.products.find({
                    "$or": [
                        {"title": regex_query},
                        {"description": {"$regex": query, "$options": "i"}},
                        {"features": {"$regex": query, "$options": "i"}}
                    ]
                }).limit(num))
                return [
                    {'asin': p.get('asin', p.get('parent_asin')), 'score': 1.0, 'product': p}
                    for p in products
                ]
            except:
                return []
    
    def get_user_history(self, user_id, limit=20):
        """Get user's review history from MongoDB (Fix: Robust connection check)"""
        if not self.db:
            print("MongoDB not connected – skipping history")
            return []
        
        try:
            # Test ping trước query
            self.db.command('ping')
            reviews = list(self.db.reviews.find({'user_id': user_id}
            ).sort('timestamp', -1).limit(limit))
            
            for review in reviews:
                if '_id' in review:
                    del review['_id']
                if 'timestamp' in review and isinstance(review['timestamp'], (int, float)):
                    review['timestamp'] = datetime.fromtimestamp(review['timestamp'])
            
            print(f"Loaded {len(reviews)} history items for {user_id}")
            return reviews
        except Exception as e:
            print(f"Error getting user history: {e}")
            return []
    
    def stop(self):
        if hasattr(self, 'spark'):
            self.spark.stop()
        if hasattr(self, 'mongo_client'):
            self.mongo_client.close()

    def list_training_users(self, limit=100):
        if not self.indexer_model:
            print("No indexer model loaded!")
            return []
        
        user_indexer = self.indexer_model.stages[0]
        user_labels = user_indexer.labels
        
        print(f"Tổng số users trong training data: {len(user_labels)}")
        
        print("Sample users (top 10):")
        for i, user in enumerate(user_labels[:10]):
            print(f"  {i}: {user}")
        
        with open("training_users_sample.txt", "w") as f:
            for user in user_labels[:1000]:
                f.write(f"{user}\n")
        print("Full sample saved to training_users_sample.txt")
        
        return user_labels

# Test code (giữ nguyên, nhưng fix asin mẫu nếu cần)
if __name__ == "__main__":
    recommender = HybridRecommender(es_host='localhost', mongo_host='localhost')
    users = recommender.list_training_users(limit=50)
    
    print("Testing collaborative filtering...")
    user_id = "AEZP6Z2C5AVQDZAJECQYZWQRNG3Q"
    cf_recs = recommender.get_collaborative_recommendations(user_id, num=5)
    print(f"CF recommendations for user {user_id}:")
    for rec in cf_recs:
        print(f"  - {rec['asin']}: {rec['score']:.3f}")
    
    print("\nTesting content-based recommendations...")
    asin = "B075XCMQ6C"  # Fix: Dùng ASIN thực từ data (từ CF test trước)
    product, similar = recommender.get_similar_items_by_vector(asin, num=5)
    if product:
        print(f"Similar items to {product.get('title', asin)[:50]}...:")
        for item in similar:
            title = item['_source'].get('title', item['_id'])[:50] + "..."
            print(f"  - {title}: {item['_score']:.3f}")
    
    print("\nTesting search recommendations...")
    search_recs = recommender.search_and_recommend("hair spray", num=5)
    print("Search results for 'hair spray':")
    for rec in search_recs:
        title = rec['product'].get('title', rec['asin'])[:50] + "..."
        print(f"  - {title}: {rec['score']:.3f}")
    
    print("\nTesting user history...")
    history = recommender.get_user_history(user_id, limit=3)
    print(f"Recent reviews for {user_id}: {len(history)} items")
    for rev in history[:2]:
        print(f"  - ASIN: {rev.get('asin')}, Rating: {rev.get('rating')}, Date: {rev.get('timestamp')}")
    
    recommender.stop()