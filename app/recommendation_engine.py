from elasticsearch import Elasticsearch
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.recommendation import ALSModel
import numpy as np
import os

class HybridRecommender:
    def __init__(self, es_host='localhost', es_port=9200,  # Sửa: default localhost cho local
                 mongo_host='localhost', mongo_port=27017):  # Sửa: default localhost
        self.es = Elasticsearch([f'http://{es_host}:{es_port}'])
        
        # Sử dụng env vars cho flexibility (Docker: MONGO_HOST=mongodb, local: localhost)
        mongo_host = os.getenv("MONGO_HOST", mongo_host)  # Fallback to param if env not set
        mongo_port = int(os.getenv("MONGO_PORT", mongo_port))
        mongo_user = os.getenv("MONGO_USER", "root")
        mongo_password = os.getenv("MONGO_PASSWORD", "admin123")
        mongo_db = os.getenv("MONGO_DATABASE", "beauty_db")

        # MongoDB (có authSource=admin)
        self.mongo_client = MongoClient(
            f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/{mongo_db}?authSource=admin"
        )
        self.db = self.mongo_client[mongo_db]  # Sửa: dùng mongo_db từ env
        
        # Test connection (optional, để debug)
        try:
            self.db.command('ping')
            print("MongoDB connected successfully")
        except Exception as e:
            print(f"MongoDB connection error: {e}")
        
        # Initialize Spark (thêm Mongo connector nếu cần load từ Mongo, nhưng ở đây chỉ load models local)
        self.spark = SparkSession \
            .builder \
            .appName("RecommendationEngine") \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
            .getOrCreate()
        
        # Load models (thêm try-except để tránh crash nếu models missing)
        try:
            self.als_model = ALSModel.load("models/als_model")
            self.indexer_model = PipelineModel.load("models/indexer_model")
            print("Models loaded successfully")
        except Exception as e:
            print(f"Error loading models: {e}. Using dummy mode.")
            self.als_model = None
            self.indexer_model = None
    
    def vector_similarity_query(self, query_vector, category=None, 
                                size=10, cosine=True):
        """
        Create Elasticsearch vector similarity query
        """
        if cosine:
            score_fn = "doc['model_factor'].size() == 0 ? 0 : cosineSimilarity(params.query_vector, 'model_factor') + 1.0"  # Sửa: params.query_vector
        else:
            score_fn = "doc['model_factor'].size() == 0 ? 0 : sigmoid(1, Math.E, -dotProduct(params.query_vector, 'model_factor'))"  # Sửa: params.query_vector
        
        query_body = {
            "query": {
                "script_score": {
                    "query": {"match_all": {}},
                    "script": {
                        "source": score_fn,
                        "params": {
                            "query_vector": query_vector  # Sửa: key đúng
                        }
                    }
                }
            },
            "size": size
        }
        
        # Add category filter if specified
        if category:
            query_body["query"]["script_score"]["query"] = {
                "bool": {
                    "must": [  # Sửa: dùng "must" thay vì "filter" để kết hợp với script_score
                        {
                            "term": {"main_category": category}
                        }
                    ]
                }
            }
        
        return query_body
    
    def get_similar_items_by_vector(self, asin, num=10):
        """
        Content-based recommendation using item vectors
        """
        try:
            response = self.es.get(index='beauty_products', id=asin)
            source = response['_source']
            
            if 'model_factor' not in source or not source['model_factor']:
                print(f"No vector for ASIN {asin}")
                return None, []
            
            query_vector = source['model_factor']
            category = source.get('main_category')
            
            # Search for similar items
            query = self.vector_similarity_query(query_vector, category, 
                                                 size=num+1, cosine=True)
            results = self.es.search(index='beauty_products', body=query)
            
            # Exclude the query item itself
            hits = [hit for hit in results['hits']['hits'] 
                    if hit['_id'] != asin][:num]
            
            return source, hits
        
        except Exception as e:
            print(f"Error getting similar items: {e}")
            return None, []
    
    def get_collaborative_recommendations(self, user_id, num=10):
        """
        Collaborative filtering recommendations using ALS
        """
        if not self.als_model or not self.indexer_model:
            print("Models not loaded, skipping CF")
            return []
        
        try:
            # Get user index
            user_indexer = self.indexer_model.stages[0]
            user_labels = user_indexer.labels
            
            if user_id not in user_labels:
                print(f"User {user_id} not found in training data")
                return []
            
            user_idx = user_labels.index(user_id)
            
            # Create user dataframe
            from pyspark.sql import Row
            user_df = self.spark.createDataFrame([Row(reviewerId_index=float(user_idx))])
            
            # Get recommendations
            recs = self.als_model.recommendForUserSubset(user_df, num)
            
            # Extract ASINs
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
            
            return recommendations[:num]  # Đảm bảo đúng số lượng
        
        except Exception as e:
            print(f"Error getting collaborative recommendations: {e}")
            return []
    
    def get_hybrid_recommendations(self, user_id, num=10, 
                                   cf_weight=0.6, cb_weight=0.4):
        """
        Hybrid recommendation combining collaborative and content-based
        """
        # Get collaborative filtering recommendations
        cf_recs = self.get_collaborative_recommendations(user_id, num*2)
        
        if not cf_recs:
            # Fallback to popular items nếu CF fail
            try:
                popular = self.es.search(
                    index='beauty_products',
                    body={"query": {"function_score": {"query": {"match_all": {}}, "functions": [{"field_value_factor": {"field": "rating_number", "factor": 1.0}}]}}}
                )
                return [
                    {'asin': hit['_id'], 'score': hit['_score'], 'product': hit['_source']}
                    for hit in popular['hits']['hits'][:num]
                ]
            except:
                return []
        
        # Get product details from Elasticsearch
        hybrid_scores = {}
        
        for rec in cf_recs:
            asin = rec['asin']
            cf_score = rec['score']
            
            try:
                # Get product from ES
                response = self.es.get(index='beauty_products', id=asin)
                product = response['_source']
                
                # Calculate hybrid score
                hybrid_score = cf_weight * cf_score
                
                # Add content-based boost if available
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
        
        # Sort by score
        sorted_recs = sorted(hybrid_scores.values(), 
                             key=lambda x: x['score'], 
                             reverse=True)[:num]
        
        return sorted_recs
    
    def search_and_recommend(self, query, num=10):
        """
        Search-based recommendations
        """
        try:
            search_body = {
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": ["title^3", "description", "features^2", 
                                   "main_category^2", "store"]
                    }
                },
                "size": num,
                "sort": [{"_score": {"order": "desc"}}]  # Thêm sort để ổn định
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
            return []
    
    def get_user_history(self, user_id, limit=20):
        """Get user's review history from MongoDB"""
        try:
            reviews = list(self.db.reviews.find(  # Sửa: dùng self.db thay vì global db
                {'user_id': user_id}
            ).sort('timestamp', -1).limit(limit))
            
            # Clean up _id and convert timestamp if needed
            for review in reviews:
                if '_id' in review:
                    del review['_id']
                if 'timestamp' in review and isinstance(review['timestamp'], int):
                    review['timestamp'] = datetime.fromtimestamp(review['timestamp'])
            
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
        """Extract danh sách user_ids có trong training data (từ indexer labels)"""
        if not self.indexer_model:
            print("No indexer model loaded!")
            return []
        
        # User indexer là stages[0] (reviewerId_index)
        user_indexer = self.indexer_model.stages[0]
        user_labels = user_indexer.labels  # List tất cả user_id strings
        
        print(f"Tổng số users trong training data: {len(user_labels)}")
        
        # In sample (limit để tránh spam, vì có ~500k users)
        print("Sample users (top 10):")
        for i, user in enumerate(user_labels[:limit]):
            print(f"  {i}: {user}")
        
        # Lưu full list vào file nếu cần (optional)
        with open("training_users_sample.txt", "w") as f:
            for user in user_labels[:1000]:  # Lưu 1000 đầu
                f.write(f"{user}\n")
        print("Full sample saved to training_users_sample.txt")
        
        return user_labels
if __name__ == "__main__":
    # Test với local defaults
    recommender = HybridRecommender(es_host='localhost', mongo_host='localhost')
    users = recommender.list_training_users(limit=50)  # In 50 users đầu
    # Test collaborative filtering
    print("Testing collaborative filtering...")
    user_id = "AEZP6Z2C5AVQDZAJECQYZWQRNG3Q"  # User_id mẫu, thay nếu cần
    cf_recs = recommender.get_collaborative_recommendations(user_id, num=5)
    print(f"CF recommendations for user {user_id}:")
    for rec in cf_recs:
        print(f"  - {rec['asin']}: {rec['score']:.3f}")
    
    # Test content-based
    print("\nTesting content-based recommendations...")
    asin = "B00YQ6X8EO"  # ASIN mẫu, thay nếu cần
    product, similar = recommender.get_similar_items_by_vector(asin, num=5)
    if product:
        print(f"Similar items to {product.get('title', asin)[:50]}...:")
        for item in similar:
            title = item['_source'].get('title', item['_id'])[:50] + "..."
            print(f"  - {title}: {item['_score']:.3f}")
    
    # Test search
    print("\nTesting search recommendations...")
    search_recs = recommender.search_and_recommend("hair spray", num=5)
    print("Search results for 'hair spray':")
    for rec in search_recs:
        title = rec['product'].get('title', rec['asin'])[:50] + "..."
        print(f"  - {title}: {rec['score']:.3f}")
    
    recommender.stop()