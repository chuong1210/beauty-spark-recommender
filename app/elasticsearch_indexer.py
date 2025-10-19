from elasticsearch import Elasticsearch, helpers
from pymongo import MongoClient
import json
import os

class ElasticsearchIndexer:
    def __init__(self, es_host='localhost', es_port=9200, 
                 mongo_host='localhost', mongo_port=27017):
        mongo_host = os.getenv("MONGO_HOST", "localhost")
        mongo_port = os.getenv("MONGO_PORT", 27017)
        mongo_user = os.getenv("MONGO_USER", "root")
        mongo_password = os.getenv("MONGO_PASSWORD", "admin123")
        mongo_db = os.getenv("MONGO_DATABASE", "beauty_db")

        # Elasticsearch
        self.es = Elasticsearch([f"http://{es_host}:{es_port}"])

        # MongoDB (có authSource=admin)
        self.mongo_client = MongoClient(
            f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/{mongo_db}?authSource=admin"
        )
        self.db = self.mongo_client[mongo_db]
        self.vector_dim = 25
    
    def create_product_index(self, index_name='beauty_products'):
        """Create Elasticsearch index with mapping"""
        # Delete if exists
        if self.es.indices.exists(index=index_name):
            self.es.indices.delete(index=index_name)
            print(f"Deleted existing index: {index_name}")
        
        # Create index with mapping
        mapping = {
            "mappings": {
                "properties": {
                    "asin": {"type": "keyword"},
                    "parent_asin": {"type": "keyword"},
                    "title": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword"}}
                    },
                    "main_category": {"type": "keyword"},
                    "average_rating": {"type": "double"},
                    "rating_number": {"type": "long"},
                    "features": {"type": "text"},
                    "description": {"type": "text"},
                    "price": {"type": "text"},
                    "store": {"type": "keyword"},
                    "categories": {"type": "keyword"},
                    "images": {"type": "keyword"},
                    "model_factor": {
                        "type": "dense_vector",
                        "dims": self.vector_dim
                    },
                    "model_version": {"type": "keyword"},
                    "model_timestamp": {"type": "date"}
                }
            }
        }
        
        self.es.indices.create(index=index_name, body=mapping)
        print(f"Created index: {index_name}")
    
    def load_products_from_mongo(self):
        """Load product metadata from MongoDB"""
        products = list(self.db.products.find({}))
        print(f"Loaded {len(products)} products from MongoDB")
        return products
    
    def load_item_vectors_from_mongo(self):
        """Load item factor vectors from MongoDB"""
        vectors = {}
        for item in self.db.item_vectors.find({}):
            asin = item['asin']
            vectors[asin] = {
                'model_factor': item['model_factor'],
                'model_version': item['model_version'],
                'model_timestamp': item['model_timestamp']
            }
        print(f"Loaded {len(vectors)} item vectors from MongoDB")
        return vectors
    
    def merge_product_with_vectors(self, products, vectors):
        """Merge product metadata with factor vectors"""
        merged = []
        for product in products:
            asin = product.get('parent_asin')
            if asin and asin in vectors:
                product.update(vectors[asin])
                merged.append(product)
            elif product.get('parent_asin'):
                # Product without vector (new product)
                merged.append(product)
        
        print(f"Merged {len(merged)} products with vectors")
        return merged
    
    def bulk_index_products(self, products, index_name='beauty_products'):
        """Bulk index products to Elasticsearch"""
        def generate_actions():
            for product in products:
                # Remove MongoDB _id
                if '_id' in product:
                    del product['_id']
                
                # Extract image URLs
                if 'images' in product and isinstance(product['images'], list):
                    image_urls = []
                    for img in product['images']:
                        if isinstance(img, dict):
                            if 'large' in img:
                                image_urls.append(img['large'])
                            elif 'thumb' in img:
                                image_urls.append(img['thumb'])
                    product['images'] = image_urls
                
                # Use parent_asin as document ID
                doc_id = product.get('parent_asin', product.get('asin'))
                
                yield {
                    "_index": index_name,
                    "_id": doc_id,
                    "_source": product
                }
        
        success, failed = helpers.bulk(self.es, generate_actions(), 
                                       raise_on_error=False, 
                                       stats_only=False)
        
        print(f"Successfully indexed {success} documents")
        if failed:
            print(f"Failed to index {len(failed)} documents")
        
        # Refresh index
        self.es.indices.refresh(index=index_name)
        
        # Verify count
        count = self.es.count(index=index_name)['count']
        print(f"Total documents in index: {count}")
    
    def index_all(self):
        """Complete indexing pipeline"""
        print("Creating Elasticsearch index...")
        self.create_product_index()
        
        print("Loading products from MongoDB...")
        products = self.load_products_from_mongo()
        
        print("Loading item vectors from MongoDB...")
        vectors = self.load_item_vectors_from_mongo()
        
        print("Merging products with vectors...")
        merged_products = self.merge_product_with_vectors(products, vectors)
        
        print("Bulk indexing to Elasticsearch...")
        self.bulk_index_products(merged_products)
        
        print("Indexing complete!")
    
    def search_products(self, query, size=10):
        """Test search functionality"""
        body = {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["title^3", "description", "features", "main_category^2"]
                }
            },
            "size": size
        }
        
        results = self.es.search(index='beauty_products', body=body)
        return results['hits']['hits']
    
    def close(self):
        self.mongo_client.close()


if __name__ == "__main__":
    indexer = ElasticsearchIndexer()
    indexer.index_all()
    
    # Test search
    print("\nTesting search...")
    results = indexer.search_products("hair spray", size=3)
    for hit in results:
        print(f"- {hit['_source']['title']} (score: {hit['_score']})")
    
    indexer.close()