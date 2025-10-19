# index_item_vectors_es.py
from elasticsearch import Elasticsearch, helpers
import json

ES_HOST = "http://localhost:9200"
INDEX = "amazon_item_vectors"
ITEM_VECTORS_PATH = "/tmp/item_vectors.jsonl"
DIM = 50   # = RANK bạn dùng trong ALS

es = Elasticsearch(ES_HOST)

# create index with dense_vector mapping (ES 7.3+)
if es.indices.exists(INDEX):
    es.indices.delete(INDEX)

mapping = {
  "mappings": {
    "properties": {
      "asin": {"type": "keyword"},
      # If your ES supports dense_vector
      "features": {"type": "dense_vector", "dims": DIM},
      "timestamp_indexed": {"type": "date"}
    }
  }
}
es.indices.create(index=INDEX, body=mapping)

# bulk index
def gen():
    with open(ITEM_VECTORS_PATH, "r", encoding="utf-8") as f:
        for line in f:
            doc = json.loads(line)
            yield {
                "_index": INDEX,
                "_id": doc["asin"],
                "_source": {"asin": doc["asin"], "features": doc["features"]}
            }

helpers.bulk(es, gen(), chunk_size=500)
print("Indexed vectors into", INDEX)
