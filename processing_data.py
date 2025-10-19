
import json
from pymongo import MongoClient

# Kết nối MongoDB
client = MongoClient("mongodb://root:101204@localhost:27017/?authSource=admin",
                     serverSelectionTimeoutMS=5000)
db = client["amazon"]
reviews_col = db["reviews"]
meta_col = db["metadata"]

# Insert reviews
with open("All_Beauty.jsonl", "r", encoding="utf-8") as f:
    batch = []
    for line in f:
        doc = json.loads(line)
        batch.append(doc)
        if len(batch) == 1000:  # batch insert để nhanh hơn
            reviews_col.insert_many(batch)
            batch = []
    if batch:
        reviews_col.insert_many(batch)

# Insert metadata
with open("meta_All_Beauty.jsonl", "r", encoding="utf-8") as f:
    batch = []
    for line in f:
        doc = json.loads(line)
        batch.append(doc)
        if len(batch) == 1000:
            meta_col.insert_many(batch)
            batch = []
    if batch:
        meta_col.insert_many(batch)

print("✅ Import xong vào MongoDB!")
