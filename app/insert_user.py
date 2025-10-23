import os
from pymongo import MongoClient
from werkzeug.security import generate_password_hash
from datetime import datetime

# Config
MONGO_URI = "mongodb://root:admin123@localhost:27017/?authSource=admin"
DB_NAME = "beauty_db"
COL_USERS = "users"
SAMPLE_FILE = "training_users_sample.txt"  # Path tương đối
DUMMY_PASSWORD = "password123"  # Để test login

# Sample user_ids (nếu file missing, dùng 50 cái từ history)
SAMPLE_USER_IDS = [
    "AG73BVBKUOH22USSFJA5ZWL7AKXA", "AEZP6Z2C5AVQDZAJECQYZWQRNG3Q", "AEMP3A7IKW37CMWFXNKXWW6HGJHA_1",
    "AGZUJTI7A3JFKB4FP5JOH6NVAJIQ_1", "AFDYIK3FNPY2JFBQYUWC6GSBMIRQ_2", "AFXF3EGQTQDXMRLDWFU7UBFQZB7Q",
    "AEJU3Z6HDAERETMYI2CXBQVPPDFA", "AHY2TURQPNIDXZGH2CMQLZ343YMQ", "AEOK4TQIKGO23SJKZ6PW4FETNNDA_1",
    "AEAXAJACFMXIAAH4WOHRMXPSZWFA", "AFDYIK3FNPY2JFBQYUWC6GSBMIRQ_1", "AEHWKRPNWNMOAJSMO2F6O7RFRTNA",
    "AF2BLE54TEMGZ546U763ZHZRXC4A", "AH3BXW7KLIS2VAE56UXJS2NS7I5A", "AHPGHDFIU3BUB3RQBP56RQQA7W4Q",
    "AHGIDR4IJFS23Q4GTZ33FI5LYDSQ_1", "AF2YKZQRMRGJ655I3MKQUYFGRQGA", "AGYVC7KVHP2AWM7BDCEYNHFA6F3Q",
    "AHDVSLWHSORYGG3S5QZMVDFNOXUQ", "AEODUUL6REW3PW2ZGFREKGWBYUZA", "AE5ESL52LWWBJTSFOAXSFZA3XCGQ",
    "AG4D44BNNLUEZNG7COK2CNRYUHYQ", "AGFAOH3NMW2D7YV3QVZSTXMTSKIQ", "AGOLMT3QETKYNESRYKBA6D7XXS7A",
    "AHV6QCNBJNSGLATP56JAWJ3C4G2A", "AGFLDIUYV2PKF5Q7IEMULR52GW2Q", "AHGQDSOBAIVAAKJFIVEGS344MSXA",
    "AHPG65LKS3QKRWDUWAKZNLEK5RZQ", "AHPG65LKS3QKRWDUWAKZNLEK5RZQ_1", "AECADZLPUNH3BDNACLFF7PSHN5MQ",
    "AHBWH2LBU3NFLD46GKJKIBAHKXEQ", "AHT6AM6BNIZUHFJB5V2M6XM72G7Q", "AFNCHMAKUAJOGVCKOA4XGLINHPDQ",
    "AFQQQ5LGNSQUEBGDCYBAZZE5T3DA", "AFUWF5DARSSBPDLHSY67Q3LCI54Q", "AHYOSWORVZFXM5QMRIAW3JTTFFIQ",
    "AFPPNF3RSFMMNC5UAM6V4B475MBQ", "AFZIHXLLRIZYAZDRCGC3Z4DYUMQQ", "AGN5KJZU3FYSKVWXWM66LXYWL5CQ",
    "AEH7RAIDBU7QALXTMWAA73PTL4JA", "AES3YWD3ONJOUDRWFIV2ZO44QDAQ", "AEYVPPWR4CIKWX4BGYKCBCDL2CZQ",
    "AGQIUTI7M4XUGCRV6E66FAOCX5PQ", "AGV5NQ4JDQD6NHXVF6AVZPVDX66Q"
]

def generate_sample_file():
    """Generate file nếu missing"""
    if not os.path.exists(SAMPLE_FILE):
        print(f"File {SAMPLE_FILE} missing. Generating with 50 sample users...")
        with open(SAMPLE_FILE, 'w') as f:
            for user_id in SAMPLE_USER_IDS:
                f.write(user_id + '\n')
        print("✓ File generated!")
    else:
        print(f"File {SAMPLE_FILE} exists.")

def insert_users_to_mongo():
    """Insert users vào MongoDB"""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    users_collection = db[COL_USERS]
    
    # Read user_ids từ file
    with open(SAMPLE_FILE, 'r') as f:
        user_ids = [line.strip() for line in f if line.strip()]
    
    print(f"Found {len(user_ids)} user IDs in file.")
    
    inserted_count = 0
    skipped_count = 0
    for idx, user_id in enumerate(user_ids):
        # Check duplicate
        if users_collection.find_one({'user_id': user_id}):
            skipped_count += 1
            continue
        
        # Generate dummy data
        username = f"user_{idx:03d}_{user_id[:8]}"  # e.g., user_000_AG73BVBK
        password_hash = generate_password_hash(DUMMY_PASSWORD)
        email = f"{username}@beautyrecs.com"
        created_at = datetime.now()
        
        doc = {
            'user_id': user_id,
            'username': username,
            'password': password_hash,
            'email': email,
            'created_at': created_at
        }
        
        result = users_collection.insert_one(doc)
        if result.acknowledged:
            inserted_count += 1
            print(f"✓ Inserted: {username} (ID: {user_id[:10]}...)")
    
    print(f"\nSummary: Inserted {inserted_count} new users, skipped {skipped_count} duplicates.")
    
    # Verify sample
    print("\nSample users in DB (top 5):")
    for doc in users_collection.find().sort('created_at', -1).limit(5):
        print(f"- Username: {doc['username']}, User ID: {doc['user_id'][:20]}..., Email: {doc['email']}")
    
    client.close()

if __name__ == "__main__":
    generate_sample_file()
    insert_users_to_mongo()
    print("\nDone! Now test login with username 'user_000_AG73BVBK' and password 'password123'")