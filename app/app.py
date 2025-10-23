import os
import redis
from datetime import datetime
from functools import wraps
from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from werkzeug.security import generate_password_hash, check_password_hash
from pymongo import MongoClient
from kafka import KafkaProducer, KafkaConsumer
import json
import threading
from viz import generate_all_viz  # Import
# Redis Cache Wrapper Class (sửa: thêm class này để implement các method)
class RedisCache:
    def __init__(self, host='localhost', port=6379, db=0):
        self.redis = redis.Redis(host=host, port=port, db=db)
        self.redis.ping()  # Test connection
    
    def check_rate_limit(self, user_id, action, limit, window):
        """Check and update rate limit using sliding window"""
        key = f"rate_limit:{user_id}:{action}"
        current_time = int(datetime.now().timestamp())
        pipe = self.redis.pipeline()
        pipe.zrem(key, 0)  # Remove old timestamps
        pipe.zrangebyscore(key, 0, current_time - window)
        old_count = pipe.execute()[1]
        current_count = len(self.redis.zrange(key, 0, -1))
        if current_count >= limit:
            return False, 0
        self.redis.zadd(key, {str(current_time): current_time})
        self.redis.expire(key, window)
        return True, limit - (current_count + 1)
    
    def get_cached_recommendations(self, user_id, method):
        """Get cached recommendations"""
        key = f"recs:{user_id}:{method}"
        data = self.redis.get(key)
        return json.loads(data) if data else None
    
    def cache_recommendations(self, user_id, method, recs, ttl=3600):
        """Cache recommendations"""
        key = f"recs:{user_id}:{method}"
        self.redis.setex(key, ttl, json.dumps(recs))
    
    def get_cached_product(self, asin):
        """Get cached product"""
        key = f"product:{asin}"
        data = self.redis.get(key)
        return json.loads(data) if data else None
    
    def cache_product(self, asin, product, ttl=7200):
        """Cache product"""
        key = f"product:{asin}"
        self.redis.setex(key, ttl, json.dumps(product))
    
    def get_cached_search_results(self, query):
        """Get cached search results"""
        key = f"search:{hash(query)}"  # Use hash for query key
        data = self.redis.get(key)
        return json.loads(data) if data else None
    
    def cache_search_results(self, query, results, ttl=1800):
        """Cache search results"""
        key = f"search:{hash(query)}"
        self.redis.setex(key, ttl, json.dumps(results))
    
    def track_search_query(self, query):
        """Track search query popularity"""
        key = "search_popular"
        self.redis.zincrby(key, 1, query)
        self.redis.expire(key, 86400)  # 24h
    
    def track_user_view(self, user_id, asin):
        """Track user view"""
        key = f"user_views:{user_id}"
        self.redis.lpush(key, asin)
        self.redis.ltrim(key, 0, 99)  # Keep last 100
        self.redis.expire(key, 86400)
    
    def increment_product_views(self, asin):
        """Increment product views"""
        key = f"views:{asin}"
        self.redis.incr(key)
    
    def increment_category_product(self, category, asin):
        """Increment category product count"""
        key = f"cat_pop:{category}"
        self.redis.zincrby(key, 1, asin)
        self.redis.expire(key, 86400)
    
    def add_to_recent_views(self, asin):
        """Add to global recent views"""
        key = "global_recent_views"
        self.redis.lpush(key, asin)
        self.redis.ltrim(key, 0, 99)  # Keep last 100
        self.redis.expire(key, 3600)
    
    def get_trending_products(self, num=20):
        """Get trending products by views"""
        key = "product_views"  # Use sorted set for views
        # Note: Implement as sorted set elsewhere, here dummy
        return self.redis.zrevrange(key, 0, num-1, withscores=True) if self.redis.exists(key) else []
    
    def get_hot_products(self, num=20):
        """Get hot products by purchases (dummy, assume views as proxy)"""
        return self.get_trending_products(num)
    
    def get_category_popular(self, category, num=20):
        """Get popular in category"""
        key = f"cat_pop:{category}"
        return self.redis.zrevrange(key, 0, num-1, withscores=True)
    
    def get_user_history(self, user_id, limit=20):
        """Get user history from Redis"""
        key = f"user_views:{user_id}"
        return self.redis.lrange(key, 0, limit-1)
    
    def get_recently_viewed(self, num=20):
        """Get global recent views"""
        key = "global_recent_views"
        return self.redis.lrange(key, 0, num-1)

# Initialize Redis Cache
redis_cache = RedisCache()

# Rest of the Flask app code remains the same, but update imports and init
from recommendation_engine import HybridRecommender

app = Flask(__name__, template_folder='../templates')
app.secret_key = os.environ.get('SECRET_KEY', 'beauty-rec-secret-key-2024')

# Initialize components
mongo_client = MongoClient('mongodb://root:admin123@localhost:27017/?authSource=admin')
db = mongo_client['beauty_db']
users_collection = db['users']

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9093',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Initialize recommender
recommender = HybridRecommender()

# Kafka consumer for real-time events
def kafka_consumer_thread():
    """Background thread to consume Kafka messages"""
    consumer = KafkaConsumer(
        'recommendation_events',
        bootstrap_servers='localhost:9093',
        group_id='web_app_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )
    
    for message in consumer:
        event = message.value
        print(f"Received event: {event}")
        # Process events (e.g., update user activity, retrain models)

# Start Kafka consumer in background
consumer_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
consumer_thread.start()

# Rate limiting decorator (now uses RedisCache methods)
def rate_limit(limit=100, window=3600, action='api_call'):
    """Rate limiting decorator using Redis"""
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            if 'user_id' in session:
                user_id = session['user_id']
                allowed, remaining = redis_cache.check_rate_limit(
                    user_id, action, limit, window
                )
                if not allowed:
                    return jsonify({
                        'error': 'Rate limit exceeded',
                        'retry_after': window
                    }), 429
            return f(*args, **kwargs)
        return wrapped
    return decorator

# All other routes remain exactly the same as provided...
# (Paste the full routes code here, no changes needed since redis_cache now has methods)
# Thêm import nếu cần
from recommendation_engine import HybridRecommender  # Giả sử đã có

# Trong app init (sau recommender = ...)
user_sample_file = "training_users_sample.txt"
with open(user_sample_file, 'r') as f:
    all_users = [line.strip() for line in f.readlines()[:50]]  # 50 users demo


# Route mới: /user/<user_id>
@app.route('/user/<user_id>')
def user_page(user_id):
    """Trang user cá nhân với recs"""
    # Simulate login (không cần session thật cho demo)
    if user_id not in all_users:
        return "User not found", 404
    
    # Get recs cá nhân
    recs = recommender.get_hybrid_recommendations(user_id, num=10)
    
    # Enrich với product details (nếu cần, từ ES/Mongo)
    enriched_recs = []
    for rec in recs:
        try:
            # Từ ES hoặc Mongo
            product = recommender.es.get(index='beauty_products', id=rec['asin'])['_source']
            enriched_recs.append({
                'asin': rec['asin'],
                'score': rec['score'],
                'product': product  # title, images, price, etc.
            })
        except:
            enriched_recs.append(rec)  # Fallback
    
    return render_template('user.html', 
                          user_id=user_id,
                          recommendations=enriched_recs)
import os  # Thêm import nếu chưa có

@app.route('/')
def index():
    """Home page"""
    if 'user_id' not in session:
        return redirect(url_for('login'))
    
    user_id = session['user_id']
    username = session['username']
    
    # Get user's recent activity
    recent_reviews = recommender.get_user_history(user_id, limit=5)
    quick_recs = recommender.get_hybrid_recommendations(user_id, 3)  # Thêm: 3 quick recs
    
    # Mới: Check viz images tồn tại (tránh 404)
    viz_images = []
    image_files = ['rating_histogram.png', 'categories_pie.png']
    for img in image_files:
        if os.path.exists(os.path.join('static', img)):
            viz_images.append(img)
        else:
            print(f"Warning: {img} not found in static/ – Run viz.py first!")
    
    return render_template('index.html', 
                          username=username,
                          recent_reviews=recent_reviews,
                          viz_images=viz_images,  # Pass list filtered
                          quick_recs=quick_recs)  # Pass vào template
@app.route('/register', methods=['GET', 'POST'])
def register():
    """User registration"""
    if request.method == 'GET':
        return render_template('register.html')
    
    data = request.form
    username = data.get('username')
    password = data.get('password')
    email = data.get('email')
    
    # Check if user exists
    if users_collection.find_one({'username': username}):
        return jsonify({'error': 'Username already exists'}), 400
    
    # Create user
    user_id = f"USER_{int(datetime.now().timestamp())}"
    users_collection.insert_one({
        'user_id': user_id,
        'username': username,
        'password': generate_password_hash(password),
        'email': email,
        'created_at': datetime.now()
    })
    
    # Log event to Kafka
    producer.send('recommendation_events', {
        'event_type': 'user_registration',
        'user_id': user_id,
        'timestamp': datetime.now().isoformat()
    })
    
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    """User login"""
    if request.method == 'GET':
        return render_template('login.html')
    
    data = request.form
    username = data.get('username')
    password = data.get('password')
    
    user = users_collection.find_one({'username': username})
    
    if user and check_password_hash(user['password'], password):
        session['user_id'] = user['user_id']
        session['username'] = user['username']
        
        # Log event to Kafka
        producer.send('recommendation_events', {
            'event_type': 'user_login',
            'user_id': user['user_id'],
            'timestamp': datetime.now().isoformat()
        })
        
        return redirect(url_for('index'))
    
    return jsonify({'error': 'Invalid credentials'}), 401

@app.route('/logout')
def logout():
    """User logout"""
    session.clear()
    return redirect(url_for('login'))

@app.route('/recommendations')
@rate_limit(limit=50, window=3600, action='recommendations')
def recommendations():
    """Get personalized recommendations with Redis caching"""
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    user_id = session['user_id']
    num = request.args.get('num', 10, type=int)
    method = request.args.get('method', 'hybrid')
    
    # Try to get from Redis cache first
    cached_recs = redis_cache.get_cached_recommendations(user_id, method)
    if cached_recs:
        print(f"Cache HIT for user {user_id}, method {method}")
        return render_template('recommendations.html', 
                              recommendations=cached_recs[:num],
                              method=method,
                              from_cache=True)
    
    print(f"Cache MISS for user {user_id}, method {method}")
    
    # Log event to Kafka
    producer.send('recommendation_events', {
        'event_type': 'recommendation_request',
        'user_id': user_id,
        'method': method,
        'timestamp': datetime.now().isoformat()
    })
    
    # Generate recommendations
    if method == 'hybrid':
        recs = recommender.get_hybrid_recommendations(user_id, num)
    elif method == 'cf':
        recs = recommender.get_collaborative_recommendations(user_id, num)
        # Enrich with product details
        enriched_recs = []
        for rec in recs:
            try:
                # Check cache first
                product = redis_cache.get_cached_product(rec['asin'])
                if not product:
                    response = recommender.es.get(index='beauty_products', 
                                                 id=rec['asin'])
                    product = response['_source']
                    redis_cache.cache_product(rec['asin'], product)
                
                enriched_recs.append({
                    'asin': rec['asin'],
                    'score': rec['score'],
                    'product': product
                })
            except:
                continue
        recs = enriched_recs
    else:
        recs = []
    
    # Cache recommendations for 1 hour
    redis_cache.cache_recommendations(user_id, method, recs, ttl=3600)
    
    return render_template('recommendations.html', 
                          recommendations=recs,
                          method=method,
                          from_cache=False)

@app.route('/search')
@rate_limit(limit=100, window=3600, action='search')
def search():
    """Search products with Redis caching"""
    query = request.args.get('q', '')
    num = request.args.get('num', 20, type=int)
    
    if not query:
        return render_template('search.html', results=[], query='')
    
    # Try cache first
    cached_results = redis_cache.get_cached_search_results(query)
    if cached_results:
        print(f"Cache HIT for search query: {query}")
        # Track popular search
        redis_cache.track_search_query(query)
        return render_template('search.html', 
                              results=cached_results[:num],
                              query=query,
                              from_cache=True)
    
    print(f"Cache MISS for search query: {query}")
    
    # Log event to Kafka
    if 'user_id' in session:
        producer.send('recommendation_events', {
            'event_type': 'search',
            'user_id': session['user_id'],
            'query': query,
            'timestamp': datetime.now().isoformat()
        })
    
    # Perform search
    results = recommender.search_and_recommend(query, num)
    
    # Cache for 30 minutes
    redis_cache.cache_search_results(query, results, ttl=1800)
    
    # Track popular search
    redis_cache.track_search_query(query)
    
    return render_template('search.html', 
                          results=results,
                          query=query,
                          from_cache=False)

@app.route('/product/<asin>')
def product_detail(asin):
    """Product detail page with caching and activity tracking"""
    try:
        # Try cache first
        product = redis_cache.get_cached_product(asin)
        if not product:
            response = recommender.es.get(index='beauty_products', id=asin)
            product = response['_source']
            redis_cache.cache_product(asin, product, ttl=7200)
        
        # Try cached similar items
        similar_items = redis_cache.get_cached_similar_products(asin)
        if not similar_items:
            _, similar_items = recommender.get_similar_items_by_vector(asin, num=6)
            redis_cache.cache_similar_products(asin, similar_items, ttl=3600)
        
        # Track user activity
        if 'user_id' in session:
            user_id = session['user_id']
            # Track in Redis
            redis_cache.track_user_view(user_id, asin)
            # Track product views globally
            redis_cache.increment_product_views(asin)
            # Track in category
            if 'main_category' in product:
                redis_cache.increment_category_product(
                    product['main_category'], 
                    asin
                )
            
            # Log to Kafka
            producer.send('recommendation_events', {
                'event_type': 'product_view',
                'user_id': user_id,
                'asin': asin,
                'timestamp': datetime.now().isoformat()
            })
        
        # Add to recently viewed (global)
        redis_cache.add_to_recent_views(asin)
        
        return render_template('product_detail.html',
                              product=product,
                              similar_items=similar_items)
    
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'error': str(e)}), 404

@app.route('/trending')
def trending_products():
    """Get trending products from Redis"""
    trending = redis_cache.get_trending_products(20)
    
    # Enrich with product details
    enriched = []
    for asin, views in trending:
        try:
            product = redis_cache.get_cached_product(asin)
            if not product:
                response = recommender.es.get(index='beauty_products', id=asin)
                product = response['_source']
                redis_cache.cache_product(asin, product)
            
            enriched.append({
                'asin': asin,
                'views': views,
                'product': product
            })
        except:
            continue
    
    return render_template('trending.html', products=enriched)

@app.route('/hot-products')
def hot_products():
    """Get hot products (most purchased)"""
    hot = redis_cache.get_hot_products(20)
    
    enriched = []
    for asin, purchases in hot:
        try:
            product = redis_cache.get_cached_product(asin)
            if not product:
                response = recommender.es.get(index='beauty_products', id=asin)
                product = response['_source']
                redis_cache.cache_product(asin, product)
            
            enriched.append({
                'asin': asin,
                'purchases': purchases,
                'product': product
            })
        except:
            continue
    
    return render_template('hot_products.html', products=enriched)

@app.route('/recently-viewed')
def recently_viewed():
    """Get recently viewed products globally"""
    if 'user_id' not in session:
        user_history = redis_cache.get_recently_viewed(20)
    else:
        user_history = redis_cache.get_user_history(session['user_id'], 20)
    
    enriched = []
    for asin in user_history:
        try:
            product = redis_cache.get_cached_product(asin)
            if not product:
                response = recommender.es.get(index='beauty_products', id=asin)
                product = response['_source']
                redis_cache.cache_product(asin, product)
            
            enriched.append({
                'asin': asin,
                'product': product
            })
        except:
            continue
    
    return render_template('recently_viewed.html', products=enriched)

@app.route('/category/<category>')
def category_products(category):
    """Get popular products in category"""
    popular = redis_cache.get_category_popular(category, 20)
    
    enriched = []
    for asin, popularity in popular:
        try:
            product = redis_cache.get_cached_product(asin)
            if not product:
                response = recommender.es.get(index='beauty_products', id=asin)
                product = response['_source']
                redis_cache.cache_product(asin, product)
            
            enriched.append({
                'asin': asin,
                'popularity': popularity,
                'product': product
            })
        except:
            continue
    
    return render_template('category.html', 
                          category=category,
                          products=enriched)

@app.route('/api/recommendations')
def api_recommendations():
    """API endpoint for recommendations"""
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    user_id = session['user_id']
    num = request.args.get('num', 10, type=int)
    method = request.args.get('method', 'hybrid')
    
    if method == 'hybrid':
        recs = recommender.get_hybrid_recommendations(user_id, num)
    elif method == 'cf':
        recs = recommender.get_collaborative_recommendations(user_id, num)
    else:
        recs = []
    
    return jsonify({
        'user_id': user_id,
        'method': method,
        'recommendations': recs
    })

@app.route('/api/similar/<asin>')
def api_similar(asin):
    """API endpoint for similar items"""
    num = request.args.get('num', 10, type=int)
    
    product, similar = recommender.get_similar_items_by_vector(asin, num)
    
    if not product:
        return jsonify({'error': 'Product not found'}), 404
    
    similar_items = []
    for item in similar:
        similar_items.append({
            'asin': item['_id'],
            'score': item['_score'],
            'product': item['_source']
        })
    
    return jsonify({
        'asin': asin,
        'product': product,
        'similar_items': similar_items
    })

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat()
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)