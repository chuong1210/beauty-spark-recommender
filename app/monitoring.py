import redis
import json
from datetime import datetime, timedelta
from collections import defaultdict

class RedisMonitoring:
    """
    Real-time Redis monitoring and analytics
    """
    
    def __init__(self, redis_client):
        self.redis = redis_client
        self.monitoring_key = 'monitoring:stats'
    
    def get_memory_stats(self):
        """Get Redis memory statistics"""
        info = self.redis.info('memory')
        max_memory = info.get('maxmemory', 0)
        
        used_memory_percent = "N/A" if max_memory == 0 else f"{(info['used_memory'] / max_memory * 100):.2f}%"
        
        return {
            'used_memory': self._format_bytes(info['used_memory']),
            'used_memory_human': info['used_memory_human'],
            'used_memory_peak': self._format_bytes(info['used_memory_peak']),
            'used_memory_peak_human': info['used_memory_peak_human'],
            'used_memory_percent': used_memory_percent,
            'maxmemory': "Unlimited" if max_memory == 0 else self._format_bytes(max_memory),
            'memory_efficiency': self._calculate_efficiency(info)
        }
    
    def get_key_stats(self):
        """Get key statistics"""
        info = self.redis.info('keyspace')
        total_keys = 0
        stats = {}
        
        for db, data in info.items():
            if db.startswith('db'):
                keys = data['keys']
                total_keys += keys
                stats[db] = {
                    'keys': keys,
                    'expires': data.get('expires', 0),
                    'avg_ttl': data.get('avg_ttl', 0)
                }
        
        return {
            'total_keys': total_keys,
            'databases': stats
        }
    
    def get_performance_stats(self):
        """Get performance metrics"""
        info = self.redis.info()  # 👈 Lấy toàn bộ info thay vì chỉ 'stats'
        
        hits = info.get('keyspace_hits', 0)
        misses = info.get('keyspace_misses', 0)
        total = hits + misses
        
        return {
            'keyspace_hits': hits,
            'keyspace_misses': misses,
            'total_commands': info.get('total_commands_processed', 0),
            'commands_per_sec': info.get('instantaneous_ops_per_sec', 0),
            'cache_hit_rate': f"{(hits / total * 100):.2f}%" if total > 0 else "0%",
            'connected_clients': info.get('connected_clients', 0),
            'rejected_connections': info.get('rejected_connections', 0)
        }

    
    def get_cache_key_sizes(self, pattern='*'):
        """Get size distribution of cached keys"""
        key_sizes = defaultdict(int)
        total_size = 0
        
        for key in self.redis.scan_iter(match=pattern):
            size = self.redis.memory_usage(key)
            if size:
                key_sizes[key] = size
                total_size += size
        
        # Sort by size descending
        sorted_keys = sorted(key_sizes.items(), key=lambda x: x[1], reverse=True)
        
        return {
            'total_size': self._format_bytes(total_size),
            'total_keys': len(key_sizes),
            'top_10_largest': [
                {
                    'key': k.decode() if isinstance(k, bytes) else k,
                    'size': self._format_bytes(v)
                }
                for k, v in sorted_keys[:10]
            ]
        }
    
    def get_key_type_distribution(self):
        """Get distribution of key types"""
        types = defaultdict(int)
        
        for key in self.redis.scan_iter():
            key_type = self.redis.type(key)
            types[key_type] += 1
        
        return {
            'distribution': dict(types),
            'total': sum(types.values())
        }
    
    def get_expired_keys_count(self):
        """Get count of keys expiring soon"""
        expiring_soon = 0
        expiring_1h = 0
        expiring_1d = 0
        
        now = datetime.now()
        one_hour_later = now + timedelta(hours=1)
        one_day_later = now + timedelta(days=1)
        
        for key in self.redis.scan_iter():
            ttl = self.redis.ttl(key)
            if ttl > 0:
                expiring_soon += 1
                if ttl < 3600:
                    expiring_1h += 1
                if ttl < 86400:
                    expiring_1d += 1
        
        return {
            'total_expiring': expiring_soon,
            'expiring_within_1h': expiring_1h,
            'expiring_within_1d': expiring_1d
        }
    
    def analyze_recommendation_cache(self):
        """Analyze recommendation caching statistics"""
        rec_keys = list(self.redis.scan_iter(match='rec:*'))
        
        analysis = {
            'total_cached_recommendations': len(rec_keys),
            'users_with_cache': len(set(k.decode().split(':')[1] for k in rec_keys))
        }
        
        return analysis
    
    def analyze_search_cache(self):
        """Analyze search result caching"""
        search_keys = list(self.redis.scan_iter(match='search:*'))
        
        analysis = {
            'total_cached_searches': len(search_keys),
            'unique_queries': len(search_keys)
        }
        
        return analysis
    
    def analyze_trending_data(self):
        """Analyze trending products data"""
        trending = self.redis.zrevrange('trending:views', 0, 99, withscores=True)
        hot = self.redis.zrevrange('trending:purchases', 0, 99, withscores=True)
        
        return {
            'total_viewed_products': len(trending),
            'total_purchased_products': len(hot),
            'top_viewed': [
                {
                    'asin': item[0].decode() if isinstance(item[0], bytes) else item[0],
                    'views': int(item[1])
                }
                for item in trending[:10]
            ],
            'top_purchased': [
                {
                    'asin': item[0].decode() if isinstance(item[0], bytes) else item[0],
                    'purchases': int(item[1])
                }
                for item in hot[:10]
            ]
        }
    
    def get_complete_dashboard(self):
        """Get complete monitoring dashboard"""
        return {
            'timestamp': datetime.now().isoformat(),
            'memory': self.get_memory_stats(),
            'keys': self.get_key_stats(),
            'performance': self.get_performance_stats(),
            'cache_sizes': self.get_cache_key_sizes(),
            'key_types': self.get_key_type_distribution(),
            'expiring_keys': self.get_expired_keys_count(),
            'recommendations': self.analyze_recommendation_cache(),
            'search': self.analyze_search_cache(),
            'trending': self.analyze_trending_data()
        }
    
    def export_dashboard_html(self, filename='redis_dashboard.html'):
        """Export dashboard as HTML"""
        data = self.get_complete_dashboard()
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Redis Monitoring Dashboard</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    margin: 20px;
                    background-color: #f5f5f5;
                }}
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                }}
                .card {{
                    background: white;
                    padding: 20px;
                    margin: 10px 0;
                    border-radius: 8px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                h1 {{
                    color: #333;
                }}
                h2 {{
                    color: #555;
                    border-bottom: 2px solid #007bff;
                    padding-bottom: 10px;
                }}
                .stat {{
                    display: inline-block;
                    width: 30%;
                    margin: 10px;
                    padding: 10px;
                    background: #f9f9f9;
                    border-left: 4px solid #007bff;
                }}
                .stat-label {{
                    font-size: 12px;
                    color: #666;
                    text-transform: uppercase;
                }}
                .stat-value {{
                    font-size: 24px;
                    font-weight: bold;
                    color: #007bff;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin-top: 10px;
                }}
                th, td {{
                    padding: 10px;
                    text-align: left;
                    border-bottom: 1px solid #ddd;
                }}
                th {{
                    background-color: #007bff;
                    color: white;
                }}
                .warning {{
                    background-color: #fff3cd;
                    border-left: 4px solid #ffc107;
                }}
                .success {{
                    background-color: #d4edda;
                    border-left: 4px solid #28a745;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Redis Monitoring Dashboard</h1>
                <p>Generated: {data['timestamp']}</p>
                
                <div class="card">
                    <h2>Memory Usage</h2>
                    <div class="stat">
                        <div class="stat-label">Used Memory</div>
                        <div class="stat-value">{data['memory']['used_memory_human']}</div>
                    </div>
                    <div class="stat">
                        <div class="stat-label">Max Memory</div>
                        <div class="stat-value">{data['memory']['maxmemory']}</div>
                    </div>
                    <div class="stat">
                        <div class="stat-label">Usage Percent</div>
                        <div class="stat-value">{data['memory']['used_memory_percent']}</div>
                    </div>
                </div>
                
                <div class="card">
                    <h2>Performance Metrics</h2>
                    <div class="stat">
                        <div class="stat-label">Cache Hit Rate</div>
                        <div class="stat-value">{data['performance']['cache_hit_rate']}</div>
                    </div>
                    <div class="stat">
                        <div class="stat-label">Commands/sec</div>
                        <div class="stat-value">{data['performance']['commands_per_sec']}</div>
                    </div>
                    <div class="stat">
                        <div class="stat-label">Total Commands</div>
                        <div class="stat-value">{data['performance']['total_commands']:,}</div>
                    </div>
                </div>
                
                <div class="card">
                    <h2>Key Statistics</h2>
                    <div class="stat">
                        <div class="stat-label">Total Keys</div>
                        <div class="stat-value">{data['keys']['total_keys']:,}</div>
                    </div>
                    <div class="stat">
                        <div class="stat-label">Key Types</div>
                        <div class="stat-value">{data['key_types']['total']}</div>
                    </div>
                </div>
                
                <div class="card">
                    <h2>Top 10 Largest Keys</h2>
                    <table>
                        <tr>
                            <th>Key Name</th>
                            <th>Size</th>
                        </tr>
        """
        
        for item in data['cache_sizes']['top_10_largest']:
            html += f"""
                        <tr>
                            <td>{item['key']}</td>
                            <td>{item['size']}</td>
                        </tr>
            """
        
        html += """
                    </table>
                </div>
                
                <div class="card">
                    <h2>Trending Analysis</h2>
                    <p>Total Viewed Products: {}</p>
                    <p>Total Purchased Products: {}</p>
                    <h3>Top 10 Viewed Products</h3>
                    <table>
                        <tr>
                            <th>ASIN</th>
                            <th>Views</th>
                        </tr>
        """.format(
            data['trending']['total_viewed_products'],
            data['trending']['total_purchased_products']
        )
        
        for item in data['trending']['top_viewed']:
            html += f"""
                        <tr>
                            <td>{item['asin']}</td>
                            <td>{item['views']:,}</td>
                        </tr>
            """
        
        html += """
                    </table>
                </div>
                
                <div class="card">
                    <h2>Cache Analysis</h2>
                    <div class="stat">
                        <div class="stat-label">Cached Recommendations</div>
                        <div class="stat-value">{}</div>
                    </div>
                    <div class="stat">
                        <div class="stat-label">Cached Searches</div>
                        <div class="stat-value">{}</div>
                    </div>
                </div>
            </div>
        </body>
        </html>
        """.format(
            data['recommendations']['total_cached_recommendations'],
            data['search']['total_cached_searches']
        )
        
        with open(filename, 'w') as f:
            f.write(html)
        
        print(f"Dashboard exported to {filename}")
    
    @staticmethod
    def _format_bytes(bytes_size):
        """Format bytes to human readable"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_size < 1024:
                return f"{bytes_size:.2f} {unit}"
            bytes_size /= 1024
        return f"{bytes_size:.2f} TB"
    
    @staticmethod
    def _calculate_efficiency(info):
        """Calculate memory efficiency ratio"""
        try:
            return f"{(info['used_memory'] / info['used_memory_peak'] * 100):.2f}%"
        except:
            return "N/A"


# Flask endpoint integration
def setup_monitoring_endpoints(app, redis_client):
    """Setup Flask monitoring endpoints"""
    monitor = RedisMonitoring(redis_client)
    
    @app.route('/admin/monitoring/dashboard')
    def monitoring_dashboard():
        """Full dashboard JSON"""
        return monitor.get_complete_dashboard()
    
    @app.route('/admin/monitoring/memory')
    def monitoring_memory():
        """Memory stats"""
        return monitor.get_memory_stats()
    
    @app.route('/admin/monitoring/performance')
    def monitoring_performance():
        """Performance stats"""
        return monitor.get_performance_stats()
    
    @app.route('/admin/monitoring/trending')
    def monitoring_trending():
        """Trending analysis"""
        return monitor.analyze_trending_data()
    
    @app.route('/admin/monitoring/export-html')
    def export_dashboard():
        """Export dashboard as HTML"""
        monitor.export_dashboard_html('/tmp/redis_dashboard.html')
        return {'status': 'exported', 'file': '/tmp/redis_dashboard.html'}


if __name__ == "__main__":
    # Test monitoring
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    monitor = RedisMonitoring(redis_client)
    
    # Print dashboard
    import json
    dashboard = monitor.get_complete_dashboard()
    print(json.dumps(dashboard, indent=2))
    
    # Export HTML
    monitor.export_dashboard_html('redis_dashboard.html')
    print("Dashboard exported to redis_dashboard.html")