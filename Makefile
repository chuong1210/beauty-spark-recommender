.PHONY: help setup start stop restart logs clean data train index test

help:
	@echo "Beauty Recommendation System - Available Commands"
	@echo ""
	@echo "  make setup       - Initial setup (run once)"
	@echo "  make start       - Start all services"
	@echo "  make stop        - Stop all services"
	@echo "  make restart     - Restart all services"
	@echo "  make logs        - View logs"
	@echo "  make clean       - Stop and remove all containers and volumes"
	@echo "  make data        - Process data"
	@echo "  make train       - Train ML model"
	@echo "  make index       - Index to Elasticsearch"
	@echo "  make test        - Run tests"
	@echo ""

setup:
	@echo "Starting initial setup..."
	chmod +x setup.sh
	./setup.sh

start:
	@echo "Starting all services..."
	docker-compose up -d

stop:
	@echo "Stopping all services..."
	docker-compose down

restart:
	@echo "Restarting all services..."
	docker-compose restart

logs:
	docker-compose logs -f

logs-web:
	docker-compose logs -f web

logs-spark:
	docker-compose logs -f spark-master spark-worker

clean:
	@echo "WARNING: This will remove all containers and data!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose down -v; \
		rm -rf models/*; \
		echo "Cleanup complete!"; \
	fi

data:
	@echo "Processing data..."
	docker-compose run --rm spark-master spark-submit \
		--master local[*] \
		--packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
		/app/data_processing.py

train:
	@echo "Training model..."
	docker-compose run --rm spark-master spark-submit \
		--master local[*] \
		--packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
		/app/model_training.py

index:
	@echo "Indexing to Elasticsearch..."
	docker-compose run --rm web python elasticsearch_indexer.py

test:
	@echo "Running tests..."
	docker-compose run --rm web python -m pytest tests/

# Database operations
db-shell:
	docker-compose exec mongodb mongosh beauty_db

es-health:
	@curl -s http://localhost:9200/_cluster/health | python -m json.tool

kafka-topics:
	docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Development
dev-web:
	docker-compose up web

rebuild:
	docker-compose build --no-cache
	docker-compose up -d