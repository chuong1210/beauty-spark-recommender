#!/bin/bash

echo "==================================="
echo "Beauty Recommendation System Setup"
echo "==================================="

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Create directory structure
echo -e "${YELLOW}Creating directory structure...${NC}"
mkdir -p data models app templates static

# Create app directory structure
mkdir -p app/templates app/static

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Docker Compose is not installed. Please install Docker Compose first.${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker and Docker Compose are installed${NC}"

# Start services
echo -e "${YELLOW}Starting Docker services...${NC}"
docker-compose up -d mongodb elasticsearch zookeeper kafka

echo -e "${YELLOW}Waiting for services to be healthy...${NC}"
sleep 30

# Check service health
echo -e "${YELLOW}Checking MongoDB...${NC}"
docker-compose exec -T mongodb mongosh --eval "db.adminCommand('ping')" || echo -e "${RED}MongoDB is not ready${NC}"

echo -e "${YELLOW}Checking Elasticsearch...${NC}"
curl -s http://localhost:9200/_cluster/health || echo -e "${RED}Elasticsearch is not ready${NC}"

echo -e "${GREEN}✓ Core services are running${NC}"

# Create Kafka topics
echo -e "${YELLOW}Creating Kafka topics...${NC}"
docker-compose exec -T kafka kafka-topics --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic recommendation_events

echo -e "${GREEN}✓ Kafka topics created${NC}"

# Process data
echo -e "${YELLOW}Processing data...${NC}"
if [ -f "data/All_Beauty.jsonl" ] && [ -f "data/meta_All_Beauty.jsonl" ]; then
    echo -e "${GREEN}Data files found. Starting data processing...${NC}"
    docker-compose run --rm spark-master spark-submit \
        --master local[*] \
        --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
        /app/data_processing.py
else
    echo -e "${RED}Data files not found in data/ directory${NC}"
    echo -e "${YELLOW}Please place All_Beauty.jsonl and meta_All_Beauty.jsonl in the data/ directory${NC}"
    exit 1
fi

# Train model
echo -e "${YELLOW}Training recommendation model...${NC}"
docker-compose run --rm spark-master spark-submit \
    --master local[*] \
    --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
    /app/model_training.py

echo -e "${GREEN}✓ Model trained successfully${NC}"

# Index to Elasticsearch
echo -e "${YELLOW}Indexing products to Elasticsearch...${NC}"
docker-compose run --rm web python elasticsearch_indexer.py

echo -e "${GREEN}✓ Products indexed to Elasticsearch${NC}"

# Start web application
echo -e "${YELLOW}Starting web application...${NC}"
docker-compose up -d web

echo -e "${GREEN}✓ Web application is running${NC}"

echo ""
echo "==================================="
echo -e "${GREEN}Setup Complete!${NC}"
echo "==================================="
echo ""
echo "Services running:"
echo "  - Web Application: http://localhost:5000"
echo "  - Elasticsearch: http://localhost:9200"
echo "  - MongoDB: mongodb://localhost:27017"
echo "  - Kafka: localhost:9092"
echo "  - Spark Master UI: http://localhost:8080"
echo ""
echo "To view logs:"
echo "  docker-compose logs -f web"
echo ""
echo "To stop all services:"
echo "  docker-compose down"
echo ""
echo "To stop and remove all data:"
echo "  docker-compose down -v"
echo ""