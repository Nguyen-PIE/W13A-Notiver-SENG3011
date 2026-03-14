#!/bin/bash

set -e

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Starting the Notiver Data Pipeline..."
echo "=========================================="


echo ""
echo "PHASE 1: DATA COLLECTION"
echo "------------------------------------------"
cd "$BASE_DIR/services/data-collection/app"

echo "Fetching new Guardian URLs..."
python3 fetch_urls.py

echo "Scraping articles and uploading to S3..."
python3 main.py
echo "Collection complete."


echo ""
echo "PHASE 2: DATA PROCESSING"
echo "------------------------------------------"
cd "$BASE_DIR/services/data-processing/app"

echo "Running RoBERTa Sentiment Analysis..."
python3 main.py

echo ""
echo "=========================================="
echo "PIPELINE COMPLETE!"