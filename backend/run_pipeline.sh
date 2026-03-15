#!/bin/bash

set -e

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

COLLECTION_PYTHON="$BASE_DIR/services/data-collection/.venv/bin/python"
PROCESSING_PYTHON="$BASE_DIR/services/data-processing/.venv/bin/python"

echo "Starting the Notiver Data Pipeline..."
echo "=========================================="

echo ""
echo "PHASE 1: DATA COLLECTION"
echo "------------------------------------------"
cd "$BASE_DIR/services/data-collection/app"

echo "Fetching new Guardian URLs..."
"$COLLECTION_PYTHON" fetch_urls.py

echo "Scraping articles and uploading to S3..."
"$COLLECTION_PYTHON" main.py
echo "Collection complete."

echo ""
echo "PHASE 2: DATA PROCESSING"
echo "------------------------------------------"
cd "$BASE_DIR/services/data-processing"

echo "Running RoBERTa Sentiment Analysis..."
"$PROCESSING_PYTHON" -c "from app.services.processor import run_nlp_pipeline; print(run_nlp_pipeline())"

echo ""
echo "=========================================="
echo "PIPELINE COMPLETE!"