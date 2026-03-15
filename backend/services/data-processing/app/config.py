import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PROCESSING_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
SERVICES_DIR = os.path.abspath(os.path.join(DATA_PROCESSING_DIR, ".."))

if DATA_PROCESSING_DIR not in sys.path:
    sys.path.append(DATA_PROCESSING_DIR)

LGA_JSON_PATH = os.path.join(SERVICES_DIR, "data-retrieval", "SUBURB_TO_LGA_DATA.json")
S3_BUCKET_NAME = "nsw-crime-data-bucket"
REGION = "ap-southeast-2"
PROFILE_NAME = "notiver"
NEWS_BUCKET_NAME = "news"
NLP_BUCKET_NAME = "nlp_processed"