import boto3
import json
import sys
from datetime import datetime
from transformers import pipeline

from app import config
from utils.crime_classifier import classify_crime

try:
    session = boto3.Session(profile_name=config.PROFILE_NAME)
    s3 = session.client('s3', region_name=config.REGION)
except Exception:
    s3 = boto3.client('s3', region_name=config.REGION)

print("Loading RoBERTa model...")
sentiment_task = pipeline(
    "sentiment-analysis", 
    model="cardiffnlp/twitter-roberta-base-sentiment-latest", 
    top_k=None
)

print("Loading JSON Suburb data...")
try:
    with open(config.LGA_JSON_PATH, 'r') as file:
        suburb_data = json.load(file)
except FileNotFoundError:
    print(f"Error: Could not find JSON LGA data at {config.LGA_JSON_PATH}")
    sys.exit(1)

sorted_suburbs = sorted(suburb_data.keys(), key=len, reverse=True)

def get_location_metadata(text):
    text_lower = text.lower()
    for suburb_name in sorted_suburbs:
        if f" {suburb_name.lower()} " in f" {text_lower} ":            
            suburb_info = suburb_data[suburb_name]
            return {
                "suburb": suburb_name.title(),
                "lga": suburb_info.get("councilname", "Unknown LGA").title(),
                "postcode": str(suburb_info.get("postcode", "0000")) 
            }
    return {"suburb": "NSW General", "lga": "Unknown", "postcode": "0000"}

def run_nlp_pipeline():
    """Fetches articles from S3, processes them, and uploads the JSON results."""
    
    prefix = f"{config.NEWS_BUCKET_NAME}/"
    print(f"Scanning S3: {config.S3_BUCKET_NAME}/{prefix}")
    
    response = s3.list_objects_v2(Bucket=config.S3_BUCKET_NAME, Prefix=prefix)
    
    if 'Contents' not in response:
        return {"status": "success", "message": "No articles found in bucket."}

    processed_count = 0
    skipped_count = 0
    
    all_processed_data = []

    for obj in response['Contents']:
        file_key = obj['Key']
        if not file_key.endswith('.txt'): 
            continue

        try:
            file_obj = s3.get_object(Bucket=config.S3_BUCKET_NAME, Key=file_key)
            text_content = file_obj['Body'].read().decode('utf-8')
            
            loc = get_location_metadata(text_content)
            offence = classify_crime(text_content)
            
            if offence == "General Crime" and loc["suburb"] == "NSW General":
                print(f"Skipping {file_key}: General crime with no specific location.")
                skipped_count += 1
                continue
            
            metadata = file_obj.get('Metadata', {})
            article_date = metadata.get('publish_date', datetime.now().isoformat())
            
            sentiment_results = sentiment_task(text_content[:1500])
            scores = {res['label']: round(res['score'], 4) for res in sentiment_results[0]}
            negative_sentiment = scores.get('negative', 0)

            base_id = file_key.split('/')[-1].replace('.txt', '')
            output_json = {
                "object_id": base_id,
                "source_type": "news",
                "offence_type": offence,
                "sentiment_score": negative_sentiment,
                "when": article_date,
                "suburb": loc['suburb'],
                "lga": loc['lga'],
                "postcode": loc['postcode']
            }

            all_processed_data.append(output_json)
            processed_count += 1
            
        except Exception as e:
            print(f"Error processing {file_key}: {e}")

    if all_processed_data:
        try:
            final_json_data = json.dumps(all_processed_data, indent=4)
            bulk_s3_key = f"{config.NLP_BUCKET_NAME}/all_processed_articles.json"
            
            s3.put_object(
                Bucket=config.S3_BUCKET_NAME,
                Key=bulk_s3_key,
                Body=final_json_data,
                ContentType='application/json'
            )
            print(f"Successfully uploaded bulk file: s3://{config.S3_BUCKET_NAME}/{bulk_s3_key}")
        except Exception as e:
            print(f"Error uploading bulk JSON file: {e}")

    return {
        "status": "success", 
        "processed": processed_count, 
        "skipped": skipped_count
    }

def fetch_processed_data():
    """Fetches the aggregated JSON file from S3."""
    bulk_s3_key = f"{config.NLP_BUCKET_NAME}/all_processed_articles.json"
    
    try:
        response = s3.get_object(Bucket=config.S3_BUCKET_NAME, Key=bulk_s3_key)
        file_content = response['Body'].read().decode('utf-8')
        
        # Parse JSON string into a Python list/dictionary
        return json.loads(file_content)
        
    except s3.exceptions.NoSuchKey:
        return {"error": "No processed data found. Please run the NLP pipeline first."}
    except Exception as e:
        raise Exception(f"Failed to fetch data from S3: {str(e)}")