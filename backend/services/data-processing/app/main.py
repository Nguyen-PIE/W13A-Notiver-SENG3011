import os
import sys
import boto3
import json
from datetime import datetime
from transformers import pipeline

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
SERVICES_DIR = os.path.abspath(os.path.join(PARENT_DIR, ".."))

if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)

from utils.crime_classifier import classify_crime

LGA_JSON = os.path.join(SERVICES_DIR, "data-retrieval", "SUBURB_TO_LGA_DATA.json")

S3_BUCKET_NAME = "nsw-crime-data-bucket"
REGION = "ap-southeast-2"
PROFILE_NAME = "notiver"

# 1. Initialise AWS Session
try:
    session = boto3.Session(profile_name=PROFILE_NAME)
    s3 = session.client('s3', region_name=REGION)
except Exception:
    s3 = boto3.client('s3', region_name=REGION)

# 2. Initialise NLP Model
print("Loading RoBERTa model...")
sentiment_task = pipeline(
    "sentiment-analysis", 
    model="cardiffnlp/twitter-roberta-base-sentiment-latest", 
    top_k=None
)

# 3. Load Suburb Data
print("Loading JSON Suburb data...")
try:
    with open(LGA_JSON, 'r') as file:
        suburb_data = json.load(file)
except FileNotFoundError:
    print(f"Error: Could not find JSON LGA data at {LGA_JSON}")
    sys.exit(1)

sorted_suburbs = sorted(suburb_data.keys(), key=len, reverse=True)


## need to determine a function that disregards article if the crime is general and not location based (edge case: politician tax evasion)
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

def process_and_finalize():
    print(f"Scanning S3: {S3_BUCKET_NAME}/news/")
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="news/")
    
    if 'Contents' not in response:
        print("No articles found.")
        return

    for obj in response['Contents']:
        file_key = obj['Key']
        if not file_key.endswith('.txt'): continue

        try:
            file_obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
            text_content = file_obj['Body'].read().decode('utf-8')
            metadata = file_obj.get('Metadata', {})
            article_date = metadata.get('publish_date', datetime.now().isoformat())
            
            sentiment_results = sentiment_task(text_content[:1500])
            scores = {res['label']: round(res['score'], 4) for res in sentiment_results[0]}
            negative_severity = scores.get('negative', 0)

            loc = get_location_metadata(text_content)
            offence = classify_crime(text_content)
            
            base_id = file_key.split('/')[-1].replace('.txt', '')
            output_json = {
                "object_id": base_id,
                "source_type": "news",
                "offence_type": offence,
                "severity_score": negative_severity,
                "when": article_date,
                "suburb": loc['suburb'],
                "lga": loc['lga'],
                "postcode": loc['postcode']
            }

            # Verifying JSON output, manual testing..
            print("=" * 30)
            print(json.dumps(output_json, indent=4))
            json_data = json.dumps(output_json, indent=4)
            new_s3_key = f"nlp_processed/{base_id}.json"
            
            s3.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=new_s3_key,
                Body=json_data,
                ContentType='application/json'
            )
            
            print(f"Successfully processed and uploaded: s3://{S3_BUCKET_NAME}/{new_s3_key}\n")
            
        except Exception as e:
            print(f"Error processing {file_key}: {e}")

if __name__ == "__main__":
    process_and_finalize()