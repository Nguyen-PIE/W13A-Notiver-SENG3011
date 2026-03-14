import os
from datetime import datetime
import boto3
import newspaper
from botocore.exceptions import ProfileNotFound

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FILE_PATH = os.path.join(BASE_DIR, "..", "guardian_crime_urls.txt")

S3_BUCKET_NAME = "nsw-crime-data-bucket"
REGION = "ap-southeast-2"
PROFILE_NAME = "notiver"

def get_s3_client():
    try:
        session = boto3.Session(profile_name=PROFILE_NAME)
        return session.client('s3', region_name=REGION)
    except ProfileNotFound:
        return boto3.client('s3', region_name=REGION)

def upload_to_s3(s3_client, text_content, s3_key, publish_date):
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME, 
            Key=s3_key, 
            Body=text_content,
            Metadata={'publish_date': str(publish_date)}
        )
        print(f"Uploaded: s3://{S3_BUCKET_NAME}/{s3_key}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def process_articles():
    s3_client = get_s3_client()

    if not os.path.exists(FILE_PATH):
        print(f"Error: File not found at {FILE_PATH}")
        return

    with open(FILE_PATH, 'r') as f:
        urls = [line.strip() for line in f if line.strip()]

    print(f"Found {len(urls)} URLs. Starting extraction...")

    for i, url in enumerate(urls):
        try:
            article = newspaper.article(url)
            article.download()
            article.parse()
            
            content = article.text
            
            pub_date = article.publish_date
            date_str = pub_date.isoformat() if pub_date else datetime.now().isoformat()

            if content:
                s3_filename = f"news/article_{i+1}.txt"
                upload_to_s3(s3_client, content, s3_filename, date_str)
            else:
                print(f"Skipping {url}: No text content found.")
        except Exception as e:
            print(f"Failed to process {url}: {e}")

if __name__ == "__main__":
    process_articles()