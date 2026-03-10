import os
import boto3
import newspaper
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

load_dotenv()

# Constants
FILE_PATH = "guardian_crime_urls.txt"
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME") 
S3_REGION = os.getenv("AWS_DEFAULT_REGION")

def upload_to_s3(text_content, s3_key):
    """Uploads string content directly to an S3 object."""
    s3 = boto3.client('s3', region_name=S3_REGION)
    try:
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=text_content)
        print(f"Successfully uploaded to s3://{S3_BUCKET_NAME}/{s3_key}")
    except NoCredentialsError:
        print("Credentials not available. Check your AWS CLI config.")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def process_articles():
    # 1. Read URLs from file
    if not os.path.exists(FILE_PATH):
        print(f"Error: File not found at {FILE_PATH}")
        return

    with open(FILE_PATH, 'r') as f:
        urls = [line.strip() for line in f if line.strip()]

    print(f"Found {len(urls)} URLs. Starting scrape...")

    # 2. Scrape and Upload
    for i, url in enumerate(urls):
        try:
            # Newspaper4k extraction
            article = newspaper.article(url)
            article.download()
            article.parse()
            
            # Clean text
            content = article.text
            
            if content:
                s3_filename = f"news/article_{i}.txt"
                
                # 3. Send to S3
                upload_to_s3(content, s3_filename)
            else:
                print(f"Skipping {url}: No text content found.")

        except Exception as e:
            print(f"Failed to process {url}: {e}")

if __name__ == "__main__":
    process_articles()