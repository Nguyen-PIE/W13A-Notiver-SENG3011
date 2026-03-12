import boto3
import json
import sys
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))

if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)

from collections import defaultdict
from crime_dict import CRIME_WEIGHTS
from LGAData import get_lga_population

S3_BUCKET_NAME = "nsw-crime-data-bucket"
REGION = "ap-southeast-2"
PROFILE_NAME = "notiver"

events = []

# Initialise AWS session and connect to S3 data
try:
    session = boto3.Session(profile_name=PROFILE_NAME)
    s3 = session.client('s3', region_name=REGION)
except Exception:
    s3 = boto3.client('s3', region_name=REGION)

# Connect to DynamoDB data
dynamodb = session.resource('dynamodb', region_name=REGION)
lga_overall_table = dynamodb.Table('lga-overall')
lga_by_year_table = dynamodb.Table('lga-by-year')


def process_retrieval():
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="nlp_processed/")

    if 'Contents' not in response:
        print("No articles found.")
        return
    
    for obj in response['Contents']:
        file_key = obj['Key']
        if not file_key.endswith('.json'):
            continue
        
        try:
            file_obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
            text_content = file_obj['Body'].read().decode('utf-8')
            
            data = json.loads(text_content)
            events.append(data)

        except Exception as e:
            print(f"Error processing {file_key}: {e}")

    lga_sentiment_scores = sentiment_scores(events)
    lga_stat_scores = stat_score(lga_aggregate(events))
    lga_total_crimes = count_total_crimes(())
    lga_total_articles = count_total_articles((events))

    upload_lga_overall_data(lga_sentiment_scores, lga_stat_scores, lga_total_crimes, lga_total_articles)


    # put into dynamo 
    
    # Overall stats
    # lga | sentiment | statistical | total crimes | total articles


    # By year
    # lga | year | sentiment | stats | total | violent | theft | property | drugs | judicial | other 


def upload_lga_overall_data(lga_sentiment_scores, lga_stat_scores, lga_total_crimes, lga_total_articles):
    table_entries = defaultdict(lambda: {
        "sentiment_score": 0,
        "statistical_score": 0,
        "total_crimes": 0,
        "total_articles": 0
    })

    for lga, sent_score in lga_sentiment_scores.items():
        table_entries[lga]["sentiment_score"] = str(sent_score)
    
    for lga, stat_score in lga_stat_scores.items():
        table_entries[lga]["statistical_score"] = str(stat_score)

    for lga, crime_count in lga_total_crimes.items():
        table_entries[lga]["total_crimes"] = crime_count

    for lga, article_count in lga_total_articles.items():
        table_entries[lga]["total_articles"] = article_count

    data = []
    for key, value in table_entries.items():
        item = {
            "lga": key,
            "sentiment_score": value["sentiment_score"],
            "statistical_score": value["statistical_score"],
            "total_crimes": value["total_crimes"],
            "total_articles": value["total_articles"]
        }
        data.append(item)

    with lga_overall_table.batch_writer() as writer:
        for item in data:
            writer.put_item(Item=item)

    # Note that it looks like table format isn't set up on AWS yet??


def upload_lga_by_year_data():

    # Implementation

    return


def count_total_articles(events):
    total_articles = defaultdict(int)

    for event in events:
        lga = event.get("lga")

        if lga != "LGA not found":
            total_articles[lga] += 1
    
    return total_articles


def count_total_crimes(events):
    total_crimes = {}

    # Calculate total crimes per lga

    return total_crimes


def lga_aggregate(events):
    lga_stats = defaultdict(lambda: {
        "weighted_crime_score": 0,
        "total_crimes": 0
    })

    for event in events:

        suburb = event.get("suburb")
        offence_type = event.get("offence_type")
        lga = event.get("lga")

        if not suburb or not offence_type or lga == "LGA not found":
            continue

        offence_type = offence_type.lower()

        # get weight of crime
        weight = CRIME_WEIGHTS.get(offence_type, 1)

        lga_stats[lga]["weighted_crime_score"] += weight
        lga_stats[lga]["total_crimes"] += 1

    return lga_stats


def sentiment_scores(events):
    sent_sum = {}
    sent_count = {}

    for event in events:

        severity = event.get("severity_score")
        suburb = event.get("suburb")
        lga = event.get("lga")

        if severity is None or not suburb or lga == "LGA not found":
            continue

        sent_sum[lga] = sent_sum.get(lga, 0) + severity
        sent_count[lga] = sent_count.get(lga, 0) + 1

    sent_scores = {}

    for lga in sent_sum:
        sent_scores[lga] = sent_sum[lga] / sent_count[lga]

    return sent_scores


# might need to revise statistical scores - don't think we're using Shanelle's data yet so this might be calculated wrong?
def stat_score(lga_stats):
    exponent = 1000000
    stat_scores = {}

    for lga, stats in lga_stats.items():

        # need to get population data somewhere
        pop = get_lga_population(lga)

        if pop == -1 or pop == 0:
            continue

        weighted_sum = stats["weighted_crime_score"]

        score = (weighted_sum / pop) * exponent

        stat_scores[lga] = score

    return stat_scores


if __name__ == "__main__":
    process_retrieval()
