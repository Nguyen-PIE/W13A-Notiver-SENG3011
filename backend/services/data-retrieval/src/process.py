from collections import defaultdict
from crime_categories import CRIME_WEIGHTS

def get_lga_from_suburb(suburb):
    return "PLACEHOLDER_LGA"


def lga_aggregate(events):
    lga_stats = defaultdict(lambda: {
        "weighted_crime_score": 0,
        "total_crimes": 0
    })

    for event in events:

        suburb = event.get("suburb")
        offence_type = event.get("offence_type")

        if not suburb or not offence_type:
            continue

        lga = get_lga_from_suburb(suburb)

        if not lga:
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

        sentiment = event.get("sentiment")
        suburb = event.get("suburb")

        if sentiment is None or not suburb:
            continue

        lga = get_lga_from_suburb(suburb)

        if not lga:
            continue

        sent_sum[lga] = sent_sum.get(lga, 0) + sentiment
        sent_count[lga] = sent_count.get(lga, 0) + 1

    sent_scores = {}

    for lga in sent_sum:
        sent_scores[lga] = sent_sum[lga] / sent_count[lga]

    return sent_scores


def stat_score(lga_stats, pop_data):
    exponent = 1000000
    stat_scores = {}

    for lga, stats in lga_stats.items():

        # need to get population data somewhere
        pop = pop_data.get(lga)

        if not pop or pop == 0:
            continue

        weighted_sum = stats["weighted_crime_score"]

        score = (weighted_sum / pop) * exponent

        stat_scores[lga] = score

    return stat_scores


def process_retrieval():
    
    # 1. getting from s3 
    
    #p rocessing stuff
    
    # put into dynamo 
    
    # Overall stats
    # lga | sentiment | statistical | total crimes | total articles


    # By year
    # lga | year | sentiment | stats | total | violent | theft | property | drugs | judicial | other 


if __name__ == "__main__":
    process_retrieval()