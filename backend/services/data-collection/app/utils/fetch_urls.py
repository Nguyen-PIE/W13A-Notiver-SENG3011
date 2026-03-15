import requests
import time
import random
import calendar
import os


def get_random_monthly_articles(start_year, end_year, filename="guardian_crime_urls.txt"):
    print(f"Fetching 1 random article per month from {start_year} to {end_year}...")
    print(f"Saving URLs to: {filename}\n")
    print("=" * 60)
    
    url = "https://content.guardianapis.com/search"
    
    with open(filename, "w") as file:
        
        # Loop through every year
        for year in range(start_year, end_year + 1):
             
             for month in range(1, 13):
                _, max_days = calendar.monthrange(year, month)                
                random_day = random.randint(1, max_days)
                
                target_date = f"{year}-{month:02d}-{random_day:02d}"
                
                params = {
                    "q": "NSW AND (police OR crime)", 
                    "section": "australia-news",      
                    "from-date": target_date,        
                    "to-date": target_date,          
                    "page-size": 1,                   
                    "api-key": "test"                 
                }
                
                try:
                    response = requests.get(url, params=params, timeout=10)
                    
                    if response.status_code == 200:
                        articles = response.json().get("response", {}).get("results", [])
                        
                        if articles:
                            article_url = articles[0].get('webUrl')
                            print(f"{target_date}: {article_url}")
                            file.write(f"{article_url}\n")
                            
                        # If the random day was quiet, pick a random article from the whole month
                        else:
                            fallback_params = {
                                "q": "NSW AND (police OR crime)", 
                                "section": "australia-news",      
                                "from-date": f"{year}-{month:02d}-01",        
                                "to-date": f"{year}-{month:02d}-{max_days}",          
                                "page-size": 50,
                                "api-key": "test"                 
                            }
                            fb_response = requests.get(url, params=fallback_params, timeout=10)
                            fb_articles = fb_response.json().get("response", {}).get("results", [])
                            
                            if fb_articles:
                                random_article = random.choice(fb_articles)
                                fallback_url = random_article.get('webUrl')
                                actual_date = random_article.get('webPublicationDate')[:10]
                                print(f"{target_date} was quiet. Fallback to {actual_date}: {fallback_url}")
                                file.write(f"{fallback_url}\n")
                            else:
                                print(f"{year}-{month:02d}: No crime articles found all month!")
                                
                except Exception as e:
                    print(f"Request failed for {year}-{month:02d}: {e}")

                time.sleep(2)
                
    print("\nDone! All links successfully saved.")

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    SAVE_PATH = os.path.join(BASE_DIR, "..", "guardian_crime_urls.txt")
  
    get_random_monthly_articles(2021, 2025, filename=SAVE_PATH)
