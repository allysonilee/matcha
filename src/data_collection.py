from yelpapi import YelpAPI
import pandas as pd
import numpy as np
import os
import time
import requests
import sys
from collections import defaultdict
import spacy

API_KEY = "aiJ6UT4D0i_ax7xv0-ZAyZIvA57BulH-DKTcKnNxNCbVzCLNoIZIJ5EjlCFGSUORXboCE0wLYig1hjDTELmWpFTsQ1NLRNXYl7-VsMLHg02WRs6JQlQOjkVLl07taXYx"

yelp_api = YelpAPI(API_KEY)

def get_matcha_spots(location):
    response = yelp_api.search_query(
        term = "matcha",
        location = location,
        limit = 50
    )
    businesses = response["businesses"]
    data = []
    for b in businesses: 
        data.append({
            "id": b["id"],
            "name": b["name"],
            "rating": b.get("rating"),
            "reviews": b.get("review_count"),
            "address": " ".join(b["location"]["display_address"]),
            "location_query": location
        })
    return pd.DataFrame(data)

bay_cities = ['Alameda, CA', 'Albany, CA', 'American Canyon, CA', 
              'Antioch, CA', 'Atherton, CA', 'Belmont, CA', 
              'Belvedere, CA', 'Benicia, CA', 'Berkeley, CA', 
              'Brisbane, CA', 'Burlingame, CA', 'Campbell, CA', 
              'Clayton, CA', 'Colma, CA', 'Concord, CA', 
              'Corte Madera, CA', 'Cupertino, CA', 'Daly City, CA', 
              'Danville, CA', 'Dublin, CA', 'East Palo Alto, CA', 
              'El Cerrito, CA', 'Emeryville, CA', 'Fairfax, CA', 
              'Foster City, CA', 'Fremont, CA', 'Gilroy, CA', 
              'Half Moon Bay, CA', 'Hayward, CA', 'Healdsburg, CA', 
              'Hercules, CA', 'Hillsborough, CA', 'Lafayette, CA', 
              'Larkspur, CA', 'Livermore, CA', 'Los Altos, CA', 
              'Los Altos Hills, CA', 'Los Gatos, CA', 'Martinez, CA', 
              'Menlo Park, CA', 'Mill Valley, CA', 'Millbrae, CA', 
              'Milpitas, CA', 'Moraga, CA', 'Morgan Hill, CA',
              'Mountain View, CA', 'Newark, CA', 'Novato, CA', 
              'Oakland, CA', 'Oakley, CA', 'Orinda, CA', 
              'Pacifica, CA', 'Palo Alto, CA', 'Petaluma, CA', 
              'Piedmont, CA', 'Pinole, CA', 'Pittsburg, CA', 
              'Pleasanton, CA', 'Pleasant Hill, CA', 'Port Costa, CA', 
              'Redwood City, CA', 'Richmond, CA', 'Rohnert Park, CA', 
              'Ross, CA', 'Helena, CA', 'San Anselmo, CA', 
              'San Bruno, CA', 'San Carlos, CA', 'San Francisco, CA', 
              'San Jose, CA', 'San Leandro, CA', 'San Mateo, CA', 
              'San Pablo, CA', 'San Rafael, CA', 'San Ramon, CA', 
              'Santa Clara, CA', 'Santa Rosa, CA', 'Saratoga, CA', 
              'Sausalito, CA', 'Sebastopol, CA', 'Sonoma, CA', 
              'South San Francisco, CA', 'Suisun City, CA', 'Sunnyvale, CA', 
              'Tiburon, CA', 'Union City, CA', 'Vallejo, CA', 
              'Walnut Creek, CA', 'Windsor, CA', 'Woodside, CA']

all_dfs = []

for city in bay_cities:
    df = get_matcha_spots(city)
    all_dfs.append(df)

bay_matcha_df = pd.concat(all_dfs, ignore_index = True)
bay_matcha_df = bay_matcha_df.drop_duplicates(subset = ["id", "name", "address"])
bay_matcha_df['rating'] = bay_matcha_df['rating'].fillna(0)
bay_matcha_df['reviews'] = bay_matcha_df['reviews'].fillna(0)
bay_matcha_df['name'] = bay_matcha_df['name'].str.strip()

reviews_ser = bay_matcha_df['reviews']
ratings_ser = bay_matcha_df['rating']
avg_rating = bay_matcha_df['rating'].mean()
min_threshold = bay_matcha_df['rating'].quantile(0.7)
bay_matcha_df['weighted_rating'] = ((reviews_ser * ratings_ser) + (min_threshold * avg_rating)) / (min_threshold + reviews_ser)
bay_matcha_df = bay_matcha_df.sort_values('weighted_rating', ascending = False)

os.makedirs("data", exist_ok=True)
bay_matcha_df.to_csv("data/bay_area_matcha.csv", index = False)


nlp = spacy.load("en_core_web_sm")
def lemmatize(text):
    doc = nlp(text.lower())
    return " ".join([token.lemma_ for token in doc])

specialty_items = ['matcha latte', 'matcha', 'hojicha latte', 'hojicha',
    'einspanner', 'matcha einspanner', 'strawberry matcha',
    'ceremonial matcha', 'ceremonial grade', 'uji matcha',
    'double matcha', 'matchacano', 'matcha americano',
    'soft serve', 'matcha soft serve', 'matcha ice cream',
    'matcha affogato']
def get_specialties(df):
    results = []
    for _, row in df.iterrows():
        biz_id = row["id"]
        try:
            response = yelp_api.reviews_query(id=biz_id)
            reviews = response.get("reviews", [])
            specialty_counts = defaultdict(int)
            for r in reviews: 
                text = r.get("text", "")
                clean_text = lemmatize(text)
                for item in specialty_items:
                    if all(word in clean_text for word in item.split()): 
                        specialty_counts[item] += 1
            sorted_items = sorted(specialty_counts.items(), key=lambda x: x[1], reverse=True)
            top_items = [item for item, count in sorted_items[:2]]
            results.append({
                "name": row["name"],
                "address": row["address"],
                "specialties": top_items
            })
            time.sleep(0.1)

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code
            if status_code == 429:
                print("!!! RATE LIMIT REACHED. Saving progress and exiting...")
                pd.DataFrame(results).to_csv("data/matcha_partial_results.csv", index=False)
                sys.exit()
            elif status_code == 404:
                print(f"Skipping {row['name']}: No reviews found in Yelp database.")
                continue
            else:
                print(f"Unexpected error {status_code} for {row['name']}. Skipping.")
                continue

    return pd.DataFrame(results)

matcha_specialties_df = get_specialties(bay_matcha_df)
os.makedirs("data", exist_ok=True)
matcha_specialties_df.to_csv("data/matcha_specialties.csv", index = False)