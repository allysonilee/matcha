from yelpapi import YelpAPI
import pandas as pd

API_KEY = "YzLUByJv2nAzfUfdDmgzycsDHvG6LPTJm_D9dla2ZkbYgT6iL__GrmBedPPc1-DRjz3o2oeJzOcvTrF7oKavzYRCHwATI_W1CVS_BMNqW6iEE5AA3ivbUSWhtcXqaXYx"

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
            "name": b["name"],
            "rating": b.get("rating"),
            "reviews": b.get("review_count"),
            "address": " ".join(b["location"]["display_address"]),
            "location_query": location
        })
    return pd.DataFrame(data)

bay_cities = ['Alameda, CA',
 'Albany, CA',
 'American Canyon, CA',
 'Antioch, CA',
 'Atherton, CA',
 'Belmont, CA',
 'Belvedere, CA',
 'Benicia, CA',
 'Berkeley, CA',
 'Brisbane, CA',
 'Burlingame, CA',
 'Campbell, CA',
 'Clayton, CA',
 'Colma, CA',
 'Concord, CA',
 'Corte Madera, CA',
 'Cupertino, CA',
 'Daly City, CA',
 'Danville, CA',
 'Dublin, CA',
 'East Palo Alto, CA',
 'El Cerrito, CA',
 'Emeryville, CA',
 'Fairfax, CA',
 'Foster City, CA',
 'Fremont, CA',
 'Gilroy, CA',
 'Half Moon Bay, CA',
 'Hayward, CA',
 'Healdsburg, CA',
 'Hercules, CA',
 'Hillsborough, CA',
 'Lafayette, CA',
 'Larkspur, CA',
 'Livermore, CA',
 'Los Altos, CA',
 'Los Altos Hills, CA',
 'Los Gatos, CA',
 'Martinez, CA',
 'Menlo Park, CA',
 'Mill Valley, CA',
 'Millbrae, CA',
 'Milpitas, CA',
 'Moraga, CA',
 'Morgan Hill, CA',
 'Mountain View, CA',
 'Newark, CA',
 'Novato, CA',
 'Oakland, CA',
 'Oakley, CA',
 'Orinda, CA',
 'Pacifica, CA',
 'Palo Alto, CA',
 'Petaluma, CA',
 'Piedmont, CA',
 'Pinole, CA',
 'Pittsburg, CA',
 'Pleasanton, CA',
 'Pleasant Hill, CA',
 'Port Costa, CA',
 'Redwood City, CA',
 'Richmond, CA',
 'Rohnert Park, CA',
 'Ross, CA',
 'Helena, CA',
 'San Anselmo, CA',
 'San Bruno, CA',
 'San Carlos, CA',
 'San Francisco, CA',
 'San Jose, CA',
 'San Leandro, CA',
 'San Mateo, CA',
 'San Pablo, CA',
 'San Rafael, CA',
 'San Ramon, CA',
 'Santa Clara, CA',
 'Santa Rosa, CA',
 'Saratoga, CA',
 'Sausalito, CA',
 'Sebastopol, CA',
 'Sonoma, CA',
 'South San Francisco, CA',
 'Suisun City, CA',
 'Sunnyvale, CA',
 'Tiburon, CA',
 'Union City, CA',
 'Vallejo, CA',
 'Walnut Creek, CA',
 'Windsor, CA',
 'Woodside, CA']

all_dfs = []

for city in bay_cities:
    df = get_matcha_spots(city)
    all_dfs.append(df)

bay_matcha_df = pd.concat(all_dfs, ignore_index = True)
bay_matcha_df = bay_matcha_df.drop_duplicates(subset = ["name", "address"])
bay_matcha_df['rating'] = bay_matcha_df['rating'].fillna(0)
## bay_matcha_df["rating"] = bay_matcha_df["rating"].fillna(bay_matcha_df["rating"].median())
## better to fillna with 0 or median? or drop?
bay_matcha_df['reviews'] = bay_matcha_df['reviews'].fillna(0)
bay_matcha_df['name'] = bay_matcha_df['name'].str.strip()

bay_matcha_df.to_csv("data/bay_area_matcha.csv", index = False)