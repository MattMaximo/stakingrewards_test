# %%

import streamlit as st
import requests
import pandas as pd
from datetime import date, timedelta
import base64

url = 'https://api.stakingrewards.com/public/query'

api_key = '1c9f355d-73c0-4678-af32-552e29b5de32'

headers = {"X-API-Key": api_key} 

def fetch_graphql_data(query, vars):

    response = requests.post(url, headers=headers, json={"query": query, "variables": vars})

    #print(response.text)
    response_dict = response.json()
    assets_dict = response_dict['data']['assets']

    return pd.DataFrame(assets_dict)



query_asset_list = """
query query1_get_asset_slugs($metricKey_desc: String!, $limit: Int, $offset: Int) {
  assets(where:{isActive:true}, order: {metricKey_desc: $metricKey_desc}, limit: $limit, offset: $offset) {
    name
    slug
    symbol
    notification
    stakingContractAudited
    credits
    stakingLaunchDate
    mainnetLaunchDate

  }
}

"""

#    symbol
#    notification
#    stakingContractAudited
#    credits
#    stakingLaunchDate
#    mainnetLaunchDate


vars = {
    "metricKey_desc": "marketcap", # Metric to rank by
    "limit": 100, # Number of assets to return
    "offset": 0 # Use to Paginate -- offset in intervals of limit
    }

#data = fetch_data(query_asset_list, vars)

# %%

import logging

# Set up logging
logging.basicConfig(level=logging.INFO)


data_frames = [] # To store the fetched data

while True:
    try:
        logging.info(f"Fetching data with offset: {vars['offset']}")

        df = fetch_graphql_data(query_asset_list, vars)
        
        # Check if the DataFrame is empty
        if df.empty:
            logging.info('Returned an empty DataFrame. Done fetching.')
            break
        
        # Append the fetched data to the list
        data_frames.append(df)
        
        # Increment the offset by the limit value
        vars['offset'] += vars['limit']

    except Exception as e:
        logging.error(f"An error occurred: {e}. Done fetching.")
        break


data = pd.concat(data_frames, ignore_index=True)
# %%
