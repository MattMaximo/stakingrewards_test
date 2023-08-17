# %%

import requests
import pandas as pd
from datetime import date, timedelta


url = 'https://api.stakingrewards.com/public/query'

api_key = ""

headers = {"X-API-Key": api_key} 


slugs = ['ethereum-2-0', 'polkadot']


metrics = [
    "reward_rate",
    "benchmark_reward_rate",
    "real_reward_rate",
    "r_r_ratio",
    "marketcap",
    "block_reward",
    "inflation_rate",
    "net_staking_flow_7d",
    "staked_tokens_change_7d",
    "staked_tokens_trend",
    "staking_risk_rating",
    "staking_roi",
    "staking_roi_90d",
    "staking_roi_365d",
    "staking_marketcap",
    "staking_ratio",


]


startDay = date.today() 
endDay  = startDay + timedelta(days=1)



query = """
query getHistoricalMetrics($slugs: [String!], $assetLimit: Int, $metricLimit: Int, $metricOffset: Int, $metrics: [String!], $startDay: Date, $endDay: Date, $isActive: Boolean) {
  assets(where: {slugs: $slugs, isActive: $isActive}, limit: $assetLimit) {
    name
    metrics(
      where: {metricKeys: $metrics, createdAt_gt: $startDay, createdAt_lt: $endDay}
      limit: $metricLimit
      offset: $metricOffset
      order: {createdAt: asc}
    ) {
      defaultValue
      createdAt
      label
    }
  }
}
"""




variables = {
    "slugs": slugs,
    "assetLimit": len(slugs),
    "metricLimit": 5,
    "metricOffset": 0,
    "metrics": metrics,
    "startDay": str(startDay),
    "endDay": str(endDay),
    "isActive": True,
}
# %%


def fetch_graphql_data(query, vars):

    if vars['metricLimit'] != 500:
        print("set metricLimit to 500 to pull all data")

    response = requests.post(url, headers=headers, json={"query": query, "variables": vars})

    #print(response.text)
    response_dict = response.json()
    assets_dict = response_dict['data']['assets']

    df = pd.json_normalize(assets_dict, record_path='metrics', meta=['name'], record_prefix='metric_')


    return df


def clean_data(dataframe):
    # Pivot Dataframe to have metrics as columns and normalize the date
    pivoted_df = dataframe.pivot_table(
        index=['name', 'metric_createdAt'],
        columns='metric_label',
        values='metric_defaultValue',
        aggfunc='mean'  # or another aggregation function if needed
    )


    pivoted_df.reset_index(inplace=True)

    # Round dates down to the nearest hour
    pivoted_df['metric_createdAt'] = pd.to_datetime(pivoted_df['metric_createdAt'], format='mixed').dt.floor('H')


    # Rename Columns
    pivoted_df.rename(columns={
        "name": "token",
        "metric_createdAt": "datetime", 
        'Nominal Reward Rate': 'nominal_reward_rate', 
        'Reward Rate': 'reward_rate'
        }, inplace=True)

    return pivoted_df

def get_historical_staking_data():
    # Set up logging
    # Chunk list of assets into smaller pieces (10?)
    # For each chunk, fetch data
    # Clean data
    # Append data to dataframe
    pass

# %%

if __name__ == "__main__":
    data = fetch_graphql_data(query, variables)
    data = clean_data(data)

