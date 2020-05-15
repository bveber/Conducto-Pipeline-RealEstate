"""
This pipeline gets data from the Realtor API at rapidapi. It finds single family homes listed for rent
and listed for sale in given zip codes.  It then outputs the ratio of median Rental Price/SqFt to
median For Sale Price/SqFt. This ratio is a simple metric used to rank the potential profitability of investing
in rental properties in the given zip codes.
"""

import conducto as co
import utils
import time
import requests
import json
import pandas as pd
from io import StringIO
from datetime import datetime

pd.options.display.max_columns = None

# Constant parameters used in API request
RADIUS = '10'
SORT = 'price_low'
ZIP_CODES = ['55414', '53711', '53545', '53202', '55806']


def run(api_key: str) -> co.Serial:
    """
    This function defines the entire Conducto pipeline
    :param api_key: The API key required by rapidapi
    :return: Conducto pipeline
    """
    run.__doc__ = __doc__
    with co.Serial(image=utils.IMG, doc=co.util.magic_doc()) as output:
        # Define the download portion of the pipeline
        output['Download'] = co.Serial()
        # Get rental data by iterating through ZIP_CODES Global variable
        output['Download']['gen_rental_data'] = co.Serial()
        for zip_code in ZIP_CODES:
            rental_path = f'data/rentals_{zip_code}.csv'
            output['Download']['gen_rental_data'][str(zip_code)] = n = co.Exec(
                gen_rental_data,
                rental_path,
                api_key,
                zip_code
            )
            n.doc = co.util.magic_doc(func=gen_rental_data)
        # Pause in order to avoid API rate limits
        output['Download']['Pause'] = n = co.Exec(pause, 5)
        n.doc = co.util.magic_doc(func=pause)
        # Get for sale data by iterating through ZIP_CODES Global variable
        output['Download']['gen_sales_data'] = co.Serial()
        for zip_code in ZIP_CODES:
            sales_path = f'data/sales_{zip_code}.csv'
            output['Download']['gen_sales_data'][str(zip_code)] = n = co.Exec(
                gen_sales_data,
                sales_path,
                api_key,
                zip_code
            )
            n.doc = co.util.magic_doc(func=gen_sales_data)
        # Perform Analysis on downloaded data
        output['Analyze'] = n = co.Exec(analyze)
        n.doc = co.util.magic_doc(func=analyze)
    return output


def pause(n: int):
    """
    Pause execution in order to not exceed API limits
    :param n: Number of seconds to pause
    :return:
    """
    time.sleep(n)


def check_run_today(path: str):
    """
    Check if the data has already been downloaded today
    :param path: The path used to save data
    :return: False if data doesn't exist, or run_date if it does exist
    """
    if co.data.pipeline.exists(path):
        df = pd.read_csv(StringIO(co.data.pipeline.gets(path).decode('utf-8')))
        return df['run_date'].iloc[0]
    else:
        return False


def gen_rental_data(path: str,
                    api_key: str,
                    zip_code: str
                    ):
    """
    Get all rental data for a given zip code
    :param path: The path used to save data
    :param api_key: API key required by rapidapi
    :param zip_code: The zip code in the area of interest
    :return: None
    """
    # Check if data was already collected today
    run_check = check_run_today(path)
    if run_check != datetime.now().strftime('%Y-%m-%d'):
        data = _get_rental_data(api_key, zip_code)
    else:
        print('Data already collected today')
        return
    # Convert JSON string to Pandas dataframe
    try:
        df = pd.DataFrame(json.loads(data)['properties'])
    except (KeyError, json.decoder.JSONDecodeError):
        print(f'Error: {data}')
        return gen_rental_data(path, api_key, zip_code)
    # Check if there are any rentals in the area
    if len(df) == 0:
        print(f'No Rentals for: {zip_code}')
        return
    # Expand columns with nested JSON data
    cols_to_expand = [
        'address',
        'lot_size',
        'building_size',
    ]
    for col in cols_to_expand:
        if col in ['address']:
            df = pd.concat([df.drop(col, axis=1), df[col].apply(pd.Series)], axis=1)
        else:
            df = pd.concat([df.drop(col, axis=1), df[col].apply(pd.Series).add_prefix(col + '_')], axis=1)
    # Add run_date to dataframe
    df['run_date'] = datetime.now().strftime('%Y-%m-%d')
    # Calculate KPIs for each property
    df = get_kpis(df)
    # Save data
    df.to_csv(path)
    co.data.pipeline.put(path, path)
    print('CSV written')
    return


def _get_rental_data(api_key, zip_code):
    """
    Get raw data from API for rental properties
    :param api_key: API key required by rapidapi
    :param zip_code: The zip code in the area of interest
    :return: API response
    """
    # API endpoint
    url = "https://realtor.p.rapidapi.com/properties/v2/list-for-rent"
    # Query parameters
    querystring = {
        "postal_code": zip_code,
        "SORT": SORT,
        "RADIUS": RADIUS,
        "limit": "200",
        "offset": "0",
        "prop_type": "single_family",
    }
    # Authentication
    headers = {
        'x-rapidapi-host': "realtor.p.rapidapi.com",
        'x-rapidapi-key': api_key
    }
    return requests.request("GET", url, headers=headers, params=querystring).text


def gen_sales_data(path: str,
                   api_key: str,
                   zip_code: str
                   ):
    """
    Get all for sale data for a given zip code
    :param path: The path used to save data
    :param api_key: API key required by rapidapi
    :param zip_code: The zip code in the area of interest
    :return: None
    """
    # Check if data was already collected today
    run_check = check_run_today(path)
    if run_check != datetime.now().strftime('%Y-%m-%d'):
        data = _get_sales_data(api_key, zip_code)
    else:
        print('Data already collected today')
        return
    # Convert JSON string to Pandas dataframe
    try:
        df = pd.DataFrame(json.loads(data)['properties'])
    except (KeyError, json.decoder.JSONDecodeError):
        print(f'Error: {data}')
        return gen_sales_data(path, api_key, zip_code)
    # Check if there are any properties for sale in the area
    if len(df) == 0:
        print(f'No properties for sale in: {zip_code}')
        return
    # Expand columns with nested JSON data
    cols_to_expand = [
        'address',
        'building_size',
        'lot_size',
    ]
    for col in cols_to_expand:
        if col in ['address']:
            df = pd.concat([df.drop(col, axis=1), df[col].apply(pd.Series)], axis=1)
        else:
            df = pd.concat([df.drop(col, axis=1), df[col].apply(pd.Series).add_prefix(col + '_')], axis=1)
    # Add run_date to dataframe
    df['run_date'] = datetime.now().strftime('%Y-%m-%d')
    # Calculate KPIs for each property
    df = get_kpis(df)
    # Save data
    df.to_csv(path)
    co.data.pipeline.put(path, path)
    print('CSV written')
    return


def _get_sales_data(api_key, zip_code):
    """
    Get raw data from API for for sale properties
    :param api_key: API key required by rapidapi
    :param zip_code: The zip code in the area of interest
    :return: API response
    """
    # API endpoint
    url = "https://realtor.p.rapidapi.com/properties/v2/list-for-sale"
    # Query parameters
    querystring = {
        "prop_type": "single_family",
        "postal_code": zip_code,
        "SORT": SORT,
        "RADIUS": RADIUS,
        "limit": "200",
        "offset": "0",
    }
    # Authentication
    headers = {
        'x-rapidapi-host': "realtor.p.rapidapi.com",
        'x-rapidapi-key': api_key
    }
    return requests.request("GET", url, headers=headers, params=querystring).text


def get_kpis(df):
    """
    Calculate important KPIs per property
    :param df: Pandas Dataframe with 1 row per property
    :return: Pandas Dataframe with appended KPIs
    """
    df.loc[:, 'price_per_sqft'] = df['price'] / df['building_size_size']
    df.loc[:, 'price_per_beds'] = df['price'] / df['beds']
    df.loc[:, 'price_per_bath'] = df['price'] / df['baths']
    return df


def analyze():
    # Initialize dictionary to save analyzed data for each zip code
    d = {}
    # Iterate through zip codes
    for zip_code in ZIP_CODES:
        # Check if there is rental data for this zip code
        rental_fname = f'data/rentals_{zip_code}.csv'
        if co.data.pipeline.exists(rental_fname):
            rental_df = pd.read_csv(StringIO(co.data.pipeline.gets(rental_fname).decode('utf-8')))
        else:
            print(f'No rentals for: {zip_code}')
            rental_df = None
        # Check if there is for sale data for this zip code
        sales_fname = f'data/sales_{zip_code}.csv'
        if co.data.pipeline.exists(sales_fname):
            sales_df = pd.read_csv(StringIO(co.data.pipeline.gets(sales_fname).decode('utf-8')))
        else:
            print(f'No properties for sale in {zip_code}')
            sales_df = None
        # If data is available calculate profitabilty metric and show top-level KPIs for each zip code
        if rental_df is not None and sales_df is not None:
            d[zip_code] = rental_df['price_per_sqft'].median()/sales_df['price_per_sqft'].median()
            print(f"""
                Number of rentals: {len(rental_df)}
                Number of properties for sale: {len(sales_df)}
                """)
            print(
                f"""
KPI data for {zip_code}:
<ConductoMarkdown>
{rental_df[['price_per_sqft', 'price_per_beds', 'price_per_bath']].median(axis=0).to_markdown()}\n
{sales_df[['price_per_sqft', 'price_per_beds', 'price_per_bath']].median(axis=0).to_markdown()}
</ConductoMarkdown>"""
                  )
    # Show zip codes sorted by profitability
    print('Zip codes sorted by potential profitability')
    print(
        f"""
<ConductoMarkdown>
{pd.Series(d).rename('Rentals($/SqFt) / For-Sale($/SqFt)').sort_values(ascending=False).to_markdown()}
</ConductoMarkdown>
        """
    )


if __name__ == "__main__":
    print(__doc__)
    co.main(default=run)
