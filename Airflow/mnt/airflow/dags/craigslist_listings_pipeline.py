from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


import pandas as pd
import os
import requests
from datetime import datetime
from requests import get
from bs4 import BeautifulSoup
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
import googlemaps
from haversine import haversine

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


def pull_craigslist_rates_main():

    # Create the access_beautiful_soup function
    def access_beautiful_soup(url):
        # Call a get instance with the URL
        response = requests.get(url)
    
        # Sleep in order to not overwhelm servers
        time.sleep(5 + 10 * random.random())
    
        # Find all the listings links on the page
        soup = BeautifulSoup(response.text, 'html.parser')
    
        return soup
    
    # Read in the DataFrame with the links that
    # We are about to parse the data from.
    links_df = pd.read_csv('/opt/airflow/cron_job/cron_output/craigslist_links.csv')

    # Convert the 'URL' column to a list for below use
    all_links = links_df['URL'].tolist()

    # Initialize the DataFrame
    df_columns = ["Title", "Price", "Bedrooms", "Square Feet", "Full Address"]
    todays_listings_df = pd.DataFrame(columns=df_columns)

    # Set up links_and_soups dict to be used in pair_links_and_soups function
    links_and_soups = {}

    # Function to pair links with soup content
    def pair_links_and_soups(list_of_links):
        for link in list_of_links:
            the_soup = access_beautiful_soup(link)
            links_and_soups[link] = the_soup

    # Call pair_links_and_soups
    pair_links_and_soups(all_links)

    # Define your master list of attributes
    master_attributes = [
        "Title", "Price", "Bedrooms", "Square Feet", "Full Address", "monthly",
        "apartment", "cats are OK - purrr", "dogs are OK - wooof", "laundry on site",
        "air conditioning", "off-street parking", "EV charging", "w/d in unit",
        "carport", "no smoking", "attached garage", "detached garage", "laundry in bldg",
        "Fee Needed To Apply", "wheelchair accessible", "no parking", "furnished",
        "street parking", "no laundry on site", "house", "w/d hookups", "date_added"
    ]

    # Function to initialize the DataFrame with all required columns
    def initialize_dataframe():
        # Create a DataFrame with all columns initialized to None or a suitable default
        return pd.DataFrame(columns=master_attributes)

    # Function to create a new entry for the DataFrame
    def create_new_entry(data):
        return {attr: data.get(attr, 0) if attr not in ['Title', 
                                                        'Price', 
                                                        'Bedrooms', 
                                                        'Square Feet', 
                                                        'Full Address', 
                                                        'date_added'] else data.get(attr, 0) for attr in master_attributes}
    # Set up global attribute counts dict
    global_attribute_counts = {}

    # Define the count_attributes_function to view all the attributes used in apartment listings
    def process_attributes(the_soup):
        attribute_search = the_soup.find_all('div', class_='attr')
        attributes = []
        fee_needed = 0  # Initialize a flag for fees
    
        fee_pattern = re.compile(r'\b\d+\b')  # Regex to identify fee-related attributes
    
        for listing in attribute_search:
            value_span = listing.find('span', class_='valu')
            if value_span:
                attribute = value_span.text.strip()
                global_attribute_counts[attribute] = global_attribute_counts.get(attribute, 0) + 1 
                if fee_pattern.search(attribute):  # Check if attribute suggests a fee
                    fee_needed = 1
                else:
                    attributes.append(attribute)  # Only add non-fee attributes to the list
    
        return attributes, fee_needed

    # Run process_attributes using the info in the links_and_soups dictionary
    for link, soup in links_and_soups.items():
        attributes = process_attributes(soup)

    # Storing the object results in a variable called "raw_attributes" 
    raw_attributes = global_attribute_counts

    # Function to group together fee related attributes
    def clean_up_the_fees(attributes_dictionary):
        
        # Initialize a count for "Fees Needed To Apply Key"
        fees_needed_to_apply = 0
    
        # Set up a Regex to identify keys containing integers
        # We will use the re package to do this
        fee_pattern = re.compile(r'\b\d+\b')
    
        # Iterate through the dictionary, summing up counts for fee-related attributes
        for key, value in raw_attributes.items():
            if fee_pattern.search(key):
                fees_needed_to_apply += value
        
        # Update the dictionary and add in a key called "Fee Needed To Apply"
        cleaned_attributes = {key: value for key, value in raw_attributes.items() if not fee_pattern.search(key)}
        cleaned_attributes["Fee Needed To Apply"] = fees_needed_to_apply
    
        return cleaned_attributes

    # Run the clean_up_the_fees function using the raw_attributes as input
    cleaned_attributes = clean_up_the_fees(raw_attributes)

    # Sort the cleaned_attributes in descending order of instance count
    cleaned_attributes = dict(sorted(cleaned_attributes.items(), key=lambda item: item[1], reverse=True))

    # Collecting basic info from the soup content
    def collect_basic_information(the_soup):
        title_element = the_soup.find("span", id="titletextonly")
        title = title_element.text.strip() if title_element else "Title Not Found"
        
        price_element = the_soup.find("span", class_="price")
        price = price_element.text.strip() if price_element else "Price Not Found"
        
        housing_element = the_soup.find("span", class_="housing")
        if housing_element:
            try:
                bedroom_info = housing_element.text.split("/")[1].split("-")[0].strip()
                square_feet = housing_element.text.split("-")[1].split("ft")[0].strip()
            except IndexError:
                bedroom_info = "Bedrooms Info Not Found"
                square_feet = "Square Feet Not Found"
        else:
            bedroom_info = "Bedrooms Info Not Found"
            square_feet = "Square Feet Not Found"
        
        full_address_element = the_soup.find("h2", class_="street-address")
        full_address = full_address_element.text.strip() if full_address_element else "None listed"
    
        return {
            "Title": title,
            "Price": price,
            "Bedrooms": bedroom_info,
            "Square Feet": square_feet,
            "Full Address": full_address
        }

    def create_dataframe(links_and_soups):
        all_entries = []
        
        for link, soup in links_and_soups.items():
            basic_info = collect_basic_information(soup)
            listing_attributes, fee_needed = process_attributes(soup)
            new_row_data = {
                **basic_info,
                **{attr: 1 if attr in listing_attributes else 0 for attr in cleaned_attributes},
                "Fee Needed To Apply": fee_needed,
                "date_added": datetime.now().strftime('%Y-%m-%d')
            }
            
            # Create a new entry ensuring all master attributes are included
            complete_entry = create_new_entry(new_row_data)
            all_entries.append(complete_entry)
        
        return pd.DataFrame(all_entries)

    # Create the dataframe
    todays_listings_df = create_dataframe(links_and_soups)

    # Export data
    todays_listings_df.to_csv('/opt/airflow/dags/files/raw_daily_craigslist_listings.csv', index = False)




def add_locations_data_to_raw_CL_listings():
    
    # Read in the DataFrame with the daily Craigslist data
    df = pd.read_csv('/opt/airflow/dags/files/raw_daily_craigslist_listings.csv')

    # Use the API key associated with my account
    google_api_key = 'AIzaSyDzEKFusqv8uYLrOr5siGfe2pPCscdpcCQ'

    # Set up the API key in Google Maps
    gmaps = googlemaps.Client(key=google_api_key)

    # Loops through the Craigslist data and creates latitudes and longitudes.
    def geocode_address(address):
        # Geocode the address using Google Maps API
        geocode_result = gmaps.geocode(address)

        if geocode_result:
            lat = geocode_result[0]['geometry']['location']['lat']
            long = geocode_result[0]['geometry']['location']['lng']
            return lat, long
        else:
            return None, None # Return none if geocoding fails


    df['latitude'], df['longitude'] = zip(*df['Full Address'].map(geocode_address))

    # Checking the dataframe to view a sample of what we pulled
    df[['Full Address', 'latitude', 'longitude']].head()

    
    # Convert the radius from miles to meters
    def miles_to_meters(miles):
        return miles * 1609.34

    # Santa Monica's latitude and longitude
    santa_monica_lat = 34.0259
    santa_monica_lng = -118.4965
    santa_monica_location = (santa_monica_lat, santa_monica_lng)
    default_radius = miles_to_meters(3.6) # 3.6 mile radius within Santa Monica 


    # Define the function to find stores
    def find_stores(store_list, store_address_info):

        for the_store in store_list:
    
            # Perform a nearby search for stores around Santa Monica within a 3.6-mile radius
            results = gmaps.places_nearby(location=santa_monica_location, 
                                          keyword=the_store, 
                                          radius=default_radius)
        
            # Extracting and storing the names, addresses, and coordinates in the grocery_stores_info list
            for place in results['results']:
                store_info = {
                    'Name': place['name'],
                    'Address': place.get('vicinity', 'Address not provided'),
                    'Latitude': place['geometry']['location']['lat'],
                    'Longitude': place['geometry']['location']['lng']
                }
            
                # Append store information to the list
                store_address_info.append(store_info)

    # Initialize an empty list to store grocery store information
    premium_grocery_stores_address_info = []

    # List of grocery stores to be included
    search_premium_grocery_stores = ['Whole Foods Market', 'Erewhon', 'Bristol Farms']

    # Call the function 
    find_stores(search_premium_grocery_stores, premium_grocery_stores_address_info)

    # Print the list of stores to verify
    for store in premium_grocery_stores_address_info:
        print(store)


    # Initialize an empty list to store grocery store information
    midTier_grocery_stores_address_info = []

    # List of grocery stores to be included
    search_midTier_grocery_stores = ['Ralphs Fresh Fare', 'Vons', 'Trader Joe\'s']

    # Call the function 
    find_stores(search_midTier_grocery_stores, midTier_grocery_stores_address_info)

    # Print the list of stores to verify
    for store in midTier_grocery_stores_address_info:
        print(store)



    # Initialize an empty list to store grocery store information
    budget_grocery_stores_address_info = []

    # List of grocery stores to be included
    search_budget_grocery_stores = ['Costco Wholesale', 'Smart and Final']

    # Call the function 
    find_stores(search_budget_grocery_stores, budget_grocery_stores_address_info)

    # Print the list of stores to verify
    for store in budget_grocery_stores_address_info:
        print(store)

    # Define a function to calculate haversine distance
    def haversine_distance(coord1, coord2):
        return haversine(coord1, coord2, unit='mi')  # Returns distance in miles


    # function to find the nearest premium grocery store for each listing and calculate the distance
    def find_nearest_grocery_store(listing_lat, listing_lng, grocery_store_list):

        # Check if coordinates are n/a before continuing
        if pd.isna(listing_lat) or pd.isna(listing_lng):
            return None, "N/A"

        else: 
            # Initialize min_distance to be None
            min_distance = None
    
            # Initialize the nearest store
            nearest_store = None
        
            # Loop over each premium grocery store
            for store in grocery_store_list:
                store_coord = (store['Latitude'], store['Longitude'])
                listing_coord = (listing_lat, listing_lng)
            
                # Calculate the distance
                distance = haversine_distance(listing_coord, store_coord)
            
                # Update minimum distance if it's lower than the current minimum
                # Collect the nearest store's information to put into its own column
                if min_distance is None or distance < min_distance:
                    min_distance = distance
                    nearest_grocery_store = f"{store['Name']} - {store['Address']}"
        
            # Return the minimum distance
            return min_distance, nearest_grocery_store



    
    def add_store_distances_to_dataframe (df):

        # Dictionary to hold the types of stores and their respective info lists
        store_types = {
            'budget': budget_grocery_stores_address_info,
            'midTier': midTier_grocery_stores_address_info,
            'premium': premium_grocery_stores_address_info
        }

        # Loop through each store type and calculate the nearest store and distance
        for store_type, stores_info in store_types.items():
            distance_col_name = f'nearest_{store_type}_grocery_store_distance'
            store_col_name = f'nearest_{store_type}_grocery_store'
            # Apply the find_nearest_grocery_store function and assign the results
            df[[distance_col_name, store_col_name]] = pd.DataFrame(
                df.apply(
                    lambda row: find_nearest_grocery_store(row['latitude'], row['longitude'], stores_info), 
                    axis=1).tolist(), index=df.index)
        return df


    df = add_store_distances_to_dataframe(df)

    df.to_csv('/opt/airflow/dags/files/locations_data_added_to_listings.csv', index=False)


def clean_data_for_uploading():

    # Load in the locations data
    df = pd.read_csv('/opt/airflow/dags/files/locations_data_added_to_listings.csv')

    # Check for listings without titles
    no_title_df = df[df['Title'] == 'Title Not Found']

    # Drop these rows from the DataFrame
    df = df.drop(df[df['Title'] == 'Title Not Found'].index)

    df['Price'] = pd.to_numeric(df['Price'].str.replace('$','').str.replace(',',''), errors='coerce')

    # Rename columns
    df.rename(columns={'Title': 'title',
                       'Price': 'price', 
                       'Square Feet': 'square_feet',
                       'Full Address': 'full_address',
                       'cats are OK - purrr': 'cats_allowed',
                      'dogs are OK - wooof': 'dogs_allowed',
                      'laundry on site': 'laundry_on_site',
                      'air conditioning': 'air_conditioning',
                      'off-street parking': 'off_street_parking',
                      'EV charging': 'EV_charging',
                      'w/d in unit': 'washer_dryer_in_unit',
                      'no smoking': 'no_smoking',
                      'attached garage': 'attached_garage',
                      'detached garage': 'detached_garage',
                      'laundry in bldg': 'laundry_in_bldg',
                      'Fee Needed To Apply': 'fee_needed_to_apply',
                      'wheelchair accessible': 'wheelchair_accessible',
                       'no parking': 'no_parking',
                      'street parking': 'street_parking',
                      'no laundry on site': 'no_laundry_on_site',
                      'w/d hookups': 'washer_dryer_hookups'}, inplace=True)

    # List of boolean column names to convert
    boolean_float_columns = [
        'monthly', 'apartment', 'cats_allowed', 'dogs_allowed',
        'laundry_on_site', 'air_conditioning', 'off_street_parking', 'EV_charging',
        'washer_dryer_in_unit', 'carport', 'no_smoking', 'attached_garage',
        'detached_garage', 'laundry_in_bldg', 'fee_needed_to_apply',
        'wheelchair_accessible', 'no_parking', 'furnished', 'street_parking',
        'no_laundry_on_site', 'house', 'washer_dryer_hookups'
    ]


    # Define the function to convert float boolean types to integer
    def convert_float_booleans_to_int(df, columns_to_convert):
    
        # Convert each column in the list to 'int' datatype
        for column in columns_to_convert:
            df[column] = df[column].astype(int)


    # Execute the convert_float_booleans_to_int function
    convert_float_booleans_to_int(df, boolean_float_columns)

    # Remove duplicate rows, keeping the first occurrence
    df = df.drop_duplicates()

    # Drop the rows with null square_feet measures
    df.dropna(subset=['square_feet'], inplace=True)

    # Create a copy of the DataFrame with the missing address rows filtered out
    filtered_listings_df = df[df['full_address'] != 'None listed']

    df.to_csv('/opt/airflow/dags/files/ready_for_database.csv', index=False)



with DAG(
    "craigslist_listings_pipeline",
    start_date=datetime(2024, 5, 13),
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False
) as dag:

    are_craigslist_listings_available = HttpSensor(
        task_id="are_craigslist_listings_available",
        http_conn_id ="craigslist_api",
        endpoint="search/santa-monica-ca/apa?lat=34.0315&lon=-118.461&max_bathrooms=1&max_bedrooms=1&min_bathrooms=1&min_bedrooms=1&postedToday=1&search_distance=3.6#search=1~list~0~0",
        response_check=lambda response: "cl-static-search-results" in response.text,
        poke_interval=5,
        timeout=20
    )

    are_listings_links_available = FileSensor(
        task_id="are_listings_links_available",
        fs_conn_id="craigslist_listings_path",
        filepath="craigslist_links.csv",
        poke_interval=5,
        timeout=20
    )

    downloading_raw_listings = PythonOperator(
        task_id = "downloading_raw_listings",
        python_callable = pull_craigslist_rates_main
    )

    is_raw_craigslist_data_available = FileSensor(
        task_id="is_raw_craigslist_data_available",
        fs_conn_id="craigslist_listings_path",
        filepath="raw_daily_craigslist_listings.csv",
        poke_interval=5,
        timeout=20
    )

    adding_location_data_to_raw_listings = PythonOperator(
        task_id = "adding_location_data_to_raw_listings",
        python_callable = add_locations_data_to_raw_CL_listings
    )

    are_locations_added = FileSensor(
        task_id="are_locations_added",
        fs_conn_id="craigslist_listings_path",
        filepath="locations_data_added_to_listings.csv",
        poke_interval=5,
        timeout=20
    )

    cleaning_data_for_uploading_to_database = PythonOperator(
        task_id = "cleaning_data_for_uploading_to_database",
        python_callable = clean_data_for_uploading
    )

    
    saving_listings = BashOperator(
        task_id = "saving_listings",
        bash_command="""
            hdfs dfs -mkdir -p /craigslist_listings && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/ready_for_database.csv /craigslist_listings
        """
    )

    creating_listings_table = HiveOperator(
        task_id="creating_listings_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS craigslist_listings(
                title STRING,
                price DOUBLE,
                Bedrooms INT,
                square_feet INT,
                full_address STRING,
                monthly BOOLEAN,
                apartment BOOLEAN,
                cats_allowed BOOLEAN,
                dogs_allowed BOOLEAN,
                laundry_on_site BOOLEAN,
                air_conditioning BOOLEAN,
                off_street_parking BOOLEAN,
                EV_charging BOOLEAN,
                washer_dryer_in_unit BOOLEAN,
                carport BOOLEAN,
                no_smoking BOOLEAN,
                attached_garage BOOLEAN,
                detached_garage BOOLEAN,
                laundry_in_bldg BOOLEAN,
                fee_needed_to_apply BOOLEAN,
                wheelchair_accessible BOOLEAN,
                no_parking BOOLEAN,
                furnished BOOLEAN,
                street_parking BOOLEAN,
                no_laundry_on_site BOOLEAN,
                house BOOLEAN,
                washer_dryer_hookups BOOLEAN,
                date_added DATE,
                latitude DOUBLE,
                longitude DOUBLE,
                nearest_budget_grocery_store_distance DOUBLE,
                nearest_budget_grocery_store STRING,
                nearest_midTier_grocery_store_distance DOUBLE,
                nearest_midTier_grocery_store STRING,
                nearest_premium_grocery_store_distance DOUBLE,
                nearest_premium_grocery_store STRING
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )

    spark_data_processing = SparkSubmitOperator(
        task_id="spark_data_processing",
        application="/opt/airflow/dags/scripts/cL_spark_processing.py",
        conn_id="spark_conn",
        verbose=True
    )






















    
