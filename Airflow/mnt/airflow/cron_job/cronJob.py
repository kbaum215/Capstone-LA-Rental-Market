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
import os


def pull_craigslist_links():
    
    # Define the URLs
    craigslist_base_url = 'https://losangeles.craigslist.org/search/santa-monica-ca/apa?lat=34.0315&lon=-118.461&max_bathrooms=1&max_bedrooms=1&min_bathrooms=1&min_bedrooms=1&postedToday=1&search_distance=3.6#search=1'
    craigslist_search_first_page_url = 'https://losangeles.craigslist.org/search/santa-monica-ca/apa?lat=34.0315&lon=-118.461&max_bathrooms=1&max_bedrooms=1&min_bathrooms=1&min_bedrooms=1&postedToday=1&search_distance=3.6#search=1~list~0~0'

    # Get the current working directory
    current_working_directory = os.getcwd()
    
    # Construct the full path to the chromedriver
    #chrome_driver_path = os.path.join(current_working_directory, 'chromedriver-mac-arm64', 'chromedriver')

    chrome_driver_path = '/Users/kevinbaum/Desktop/Project_Repos/Capstone_Project/capstone/Airflow/mnt/airflow/cron_job/chromedriver-mac-arm64/chromedriver'

    # Create the access_beautiful_soup function
    def access_beautiful_soup(url):
        # Call a get instance with the URL
        response = requests.get(url)
    
        # Sleep in order to not overwhelm servers
        time.sleep(5 + 10 * random.random())
    
        # Find all the listings links on the page
        soup = BeautifulSoup(response.text, 'html.parser')
    
        return soup

    # Function to return the number of posts at a given time
    def get_postings_count(website, path):
        
        # # prevent a window from opening in Selenium
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        
        # set up the Chrome driver path for Selenium usage
        service = Service(path)
        driver = webdriver.Chrome(service=service, options=options)
    
        # Call a "get" instance of the initial Craigslist page to initialize Selenium
        driver.get(website)
    
        # Put in a function stopper to let the page render.
        # 15 seconds should be plenty, but if the result is coming back as
        # "no results," the first troubleshoot would be to increase this time
        # to see if that fixes it.
        time.sleep(15)  
    
        try:
    
            # Use JavaScript to set up a script to return the postings count
            postings_count_script = """
                var postingsDiv = document.querySelector('.cl-count-save-bar > div');
                return postingsDiv ? postingsDiv.textContent : 'Postings count not found';
            """
    
            # Execute the script to get the post count and return it
            postings_count = driver.execute_script(postings_count_script)
    
            # Quit Selenium
            driver.quit()
    
            # Return the postings count
            return postings_count
        except Exception as e:
            print(f"Error encountered: {e}")

    # Call the get_postings_count function
    postings_count = get_postings_count(craigslist_search_first_page_url, chrome_driver_path)

    # Function to calculate the number of pages for us to loop through
    def calculate_pages_from_postings(postings_count_str):
        
        # Remove commas and extract the numerical part of the string
        num_postings = int(postings_count_str.replace(" postings", "").replace(",", ""))
        
        # 120 posts per page
        postings_per_page = 120
        
        # Calculate the number of pages needed to display all postings, accounting for remainder
        num_pages = -(-num_postings // postings_per_page)  
        
        return num_pages

    # Call the calculate_pages_from_postings function
    number_of_pages = calculate_pages_from_postings(postings_count)

    # Extract the links from each of the pages
    def extract_listing_links(path, base_url, number_of_pages):
        all_listing_links = []
        
        for page_number in range(number_of_pages):
            page_url = f'{base_url}~list~{page_number}~0'
            
            # prevent a window from opening in Selenium
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--disable-gpu')
            
            # set up the Chrome driver path for Selenium usage
            service = Service(path)
            driver = webdriver.Chrome(service=service, options=options)
            
            driver.get(page_url)
            
            # Wait for the listings to be present
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.cl-search-result.cl-search-view-mode-list"))
            )
            # Now that the page is loaded, find all the `a` tags within the listings
            listing_links = [a.get_attribute('href') for a in driver.find_elements(By.CSS_SELECTOR, "li.cl-search-result.cl-search-view-mode-list a")]
    
            all_listing_links.extend(listing_links)
    
            driver.quit()
            
        return all_listing_links

    all_links = extract_listing_links(chrome_driver_path, craigslist_base_url, number_of_pages)

    # Convert the list of links to a DataFrame
    links_df = pd.DataFrame(all_links, columns=['URL'])

    links_export_path = os.path.join(current_working_directory, 'cron_output', 'craigslist_links.csv')
    
    # Save the DataFrame to a CSV file
    #links_df.to_csv(links_export_path, index=False)

    # Save the DataFrame to a CSV file
    links_df.to_csv('/Users/kevinbaum/Desktop/Project_Repos/Capstone_Project/capstone/Airflow/mnt/airflow/cron_job/cron_output/craigslist_links.csv', index=False)

    print("CSV file has been created successfully.")


if __name__ == "__main__":
    pull_craigslist_links()
