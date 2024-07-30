import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_simple_urls():
    """
    Retrieve the list of URLs from the SIMPLE_URLS environment variable.
    """
    urls = os.getenv('SIMPLE_URLS')
    if urls:
        return urls.split(',')
    else:
        print("No SIMPLE_URLS found in the .env file.")
        return []

def scrape_simple_site(url):
    """
    Scrape data from the given URL.
    """
    print(f"Processing URL: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad responses
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract the entire HTML content
        data = {
            'url': url,
            'html_content': str(soup)  # Capture the entire HTML as a string
        }

        print(f"Successfully scraped: {url}")
        return data
    except Exception as e:
        print(f"\n\nError scraping {url}: {e} \n\n")
        return None  # Return None if there's an error

def save_data(data, file_path):
    """
    Save the scraped data to a CSV file.
    """
    if data is None:
        print(f"\nNo data to save for {file_path}\n")
        return

    df = pd.DataFrame([data])  # Create a DataFrame from the data
    df.to_csv(file_path, index=False)
    print(f"Saved data to {file_path}\n")
