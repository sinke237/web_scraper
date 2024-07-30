from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import os
import logging
import re
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(filename='bafin_institution_scraper.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get environment variables
BASE_URL = os.getenv('BAFIN_INSTITUTION')
CATEGORY_ID = os.getenv('BAFIN_INSTITUTION_CATEGORY_ID')

def create_driver():
    options = Options()
    options.headless = True
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Remote(
        command_executor='http://localhost:4444/wd/hub',
        options=options
    )
    return driver

def sanitize_filename(name):
    return re.sub(r'[<>:"/\\|?*]', '_', name)  # Replace invalid characters with underscores

def scrape_bafin_institution(base_url, category_id):
    driver = create_driver()
    all_scrapable_links = []
    category_name = ""

    try:
        # Navigate to the base URL
        driver.get(base_url)
        logging.info(f"Navigated to base URL: {base_url}")

        # Select the category from the dropdown and submit
        category_name = select_category(driver, category_id)

        # Scrape data from the category page
        all_scrapable_links.extend(scrape_category_pages(driver, category_id))

        # Scrape content from each scrapable link and save to CSV
        for link in all_scrapable_links:
            content = scrape_page_content(driver, link)
            if content:
                title = extract_title_from_link(link)
                save_data(content, title, category_name)

        logging.info(f"Successfully scraped {len(all_scrapable_links)} links from Bafin Institution site.")
    except Exception as e:
        logging.error(f"Error during scraping: {e}")
    finally:
        driver.quit()

    return all_scrapable_links, category_name  # Return both data and category_name

def select_category(driver, category_id):
    driver.get("https://portal.mvp.bafin.de/database/ZahlInstInfo/suche.do")
    logging.info("Navigated to the category search page.")

    # Wait for the dropdown to be present and select the category
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'filterObjektart')))
    category_select = driver.find_element(By.ID, 'filterObjektart')
    category_select.click()  # Open the dropdown
    logging.info("Opened category dropdown.")

    options = category_select.find_elements(By.TAG_NAME, 'option')
    category_name = ""
    for option in options:
        if option.get_attribute('value') == str(category_id):
            category_name = option.text.strip()  # Get the category name
            option.click()  # Select the category
            logging.info(f"Selected category: {category_name} (ID: {category_id})")
            break

    submit_button = driver.find_element(By.ID, 'nameZahlungsinstitutButton')
    submit_button.click()
    logging.info("Clicked the search button to navigate to the selected category page.")

    return category_name

def scrape_category_pages(driver, category_id):
    scrapable_links = []
    page_number = 1

    while True:
        logging.info(f"Processing category page: {driver.current_url}")
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'tbody')))

        rows = driver.find_elements(By.CSS_SELECTOR, 'tbody tr')
        if not rows:
            logging.info("No rows found, ending scraping.")
            break

        for row in rows:
            link_element = row.find_element(By.TAG_NAME, 'a')
            extracted_link = link_element.get_attribute('href').replace("amp;", "")
            scrapable_link = f"{BASE_URL}/{extracted_link.split('/')[-1]}"
            scrapable_links.append(scrapable_link)
            logging.info(f"Extracted scrapable link: {scrapable_link}")

        logging.info(f"Found {len(rows)} links on page {page_number}.")

        next_button = driver.find_elements(By.CSS_SELECTOR, 'a[title="zum Abschnitt {}"]'.format(page_number + 1))

        if next_button:
            page_number += 1
            next_page_url = next_button[0].get_attribute('href')
            driver.get(next_page_url)
            logging.info(f"Navigating to page {page_number}...")
        else:
            logging.info("No more pages to process.")
            break

    return scrapable_links

def scrape_page_content(driver, url):
    content = ""

    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        content = driver.page_source
        logging.info(f"Scraped content from {url}.")
    except Exception as e:
        logging.error(f"Error scraping page content from {url}: {e}")

    return content if content else ""

def extract_title_from_link(link):
    # Extract a title for the file from the link
    title = link.split('=')[-1]  # Extract the ID from the link
    title = title.replace('/', '_').replace('.', '').replace(',', '').replace(':', '').replace(' ', '_')
    return title

def save_data(content, title, category_name):
    # Save the scraped content to a CSV file in the specified directory
    directory = f"uploads/current_state/bafin/BAFIN_INSTITUTION/{sanitize_filename(category_name)}"

    # Create the directory if it doesn't exist
    os.makedirs(directory, exist_ok=True)

    # Define the file path
    file_path = os.path.join(directory, f"{title}.csv")

    # Create a DataFrame from the content
    df = pd.DataFrame([content])  # Assuming content can be converted to a DataFrame

    # Save the DataFrame to CSV
    df.to_csv(file_path, index=False, encoding='utf-8')
    logging.info(f"Saved data to {file_path}")
