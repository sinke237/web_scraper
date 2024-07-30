# fma_scraper.py
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import os
import logging
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(filename='fma_scraper.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the specific categories to scrape
SPECIFIC_CATEGORIES = [
    'Banks - Banks licensed in Austria',
    'Payment institutions - Account information service provider (AISP)',
    'Payment institutions - Payment initiation service provider (PISP)'
]

def scrape_fma_site(base_url):
    options = Options()
    options.headless = True  # Run Chrome in headless mode
    options.add_argument("--no-sandbox")  # Bypass OS security model
    options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems

    driver = webdriver.Remote(
        command_executor='http://localhost:4444/wd/hub',
        options=options
    )

    all_data = []

    try:
        driver.get(base_url)

        # Extract category numbers and names from the <select> element
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'category')))
        select_element = driver.find_element(By.ID, 'category')
        options = select_element.find_elements(By.TAG_NAME, 'option')

        categories = [(option.get_attribute('value'), option.text) for option in options if option.get_attribute('value') and option.text in SPECIFIC_CATEGORIES]

        for category_number, category_name in categories:
            if category_number:  # Skip empty values
                page_number = 1
                while True:
                    category_url = f"{base_url}?cname=&place=&bic=&category={category_number}&per_page=10&submitted=1&to={page_number}"
                    logging.info(f"Processing category URL: {category_url}")
                    driver.get(category_url)

                    try:
                        # Wait for links to load
                        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '.print-view-button-wrap a')))
                        links = driver.find_elements(By.CSS_SELECTOR, '.print-view-button-wrap a')

                        for link in links:
                            href = link.get_attribute('href').replace('amp;', '')
                            corrected_url = href.replace('https://', '').replace('/', '-')
                            all_data.append({'category': category_name, 'link': href, 'corrected_url': corrected_url})

                        # Check for next page
                        next_button = driver.find_element(By.CSS_SELECTOR, 'li.copy.next a')
                        if 'disabled' in next_button.get_attribute('class'):
                            break
                        page_number += 1

                    except Exception as link_error:
                        logging.error(f"Error processing links for category {category_name}: {link_error}")
                        break

        logging.info(f"Successfully scraped {len(all_data)} links from FMA site.")
    except Exception as e:
        logging.error(f"Error during scraping: {e}")
    finally:
        driver.quit()

    return all_data

def scrape_page_content(url):
    options = Options()
    options.headless = True
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Remote(
        command_executor='http://localhost:4444/wd/hub',
        options=options
    )

    page_content = ""

    try:
        driver.get(url)
        # Wait for the page content to load
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'html')))
        page_content = driver.page_source
    except Exception as e:
        logging.error(f"Error scraping page content from {url}: {e}")
    finally:
        driver.quit()

    return page_content

def save_data(data, base_path):
    for entry in data:
        category_dir = os.path.join(base_path, 'fma', entry['category'])
        if not os.path.exists(category_dir):
            os.makedirs(category_dir)

        # Scrape the content from each corrected URL
        page_content = scrape_page_content(entry['link'])

        # Extract title content for filename
        title = extract_title(page_content)
        file_path = os.path.join(category_dir, f"{title}.csv")

        df = pd.DataFrame([{'category': entry['category'], 'link': entry['link'], 'content': page_content}])
        df.to_csv(file_path, index=False)
        logging.info(f"Saved data to {file_path}")

def extract_title(page_content):
    soup = BeautifulSoup(page_content, 'html.parser')
    title = soup.title.string.strip().replace('/', '-').replace('\\', '-').replace(':', '-')
    return title
