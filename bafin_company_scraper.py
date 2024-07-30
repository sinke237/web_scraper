import os
import logging
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(filename='bafin_company_scraper.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get environment variables
BASE_URL_COMPANY = os.getenv('BAFIN_DB_COMPANY')  # Base URL for the Bafin database
CATEGORY_ID_COMPANY = os.getenv('BAFIN_DB_COMPANY_CATEGORY_ID')  # Category ID to scrape
SEARCH_BUTTON_LABEL = os.getenv('BAFIN_SEARCH_BUTTON_LABEL', 'Suche')  # Default search button label

def create_driver():
    options = Options()
    options.headless = True
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Remote(
        command_executor='http://localhost:4444/wd/hub',
        options=options,
        keep_alive=True
    )
    driver.set_page_load_timeout(120)
    driver.implicitly_wait(30)
    return driver

def scrape_bafin_company(base_url, category_id):
    all_data = []
    driver = create_driver()
    links_to_scrape = set()  # Use a set to avoid duplicates
    category_name = ""

    try:
        # Construct the category URL
        category_url = f"{base_url}/sucheForm.do?institutName=&institutId=&institutBakNr=&institutRegNr=&kategorieId={category_id}&sucheButtonInstitut={SEARCH_BUTTON_LABEL}&locale=en_GB"
        logging.info(f"Processing category URL: {category_url}")

        driver.get(category_url)

        # Extract category name from the <select> element
        category_select = driver.find_element(By.ID, 'institutKategorie')
        options = category_select.find_elements(By.TAG_NAME, 'option')
        for option in options:
            if option.get_attribute('value') == category_id:
                category_name = option.text.strip()
                break

        logging.info(f"Extracted category name: {category_name}")

        page_number = 1

        while True:
            # Construct the pagination URL
            paginated_url = f"{category_url}&d-4012550-p={page_number}"
            logging.info(f"Fetching URL: {paginated_url}")
            driver.get(paginated_url)

            # Extract links for each institution
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, 'tbody')))
            rows = driver.find_elements(By.CSS_SELECTOR, 'tbody tr')

            if not rows:
                logging.info("No more rows found. Ending pagination.")
                break  # Exit loop if no rows are found

            # Extract links from the current page
            for row in rows:
                try:
                    link_element = row.find_element(By.TAG_NAME, 'a')
                    link = link_element.get_attribute('href').replace("amp;", "")
                    if link:
                        full_link = f"{base_url}/{link}" if not link.startswith('http') else link
                        links_to_scrape.add(full_link)  # Add link to the set for uniqueness
                        logging.info(f"Extracted link: {full_link}")
                    else:
                        logging.warning("Found an anchor tag without an href attribute.")
                except Exception as e:
                    logging.error(f"Error extracting link from row: {e}")

            # Check for the next page
            try:
                pagination_links = driver.find_elements(By.CSS_SELECTOR, "span.pagelinks a")
                next_page_found = False

                for link in pagination_links:
                    if "Next" in link.text or "vor" in link.text:  # Check for 'Next' or 'vor'
                        next_page_found = True
                        page_number += 1  # Increment page number for the next iteration
                        break

                if not next_page_found:
                    logging.info("No next page found. Ending pagination.")
                    break  # Exit if there is no next page
            except Exception:
                logging.info("No pagination links found. Ending pagination.")
                break  # Exit if no pagination links are found

        # Now scrape data from the collected links
        for link in links_to_scrape:
            try:
                title, content = scrape_page_content(driver, link)
                if title and content:
                    all_data.append({'category': category_name, 'link': link, 'title': title, 'content': content})
                    logging.info(f"Scraped content from {link} with title: {title}")
            except Exception as e:
                logging.error(f"Error scraping page content from {link}: {e}")

    except Exception as e:
        logging.error(f"Error during scraping: {e}")
    finally:
        driver.quit()

    logging.info(f"Successfully scraped {len(all_data)} links from Bafin Company site.")
    return all_data

def scrape_page_content(driver, url):
    page_content = ""
    title = ""

    try:
        driver.get(url)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        page_content = driver.page_source
        title = extract_title(page_content)
    except Exception as e:
        logging.error(f"Error loading page content from {url}: {e}")
        return None, None  # Return None if there's an error

    return title, page_content

def extract_title(page_content):
    soup = BeautifulSoup(page_content, 'html.parser')

    # Find the <div> with id 'content' within 'wrapperContent'
    content_div = soup.find('div', id='wrapperContent').find('div', id='content')

    if content_div:
        # Find the first <p> tag within the content div
        first_paragraph = content_div.find('p')
        if first_paragraph:
            strong_element = first_paragraph.find('strong')  # Look for the <strong> tag
            if strong_element:
                title = strong_element.text.strip()
                # Replace problematic characters
                title = title.replace('/', '-').replace('\\', '-').replace(':', '-').replace(' ', '_').replace('.', '')
                return title
            else:
                logging.warning("No <strong> tag found in the first <p>.")
        else:
            logging.warning("No <p> tag found in the content div.")
    else:
        logging.warning("No content div found within the wrapper content.")

    return None  # Return None if title is not found

def save_data(data, base_path):
    for entry in data:
        category_dir = os.path.join(base_path, 'bafin', 'BAFIN_DB_COMPANY', entry['category'])
        if not os.path.exists(category_dir):
            os.makedirs(category_dir)

        title = entry['title'].replace(',', '').strip()  # Remove commas
        title = title[:40]  # Truncate to 40 characters
        file_path = os.path.join(category_dir, f"{title}.csv")

        try:
            df = pd.DataFrame([{'category': entry['category'], 'link': entry['link'], 'title': title}])
            df.to_csv(file_path, index=False)
            logging.info(f"Saved data to {file_path}")
        except PermissionError as e:
            logging.error(f"Permission error when saving {file_path}: {e}")
        except Exception as e:
            logging.error(f"Error saving {file_path}: {e}")

