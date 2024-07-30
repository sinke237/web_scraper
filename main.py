import os
import time
from simple_scraper import scrape_simple_site, save_data as save_simple_data
from fma_scraper import scrape_fma_site, save_data as save_fma_data
from bafin_company_scraper import scrape_bafin_company, save_data as save_bafin_company_data
from bafin_institution_scraper import scrape_bafin_institution, save_data as save_bafin_institution_data
from notifier import send_slack_notification
from utils import (
    get_simple_urls,
    get_fma_url,
    get_bafin_db_company_url,
    get_bafin_institution_url,
    get_bafin_db_company_category_id,
    get_bafin_institution_category_id,
    compare_data,
    create_directories
)

def main():
    first_run = not os.path.exists('uploads/current_state/')
    create_directories(first_run)

    simple_urls = get_simple_urls()
    fma_url = get_fma_url()  # Get URLs from .env
    bafin_company_url = get_bafin_db_company_url()  # Get Bafin Company URL from .env
    bafin_institution_url = get_bafin_institution_url()  # Get Bafin Institution URL from .env
    bafin_company_category_id = get_bafin_db_company_category_id()  # Get Bafin Company Category ID from .env
    bafin_institution_category_id = get_bafin_institution_category_id()  # Get Bafin Institution Category ID from .env

    # Process simple URLs
    for url in simple_urls:
        data = scrape_simple_site(url)
        if data:
            safe_url = url.replace('https://', '').replace('http://', '').replace('/', '_').replace('?', '_').replace('&', '_')
            file_name = f"{safe_url}.csv"
            if first_run:
                save_simple_data(data, f'uploads/current_state/{file_name}')
            else:
                save_simple_data(data, f'uploads/current_state2/{file_name}')
                diff = compare_data(f'uploads/current_state/{file_name}', f'uploads/current_state2/{file_name}')
                if diff:
                    send_slack_notification(f'Difference found in {url}')
                    os.remove(f'uploads/current_state/{file_name}')
                    os.rename(f'uploads/current_state2/{file_name}', f'uploads/current_state/{file_name}')
                else:
                    os.remove(f'uploads/current_state2/{file_name}')
        else:
            print(f"No data returned for {url}\n")
        time.sleep(1)

    # Process FMA URL
    print(f"\nProcessing FMA URLS...")
    if fma_url:
        print(f"Scraping FMA URL: {fma_url}")
        data = scrape_fma_site(fma_url)
        if data:
            print(f"Data is being scraped from FMA URL: {fma_url}")
        else:
            print(f"No data scraped from FMA URL: {fma_url}")
        base_path = 'uploads/current_state' if first_run else 'uploads/current_state2'
        save_fma_data(data, base_path)

    # Scrape Bafin Company
    print(f"\nProcessing Bafin Company...")
    if bafin_company_url and bafin_company_category_id:
        print(f"Scraping Bafin Company URL: {bafin_company_url}")
        bafin_company_data = scrape_bafin_company(bafin_company_url, bafin_company_category_id)
        if bafin_company_data:
            print(f"Data is being scraped from Bafin Company URL: {bafin_company_url}")
        else:
            print(f"No data scraped from Bafin Company URL: {bafin_company_url}")
        base_path_bafin_company = 'uploads/current_state' if first_run else 'uploads/current_state2'
        save_bafin_company_data(bafin_company_data, base_path_bafin_company)

    # Scrape Bafin Institution
    print(f"\nProcessing Bafin Institution...")
    if bafin_institution_url and bafin_institution_category_id:
        print(f"Scraping Bafin Institution URL: {bafin_institution_url}")
        bafin_institution_data, category_name = scrape_bafin_institution(bafin_institution_url, bafin_institution_category_id)

        if bafin_institution_data:
            print(f"Data is being scraped from Bafin Institution URL: {bafin_institution_url}")
        else:
            print(f"No data scraped from Bafin Institution URL: {bafin_institution_url}")

        base_path_bafin_institution = 'uploads/current_state' if first_run else 'uploads/current_state2'
        save_bafin_institution_data(bafin_institution_data, base_path_bafin_institution, category_name)

    # Compare and manage directories
    if not first_run:
        compare_and_manage_directories()

def compare_and_manage_directories():
    base_path_current = 'uploads/current_state'
    base_path_new = 'uploads/current_state2'

    # Check if directories exist
    if not os.path.exists(base_path_current) or not os.path.exists(base_path_new):
        print("One of the directories does not exist.")
        return

    # Compare all CSV files in the current and new directories
    changes_found = False
    for root, _, files in os.walk(base_path_new):
        for file in files:
            if file.endswith('.csv'):
                current_file = os.path.join(base_path_current, file)
                new_file = os.path.join(root, file)

                if os.path.exists(current_file):
                    diff = compare_data(current_file, new_file)
                    if diff:
                        changes_found = True
                        send_slack_notification(f'Difference found in {file}')
                        os.remove(current_file)
                        os.rename(new_file, current_file)
                    else:
                        os.remove(new_file)

    # Manage directories based on changes
    if changes_found:
        # Delete the current state folder
        os.rmdir(base_path_current)  # Ensure it's empty before removing
        # Rename the new state to current
        os.rename(base_path_new, base_path_current)
    else:
        # If no changes, delete the new state folder
        os.rmdir(base_path_new)

if __name__ == "__main__":
    main()
