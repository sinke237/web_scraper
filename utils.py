# utils.py
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

def get_env_variable(var_name):
    return os.getenv(var_name)

def get_simple_urls():
    urls = os.getenv('SIMPLE_URLS')
    return urls.split(',') if urls else []

def get_fma_url():
    return os.getenv('FMA_URL')

def get_bafin_db_company_url():
    return os.getenv('BAFIN_DB_COMPANY')

def get_bafin_db_company_category_id():
    return os.getenv('BAFIN_DB_COMPANY_CATEGORY_ID')

def get_bafin_institution_url():
    return os.getenv('BAFIN_INSTITUTION')

def get_bafin_institution_category_id():
    return os.getenv('BAFIN_INSTITUTION_CATEGORY_ID')

def get_slack_webhook_url():
    return os.getenv('SLACK_WEBHOOK_URL')

def create_directories(first_run):
    if first_run:
        if not os.path.exists('uploads/current_state'):
            os.makedirs('uploads/current_state')
    else:
        if not os.path.exists('uploads/current_state2'):
            os.makedirs('uploads/current_state2')

def compare_data(file1, file2):
    if not os.path.exists(file1) or not os.path.exists(file2):
        return None
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)
    return not df1.equals(df2)
