# notifier.py
import requests
from utils import get_slack_webhook_url

def send_slack_notification(message):
    webhook_url = get_slack_webhook_url()
    payload = {'text': message}
    requests.post(webhook_url, json=payload)
