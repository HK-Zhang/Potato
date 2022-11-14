import os
from dotenv import load_dotenv
load_dotenv()

app_insights_key = os.environ.get('APP_INSIGHT_KEY') or 'TO_BE_REPLACED'
appversion = os.environ.get('APP_VERSION')
servicebus_connection_key = os.environ.get('SERVICE_BUS_KEY') or "svc-bus-conn-general"