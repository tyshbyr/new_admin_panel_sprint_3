import os
from dotenv import load_dotenv


load_dotenv()

dsl = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT')}

EXTRACT_BATCH_SIZE = os.environ.get('EXTRACT_BATCH_SIZE')
LOAD_BATCH_SIZE = os.environ.get('LOAD_BATCH_SIZE')
