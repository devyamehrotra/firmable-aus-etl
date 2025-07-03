from airflow import DAG
from datetime import datetime
import logging
import os

with DAG("test_dag", start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    logging.info(f"Checking for {EXTRACTED_ABR_PATH}: {os.path.exists(EXTRACTED_ABR_PATH)}")
    logging.info(f"Checking for {EXTRACTED_CC_PATH}: {os.path.exists(EXTRACTED_CC_PATH)}")
    pass