from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import subprocess
import logging
import sys

# Set BASE_DIR to the root of the project (adjust as needed)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'ETL_PIPELINE_AUS'))

# Paths for scripts and data (relative to BASE_DIR)
EXTRACTED_ABR_PATH = os.path.join(BASE_DIR, 'data', 'abr', 'raw', 'companies.csv')
EXTRACTED_CC_PATH = os.path.join(BASE_DIR, 'data', 'common_crawl', 'raw', 'companies.csv')

EXTRACT_ABR_SCRIPT = os.path.join(BASE_DIR, 'scripts', 'extraction', 'extract_abr_xml.py')
EXTRACT_CC_SCRIPT = os.path.join(BASE_DIR, 'scripts', 'extraction', 'extract_common_crawl.py')
CREATE_SCHEMA_SCRIPT = os.path.join(BASE_DIR, 'scripts', 'createschema.py')
CLEAN_ABR_SCRIPT = os.path.join(BASE_DIR, 'scripts', 'cleaning', 'clean_abr.py')
CLEAN_CC_SCRIPT = os.path.join(BASE_DIR, 'scripts', 'cleaning', 'clean_common_crawl.py')
MATCH_SCRIPT = os.path.join(BASE_DIR, 'scripts', 'matching', 'entity_matching_tfidf.py')
LOAD_SCRIPT = os.path.join(BASE_DIR, 'scripts', 'loading', 'loadingcsv_topostgre_fast.py')

# Create logs directory inside BASE_DIR
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)

def get_stage_logger(stage_name):
    logger = logging.getLogger(stage_name)
    log_path = os.path.join(LOGS_DIR, f"{stage_name}.log")
    handler = logging.FileHandler(log_path)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    if not logger.hasHandlers():
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

def extract():
    logger = get_stage_logger("extract")
    try:
        logger.info(f"Checking for {EXTRACTED_ABR_PATH}: {os.path.exists(EXTRACTED_ABR_PATH)}")
        logger.info(f"Checking for {EXTRACTED_CC_PATH}: {os.path.exists(EXTRACTED_CC_PATH)}")
        if not (os.path.exists(EXTRACTED_ABR_PATH) and os.path.exists(EXTRACTED_CC_PATH)):
            logger.info("Starting extraction scripts.")
            subprocess.run([sys.executable, EXTRACT_ABR_SCRIPT], check=True)
            subprocess.run([sys.executable, EXTRACT_CC_SCRIPT], check=True)
        else:
            logger.info("Extracted files already exist. Skipping extraction.")
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise

def create_schema():
    logger = get_stage_logger("create_schema")
    try:
        logger.info("Creating schema.")
        subprocess.run([sys.executable, CREATE_SCHEMA_SCRIPT], check=True)
    except Exception as e:
        logger.error(f"Create schema failed: {e}")
        raise

def clean():
    logger = get_stage_logger("clean")
    try:
        logger.info("Cleaning data.")
        subprocess.run([sys.executable, CLEAN_ABR_SCRIPT], check=True)
        subprocess.run([sys.executable, CLEAN_CC_SCRIPT], check=True)
    except Exception as e:
        logger.error(f"Cleaning failed: {e}")
        raise

def load():
    logger = get_stage_logger("load")
    try:
        logger.info("Loading cleaned data into Postgres.")
        subprocess.run([sys.executable, LOAD_SCRIPT], check=True)
    except Exception as e:
        logger.error(f"Loading failed: {e}")
        raise

def match():
    logger = get_stage_logger("match")
    try:
        logger.info("Running entity matching.")
        subprocess.run([sys.executable, MATCH_SCRIPT], check=True)
    except Exception as e:
        logger.error(f"Matching failed: {e}")
        raise

with DAG(
    "etl_pipeline",
    start_date=datetime(2024, 7, 1),
    schedule=None,
    catchup=False,
) as dag:
    t1 = PythonOperator(task_id="extract", python_callable=extract)
    t2 = PythonOperator(task_id="create_schema", python_callable=create_schema)
    t3 = PythonOperator(task_id="clean", python_callable=clean)
    t4 = PythonOperator(task_id="load", python_callable=load)
    t5 = PythonOperator(task_id="match", python_callable=match)

    t1 >> t2 >> t3 >> t4 >> t5