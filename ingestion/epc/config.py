import os

from dotenv import load_dotenv

load_dotenv()

EPC_API_TOKEN = os.getenv("EPC_API_TOKEN")
EPC_DATASET_ID = "epc_domestic"
EPC_TABLE = "epc_domestic"
EPC_SCHEMA = os.getenv("DATABRICKS_EPC_SCHEMA", "epc")
EPC_BATCH_ROWS = int(os.getenv("EPC_BATCH_ROWS", "50000"))
