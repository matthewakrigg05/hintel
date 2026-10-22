import os

from dotenv import load_dotenv

load_dotenv()

EPC_SCHEMA = os.getenv("DATABRICKS_EPC_SCHEMA", "epc")
EPC_BATCH_ROWS = int(os.getenv("EPC_BATCH_ROWS", "50000"))
