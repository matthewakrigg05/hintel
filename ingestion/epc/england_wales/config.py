import os

from dotenv import load_dotenv

load_dotenv()

EPC_API_TOKEN = os.getenv("EPC_API_TOKEN")
EPC_DATASET_ID = "epc_domestic"
EPC_DATASET_IDS = {
    "domestic": EPC_DATASET_ID,
    "non_domestic": "epc_non_domestic",
}
