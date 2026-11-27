import os

from dotenv import load_dotenv

load_dotenv()

EPC_API_TOKEN = os.getenv("EPC_API_TOKEN")
EPC_DATASET_ID = "epc_domestic"
EPC_DATASET_IDS = {
    "domestic": EPC_DATASET_ID,
    "non_domestic": "epc_non_domestic",
}

EPC_BASE_URL = "https://api.get-energy-performance-data.communities.gov.uk"
EPC_DOMESTIC_CSV_URL = f"{EPC_BASE_URL}/api/files/domestic/csv"
EPC_DOMESTIC_INFO_URL = f"{EPC_DOMESTIC_CSV_URL}/info"
EPC_DATASET_URLS = {
	"domestic": EPC_DOMESTIC_CSV_URL,
	"non_domestic": f"{EPC_BASE_URL}/api/files/non-domestic/csv",
}