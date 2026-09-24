import os
from datetime import date

from dotenv import load_dotenv

load_dotenv()

PRICE_PAID_BASE_URL = os.getenv(
    "PRICE_PAID_BASE_URL",
    "https://price-paid-data.publicdata.landregistry.gov.uk/pp-2026.csv",
)
PRICE_PAID_DATASET_ID = "price_paid"
PRICE_PAID_START_YEAR = int(os.getenv("PRICE_PAID_START_YEAR", "2018"))
PRICE_PAID_END_YEAR = int(os.getenv("PRICE_PAID_END_YEAR", str(date.today().year)))
PRICE_PAID_BATCH_ROWS = int(os.getenv("PRICE_PAID_BATCH_ROWS", "50000"))

PRICE_PAID_COLUMNS = [
    "transaction_id",
    "price",
    "date_of_transfer",
    "postcode",
    "property_type",
    "old_new",
    "duration",
    "paon",
    "saon",
    "street",
    "locality",
    "town_city",
    "district",
    "county",
    "ppd_category_type",
    "record_status",
]