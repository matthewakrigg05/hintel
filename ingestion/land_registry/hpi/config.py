import os

from dotenv import load_dotenv

load_dotenv()

LAND_REGISTRY_API_KEY = os.getenv("LAND_REGISTRY_API_KEY")

LAND_REGISTRY_DATASETS = [
    {
        "dataset_id": "hpi_uk_full_file",
        "filename": "UK-HPI-full-file",
    },
    {
        "dataset_id": "hpi_average_prices",
        "filename": "Average-prices",
    },
    {
        "dataset_id": "hpi_average_prices_property_type",
        "filename": "Average-prices-Property-Type",
    },
    {
        "dataset_id": "hpi_sales",
        "filename": "Sales",
    },
    {
        "dataset_id": "hpi_cash_mortgage_sales",
        "filename": "Cash-mortgage-sales",
    },
    {
        "dataset_id": "hpi_first_time_buyer_former_owner_occupied",
        "filename": "First-Time-Buyer-Former-Owner-Occupied",
    },
    {
        "dataset_id": "hpi_new_and_old",
        "filename": "New-and-Old",
    },
    {
        "dataset_id": "hpi_indices",
        "filename": "Indices",
    },
    {
        "dataset_id": "hpi_indices_seasonally_adjusted",
        "filename": "Indices-seasonally-adjusted",
    },
    {
        "dataset_id": "hpi_average_price_seasonally_adjusted",
        "filename": "Average-price-seasonally-adjusted",
    },
    {
        "dataset_id": "hpi_repossession",
        "filename": "Repossession",
    }
]