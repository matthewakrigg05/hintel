import os
from dotenv import load_dotenv
load_dotenv()

LAND_REGISTRY_API_KEY=os.getenv("LAND_REGISTRY_API_KEY")

LAND_REGISTRY_DATASETS = [
    {
        "dataset_id": "hpi-UK-full-file-2026-05",
        "url": "https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/UK-HPI-full-file-2026-05.csv",
    },
    {
        "dataset_id": "hpi-average-prices-2026-05",
        "url": "https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/Average-prices-2026-05.csv",
    },
    {
        "dataset_id": "hpi-average-prices-property-type-2026-05",
        "url": "https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/Average-prices-Property-Type-2026-05.csv",
    },
    {
        "dataset_id": "hpi-sales-2026-05",
        "url": "https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/Sales-2026-05.csv",
    },
    {
        "dataset_id": "hpi-cash-mortgage-sales-2026-05",
        "url": "https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/Cash-mortgage-sales-2026-05.csv",
    },
    {
        "dataset_id": "hpi-FTB-former-owner-occupied-2026-05",
        "url": "https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/First-Time-Buyer-Former-Owner-Occupied-2026-05.csv",
    },
    {
        "dataset_id": "hpi-new-and-old-2026-05",
        "url": "https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/New-and-Old-2026-05.csv",
    },
    {
        "dataset_id": "hpi-indices-2026-05",
        "url": "https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/Indices-2026-05.csv",
    },
    {
        "dataset_id": "hpi-indices-seasonally-adjusted-2026-05",
        "url": "https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/Indices-seasonally-adjusted-2026-05.csv",
    },
    {
        "dataset_id": "hpi-average-price-seasonally-adjusted-2026-05",
        "url": "https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/Average-price-seasonally-adjusted-2026-05.csv",
    },
    {
        "dataset_id": "hpi-repossession-2026-05",
        "url": "https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/Repossession-2026-05.csv",
    }
]