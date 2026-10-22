from ..config import EPC_BATCH_ROWS, EPC_SCHEMA


SCOTLAND_DATASETS = {
    "domestic_current": {
        "table": "scotland_domestic_current",
        "url": (
            "https://data.gov.scot/dataset/"
            "domestic_energy_performance_certificates___dataset_to_q1_2026/"
            "resource/5ebad3f5-c40e-4f80-b508-2c69712f0d25/download"
        ),
    },
    "non_domestic_current": {
        "table": "scotland_non_domestic_current",
        "url": (
            "https://data.gov.scot/dataset/"
            "non_domestic_energy_performance_certificates___dataset_to_q1_2026/"
            "resource/ab755981-bf70-4ae5-8050-0e04f88cb8ba/download"
        ),
    },
    "domestic_extended": {
        "table": "scotland_domestic_extended",
        "url": (
            "https://data.gov.scot/dataset/"
            "domestic_energy_performance_certificates___extended_historic_dataset_to_q1_2026/"
            "resource/b7bc304c-debb-4365-9a86-1c8cb82cc60d/download"
        ),
    },
    "non_domestic_extended": {
        "table": "scotland_non_domestic_extended",
        "url": (
            "https://data.gov.scot/dataset/"
            "non_domestic_energy_performance_certificates___extended_historic_dataset_to_q1_2026/"
            "resource/c7873ae4-d19a-43ee-8d9f-415e7d0f05d7/download"
        ),
    },
}

__all__ = ["EPC_BATCH_ROWS", "EPC_SCHEMA", "SCOTLAND_DATASETS"]
