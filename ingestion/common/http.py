import requests

def get_json(url: str, headers: dict | None = None, timeout: int = 60) -> dict:
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.json()
