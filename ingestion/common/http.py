import requests

def get_json(url: str, headers: dict | None = None, timeout: int = 60) -> dict:
    """
    Perform an HTTP GET request and decode its JSON response so timeout and HTTP
    error handling stay consistent across data providers.

    Args:
        url: URL to request.
        headers: Optional HTTP request headers.
        timeout: Request timeout in seconds.

    Returns:
        The decoded JSON object.
    """
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.json()
