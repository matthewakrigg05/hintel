

def download_file(url, dest):
    pass


def request_json(url):
    pass


def calculate_sha256(path):
    """
    Skip duplicate downloads: do not process a file whose hash was already completed.
    Detect provider updates: the release date or filename may stay similar while contents change.
    Make retries safe: a failed batch can be retried without treating it as a new dataset.
    Audit provenance: record exactly which file produced each bronze batch.
    Detect corruption: verify that a downloaded file has not changed or been damaged.
    """
    pass