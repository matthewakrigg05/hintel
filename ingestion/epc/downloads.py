import csv
import io
import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from ingestion.common.download import calculate_sha256, download_file
from ingestion.common.http import get_json
from ingestion.common.storage import data_root, prepare_dataset_dir

from .config import EPC_API_TOKEN, EPC_DATASET_ID


EPC_BASE_URL = "https://api.get-energy-performance-data.communities.gov.uk"
EPC_DOMESTIC_CSV_URL = f"{EPC_BASE_URL}/api/files/domestic/csv"
EPC_DOMESTIC_INFO_URL = f"{EPC_DOMESTIC_CSV_URL}/info"


def _dataset_dir() -> Path:
	return prepare_dataset_dir("domestic", "epc")


def _manifest_path() -> Path:
	return _dataset_dir() / "manifest.json"


def _load_manifest() -> dict | None:
	path = _manifest_path()
	if not path.exists():
		return None
	return json.loads(path.read_text(encoding="utf-8"))


def get_dataset_manifest() -> dict | None:
	return _load_manifest()


def _write_manifest(details: dict) -> None:
	path = _manifest_path()
	temporary = path.with_suffix(".json.part")
	temporary.write_text(json.dumps(details, indent=2) + "\n", encoding="utf-8")
	temporary.replace(path)


def update_dataset_manifest(**details: object) -> dict:
	manifest = _load_manifest() or {}
	manifest.update(details)
	_write_manifest(manifest)
	return manifest


def api_headers() -> dict[str, str]:
	if not EPC_API_TOKEN:
		raise RuntimeError("EPC_API_TOKEN is required to access the EPC data API")
	return {
		"Authorization": f"Bearer {EPC_API_TOKEN}",
		"Accept": "application/json",
	}


def get_full_load_info() -> dict:
	response = get_json(EPC_DOMESTIC_INFO_URL, headers=api_headers())
	info = response.get("data", {})
	if not info.get("lastUpdated") or not info.get("fileSize"):
		raise ValueError("EPC full-load info response is missing fileSize or lastUpdated")
	return info


def is_download_current(last_updated: str) -> bool:
	manifest = _load_manifest()
	return bool(
		manifest
		and (_dataset_dir() / "raw.zip").exists()
		and manifest.get("last_updated") == last_updated
	)


def save_downloaded_dataset(info: dict, refresh: bool = False) -> Path:
	dataset_dir = _dataset_dir()
	destination = dataset_dir / "raw.zip"
	previous = _load_manifest()
	if (
		not refresh
		and previous
		and destination.exists()
		and previous.get("last_updated") == info["lastUpdated"]
	):
		print(f"Already retrieved {EPC_DATASET_ID} for {info['lastUpdated']}")
		return destination

	temporary = dataset_dir / "raw.zip.candidate"
	temporary.unlink(missing_ok=True)
	try:
		print(f"Downloading {EPC_DATASET_ID} from {EPC_DOMESTIC_CSV_URL}")
		download_file(EPC_DOMESTIC_CSV_URL, temporary, headers=api_headers())
		expected_size = int(info["fileSize"])
		actual_size = temporary.stat().st_size
		if actual_size != expected_size:
			raise ValueError(
				f"Downloaded EPC archive size {actual_size} does not match "
				f"the published size {expected_size}"
			)

		with zipfile.ZipFile(temporary) as archive:
			members = [
				item.filename
				for item in archive.infolist()
				if not item.is_dir() and item.filename.lower().endswith(".csv")
			]
			if not members:
				raise ValueError("Downloaded EPC archive contains no CSV files")
			for member in members:
				with archive.open(member) as source:
					text = io.TextIOWrapper(source, encoding="utf-8-sig", newline="")
					header = next(csv.reader(text), [])
					if not header:
						raise ValueError(f"EPC archive contains a CSV without a header: {member}")

		details = {
			"dataset_id": EPC_DATASET_ID,
			"url": EPC_DOMESTIC_CSV_URL,
			"last_updated": info["lastUpdated"],
			"published_file_size": expected_size,
			"file_size": actual_size,
			"file_type": "zip",
			"members": members,
			"sha256": calculate_sha256(temporary),
			"accepted_at": datetime.now(timezone.utc).isoformat(),
		}
		temporary.replace(destination)
		_write_manifest(details)
		return destination
	except Exception:
		temporary.unlink(missing_ok=True)
		temporary.with_suffix(temporary.suffix + ".part").unlink(missing_ok=True)
		raise


def existing_raw_path() -> Path:
	raw_path = _dataset_dir() / "raw.zip"
	if not raw_path.exists():
		raise FileNotFoundError(f"No EPC raw archive found in {raw_path.parent}")
	return raw_path


def append_run_log(status: str, **details: object) -> None:
	log_path = data_root() / "epc" / "run_log.jsonl"
	log_path.parent.mkdir(parents=True, exist_ok=True)
	event = {
		"timestamp": datetime.now(timezone.utc).isoformat(),
		"dataset_id": EPC_DATASET_ID,
		"status": status,
		**details,
	}
	with log_path.open("a", encoding="utf-8") as log:
		log.write(json.dumps(event) + "\n")