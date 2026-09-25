"""Extract step: download source files to the landing zone and enforce header contracts."""
import csv
import hashlib
import logging
import os
import urllib.request
from pathlib import Path

from include.ingestion.sources import BASE_URL, SOURCES

log = logging.getLogger(__name__)


class SchemaContractError(Exception):
    """Raised when an upstream file's header no longer matches its declared contract."""


def landing_dir() -> Path:
    path = Path(os.environ.get("LANDING_DIR", "data/landing"))
    path.mkdir(parents=True, exist_ok=True)
    return path


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_header(path: Path) -> tuple:
    # utf-8-sig strips the byte-order mark that one upstream file ships with
    with open(path, newline="", encoding="utf-8-sig") as fh:
        return tuple(col.strip() for col in next(csv.reader(fh)))


def validate_contract(source, path: Path) -> None:
    header = read_header(path)
    if header != source.columns:
        missing = [c for c in source.columns if c not in header]
        unexpected = [c for c in header if c not in source.columns]
        raise SchemaContractError(
            f"{source.file}: header drift detected. missing={missing} "
            f"unexpected={unexpected} order_matches={set(header) == set(source.columns)}"
        )


def extract_all(force: bool = False) -> dict:
    """Download every source (unless cached), validate its header, return {name: sha256}."""
    manifest = {}
    for source in SOURCES:
        path = landing_dir() / source.file
        if force or not path.exists():
            url = f"{BASE_URL}/{source.file}"
            log.info("downloading %s", url)
            tmp = path.with_suffix(".part")
            urllib.request.urlretrieve(url, tmp)
            tmp.replace(path)
        validate_contract(source, path)
        manifest[source.name] = sha256(path)
        log.info("%s ok (%s)", source.file, manifest[source.name][:12])
    return manifest
