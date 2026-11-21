"""Utility to download certified Delta Lake JAR dependencies for container build."""

import os
import urllib.request
from pathlib import Path

JARS_TO_DOWNLOAD = [
    (
        "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.3.0/delta-spark_2.12-3.3.0.jar",
        "delta-spark_2.12-3.3.0.jar",
    ),
    (
        "https://repo1.maven.org/maven2/io/delta/delta-storage/3.3.0/delta-storage-3.3.0.jar",
        "delta-storage-3.3.0.jar",
    ),
]


def download_jars(target_dir: Path | None = None) -> list[Path]:
    """Download certified Delta Lake 3.3.0 JAR files to target directory.

    Args:
        target_dir: Directory where JARs should be stored. Defaults to
            docker/airflow/jars.

    Returns:
        List of Path objects for downloaded JAR files.
    """
    if target_dir is None:
        target_dir = Path(__file__).resolve().parent / "jars"

    target_dir.mkdir(parents=True, exist_ok=True)
    downloaded_paths: list[Path] = []

    for url, filename in JARS_TO_DOWNLOAD:
        destination = target_dir / filename
        if not destination.exists() or destination.stat().st_size == 0:
            print(f"Downloading {url} -> {destination}...")
            urllib.request.urlretrieve(url, destination)
            print(f"Downloaded {filename} ({os.path.getsize(destination)} bytes)")
        downloaded_paths.append(destination)

    return downloaded_paths


if __name__ == "__main__":
    download_jars()
