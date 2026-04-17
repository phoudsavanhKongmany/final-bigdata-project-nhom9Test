"""Data ingestion module for Big Data final project.

This module downloads datasets automatically from Kaggle API and stores
raw files in data/1_raw as required by the course rubric.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from dotenv import load_dotenv

# You can override these dataset IDs from CLI if your team uses other sources.
DEFAULT_DATASETS = {
    "mba": "psparks/instacart-market-basket-analysis",
    "rfm": "mathchi/online-retail-ii-data-set-from-ml-repository",
}


def _configure_kaggle_credentials() -> None:
    """Load Kaggle credentials from ~/.kaggle/kaggle.json or environment."""
    load_dotenv()

    # Method 1: explicit KAGGLE_CONFIG_DIR
    config_dir_env = os.getenv("KAGGLE_CONFIG_DIR")
    if config_dir_env:
        kaggle_json = Path(config_dir_env) / "kaggle.json"
        if kaggle_json.exists():
            return

    # Method 2: default ~/.kaggle/kaggle.json
    default_json = Path.home() / ".kaggle" / "kaggle.json"
    if default_json.exists():
        os.environ["KAGGLE_CONFIG_DIR"] = str(default_json.parent)
        return

    # Method 3: env vars (can be loaded from .env)
    if os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY"):
        return

    raise RuntimeError(
        "Missing Kaggle credentials. Use one of these methods:\n"
        "1) Put kaggle.json at ~/.kaggle/kaggle.json and run: chmod 600 ~/.kaggle/kaggle.json\n"
        "2) Or set KAGGLE_USERNAME and KAGGLE_KEY in environment/.env"
    )


def fetch_kaggle_data(dataset_name: str, output_dir: str, force: bool = False) -> Path:
    """Download and unzip dataset from Kaggle into output_dir.

    Args:
        dataset_name: Kaggle dataset slug, e.g. "owner/dataset-name".
        output_dir: Target directory to store raw files.
        force: Re-download even if output directory is not empty.

    Returns:
        Path to output directory.
    """
    target = Path(output_dir)
    target.mkdir(parents=True, exist_ok=True)

    if any(target.iterdir()) and not force:
        print(f"[INGESTION] Skip download because directory is not empty: {target}")
        return target

    _configure_kaggle_credentials()

    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "Missing dependency 'kaggle'. Install with: pip install kaggle"
        ) from exc

    print(f"[INGESTION] Authenticating Kaggle API...")
    api = KaggleApi()
    api.authenticate()

    print(f"[INGESTION] Downloading dataset '{dataset_name}' into '{target}'...")
    api.dataset_download_files(dataset_name, path=str(target), unzip=True)
    print(f"[INGESTION] Done. Raw data stored at: {target}")
    return target


def run_ingestion(project: str, force: bool, mba_dataset: str | None, rfm_dataset: str | None) -> None:
    """Run ingestion for selected project mode."""
    if project in {"mba", "both"}:
        dataset = mba_dataset or DEFAULT_DATASETS["mba"]
        fetch_kaggle_data(dataset, "data/1_raw/instacart", force=force)

    if project in {"rfm", "both"}:
        dataset = rfm_dataset or DEFAULT_DATASETS["rfm"]
        fetch_kaggle_data(dataset, "data/1_raw/online_retail", force=force)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingestion step using Kaggle API")
    parser.add_argument("--project", choices=["mba", "rfm", "both"], default="both")
    parser.add_argument("--force", action="store_true", help="Force re-download datasets")
    parser.add_argument("--mba-dataset", help="Override Kaggle dataset for Market Basket")
    parser.add_argument("--rfm-dataset", help="Override Kaggle dataset for RFM")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_ingestion(
        project=args.project,
        force=args.force,
        mba_dataset=args.mba_dataset,
        rfm_dataset=args.rfm_dataset,
    )


if __name__ == "__main__":
    main()
