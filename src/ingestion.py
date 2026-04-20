"""Module ingestion cho đồ án Big Data.

Tải dữ liệu tự động từ Kaggle API và lưu vào data/1_raw theo yêu cầu bài nộp.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from dotenv import load_dotenv

# Có thể ghi đè dataset ID từ CLI nếu nhóm dùng nguồn khác.
DEFAULT_DATASETS = {
    "mba": "psparks/instacart-market-basket-analysis",
    "rfm": "mathchi/online-retail-ii-data-set-from-ml-repository",
}


def _configure_kaggle_credentials() -> None:
    """Nạp credentials Kaggle từ ~/.kaggle/kaggle.json hoặc biến môi trường."""
    load_dotenv()

    # Cách 1: đã set KAGGLE_CONFIG_DIR
    config_dir_env = os.getenv("KAGGLE_CONFIG_DIR")
    if config_dir_env:
        kaggle_json = Path(config_dir_env) / "kaggle.json"
        if kaggle_json.exists():
            return

    # Cách 2: file mặc định ~/.kaggle/kaggle.json
    default_json = Path.home() / ".kaggle" / "kaggle.json"
    if default_json.exists():
        os.environ["KAGGLE_CONFIG_DIR"] = str(default_json.parent)
        return

    # Cách 3: biến môi trường (có thể load từ .env)
    if os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY"):
        return

    raise RuntimeError(
        "Thiếu thông tin xác thực Kaggle. Dùng một trong các cách sau:\n"
        "1) Đặt file kaggle.json vào ~/.kaggle/kaggle.json rồi chạy: chmod 600 ~/.kaggle/kaggle.json\n"
        "2) Hoặc set KAGGLE_USERNAME và KAGGLE_KEY trong environment/.env"
    )


def fetch_kaggle_data(dataset_name: str, output_dir: str, force: bool = False) -> Path:
    """Tải và giải nén dataset từ Kaggle vào output_dir.

    Args:
        dataset_name: slug dataset Kaggle, ví dụ "owner/dataset-name".
        output_dir: thư mục đích để lưu raw files.
        force: tải lại dù thư mục đích đã có dữ liệu.

    Returns:
        Đường dẫn thư mục output.
    """
    target = Path(output_dir)
    target.mkdir(parents=True, exist_ok=True)

    if any(target.iterdir()) and not force:
        print(f"[INGESTION] Bỏ qua tải vì thư mục đã có dữ liệu: {target}")
        return target

    _configure_kaggle_credentials()

    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "Thiếu thư viện 'kaggle'. Hãy cài bằng: pip install kaggle"
        ) from exc

    print("[INGESTION] Đang xác thực Kaggle API...")
    api = KaggleApi()
    api.authenticate()

    print(f"[INGESTION] Đang tải dataset '{dataset_name}' vào '{target}'...")
    api.dataset_download_files(dataset_name, path=str(target), unzip=True)
    print(f"[INGESTION] Hoàn tất. Dữ liệu raw đã lưu tại: {target}")
    return target


def run_ingestion(project: str, force: bool, mba_dataset: str | None, rfm_dataset: str | None) -> None:
    """Chạy ingestion theo project được chọn."""
    if project in {"mba", "both"}:
        dataset = mba_dataset or DEFAULT_DATASETS["mba"]
        fetch_kaggle_data(dataset, "data/1_raw/instacart", force=force)

    if project in {"rfm", "both"}:
        dataset = rfm_dataset or DEFAULT_DATASETS["rfm"]
        fetch_kaggle_data(dataset, "data/1_raw/online_retail", force=force)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bước ingestion sử dụng Kaggle API")
    parser.add_argument("--project", choices=["mba", "rfm", "both"], default="both")
    parser.add_argument("--force", action="store_true", help="Tải lại dữ liệu kể cả khi thư mục đã có file")
    parser.add_argument("--mba-dataset", help="Ghi đè dataset Kaggle cho Market Basket")
    parser.add_argument("--rfm-dataset", help="Ghi đè dataset Kaggle cho RFM")
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
