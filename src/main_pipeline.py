"""Điểm vào chính cho pipeline: ingestion -> storage -> processing."""

from __future__ import annotations

import argparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Chạy pipeline Big Data từ đầu đến cuối")
    parser.add_argument("--project", choices=["mba", "rfm", "both"], default="both")
    parser.add_argument(
        "--step",
        choices=["all", "ingestion", "storage", "processing"],
        default="all",
        help="Chạy một bước cụ thể hoặc toàn bộ pipeline",
    )
    parser.add_argument("--force-download", action="store_true", help="Ép tải lại dữ liệu từ Kaggle")
    parser.add_argument("--mba-dataset", help="Ghi đè dataset Kaggle cho luồng MBA")
    parser.add_argument("--rfm-dataset", help="Ghi đè dataset Kaggle cho luồng RFM")
    parser.add_argument("--min-support", type=float, default=0.005, help="Ngưỡng support cho MBA")
    parser.add_argument(
        "--min-confidence", type=float, default=0.2, help="Ngưỡng confidence cho MBA"
    )
    parser.add_argument("--k-clusters", type=int, default=4, help="Số cụm cho RFM")
    parser.add_argument(
        "--mba-sample-fraction",
        type=float,
        default=1.0,
        help="Tỉ lệ mẫu (0-1] đơn MBA dùng ở bước storage (hữu ích khi máy yếu RAM)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.step in {"all", "ingestion"}:
        from ingestion import run_ingestion

        run_ingestion(
            project=args.project,
            force=args.force_download,
            mba_dataset=args.mba_dataset,
            rfm_dataset=args.rfm_dataset,
        )

    if args.step in {"all", "storage"}:
        from storage import run_storage

        run_storage(project=args.project, mba_sample_fraction=args.mba_sample_fraction)

    if args.step in {"all", "processing"}:
        from processing import run_processing

        run_processing(
            project=args.project,
            min_support=args.min_support,
            min_confidence=args.min_confidence,
            k_clusters=args.k_clusters,
        )


if __name__ == "__main__":
    main()
