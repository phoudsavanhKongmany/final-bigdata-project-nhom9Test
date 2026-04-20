"""Lớp storage: chuyển dữ liệu raw -> staging/curated bằng Spark + Parquet."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


RAW_ROOT = Path("data/1_raw")
STAGING_ROOT = Path("data/2_staging")
CURATED_ROOT = Path("data/3_curated")


def create_spark(app_name: str = "bigdata-storage") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.default.parallelism", "64")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _read_csv_folder(spark: SparkSession, path: Path) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(str(path / "*.csv"))
    )


def _pick_existing_column(columns: list[str], candidates: list[str]) -> str | None:
    """Trả về tên cột đầu tiên khớp với danh sách candidate (không phân biệt hoa thường)."""
    lowered = {c.lower(): c for c in columns}
    for name in candidates:
        if name.lower() in lowered:
            return lowered[name.lower()]
    return None


def stage_instacart_raw(spark: SparkSession) -> None:
    src = RAW_ROOT / "instacart"
    dst = STAGING_ROOT / "instacart"
    dst.mkdir(parents=True, exist_ok=True)

    print("[STORAGE] Đang chuyển CSV Instacart sang Parquet (staging)...")
    for file_name in ["orders", "products", "order_products__prior", "order_products__train"]:
        csv_file = src / f"{file_name}.csv"
        if csv_file.exists():
            df = spark.read.option("header", True).option("inferSchema", True).csv(str(csv_file))
            df.write.mode("overwrite").parquet(str(dst / file_name))


def curate_mba_transactions(spark: SparkSession, sample_fraction: float = 1.0) -> None:
    src = STAGING_ROOT / "instacart"
    dst = CURATED_ROOT / "mba"
    dst.mkdir(parents=True, exist_ok=True)

    prior_path = src / "order_products__prior"
    if not prior_path.exists():
        raise FileNotFoundError(
            "Thiếu file staging cho MBA: data/2_staging/instacart/order_products__prior"
        )

    order_products = spark.read.parquet(str(prior_path))
    if sample_fraction < 1.0:
        # Lấy mẫu xác định theo order_id để chạy ổn trên máy local ít RAM.
        mod_base = 1000
        threshold = max(1, int(sample_fraction * mod_base))
        order_products = order_products.filter((F.col("order_id") % mod_base) < threshold)
        print(
            f"[STORAGE] Đã áp dụng MBA sample_fraction={sample_fraction} theo order_id hash."
        )

    transactions = (
        order_products.select("order_id", F.col("product_id").cast("string").alias("product_id"))
        .groupBy("order_id")
        .agg(F.collect_set("product_id").alias("items"))
        .filter(F.size("items") > 1)
    )

    transactions.write.mode("overwrite").parquet(str(dst / "transactions"))
    print("[STORAGE] Đã ghi curated MBA transactions tại data/3_curated/mba/transactions")


def stage_online_retail_raw(spark: SparkSession) -> None:
    src = RAW_ROOT / "online_retail"
    dst = STAGING_ROOT / "online_retail"
    dst.mkdir(parents=True, exist_ok=True)

    print("[STORAGE] Đang chuyển CSV Online Retail sang Parquet (staging)...")
    df = _read_csv_folder(spark, src)
    df.write.mode("overwrite").parquet(str(dst / "sales"))


def curate_rfm_base(spark: SparkSession) -> None:
    src = STAGING_ROOT / "online_retail" / "sales"
    dst = CURATED_ROOT / "rfm"
    dst.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError("Thiếu file staging cho RFM: data/2_staging/online_retail/sales")

    sales = spark.read.parquet(str(src))

    invoice_col = _pick_existing_column(sales.columns, ["InvoiceNo", "Invoice"])
    unit_price_col = _pick_existing_column(sales.columns, ["UnitPrice", "Price"])
    customer_col = _pick_existing_column(sales.columns, ["CustomerID", "Customer ID"])
    invoice_date_col = _pick_existing_column(sales.columns, ["InvoiceDate"])
    stock_col = _pick_existing_column(sales.columns, ["StockCode"])
    description_col = _pick_existing_column(sales.columns, ["Description"])
    quantity_col = _pick_existing_column(sales.columns, ["Quantity"])

    required = {
        "InvoiceNo/Invoice": invoice_col,
        "UnitPrice/Price": unit_price_col,
        "CustomerID/Customer ID": customer_col,
        "InvoiceDate": invoice_date_col,
        "StockCode": stock_col,
        "Description": description_col,
        "Quantity": quantity_col,
    }
    missing = [k for k, v in required.items() if v is None]
    if missing:
        raise ValueError(
            f"Dữ liệu RFM thiếu cột bắt buộc: {missing}. Các cột hiện có: {sales.columns}"
        )

    # Chuẩn hóa schema retail phổ biến và loại bỏ bản ghi không hợp lệ trước khi modeling.
    cleaned = (
        sales.withColumn("InvoiceNo", F.col(invoice_col).cast("string"))
        .withColumn("StockCode", F.col(stock_col).cast("string"))
        .withColumn("Description", F.col(description_col).cast("string"))
        .withColumn("CustomerID", F.col(customer_col).cast("string"))
        .withColumn("Quantity", F.col(quantity_col).cast("double"))
        .withColumn("UnitPrice", F.col(unit_price_col).cast("double"))
        .withColumn(
            "InvoiceTS",
            F.coalesce(
                F.to_timestamp(F.col(invoice_date_col), "M/d/yyyy H:mm"),
                F.to_timestamp(F.col(invoice_date_col), "MM/dd/yyyy HH:mm"),
                F.to_timestamp(F.col(invoice_date_col)),
            ),
        )
        .filter(F.col("CustomerID").isNotNull())
        .filter(F.col("Quantity") > 0)
        .filter(F.col("UnitPrice") > 0)
        .filter(F.col("InvoiceTS").isNotNull())
        .withColumn("TotalAmount", F.col("Quantity") * F.col("UnitPrice"))
        .select(
            "InvoiceNo",
            "StockCode",
            "Description",
            "Quantity",
            "UnitPrice",
            "InvoiceTS",
            "CustomerID",
            "TotalAmount",
        )
    )

    cleaned.write.mode("overwrite").parquet(str(dst / "sales_clean"))
    print("[STORAGE] Đã ghi curated RFM base tại data/3_curated/rfm/sales_clean")


def run_storage(project: str, mba_sample_fraction: float = 1.0) -> None:
    spark = create_spark()
    try:
        if project in {"mba", "both"}:
            stage_instacart_raw(spark)
            curate_mba_transactions(spark, sample_fraction=mba_sample_fraction)

        if project in {"rfm", "both"}:
            stage_online_retail_raw(spark)
            curate_rfm_base(spark)
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bước storage: raw -> staging -> curated")
    parser.add_argument("--project", choices=["mba", "rfm", "both"], default="both")
    parser.add_argument(
        "--mba-sample-fraction",
        type=float,
        default=1.0,
        help="Tỉ lệ mẫu (0-1] của đơn MBA để chạy an toàn bộ nhớ trên local",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_storage(project=args.project, mba_sample_fraction=args.mba_sample_fraction)


if __name__ == "__main__":
    main()
