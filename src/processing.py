"""Processing layer for MBA (FP-Growth) and RFM (K-Means) with Spark DataFrame."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


CURATED_ROOT = Path("data/3_curated")
RESULT_ROOT = Path("data/3_curated/results")


def create_spark(app_name: str = "bigdata-processing") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def run_market_basket(
    spark: SparkSession,
    min_support: float = 0.005,
    min_confidence: float = 0.2,
) -> None:
    src = CURATED_ROOT / "mba" / "transactions"
    out_dir = RESULT_ROOT / "mba"
    out_dir.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError("Missing curated MBA transactions at data/3_curated/mba/transactions")

    transactions = spark.read.parquet(str(src)).select("items")

    model = FPGrowth(itemsCol="items", minSupport=min_support, minConfidence=min_confidence).fit(
        transactions
    )

    model.freqItemsets.write.mode("overwrite").parquet(str(out_dir / "frequent_itemsets"))
    model.associationRules.write.mode("overwrite").parquet(str(out_dir / "association_rules"))

    print("[PROCESSING] MBA result written to data/3_curated/results/mba")


def run_rfm_segmentation(spark: SparkSession, k_clusters: int = 4) -> None:
    src = CURATED_ROOT / "rfm" / "sales_clean"
    out_dir = RESULT_ROOT / "rfm"
    out_dir.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError("Missing curated RFM base at data/3_curated/rfm/sales_clean")

    sales = spark.read.parquet(str(src))

    snapshot = sales.select(F.max("InvoiceTS").alias("max_ts")).collect()[0]["max_ts"]
    if snapshot is None:
        raise ValueError("RFM source is empty after cleaning")

    rfm = (
        sales.groupBy("CustomerID")
        .agg(
            F.datediff(F.lit(snapshot), F.max("InvoiceTS")).alias("recency_days"),
            F.countDistinct("InvoiceNo").alias("frequency"),
            F.round(F.sum("TotalAmount"), 2).alias("monetary"),
        )
        .filter(F.col("frequency") > 0)
        .filter(F.col("monetary") > 0)
    )

    assembler = VectorAssembler(
        inputCols=["recency_days", "frequency", "monetary"], outputCol="features_raw"
    )
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    kmeans = KMeans(k=k_clusters, seed=42, featuresCol="features", predictionCol="segment")

    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    model = pipeline.fit(rfm)
    segmented = model.transform(rfm)

    segment_summary = (
        segmented.groupBy("segment")
        .agg(
            F.count("*").alias("customers"),
            F.round(F.avg("recency_days"), 2).alias("avg_recency_days"),
            F.round(F.avg("frequency"), 2).alias("avg_frequency"),
            F.round(F.avg("monetary"), 2).alias("avg_monetary"),
        )
        .orderBy("segment")
    )

    # Translate technical cluster IDs into business labels by scoring cluster profile:
    # - lower recency is better
    # - higher frequency and monetary are better
    w_recency = Window.orderBy(F.col("avg_recency_days").desc())
    w_freq = Window.orderBy(F.col("avg_frequency").asc())
    w_monetary = Window.orderBy(F.col("avg_monetary").asc())

    labeled_summary = (
        segment_summary.withColumn("recency_score", 1 - F.percent_rank().over(w_recency))
        .withColumn("frequency_score", F.percent_rank().over(w_freq))
        .withColumn("monetary_score", F.percent_rank().over(w_monetary))
        .withColumn(
            "business_score",
            F.round(
                F.col("recency_score") * F.lit(0.3)
                + F.col("frequency_score") * F.lit(0.35)
                + F.col("monetary_score") * F.lit(0.35),
                4,
            ),
        )
        .withColumn(
            "segment_label",
            F.when(F.col("business_score") >= 0.75, F.lit("VIP"))
            .when(F.col("business_score") >= 0.55, F.lit("Potential Loyalist"))
            .when(F.col("business_score") >= 0.35, F.lit("Regular"))
            .otherwise(F.lit("At Risk")),
        )
        .orderBy("segment")
    )

    customer_segments = segmented.select(
        "CustomerID", "recency_days", "frequency", "monetary", "segment"
    ).join(
        labeled_summary.select("segment", "segment_label"),
        on="segment",
        how="left",
    )

    customer_segments.write.mode("overwrite").parquet(str(out_dir / "customer_segments"))
    labeled_summary.write.mode("overwrite").parquet(str(out_dir / "segment_summary"))

    print("[PROCESSING] RFM result written to data/3_curated/results/rfm")


def run_processing(
    project: str,
    min_support: float,
    min_confidence: float,
    k_clusters: int,
) -> None:
    spark = create_spark()
    try:
        if project in {"mba", "both"}:
            run_market_basket(
                spark,
                min_support=min_support,
                min_confidence=min_confidence,
            )
        if project in {"rfm", "both"}:
            run_rfm_segmentation(spark, k_clusters=k_clusters)
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Processing step with PySpark DataFrame")
    parser.add_argument("--project", choices=["mba", "rfm", "both"], default="both")
    parser.add_argument("--min-support", type=float, default=0.005)
    parser.add_argument("--min-confidence", type=float, default=0.2)
    parser.add_argument("--k-clusters", type=int, default=4)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_processing(
        project=args.project,
        min_support=args.min_support,
        min_confidence=args.min_confidence,
        k_clusters=args.k_clusters,
    )


if __name__ == "__main__":
    main()
