"""Production-shaped Spark point-in-time adapter.

The executable reference semantics live in featureforge.dataset. This adapter deliberately
does not contribute to local claims until it is run against a managed catalog and recorded.
"""

from __future__ import annotations

import argparse


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events-uri", required=True)
    parser.add_argument("--labels-uri", required=True)
    parser.add_argument("--output-uri", required=True)
    args = parser.parse_args()
    try:
        from pyspark.sql import SparkSession, Window
        from pyspark.sql import functions as F
    except ImportError as error:
        raise SystemExit("install the spark extra to run this adapter") from error

    spark = SparkSession.builder.appName("featureforge-point-in-time").getOrCreate()
    events = spark.read.parquet(args.events_uri).alias("e")
    labels = spark.read.parquet(args.labels_uri).alias("l")
    eligible = labels.join(
        events,
        (F.col("l.customer_id") == F.col("e.customer_id"))
        & (F.col("e.event_time") <= F.col("l.label_time"))
        & (F.col("e.knowledge_time") <= F.col("l.label_time")),
        "left",
    )
    revisions = Window.partitionBy("l.label_id", "e.event_id").orderBy(
        F.col("e.knowledge_time").desc()
    )
    eligible.withColumn("revision_rank", F.row_number().over(revisions)).where(
        F.col("revision_rank") == 1
    ).write.mode("overwrite").parquet(args.output_uri)
    spark.stop()


if __name__ == "__main__":
    main()
