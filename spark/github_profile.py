#!/usr/bin/env python3
"""Standalone Spark job: spark.read.json(), PushEvent filter, rank active projects."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    parser = argparse.ArgumentParser(description="GH Archive Spark profiler")
    parser.add_argument(
        "--input",
        default="data/hdfs/data/github/gh_activity_full.json",
        help="NDJSON path or hdfs:///data/github/gh_activity_full.json",
    )
    parser.add_argument("--output", default="data/results/spark_profile")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("GHArchiveLanguageProfiling")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    df = spark.read.json(args.input)
    df.printSchema()

    pushes = df.filter(F.col("type") == "PushEvent")
    top_projects = (
        pushes.groupBy(F.col("repo.name").alias("repo"))
        .count()
        .orderBy(F.desc("count"))
    )
    top_users = (
        df.groupBy(F.col("actor.login").alias("user"))
        .count()
        .orderBy(F.desc("count"))
    )
    languages = (
        df.select(
            F.coalesce(
                F.col("payload.pull_request.base.repo.language"),
                F.col("payload.forkee.language"),
                F.col("payload.repository.language"),
            ).alias("language")
        )
        .where(F.col("language").isNotNull())
        .groupBy("language")
        .count()
        .orderBy(F.desc("count"))
    )

    top_projects.write.mode("overwrite").json(f"{args.output}/top_projects")
    top_users.write.mode("overwrite").json(f"{args.output}/top_users")
    languages.write.mode("overwrite").json(f"{args.output}/languages")
    spark.stop()


if __name__ == "__main__":
    main()
