from __future__ import annotations
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def set_catalog_and_schema_for_job(spark: SparkSession, dbutils: Any) -> None:
    """Retrives catallog and schema from job parameters and sets catalog and schema.

    If schema does not exist, it is created."""
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
    spark.sql(f"create schema if not exists `{catalog}`.`{schema}`")
    spark.sql(f"use catalog `{catalog}`")
    spark.sql(f"use schema `{schema}`")
