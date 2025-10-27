"""Functions for setting up env in a common way for Skyss jobs."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk.dbutils import RemoteDbUtils
    from pyspark.sql import SparkSession


def set_catalog_and_schema_for_job(
    spark: SparkSession, dbutils: RemoteDbUtils
) -> tuple[str, str]:
    """Retrives catalog and schema from job parameters and sets catalog and schema.

    If schema does not exist, it is created.

    Returns
    -------
    tuple: tuple[str,str]
        Catalog and schema name

    """
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
    spark.sql(f"create schema if not exists `{catalog}`.`{schema}`")
    spark.sql(f"use catalog `{catalog}`")
    spark.sql(f"use schema `{schema}`")
    return catalog, schema
