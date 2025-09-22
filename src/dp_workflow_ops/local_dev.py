"""A workaround to make local dev experience easier.

Use this from other files with:

import contextlib

with contextlib.suppress(ImportError):
    from bundles.local_dev import dbutils, spark

This will allow variables like dbutils and spark to be defined and have
intellisense in your IDE when running code locally.
"""

from databricks.sdk import WorkspaceClient
from pyspark.sql.session import SparkSession

spark: SparkSession = None  # type: ignore Just a dummy variable for type hints
dbutils = WorkspaceClient().dbutils
display = IPython.core.display.display_functions.display

__all__ = ["dbutils", "spark"]
