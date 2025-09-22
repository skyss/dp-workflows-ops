from loguru import logger
from delta.tables import DeltaTable

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession


def replace_table(table_name: str, df: DataFrame) -> None:
    """Replace or create the table including the schema with the given DataFrame.

    This will automatically follow the naming conventions for the Data Mesh project.
    """
    logger.info(
        "Overwriting {table_name}.",
        table_name=table_name,
    )
    return (
        df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


def _columns_differ(df_new: DataFrame, df_old: DataFrame) -> bool:
    missing_in_new = set(df_old.schema.names) - set(df_new.schema.names)
    if missing_in_new:
        logger.warning(
            "The dataframe is missing columns that exist in the table: {missing_in_new}",
            missing_in_new=list(missing_in_new),
        )
    missing_in_old = set(df_new.schema.names) - set(df_old.schema.names)
    if missing_in_old:
        logger.warning(
            "The dataframe has more columns than exist in the table. These will NOT be included: {missing_in_old}",
            missing_in_old=list(missing_in_old),
        )

    return bool(missing_in_new | missing_in_old)


def merge_into_table(
    spark: SparkSession,
    table_name: str,
    df: DataFrame,
    id_col: str = "id",
    allow_mismatch: bool = False,
) -> DataFrame | None:
    """Create og merge into the table with the given DataFrame."""
    if not spark.catalog.tableExists(table_name):
        if logger:
            logger.info(
                "Table {table_name} does not exist. Saving as table.",
                table_name=table_name,
            )

        df.write.saveAsTable(table_name)
        return None

    if logger:
        logger.info("Merging data into {table_name}", table_name=table_name)

    existing_df = spark.read.table(table_name).limit(0)
    if _columns_differ(df, existing_df) and not allow_mismatch:
        raise RuntimeError("Schema of tables to not match. Cannot upsert!")

    return (
        DeltaTable.forName(spark, table_name)
        .alias("target")
        .merge(
            source=df.alias("updates"),
            condition=f"""
                target.{id_col} = updates.{id_col}
                """,
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
