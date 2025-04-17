from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import DataFrame, SparkSession


def _log_df_info(df: DataFrame, logger, label: str):
    """Logs row count and schema."""
    try:
        row_count = df.count()
        logger.info(f"{label}: Writing {row_count} rows")
        logger.info(f"{label}: Schema: {df.printSchema()}")
    except Exception as e:
        logger.warning(f"{label}: Unable to log row count or schema: {e}")


def to_glue_table(
    glue_context: GlueContext,
    df: DataFrame,
    database: str,
    table_name: str,
    logger,
    mode: str = "append",
    partition_keys: list = None,
    enable_dynamic_partition_overwrite: bool = False
) -> None:
    """
    Saves a Spark DataFrame to an existing Glue Catalog table using DynamicFrame.

    Args:
        glue_context (GlueContext): GlueContext object.
        df (DataFrame): Data to write.
        database (str): Target Glue database.
        table_name (str): Target Glue table.
        logger: Glue logger.
        mode (str): Write mode: 'append' or 'overwrite'.
        partition_keys (list): Partition keys (must match table definition).
        enable_dynamic_partition_overwrite (bool): Uses 'partitionOverwriteMode = dynamic' if True.
    """
    label = f"GlueCatalog::{database}.{table_name}"
    try:
        _log_df_info(df, logger, label)

        dynamic_frame = DynamicFrame.fromDF(df, glue_context, label)

        options = {}
        if partition_keys:
            options["partitionKeys"] = partition_keys

        if enable_dynamic_partition_overwrite:
            options["enableUpdateCatalog"] = True
            options["partitionOverwriteMode"] = "dynamic"

        logger.info(f"{label}: Writing in mode='{mode}' with options: {options}")

        glue_context.write_dynamic_frame.from_catalog(
            frame=dynamic_frame,
            database=database,
            table_name=table_name,
            transformation_ctx=f"{database}_{table_name}_write",
            additional_options=options,
            mode=mode
        )

        logger.info(f"{label}: ✅ Successfully written to Glue table.")

    except Exception as e:
        logger.error(f"{label}: ❌ Failed to write to Glue table: {e}")
        raise


def to_iceberg_table(
    df: DataFrame,
    full_table_name: str,
    logger,
    mode: str = "append",
    partition_cols: list = None
) -> None:
    """
    Writes a Spark DataFrame to an Apache Iceberg table.

    Args:
        spark (SparkSession): Spark session from GlueContext.
        df (DataFrame): DataFrame to write.
        full_table_name (str): Table in format 'db.table'.
        logger: Glue logger.
        mode (str): Write mode: 'append', 'overwrite', etc.
        partition_cols (list): Used only for table creation (optional).
    """
    label = f"Iceberg::{full_table_name}"
    try:
        _log_df_info(df, logger, label)

        writer = df.write.format("iceberg").mode(mode)

        if mode == "overwrite" and partition_cols:
            logger.info(f"{label}: Overwriting with partitions: {partition_cols}")
            writer = writer.partitionBy(*partition_cols)

        logger.info(f"{label}: Writing data with mode={mode}")
        writer.save(full_table_name)
        logger.info(f"{label}: ✅ Successfully written to Iceberg table.")

    except Exception as e:
        logger.error(f"{label}: ❌ Failed to write to Iceberg table: {e}")
        raise
