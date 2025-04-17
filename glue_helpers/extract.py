from awsglue.context import GlueContext
from pyspark.sql import SparkSession, DataFrame
import boto3
from botocore.exceptions import ClientError, BotoCoreError


def _get_glue_partition_predicate(database: str, table_name: str, logger) -> str:
    """Builds a push_down_predicate for the latest Glue Catalog partition."""
    try:
        glue = boto3.client("glue")

        logger.info(f"Fetching partitions for {database}.{table_name}")
        partitions_response = glue.get_partitions(
            DatabaseName=database,
            TableName=table_name,
            MaxResults=1
        )

        if not partitions_response["Partitions"]:
            logger.warning(f"No partitions found for {database}.{table_name}")
            raise ValueError("No partitions available")

        partition_values = partitions_response["Partitions"][0]["Values"]

        table_info = glue.get_table(DatabaseName=database, Name=table_name)
        partition_keys = [key["Name"] for key in table_info["Table"]["PartitionKeys"]]

        if len(partition_keys) != len(partition_values):
            raise ValueError("Mismatch between partition keys and values")

        predicate = " AND ".join(
            f"{key}='{value}'" for key, value in zip(partition_keys, partition_values)
        )
        logger.info(f"Using push_down_predicate: {predicate}")
        return predicate

    except (ClientError, BotoCoreError) as e:
        logger.error(f"AWS Glue client error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while fetching partitions: {e}")
        raise


def _get_iceberg_partition_filter(spark: SparkSession, full_table_name: str, logger) -> str:
    """Builds a WHERE clause for the latest Iceberg partition."""
    try:
        logger.info(f"Fetching Iceberg partitions for {full_table_name}")
        partitions_df = spark.sql(f"SHOW PARTITIONS {full_table_name}")

        if partitions_df.count() == 0:
            raise ValueError("No partitions found in Iceberg table")

        latest_partition = (
            partitions_df.orderBy("partition", ascending=False).first()["partition"]
        )

        logger.info(f"Latest Iceberg partition string: {latest_partition}")
        parts = latest_partition.split("/")
        filter_expr = " AND ".join([
            f"{kv.split('=')[0]} = '{kv.split('=')[1]}'"
            for kv in parts if '=' in kv
        ])
        logger.info(f"Applying Iceberg WHERE filter: {filter_expr}")
        return filter_expr

    except Exception as e:
        logger.error(f"Failed to resolve latest Iceberg partition: {e}")
        raise


def from_catalog(
    glue_context: GlueContext,
    database: str,
    table_name: str,
    logger,
    transformation_ctx: str = None,
    additional_options: dict = None,
    latest_partition: bool = False,
    format: str = "glue_catalog"
) -> DataFrame:
    """
    Extracts a Spark DataFrame from either Glue Catalog or Iceberg table.

    Args:
        glue_context (GlueContext): Glue context with Spark session.
        database (str): Database name.
        table_name (str): Table name.
        logger: Glue logger instance.
        transformation_ctx (str): Optional transformation context.
        additional_options (dict): Extra options (e.g. predicate pushdown).
        latest_partition (bool): Whether to load only the latest partition.
        format (str): Either 'glue_catalog' or 'iceberg'.

    Returns:
        DataFrame: Spark DataFrame loaded from source.
    """
    options = additional_options.copy() if additional_options else {}
    spark: SparkSession = glue_context.spark_session
    full_table_name = f"{database}.{table_name}"

    try:
        if format == "iceberg":
            logger.info(f"Reading Iceberg table: {full_table_name}")
            if latest_partition:
                where_clause = _get_iceberg_partition_filter(spark, full_table_name, logger)
                return spark.sql(f"SELECT * FROM {full_table_name} WHERE {where_clause}")
            return spark.read.table(full_table_name)

        elif format == "glue_catalog":
            logger.info(f"Reading Glue Catalog table: {full_table_name}")
            if latest_partition:
                predicate = _get_glue_partition_predicate(database, table_name, logger)
                options["push_down_predicate"] = predicate

            dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
                database=database,
                table_name=table_name,
                transformation_ctx=transformation_ctx or f"{database}_{table_name}",
                additional_options=options
            )
            return dynamic_frame.toDF()

        else:
            raise ValueError(f"Unsupported format: {format}")

    except Exception as e:
        logger.error(f"Failed to extract table {full_table_name}: {e}")
        raise
