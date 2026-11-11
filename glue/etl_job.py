"""
Serverless Data Pipeline - ETL Job
Transforms CSV to Parquet with data quality checks and partitioning.

Author: @ingmarma | MarmaTech
"""

import sys
import logging
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, IntegerType
)

# ============================================================
# CONFIGURATION
# ============================================================

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'RAW_BUCKET',
    'PROCESSED_BUCKET',
    'DATABASE_NAME',
    'TABLE_NAME',
    'PARTITION_KEYS',
])

RAW_BUCKET = args['RAW_BUCKET']
PROCESSED_BUCKET = args['PROCESSED_BUCKET']
DATABASE_NAME = args['DATABASE_NAME']
TABLE_NAME = args['TABLE_NAME']
PARTITION_KEYS = args['PARTITION_KEYS'].split(',')

INPUT_PATH = f"s3://{RAW_BUCKET}/input/"
OUTPUT_PATH = f"s3://{PROCESSED_BUCKET}/output/{TABLE_NAME}/"
REJECTED_PATH = f"s3://{PROCESSED_BUCKET}/rejected/{TABLE_NAME}/"

# ============================================================
# LOGGING
# ============================================================

logger = logging.getLogger('etl_job')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)s] %(message)s'
))
logger.addHandler(handler)

# ============================================================
# SPARK + GLUE INIT
# ============================================================

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

# ============================================================
# SCHEMA DEFINITION
# ============================================================

EXPECTED_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("transaction_date", TimestampType(), False),
    StructField("customer_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), True),
    StructField("category", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("status", StringType(), False),
    StructField("country", StringType(), True),
    StructField("payment_method", StringType(), True),
])


def read_raw_data(path: str):
    """Read CSV files with schema inference and header detection."""
    logger.info(f"Reading raw data from: {path}")

    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .option("mode", "PERMISSIVE")
          .option("columnNameOfCorruptRecord", "_corrupt_record")
          .csv(path))

    record_count = df.count()
    logger.info(f"Raw records read: {record_count:,}")
    return df


def validate_data(df):
    """
    Apply data quality checks. Returns (valid_df, rejected_df).

    Checks:
    1. transaction_id is not null and not empty
    2. amount is positive
    3. status is in allowed values
    4. transaction_date is not in the future
    """
    logger.info("Running data quality checks...")

    allowed_statuses = ['completed', 'pending', 'failed', 'refunded']

    valid = df.filter(
        (F.col("transaction_id").isNotNull()) &
        (F.trim(F.col("transaction_id")) != "") &
        (F.col("amount") > 0) &
        (F.col("status").isin(allowed_statuses)) &
        (F.col("transaction_date") <= F.current_timestamp())
    )

    rejected = df.subtract(valid)

    valid_count = valid.count()
    rejected_count = rejected.count()
    total = valid_count + rejected_count

    logger.info(f"Validation results:")
    logger.info(f"  Total:    {total:,}")
    logger.info(f"  Valid:    {valid_count:,} ({valid_count/total*100:.1f}%)")
    logger.info(f"  Rejected: {rejected_count:,} ({rejected_count/total*100:.1f}%)")

    return valid, rejected


def enrich_data(df):
    """Add computed columns for analytics."""
    logger.info("Enriching data with computed columns...")

    enriched = (df
        .withColumn("year", F.year("transaction_date"))
        .withColumn("month", F.month("transaction_date"))
        .withColumn("day_of_week", F.dayofweek("transaction_date"))
        .withColumn("hour", F.hour("transaction_date"))
        .withColumn("amount_bucket", F.when(F.col("amount") < 10, "micro")
                     .when(F.col("amount") < 100, "small")
                     .when(F.col("amount") < 1000, "medium")
                     .when(F.col("amount") < 10000, "large")
                     .otherwise("enterprise"))
        .withColumn("is_weekend", F.when(
            F.dayofweek("transaction_date").isin([1, 7]), True
        ).otherwise(False))
        .withColumn("processing_timestamp", F.current_timestamp())
    )

    return enriched


def write_parquet(df, path: str, partition_keys: list):
    """Write DataFrame as partitioned Parquet with Snappy compression."""
    logger.info(f"Writing Parquet to: {path}")
    logger.info(f"Partition keys: {partition_keys}")

    (df.repartition(*[F.col(k) for k in partition_keys])
       .write
       .mode("overwrite")
       .partitionBy(*partition_keys)
       .option("compression", "snappy")
       .parquet(path))

    logger.info("Parquet write complete")


def write_rejected(df, path: str):
    """Write rejected records for review."""
    if df.count() > 0:
        logger.info(f"Writing {df.count():,} rejected records to: {path}")
        (df.withColumn("rejection_timestamp", F.current_timestamp())
           .write
           .mode("append")
           .json(path))


def update_catalog(database: str, table: str, path: str, partition_keys: list):
    """Update Glue Data Catalog with new table/partitions."""
    logger.info(f"Updating catalog: {database}.{table}")

    # Read back as DynamicFrame for catalog integration
    df = spark.read.parquet(path)
    dynamic_frame = DynamicFrame.fromDF(df, glue_context, "output")

    sink = glue_context.getSink(
        connection_type="s3",
        path=path,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=partition_keys,
    )
    sink.setCatalogInfo(
        catalogDatabase=database,
        catalogTableName=table,
    )
    sink.setFormat("glueparquet", compression="snappy")
    sink.writeFrame(dynamic_frame)

    logger.info("Catalog updated successfully")


def log_metrics(df):
    """Log key metrics for CloudWatch."""
    stats = df.agg(
        F.count("*").alias("total_records"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.countDistinct("category").alias("unique_categories"),
    ).collect()[0]

    logger.info("=" * 60)
    logger.info("PIPELINE METRICS")
    logger.info("=" * 60)
    logger.info(f"  Total records:      {stats['total_records']:,}")
    logger.info(f"  Total amount:       ${stats['total_amount']:,.2f}")
    logger.info(f"  Avg amount:         ${stats['avg_amount']:,.2f}")
    logger.info(f"  Unique customers:   {stats['unique_customers']:,}")
    logger.info(f"  Unique categories:  {stats['unique_categories']:,}")
    logger.info("=" * 60)


# ============================================================
# MAIN PIPELINE
# ============================================================

def main():
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info(f"ETL Job started at {start_time.isoformat()}")
    logger.info(f"Input:  {INPUT_PATH}")
    logger.info(f"Output: {OUTPUT_PATH}")
    logger.info("=" * 60)

    # Step 1: Read
    raw_df = read_raw_data(INPUT_PATH)

    # Step 2: Validate
    valid_df, rejected_df = validate_data(raw_df)

    # Step 3: Enrich
    enriched_df = enrich_data(valid_df)

    # Step 4: Write Parquet
    write_parquet(enriched_df, OUTPUT_PATH, PARTITION_KEYS)

    # Step 5: Write rejected
    write_rejected(rejected_df, REJECTED_PATH)

    # Step 6: Update catalog
    update_catalog(DATABASE_NAME, TABLE_NAME, OUTPUT_PATH, PARTITION_KEYS)

    # Step 7: Metrics
    log_metrics(enriched_df)

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"ETL Job completed in {elapsed:.1f} seconds")


try:
    main()
except Exception as e:
    logger.error(f"ETL Job failed: {str(e)}", exc_info=True)
    raise
finally:
    job.commit()
