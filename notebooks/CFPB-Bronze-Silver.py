# Databricks notebook source
CATALOG = "workspace"   # change if yours differs
path = f"/Volumes/workspace/cfpb_bronze/raw_files/filtered_complaints.csv"

raw = (spark.read
       .option("header", True)
       .option("multiLine", True)     # narratives contain line breaks
       .option("quote", '"')
       .option("escape", '"')         # narratives contain quotes
       .csv(path))

raw.printSchema()
display(raw.limit(5))

# COMMAND ----------

import re
from pyspark.sql import functions as F

def to_snake(name: str) -> str:
    return re.sub(r"[^0-9a-zA-Z]+", "_", name.strip().lower()).strip("_")

bronze_df = raw.toDF(*[to_snake(c) for c in raw.columns])   # rename only, keep all values as strings

bronze_df = (bronze_df
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path")))

(bronze_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.cfpb_bronze.complaints_raw"))

# COMMAND ----------

display(bronze_df)

# COMMAND ----------

spark.sql(f"COMMENT ON TABLE {CATALOG}.cfpb_bronze.complaints_raw IS 'Raw CFPB consumer complaints. All strings. Source: CFPB Consumer Complaint Database CSV export.'")

print("rows:", spark.table(f"{CATALOG}.cfpb_bronze.complaints_raw").count())
display(spark.sql(f"""
  SELECT count(*) AS total_rows,
         count(DISTINCT complaint_id) AS distinct_ids,
         sum(CASE WHEN complaint_id IS NULL THEN 1 ELSE 0 END) AS null_ids,
         min(date_received) AS min_date,
         max(date_received) AS max_date
  FROM {CATALOG}.cfpb_bronze.complaints_raw"""))
display(spark.sql(f"DESCRIBE DETAIL {CATALOG}.cfpb_bronze.complaints_raw"))

# COMMAND ----------

bronze = spark.table(f"{CATALOG}.cfpb_bronze.complaints_raw")
cols = set(bronze.columns)

def col_or_null(name, alias=None):
    """Return the column if it exists, otherwise a NULL string column."""
    c = F.col(name) if name in cols else F.lit(None).cast("string")
    return c.alias(alias or name)

# If your narrative column has a different name, add it to this list
narrative_candidates = [n for n in ["consumer_complaint_narrative", "complaint_what_happened"] if n in cols]
narrative_col = F.col(narrative_candidates[0]) if narrative_candidates else F.lit(None).cast("string")

typed = bronze.select(
    F.trim(F.col("complaint_id")).alias("complaint_id"),
    F.expr("try_cast(date_received as date)").alias("date_received"),
    F.expr("try_cast(date_sent_to_company as date)").alias("date_sent_to_company"),
    F.trim(F.col("product")).alias("product"),
    F.trim(col_or_null("sub_product")).alias("sub_product"),
    F.trim(F.col("issue")).alias("issue"),
    F.trim(col_or_null("sub_issue")).alias("sub_issue"),
    narrative_col.alias("narrative"),
    F.trim(col_or_null("company_public_response")).alias("company_public_response"),
    F.trim(F.col("company")).alias("company"),
    F.upper(F.trim(F.col("state"))).alias("state"),
    F.trim(col_or_null("zip_code")).alias("zip_code"),
    F.trim(col_or_null("tags")).alias("tags"),
    F.trim(col_or_null("consumer_consent_provided")).alias("consumer_consent_provided"),
    F.trim(col_or_null("submitted_via")).alias("submitted_via"),
    F.trim(F.col("company_response_to_consumer")).alias("company_response"),
    (F.trim(F.col("timely_response")) == "Yes").alias("is_timely"),
    F.col("_ingested_at"),
)

# COMMAND ----------

enriched = (typed
    .withColumn("received_month", F.date_trunc("month", "date_received").cast("date"))
    .withColumn("days_to_company_response", F.datediff("date_sent_to_company", "date_received"))
    .withColumn("got_relief", F.col("company_response").isin(
        "Closed with monetary relief", "Closed with non-monetary relief"))
    .withColumn("has_narrative",
        F.col("narrative").isNotNull() & (F.length(F.trim("narrative")) > 0)))

# COMMAND ----------

dedup = enriched.dropDuplicates(["complaint_id"])

# COMMAND ----------

flagged = dedup.withColumn("dq_reason", F.concat_ws("; ",
    F.when(F.col("complaint_id").isNull() | (F.col("complaint_id") == ""), F.lit("missing_complaint_id")),
    F.when(F.col("date_received").isNull(), F.lit("invalid_date_received")),
    F.when(F.col("date_received") > F.current_date(), F.lit("future_date_received")),
    F.when(F.col("date_sent_to_company") < F.col("date_received"), F.lit("sent_before_received")),
    F.when(F.col("company").isNull() | (F.col("company") == ""), F.lit("missing_company")),
    F.when(F.col("state").isNotNull() & (F.length("state") != 2), F.lit("invalid_state")),
))

quarantine = flagged.filter(F.col("dq_reason") != "")
silver = flagged.filter(F.col("dq_reason") == "").drop("dq_reason")

# COMMAND ----------

(silver.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.cfpb_silver.complaints"))

(quarantine.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.cfpb_silver.complaints_quarantine"))

# COMMAND ----------

b = spark.table(f"{CATALOG}.cfpb_bronze.complaints_raw").count()
d = dedup.count()
s = spark.table(f"{CATALOG}.cfpb_silver.complaints").count()
q = spark.table(f"{CATALOG}.cfpb_silver.complaints_quarantine").count()

print(f"bronze rows          : {b}")
print(f"duplicates removed   : {b - d}")
print(f"quarantined          : {q}")
print(f"silver rows          : {s}")
print(f"reconciles           : {b == (b - d) + q + s}")
display(spark.table(f"{CATALOG}.cfpb_silver.complaints_quarantine")
        .groupBy("dq_reason").count())

# COMMAND ----------

spark.sql(f"COMMENT ON TABLE {CATALOG}.cfpb_silver.complaints IS 'Cleaned and typed CFPB complaints, deduplicated on complaint_id; rows failing quality rules are in complaints_quarantine'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.cfpb_silver.complaints.is_timely IS 'True if company responded within the required timeframe'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.cfpb_silver.complaints.got_relief IS 'True if closed with monetary or non-monetary relief'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.cfpb_silver.complaints.days_to_company_response IS 'Days between date received and date sent to company'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.cfpb_silver.complaints.narrative IS 'Consumer-written free text, present only if consent given; treat as sensitive'")

# COMMAND ----------

