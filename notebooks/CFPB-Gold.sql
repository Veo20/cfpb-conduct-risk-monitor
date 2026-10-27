-- Databricks notebook source
SELECT current_catalog(), current_schema();

SHOW CATALOGS;


CREATE SCHEMA IF NOT EXISTS workspace.setup_test;
DROP SCHEMA workspace.setup_test;

-- COMMAND ----------

USE CATALOG workspace;

CREATE SCHEMA IF NOT EXISTS cfpb_bronze COMMENT 'Raw CFPB complaint data as ingested';
CREATE SCHEMA IF NOT EXISTS cfpb_silver COMMENT 'Cleaned, typed, deduplicated complaints';
CREATE SCHEMA IF NOT EXISTS cfpb_gold   COMMENT 'Business-ready aggregates for apps and BI';

CREATE VOLUME IF NOT EXISTS cfpb_bronze.raw_files COMMENT 'Landing zone for source files';

-- COMMAND ----------

CREATE OR REPLACE TABLE cfpb_gold.gold_company_monthly
COMMENT 'Monthly complaint metrics by company and product'
AS
SELECT
  company,
  product,
  received_month,
  count(*)                                              AS complaints,
  round(avg(CASE WHEN is_timely THEN 1.0 ELSE 0.0 END), 4)  AS timely_rate,
  round(avg(CASE WHEN got_relief THEN 1.0 ELSE 0.0 END), 4) AS relief_rate,
  round(avg(days_to_company_response), 2)               AS avg_days_to_response,
  round(avg(CASE WHEN has_narrative THEN 1.0 ELSE 0.0 END), 4) AS narrative_rate
FROM cfpb_silver.complaints
GROUP BY company, product, received_month;

-- COMMAND ----------

CREATE OR REPLACE TABLE cfpb_gold.gold_issue_summary
COMMENT 'Complaint counts by product, issue, and sub-issue with share within product'
AS
SELECT
  product, issue, sub_issue,
  count(*) AS complaints,
  round(count(*) / sum(count(*)) OVER (PARTITION BY product), 4) AS share_of_product
FROM cfpb_silver.complaints
GROUP BY product, issue, sub_issue;

-- COMMAND ----------

CREATE OR REPLACE TABLE cfpb_gold.gold_state_summary
COMMENT 'Complaint counts by state, product, and month'
AS
SELECT state, product, received_month, count(*) AS complaints
FROM cfpb_silver.complaints
WHERE state IS NOT NULL
GROUP BY state, product, received_month;

-- COMMAND ----------

CREATE OR REPLACE TABLE cfpb_gold.gold_company_risk_score
COMMENT 'Transparent 0-100 conduct risk score per company: 40% volume, 30% untimely rate, 20% growth, 10% relief rate (percentile ranks, latest 3 complete months)'
AS
WITH bounds AS (
  -- last COMPLETE month: the export may end mid-month, so step back one month
  SELECT add_months(date_trunc('month', max(date_received)), -1) AS latest_month
  FROM cfpb_silver.complaints
),
windowed AS (
  SELECT
    c.company,
    CASE WHEN c.received_month > add_months(b.latest_month, -3) THEN 'current'
         ELSE 'previous' END AS win,
    c.is_timely,
    c.got_relief
  FROM cfpb_silver.complaints c
  CROSS JOIN bounds b
  WHERE c.received_month <= b.latest_month
    AND c.received_month >  add_months(b.latest_month, -6)
),
agg AS (
  SELECT
    company,
    count_if(win = 'current')  AS complaints_current,
    count_if(win = 'previous') AS complaints_previous,
    avg(CASE WHEN win = 'current' THEN (CASE WHEN is_timely THEN 0.0 ELSE 1.0 END) END) AS untimely_rate,
    avg(CASE WHEN win = 'current' THEN (CASE WHEN got_relief THEN 1.0 ELSE 0.0 END) END) AS relief_rate
  FROM windowed
  GROUP BY company
),
scored AS (
  SELECT *,
    coalesce((complaints_current - complaints_previous) / nullif(complaints_previous, 0), 0) AS growth_rate
  FROM agg
  WHERE complaints_current >= 50
)
SELECT
  company,
  complaints_current,
  complaints_previous,
  round(growth_rate, 4)   AS growth_rate,
  round(untimely_rate, 4) AS untimely_rate,
  round(relief_rate, 4)   AS relief_rate,
  round(100 * (
      0.4 * percent_rank() OVER (ORDER BY complaints_current)
    + 0.3 * percent_rank() OVER (ORDER BY untimely_rate)
    + 0.2 * percent_rank() OVER (ORDER BY growth_rate)
    + 0.1 * percent_rank() OVER (ORDER BY relief_rate)
  ), 1) AS risk_score
FROM scored;

-- COMMAND ----------

-- totals must match silver
SELECT (SELECT count(*) FROM cfpb_silver.complaints)            AS silver_rows,
       (SELECT sum(complaints) FROM cfpb_gold.gold_company_monthly) AS gold_monthly_total,
       (SELECT sum(complaints) FROM cfpb_gold.gold_state_summary)   AS gold_state_total;

-- sanity-check the score
SELECT * FROM cfpb_gold.gold_company_risk_score ORDER BY risk_score DESC LIMIT 10;

-- COMMAND ----------

SELECT sum(complaints) FROM cfpb_gold.gold_company_monthly

-- COMMAND ----------

SELECT count(*) FROM cfpb_silver.complaints

-- COMMAND ----------

SHOW TABLES IN cfpb_gold

-- COMMAND ----------

