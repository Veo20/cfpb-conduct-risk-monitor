-- Databricks notebook source
USE CATALOG workspace;

-- A reusable masking function
CREATE OR REPLACE FUNCTION cfpb_gold.mask_pii(txt STRING)
RETURNS STRING
COMMENT 'Masks email addresses and phone-like numbers in free text'
RETURN regexp_replace(
         regexp_replace(txt, '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}', '[EMAIL]'),
         '\\+?\\d[\\d\\s().-]{8,}\\d', '[PHONE]');

-- The only narrative surface the app will see
CREATE OR REPLACE VIEW cfpb_gold.v_complaint_narratives
COMMENT 'Masked, truncated complaint narratives for the app; no direct access to silver needed'
AS
SELECT complaint_id,
       company,
       product,
       issue,
       date_received,
       cfpb_gold.mask_pii(left(narrative, 1500)) AS narrative_masked
FROM cfpb_silver.complaints
WHERE has_narrative;

-- COMMAND ----------

SELECT cfpb_gold.mask_pii('Call me on +1 555 123 4567 or mail jo@example.com') AS masked;

-- COMMAND ----------

SELECT * FROM cfpb_gold.v_complaint_narratives LIMIT 5;

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG workspace TO `<SP-ID>`;
GRANT USE SCHEMA  ON SCHEMA  workspace.cfpb_gold TO `<SP-ID>`;
GRANT SELECT      ON SCHEMA  workspace.cfpb_gold TO `<SP-ID>`;
GRANT EXECUTE     ON FUNCTION workspace.cfpb_gold.mask_pii TO `<SP-ID>`;

-- COMMAND ----------

SHOW GRANTS ON SCHEMA workspace.cfpb_gold;
SHOW GRANTS `<SP-ID>` ON SCHEMA workspace.cfpb_gold;

-- COMMAND ----------

