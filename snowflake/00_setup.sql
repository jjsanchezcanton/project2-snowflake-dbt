-- =====================================================================
-- Project 2 - Snowflake + dbt : 00_setup.sql
-- Run once in a Snowsight worksheet as ACCOUNTADMIN.
-- Creates: role, warehouse (XS, cost-aware), database, schemas, stage,
--          file formats, and the RAW landing tables.
-- =====================================================================

-- ---- 1. Role ---------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS PROJECT2_ENGINEER;
GRANT ROLE PROJECT2_ENGINEER TO ROLE SYSADMIN;        -- keep under sysadmin hierarchy

-- grant the role to the current user (so you can USE it)
SET my_user = (SELECT CURRENT_USER());
GRANT ROLE PROJECT2_ENGINEER TO USER IDENTIFIER($my_user);

-- ---- 2. Warehouse (cost-aware) --------------------------------------
CREATE WAREHOUSE IF NOT EXISTS WH_XS
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60            -- seconds idle -> suspend (protects trial credits)
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Project2 XS warehouse, auto-suspend 60s';

GRANT USAGE, OPERATE ON WAREHOUSE WH_XS TO ROLE PROJECT2_ENGINEER;

-- ---- 3. Database & schemas (medallion) ------------------------------
CREATE DATABASE IF NOT EXISTS NYC_TLC;
USE DATABASE NYC_TLC;
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS MARTS;

-- hand ownership of the project objects to the project role
GRANT OWNERSHIP ON DATABASE NYC_TLC      TO ROLE PROJECT2_ENGINEER REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA RAW            TO ROLE PROJECT2_ENGINEER REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA STAGING        TO ROLE PROJECT2_ENGINEER REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA MARTS          TO ROLE PROJECT2_ENGINEER REVOKE CURRENT GRANTS;

-- ---- 4. Switch to the project role for the rest ---------------------
USE ROLE PROJECT2_ENGINEER;
USE WAREHOUSE WH_XS;
USE DATABASE NYC_TLC;
USE SCHEMA RAW;

-- ---- 5. File formats -------------------------------------------------
CREATE FILE FORMAT IF NOT EXISTS RAW.FF_PARQUET TYPE = PARQUET;
CREATE FILE FORMAT IF NOT EXISTS RAW.FF_CSV
  TYPE = CSV
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL');

-- ---- 6. Internal stage ----------------------------------------------
CREATE STAGE IF NOT EXISTS RAW.STG_TLC
  COMMENT = 'Internal stage for NYC TLC raw files';

-- ---- 7. RAW landing tables ------------------------------------------
-- Trips: schema-on-read (VARIANT) -> robust to monthly schema drift.
CREATE TABLE IF NOT EXISTS RAW.YELLOW_TRIPDATA_RAW (
  record       VARIANT,
  _source_file STRING,
  _loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Zone lookup: small, clean, known schema -> typed.
CREATE TABLE IF NOT EXISTS RAW.TAXI_ZONE_LOOKUP (
  locationid   INTEGER,
  borough      STRING,
  zone         STRING,
  service_zone STRING,
  _loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---- 8. Sanity check -------------------------------------------------
SHOW WAREHOUSES LIKE 'WH_XS';
SHOW SCHEMAS IN DATABASE NYC_TLC;
SELECT CURRENT_ROLE() AS role, CURRENT_WAREHOUSE() AS wh, CURRENT_DATABASE() AS db;
