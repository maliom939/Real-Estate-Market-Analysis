DROP DATABASE IF EXISTS redfin_db;
CREATE DATABASE redfin_db;

CREATE OR REPLACE SCHEMA redfin_schema;
TRUNCATE TABLE redfin_db.redfin_schema.redfin_table;
CREATE OR REPLACE TABLE redfin_db.redfin_schema.redfin_table (
period_begin DATE,
period_end DATE,
period_duration INT,
region_type STRING,
region_type_id INT,
table_id INT,
is_seasonally_adjusted STRING,
city STRING,
state STRING,
state_code STRING,
property_type STRING,
property_type_id INT,
median_sale_price FLOAT,
median_list_price FLOAT,
median_ppsf FLOAT,
median_list_ppsf FLOAT,
homes_sold FLOAT,
inventory FLOAT,
months_of_supply FLOAT,
median_dom FLOAT,
avg_sale_to_list FLOAT,
sold_above_list FLOAT,
parent_metro_region_metro_code STRING,
last_updated DATETIME,
period_begin_in_years STRING,
period_end_in_years STRING,
period_begin_in_months STRING,
period_end_in_months STRING
);

SELECT *
FROM redfin_db.redfin_schema.redfin_table LIMIT 10;

SELECT COUNT(*) FROM redfin_db.redfin_schema.redfin_table;

CREATE OR REPLACE SCHEMA file_format_schema;
CREATE OR REPLACE file format redfin_db.file_format_schema.format_csv
    type = 'CSV'
    field_delimiter = ','
    RECORD_DELIMITER = '\n'
    skip_header = 1;
    -- error_on_column_count_mismatch = FALSE;
    

CREATE OR REPLACE SCHEMA external_stage_schema;

CREATE OR REPLACE STORAGE INTEGRATION s3_init
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = "arn:aws:iam::748317758880:role/redfin-s3-connection"
    STORAGE_ALLOWED_LOCATIONS = ("s3://redfin-project-om");

DESC INTEGRATION s3_init;


CREATE OR REPLACE STAGE redfin_db.external_stage_schema.redfin_ext_stage_yml 
    url="s3://redfin-project-om/transformed"
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = redfin_db.file_format_schema.format_csv;



list @redfin_db.external_stage_schema.redfin_ext_stage_yml;


CREATE OR REPLACE SCHEMA redfin_db.snowpipe_schema;

CREATE OR REPLACE PIPE redfin_db.snowpipe_schema.redfin_snowpipe
auto_ingest = TRUE
AS 
COPY INTO redfin_db.redfin_schema.redfin_table
FROM @redfin_db.external_stage_schema.redfin_ext_stage_yml;

SELECT COUNT(*) FROM redfin_db.redfin_schema.redfin_table;

DESC PIPE redfin_db.snowpipe_schema.redfin_snowpipe;