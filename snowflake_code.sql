DROP DATABASE IF EXISTS redfin_db;
CREATE DATABASE redfin_db;
CREATE OR REPLACE SCHEMA redfin_schema;

CREATE OR REPLACE TABLE redfin_db.redfin_schema.redfin_table_parquet_tmp (
period_duration INT,
city STRING,
state STRING,
property_type STRING,
median_sale_price FLOAT,
median_ppsf FLOAT,
homes_sold FLOAT,
inventory FLOAT,
months_of_supply FLOAT,
median_dom FLOAT,
sold_above_list FLOAT,
period_end_in_years STRING,
period_end_in_months STRING
);


CREATE OR REPLACE SCHEMA file_format_schema;

CREATE OR REPLACE file format redfin_db.file_format_schema.format_parquet
    type = 'PARQUET';
    

CREATE OR REPLACE SCHEMA external_stage_schema;

CREATE OR REPLACE STORAGE INTEGRATION s3_init
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = ""
    STORAGE_ALLOWED_LOCATIONS = ("s3://redfin-project-om");

DESC INTEGRATION s3_init;


CREATE OR REPLACE STAGE redfin_db.external_stage_schema.redfin_ext_stage_parquet
    url="s3://redfin-project-om/transformed/redfin_data.parquet"
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = redfin_db.file_format_schema.format_parquet;

list @redfin_db.external_stage_schema.redfin_ext_stage_parquet;

SELECT * FROM @redfin_db.external_stage_schema.redfin_ext_stage_parquet 
(FILE_FORMAT=> 'redfin_db.file_format_schema.format_parquet',pattern => '.*.parquet' )
LIMIT 300;

SELECT 
$1:"period_duration":: INT,
$1:"city":: VARCHAR,
$1:"state":: VARCHAR,
$1:"property_type":: VARCHAR,
$1:"median_sale_price":: FLOAT,
$1:"median_ppsf":: FLOAT,
$1:"homes_sold":: FLOAT,
$1:"inventory":: FLOAT,
$1:"months_of_supply":: FLOAT,
$1:"median_dom":: FLOAT,
$1:"sold_above_list":: FLOAT,
$1:"period_end_yr":: VARCHAR,
$1:"period_end_month":: VARCHAR
FROM @redfin_db.external_stage_schema.redfin_ext_stage_parquet 
(FILE_FORMAT=> 'redfin_db.file_format_schema.format_parquet',pattern => '.*.parquet' )
LIMIT 100;


CREATE OR REPLACE SCHEMA redfin_db.snowpipe_schema;

CREATE OR REPLACE PIPE redfin_db.snowpipe_schema.redfin_parquet_snowpipe
auto_ingest = TRUE
AS 
COPY INTO redfin_db.redfin_schema.redfin_table_parquet_tmp FROM (
    SELECT 
$1:"period_duration":: INT,
$1:"city":: VARCHAR,
$1:"state":: VARCHAR,
$1:"property_type":: VARCHAR,
$1:"median_sale_price":: FLOAT,
$1:"median_ppsf":: FLOAT,
$1:"homes_sold":: FLOAT,
$1:"inventory":: FLOAT,
$1:"months_of_supply":: FLOAT,
$1:"median_dom":: FLOAT,
$1:"sold_above_list":: FLOAT,
$1:"period_end_yr":: VARCHAR,
$1:"period_end_month":: VARCHAR
FROM @redfin_db.external_stage_schema.redfin_ext_stage_parquet
)
PATTERN = '.*.parquet'
FILE_FORMAT = redfin_db.file_format_schema.format_parquet;

SELECT * FROM redfin_db.redfin_schema.redfin_table_parquet_tmp LIMIT 100;
TRUNCATE TABLE redfin_db.redfin_schema.redfin_table_parquet_tmp;

SELECT COUNT(*) FROM redfin_db.redfin_schema.redfin_table_parquet_tmp;

DESC PIPE redfin_db.snowpipe_schema.redfin_parquet_snowpipe;