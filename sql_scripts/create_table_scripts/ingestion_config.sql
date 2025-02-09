/* ------------------------------------------------------------------------ */
/* -----------------------     ingestion_config     ----------------------- */
/* ------------------------------------------------------------------------ */

DROP TABLE IF EXISTS `dw-metadata-utilities.metadata_utilities.ingestion_config`;
CREATE TABLE IF NOT EXISTS `dw-metadata-utilities.metadata_utilities.ingestion_config` (
ingestion_config_id INT64 PRIMARY KEY NOT ENFORCED NOT NULL OPTIONS(description="Auto-incrementing Primary Key"),
ingestion_type STRING NOT NULL,
source_schema_table_name STRING NOT NULL, 
primary_key_column STRING, 
incremental_date_column STRING,
load_type STRING NOT NULL,  
extra_parameters STRING, 
partitioned_by STRING,
api_source_name STRING,    
source_system STRING NOT NULL,
connection_id INT64 NOT NULL OPTIONS(description="Key from ingestion_connection_info table."), 
connection_name STRING NOT NULL,
project_id STRING NOT NULL, 
dataset STRING NOT NULL, 
table_name STRING NOT NULL,
file_format STRING, 
header STRING,
delimiter STRING,
quote_characters STRING,
escape_characters STRING,
accepted_encoding STRING,
is_parquet  BOOL,
to_parquet BOOL NOT NULL,
bucket STRING NOT NULL,
bucket_destination STRING NOT NULL,
archive_destination STRING NOT NULL,
created_by STRING NOT NULL, 
created_date DATETIME NOT NULL,
modified_by STRING NOT NULL, 
modified_date DATETIME NOT NULL,
FOREIGN KEY (connection_id) REFERENCES `dw-metadata-utilities.metadata_utilities.ingestion_connection_info`(connection_id) NOT ENFORCED
);

INSERT INTO `dw-metadata-utilities.metadata_utilities.ingestion_config`
(
ingestion_config_id, --50000 
ingestion_type, --REQUEST
source_schema_table_name, --https://api.coincap.io/v2/assets/
primary_key_column, --ID
incremental_date_column, --NULL
load_type,  --INCR
extra_parameters, --NULL
partitioned_by, --NULL
api_source_name, --COINCAP
source_system, --COINCAP
connection_id,
connection_name, --COINCAP_ASSET
project_id, --COINCAP-DATA-HUB
dataset,    --EXTERNAL_COINCAP_DATA
table_name, --EXTERNAL_COINCAP_ASSETS
file_format, --JSON
header, --'infer'
delimiter,  --NULL
quote_characters, --NULL
escape_characters, --NULL
accepted_encoding, --gzip
is_parquet, --FALSE
to_parquet, --TRUE
bucket, --dev-coincap-data
bucket_destination, --input/
archive_destination, --archive/coincap_assets/ 
created_by, --ADMIN
created_date, --CURRENT_DATETIME
modified_by, --ADMIN
modified_date--CURRENT_DATETIME
) VALUES
VALUES (50000, 'TEST','TEST','ID',NULL,'FULL',NULL,NULL,'TEST',0,'TEST','TEST','TEST','TEST','TEST',NULL,'TEST',NULL, NULL, FALSE, TRUE, 'TEST', 'TEST','TEST',NULL,'ADMIN',CURRENT_DATETIME,'ADMIN',CURRENT_DATETIME),
((SELECT IFNULL(MAX(ingestion_config_id), 0) + 1 FROM `dw-metadata-utilities.metadata_utilities.ingestion_config`),'REQUEST_PAYLOAD','https://api.coincap.io/v2/assets/bitcoin/history/','ID','TIME','INCR','h1',NULL,'COINCAP',2,'COINCAP_BITCOIN_HISTORY','COINCAP-DATA-HUB','COINCAP_DATA','COINCAP_BITCOIN_HISTORY','JSON',NULL,'COINCAP API 2.0', NULL, NULL, FALSE, TRUE, 'dev-coincap-data', 'input/coincap_history','archive/coincap_history','gzip','ADMIN',CURRENT_DATETIME,'ADMIN',CURRENT_DATETIME),
((SELECT IFNULL(MAX(ingestion_config_id), 0) + 1 FROM `dw-metadata-utilities.metadata_utilities.ingestion_config`),'REQUEST','https://api.coincap.io/v2/exchanges/','ID',NULL,'FULL',NULL,NULL,'COINCAP',3,'COINCAP_EXCHANGES','COINCAP-DATA-HUB','COINCAP_DATA','COINCAP_EXCHANGES','JSON',NULL,'COINCAP API 2.0', NULL, NULL, FALSE, TRUE, 'dev-coincap-data', 'input/coincap_exchanges/','archive/coincap_exchanges','gzip','ADMIN',CURRENT_DATETIME,'ADMIN',CURRENT_DATETIME),
((SELECT IFNULL(MAX(ingestion_config_id), 0) + 1 FROM `dw-metadata-utilities.metadata_utilities.ingestion_config`),'REQUEST','https://api.coincap.io/v2/rates/','ID',NULL,'FULL',NULL,NULL,'COINCAP',5,'COINCAP_RATES','COINCAP-DATA-HUB','COINCAP_DATA','COINCAP_RATES','JSON',NULL,'COINCAP API 2.0', NULL, NULL, FALSE, TRUE, 'dev-coincap-data', 'input/coincap_rates/','archive/coincap_rates','gzip','ADMIN',CURRENT_DATETIME,'ADMIN',CURRENT_DATETIME),
((SELECT IFNULL(MAX(ingestion_config_id), 0) + 1 FROM `dw-metadata-utilities.metadata_utilities.ingestion_config`),'REQUEST','https://api.coincap.io/v2/assets/','ID',NULL,'FULL',NULL,NULL,'COINCAP',1,'COINCAP_ASSET','COINCAP-DATA-HUB','COINCAP_DATA','COINCAP_ASSETS','JSON',NULL,'COINCAP API 2.0', NULL, NULL, FALSE, TRUE, 'dev-coincap-data', 'input/coincap_assets/','archive/coincap_assets','gzip','ADMIN',CURRENT_DATETIME,'ADMIN',CURRENT_DATETIME),



/* AWS S3 Example */
INSERT INTO `dw-metadata-utilities.metadata_utilities.ingestion_config`
(
ingestion_config_id, --50000 
ingestion_type, --S3
source_schema_table_name, --PREFIX_PATH_FILE_NAME
primary_key_column, --ID
incremental_date_column, --NULL
load_type,  --FULL
extra_parameters, --NULL
delimiter,  --','
source_system, --S3
connection_id,
connection_name, --COINCAP_ASSET
project_id, --COINCAP-DATA-HUB
dataset,    --COINCAP_DATA
table_name, --MOVIE_REVIEW
file_format, --'csv'
quote_characters, --'"'
api_source_name, --'boto3'
partitioned_by, --NULL
escape_characters, --NULL
is_parquet, --FALSE
to_parquet, --FALSE
bucket, --dev-coincap-data
bucket_destination, --input/
archive_destination, --archive/coincap_assets/ 
accepted_encoding, --gzip
created_by, --ADMIN
created_date, --CURRENT_DATETIME
modified_by, --ADMIN
modified_date--CURRENT_DATETIME
) VALUES
((SELECT IFNULL(MAX(ingestion_config_id), 0) + 1 FROM `dw-metadata-utilities.metadata_utilities.ingestion_config`),
'S3',
'data/movie_review.csv',
'CID',
NULL,
'FULL',
NULL,
',',
'S3',
6,
'S3_COINCAP',
'COINCAP-DATA-HUB',
'COINCAP_DATA',
'MOVIE_REVIEW',
'csv',
'"',
'boto3', 
NULL, 
NULL, 
FALSE, 
FALSE, 
'dev-coincap-data', 
'input/moview_review/',
'archive/movie_review',
NULL,
'ADMIN',
CURRENT_DATETIME,
'ADMIN',
CURRENT_DATETIME);


/* GCS Example */
INSERT INTO `dw-metadata-utilities.metadata_utilities.ingestion_config`
(
ingestion_config_id, --50000 
ingestion_type, --GCS
source_schema_table_name, --PREFIX_PATH_FILE_NAME
primary_key_column, --NULL
incremental_date_column, --NULL
load_type,  --FULL
extra_parameters, --NULL
partitioned_by, --NULL
api_source_name, --'Google API
source_system, --GCS
connection_id,
connection_name, --GCS_COINCAP
project_id, --COINCAP-DATA-HUB
dataset,    --COINCAP_DATA
table_name, --MOVIE_REVIEW
file_format, --'csv'
header, --''
delimiter,  --','
quote_characters, --'"'
escape_characters, --NULL
accepted_encoding, --gzip
is_parquet, --FALSE
to_parquet, --FALSE
bucket, --dev-coincap-data
bucket_destination, --input/
archive_destination, --archive/coincap_assets/ 
created_by, --ADMIN
created_date, --CURRENT_DATETIME
modified_by, --ADMIN
modified_date--CURRENT_DATETIME
) VALUES
((SELECT IFNULL(MAX(ingestion_config_id), 0) + 1 FROM `dw-metadata-utilities.metadata_utilities.ingestion_config`),
'GCS',
'input/adhoc/Nigerian_Road_Traffic_Crashes_2020_2024.csv',
NULL,
NULL,
'FULL',
NULL,
NULL,
'Google API',
'GCS',
7,
'GCS_COINCAP',
'COINCAP-DATA-HUB',
'COINCAP_DATA',
'NIGERIAN_ROAD_TRAFFIC',
'CSV',
'',
',',
'"',
NULL,
'gzip',
FALSE, 
TRUE, 
'dev-coincap-data', 
'input/nigerian_road_traffic/',
'archive/nigerian_road_traffic/',
'ADMIN',
CURRENT_DATETIME,
'ADMIN',
CURRENT_DATETIME);