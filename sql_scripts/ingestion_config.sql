/* ------------------------------------------------------------------------ */
/* -----------------------     ingestion_config     ----------------------- */
/* ------------------------------------------------------------------------ */

DROP TABLE IF EXISTS `dw-metadata-utilities.metadata_utilities.ingestion_config`;
CREATE TABLE IF NOT EXISTS `dw-metadata-utilities.metadata_utilities.ingestion_config` (
ingestion_type STRING NOT NULL,
source_schema_table_name STRING NOT NULL, 
primary_key_column STRING, 
incremental_date_column STRING,
load_type STRING NOT NULL,  
extra_parameters STRING, 
delimiter STRING,    
source_system STRING NOT NULL, 
connection_name STRING NOT NULL,
project_id STRING NOT NULL, 
dataset STRING NOT NULL, 
table_name STRING NOT NULL, 
file_format STRING, 
quote_characters STRING,
api_source_name STRING, 
modified_by STRING NOT NULL, 
modified_date DATETIME NOT NULL,
created_by STRING NOT NULL, 
created_date DATETIME NOT NULL, 
partitioned_by STRING,
escape_characters STRING,
is_parquet  BOOL,
is_external BOOL NOT NULL,
bucket STRING NOT NULL,
bucket_destination STRING NOT NULL,
archive_destination STRING NOT NULL,
accepted_encoding STRING NOT
);

INSERT INTO `dw-metadata-utilities.metadata_utilities.ingestion_config`
(ingestion_type, --REQUEST
source_schema_table_name, --https://api.coincap.io/v2/assets/
primary_key_column, --ID
incremental_date_column, --NULL
load_type,  --INCR
extra_parameters, --NULL
delimiter,  --NULL
source_system, --COINCAP
connection_name, --COINCAP_ASSET
project_id, --COINCAP-DATA-HUB
dataset,    --EXTERNAL_COINCAP_DATA
table_name, --EXTERNAL_COINCAP_ASSETS
file_format, --JSON
quote_characters, --NULL
api_source_name, --COINCAP
modified_by, --ADMIN
modified_date,--CURRENT_DATETIME
created_by, --ADMIN
created_date, --CURRENT_DATETIME
partitioned_by, --NULL
escape_characters, --NULL
is_parquet, --FALSE
is_external, --TRUE
bucket, --dev-coincap-data
bucket_destination, --input/
archive_destination, --archive/coincap_assets/ 
accepted_encoding --gzip
) VALUES
('REQUEST_PAYLOAD','https://api.coincap.io/v2/assets/bitcoin/history/','ID','DATE','INCR','h1',NULL,'COINCAP','COINCAP_BITCOIN_HISTORY','COINCAP-DATA-HUB','EXTERNAL_COINCAP_DATA','EXTERNAL_COINCAP_BITCOIN_HISTORY','JSON',NULL,'COINCAP API 2.0','ADMIN',CURRENT_DATETIME,'ADMIN',CURRENT_DATETIME, NULL, NULL, FALSE, TRUE, 'dev-coincap-data', 'input/coincap_history','archive/coincap_history','gzip'),
('REQUEST','https://api.coincap.io/v2/exchanges/','ID',NULL,'FULL',NULL,NULL,'COINCAP','COINCAP_EXCHANGES','COINCAP-DATA-HUB','EXTERNAL_COINCAP_DATA','EXTERNAL_COINCAP_EXCHANGES','JSON',NULL,'COINCAP API 2.0','ADMIN',CURRENT_DATETIME,'ADMIN',CURRENT_DATETIME, NULL, NULL, FALSE, TRUE, 'dev-coincap-data', 'input/coincap_exchanges/','archive/coincap_exchanges','gzip'),
('REQUEST','https://api.coincap.io/v2/rates/','ID',NULL,'FULL',NULL,NULL,'COINCAP','COINCAP_RATES','COINCAP-DATA-HUB','EXTERNAL_COINCAP_DATA','EXTERNAL_COINCAP_RATES','JSON',NULL,'COINCAP API 2.0','ADMIN',CURRENT_DATETIME,'ADMIN',CURRENT_DATETIME, NULL, NULL, FALSE, TRUE, 'dev-coincap-data', 'input/coincap_rates/','archive/coincap_rates','gzip'),
('REQUEST','https://api.coincap.io/v2/assets/','ID',NULL,'FULL',NULL,NULL,'COINCAP','COINCAP_ASSET','COINCAP-DATA-HUB','EXTERNAL_COINCAP_DATA','EXTERNAL_COINCAP_ASSETS','JSON',NULL,'COINCAP API 2.0','ADMIN',CURRENT_DATETIME,'ADMIN',CURRENT_DATETIME, NULL, NULL, FALSE, TRUE, 'dev-coincap-data', 'input/coincap_assets/','archive/coincap_assets','gzip');
