/* ------------------------------------------------------------------------ */
/* ---------------------- ingestion_connection_info ----------------------- */
/* ------------------------------------------------------------------------ */
DROP TABLE IF EXISTS `dw-metadata-utilities.metadata_utilities.ingestion_connection_info`;
CREATE TABLE `dw-metadata-utilities.metadata_utilities.ingestion_connection_info` (
  connection_id INT64 PRIMARY KEY NOT ENFORCED NOT NULL OPTIONS(description="Auto-incrementing Primary Key"),
  connection_name STRING NOT NULL,
  connection_url STRING NOT NULL,
  user_name STRING NOT NULL, 
  password_encrypted STRING NOT NULL, 
  security_token STRING NOT NULL, 
  created_by STRING NOT NULL, 
  created_date DATETIME NOT NULL,
  modified_by STRING NOT NULL, 
  modified_date DATETIME NOT NULL
)
CLUSTER BY connection_id
;

INSERT INTO `dw-metadata-utilities.metadata_utilities.ingestion_connection_info` 
(connection_id, connection_name,connection_url,user_name, password_encrypted,  security_token, created_by, created_date, modified_by, modified_date) VALUES
(0,'TEST', 'TEST', 'ADMIN', '<encrypted credentials>', '<security token if needed>','ADMIN',CURRENT_DATETIME,'ADMIN',CURRENT_DATETIME),
((SELECT IFNULL(MAX(id), 0) + 1 FROM `dw-metadata-utilities.metadata_utilities.ingestion_connection_info`),'COINCAP_BITCOIN_HISTORY', 'https://api.coincap.io/v2/assets/bitcoin/history', 'ADMIN', '<encrypted credentials>', '<security token if needed>','ADMIN',CURRENT_DATETIME,'ADMIN',CURRENT_DATETIME),
((SELECT IFNULL(MAX(id), 0) + 1 FROM `dw-metadata-utilities.metadata_utilities.ingestion_connection_info`),'COINCAP_ASSET', 'https://api.coincap.io/v2/assets/', 'ADMIN', '<encrypted credentials>', '<security token if needed>','ADMIN',CURRENT_DATETIME,'ADMIN',CURRENT_DATETIME),
((SELECT IFNULL(MAX(id), 0) + 1 FROM `dw-metadata-utilities.metadata_utilities.ingestion_connection_info`),'COINCAP_BITCOIN_HISTORY', 'https://api.coincap.io/v2/assets/bitcoin', 'ADMIN', '<encrypted credentials>', '<security token if needed>','ADMIN',CURRENT_DATETIME,'ADMIN',CURRENT_DATETIME),
((SELECT IFNULL(MAX(id), 0) + 1 FROM `dw-metadata-utilities.metadata_utilities.ingestion_connection_info`),'COINCAP_EXCHANGES', 'https://api.coincap.io/v2/exchanges/', 'ADMIN', '<encrypted credentials>', '<security token if needed>','ADMIN',CURRENT_DATETIME,'ADMIN',CURRENT_DATETIME),
((SELECT IFNULL(MAX(id), 0) + 1 FROM `dw-metadata-utilities.metadata_utilities.ingestion_connection_info`),'COINCAP_RATES', 'https://api.coincap.io/v2/rates/', 'ADMIN', '<encrypted credentials>', '<security token if needed>','ADMIN',CURRENT_DATETIME,'ADMIN',CURRENT_DATETIME),
((SELECT IFNULL(MAX(id), 0) + 1 FROM `dw-metadata-utilities.metadata_utilities.ingestion_connection_info`),'COINCAP_MARKETS', 'https://api.coincap.io/v2/markets/', 'ADMIN', '<encrypted credentials>', '<security token if needed>','ADMIN',CURRENT_DATETIME,'ADMIN',CURRENT_DATETIME)
;


/* AWS S3 Example */
INSERT INTO `dw-metadata-utilities.metadata_utilities.ingestion_connection_info` 
(connection_id, connection_name,connection_url,user_name, password_encrypted,  security_token, created_by, created_date, modified_by, modified_date) VALUES
((SELECT IFNULL(MAX(id), 0) + 1 FROM `dw-metadata-utilities.metadata_utilities.ingestion_connection_info`),
'S3_COINCAP', 
'<BUCKET_NAME>', 
'ADMIN', 
'<encrypted_credentials>', 
'{"token": "<security_token_if_needed>", "access": "<access_key_if_needed>"}',
'ADMIN',
CURRENT_DATETIME,
'ADMIN',
CURRENT_DATETIME)
;


/* GCS Example */
INSERT INTO `dw-metadata-utilities.metadata_utilities.ingestion_connection_info` 
(connection_id, connection_name,connection_url,user_name, password_encrypted,  security_token, created_by, created_date, modified_by, modified_date) VALUES
((SELECT IFNULL(MAX(id), 0) + 1 FROM `dw-metadata-utilities.metadata_utilities.ingestion_connection_info`),
'GCS_COINCAP', 
'gs://dev-coincap-data', 
'dbt-user@nhl-player-statistics.iam.gserviceaccount.com', 
'<encrypted_credentials>',
'{"token": "<security_token_if_needed>", "access": "<access_key_if_needed>"}',
'ADMIN',
CURRENT_DATETIME,
'ADMIN',
CURRENT_DATETIME)
;

