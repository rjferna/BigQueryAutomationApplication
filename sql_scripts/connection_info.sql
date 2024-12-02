/* ------------------------------------------------------------------------ */
/* ---------------------- ingestion_connection_info ----------------------- */
/* ------------------------------------------------------------------------ */
DROP TABLE IF EXISTS `dw-metadata-utilities.metadata_utilities.ingestion_connection_info`;
CREATE TABLE IF NOT EXISTS `dw-metadata-utilities.metadata_utilities.ingestion_connection_info`(
  connection_name STRING NOT NULL,
  connection_url STRING NOT NULL,
  user_name STRING NOT NULL, 
  password_encrypted STRING NOT NULL, 
  security_token STRING NOT NULL
  ); 

INSERT INTO `dw-metadata-utilities.metadata_utilities.ingestion_connection_info` (connection_name,connection_url,user_name, password_encrypted, driver_name, security_token, sandbox)
VALUES('COINCAP_BITCOIN_HISTORY', 'https://api.coincap.io/v2/assets/bitcoin/history', 'ADMIN', '<encrypted credentials>', '<security token if needed>'),
('COINCAP_ASSET', 'https://api.coincap.io/v2/assets/', 'ADMIN', '<encrypted credentials>', '<security token if needed>'),
('COINCAP_BITCOIN_HISTORY', 'https://api.coincap.io/v2/assets/bitcoin', 'ADMIN', '<encrypted credentials>', '<security token if needed>'),
('COINCAP_EXCHANGES', 'https://api.coincap.io/v2/exchanges/', 'ADMIN', '<encrypted credentials>', '<security token if needed>'),
('COINCAP_RATES', 'https://api.coincap.io/v2/rates/', 'ADMIN', '<encrypted credentials>', '<security token if needed>'),
('COINCAP_MARKETS', 'https://api.coincap.io/v2/markets/', 'ADMIN', '<encrypted credentials>', '<security token if needed>')
;