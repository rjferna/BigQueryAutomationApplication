/* ------------------------------------------------------------------------ */
/* ----------------------- workflow_audit_details  ----------------------- */
/* ------------------------------------------------------------------------ */

DROP TABLE IF EXISTS `dw-metadata-utilities.metadata_utilities.workflow_audit_details`;
CREATE TABLE IF NOT EXISTS `dw-metadata-utilities.metadata_utilities.workflow_audit_details` (
audit_id INT64 PRIMARY KEY NOT ENFORCED NOT NULL OPTIONS(description="Auto-incrementing Primary Key"),
process_id INT64  NOT NULL OPTIONS(description="Key from workflow_action_history table."), 
connection_name STRING NOT NULL,
dataset STRING NOT NULL,
table_name STRING NOT NULL,
execution_start_datetime DATETIME,
execution_end_datetime DATETIME,
duration INT64,
record_cnt_before INT64,
record_cnt_after INT64,
record_cnt_delta FLOAT64,
record_cnt_avg FLOAT64,
record_cnt_variance FLOAT64,
record_cnt_std_deviation FLOAT64,
FOREIGN KEY (process_id) REFERENCES `dw-metadata-utilities.metadata_utilities.workflow_action_history`(process_id) NOT ENFORCED
);  


INSERT INTO `dw-metadata-utilities.metadata_utilities.workflow_audit_details` 
(audit_id,process_id,connection_name,dataset,table_name,execution_start_datetime,execution_end_datetime,duration,total_rows_before,total_rows_after,row_delta,total_partitions_before,total_partitions_after,total_logical_bytes_before,total_logical_bytes_after,logical_bytes_delta,total_physical_bytes_before,total_physical_bytes_after,physical_bytes_delta)
VALUES(20000001,100001,"TEST_CONNECTION","TEST_DATA_HUB","EXTERNAL_TEST",CURRENT_DATETIME,CURRENT_DATETIME,0,0,0,0,0,0,0,0,0,0,0,0);


INSERT INTO `dw-metadata-utilities.metadata_utilities.workflow_audit_details` 
(audit_id,process_id,connection_name,dataset,table_name,execution_start_datetime,execution_end_datetime,duration,total_rows_before,total_rows_after,row_delta,total_partitions_before,total_partitions_after,total_logical_bytes_before,total_logical_bytes_after,logical_bytes_delta,total_physical_bytes_before,total_physical_bytes_after,physical_bytes_delta)
VALUES(
SELECT 
(SELECT IFNULL(MAX(ingestion_config_id), 0) + 1 FROM `dw-metadata-utilities.metadata_utilities.ingestion_config`),
'{process_id}' as process_id,
'{connection_name}' as connection_name,
'{dataset}' as dataset,
'{table_name' as table_name,
w.execution_start_datetime,
w.execution_end_datetime,
NULL as execution_duration,
ts.total_rows as total_rows_before,
NULL as total_rows_after,
NULL as rows_delta,
ts.total_partitions as total_partitions_before,
NULL as total_partitions_after,
ts.total_logical_bytes as total_logical_bytes_before,
NULL as total_logical_bytes_after,
NULL as logical_bytes_delta,
ts.total_physical_bytes as total_physical_bytes_before,
NULL as total_physical_bytes_after,
NULL total_physical_bytes_delta
FROM 
  `coincap-data-hub`.`region-us-east1`.INFORMATION_SCHEMA.TABLE_STORAGE  as ts --us-east1
LEFT JOIN 
  `dw-metadata-utilities.metadata_utilities.workflow_action_history` as w ON ts.table_name = lower(w.table_name)
WHERE 
  ts.table_schema = 'ref_coincap_data'
AND 
  ts.table_name = 'bitcoin_history'
AND
  w.process_id = 100164 --{process_id}
;