/* ------------------------------------------------------------------------ */
/* ----------------------- workflow_action_history  ----------------------- */
/* ------------------------------------------------------------------------ */

DROP TABLE IF EXISTS `dw-metadata-utilities.metadata_utilities.workflow_action_history`;
CREATE TABLE IF NOT EXISTS `dw-metadata-utilities.metadata_utilities.workflow_action_history` (
process_id INT64 PRIMARY KEY NOT ENFORCED NOT NULL OPTIONS(description="Auto-incrementing Primary Key"),
connection_name STRING NOT NULL,
dataset STRING NOT NULL,
table_name STRING NOT NULL,
executed_by STRING NOT NULL, 
execution_start_datetime DATETIME NOT NULL,
execution_end_datetime DATETIME,
execution_status INT64 NOT NULL OPTIONS(description="Status of job execution: 1 = Complete, 0 = inprogress, -1 = Failure")
);


INSERT INTO `dw-metadata-utilities.metadata_utilities.workflow_action_history` 
(process_id,connection_name,dataset,table_name,executed_by,execution_start_datetime,execution_end_datetime,execution_status) 
VALUES(10000,'TEST_CONNECTION','TEST_DATA_HUB','EXTERNAL_TEST', 'ADMIN',CURRENT_DATETIME,NULL,0);