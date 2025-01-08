--***********************************************************************--
--*                                                                     *--
--* Author: rjferna                                                     *-
--* Create Date: 2025-01-08                                             *-
--* Description: Stored Procedure utilized by Data Ingestion Framework  *-
--*         to set Workflow Action Audit Details                        *-
--*                                                                     *-
--**********************************************************************--

CREATE OR REPLACE PROCEDURE `dw-metadata-utilities.metadata_utilities.sp_setWorkflowAuditDetails`(param1 STRING, param2 STRING, param3 STRING, param4 STRING, param5 STRING, param6 STRING)
BEGIN 
  --* Declare local variables *--
  DECLARE v_process_id INT64 DEFAULT CAST(param1 as INT64);
  DECLARE v_project_id STRING DEFAULT lower(param2);
  DECLARE v_connection_name STRING DEFAULT lower(param3);
  DECLARE v_dataset STRING DEFAULT lower(param4);
  DECLARE v_table_name STRING DEFAULT lower(param5);
  DECLARE v_execution_start_datetime DATETIME DEFAULT CAST(param6 AS DATETIME);

  DECLARE v_query STRING;

  -- Build Query 
  SET v_query =
  'INSERT INTO `dw-metadata-utilities.metadata_utilities.workflow_audit_details` ' ||
  'SELECT (SELECT IFNULL(MAX(audit_id), 0) + 1 FROM `dw-metadata-utilities.metadata_utilities.workflow_audit_details`) as audit_id, ' ||
  v_process_id  || 'as process_id, ' || 
  v_connection_name || 'as connection_name, ' ||
  v_dataset || 'as dataset, ' || 
  v_table_name || 'as table_name, ' || 
  v_execution_start_datetime || 'as execution_start_datetime, ' ||
  'NULL as execution_end_datetime,   NULL as execution_duration,  ts.total_rows as total_rows_before,  NULL as total_rows_after,  NULL as rows_delta,  ts.total_partitions as total_partitions_before,  NULL as total_partitions_after, ' ||
  'ts.total_logical_bytes as total_logical_bytes_before,   NULL as total_logical_bytes_after,  NULL as logical_bytes_delta,  ts.total_physical_bytes as total_physical_bytes_before,  NULL as total_physical_bytes_after, NULL total_physical_bytes_delta ' ||
  'FROM `dw-metadata-utilities.metadata_utilities.workflow_action_history` as w ' ||
  'LEFT JOIN `' || v_project_id || '`.`region-us-east1`.INFORMATION_SCHEMA.TABLE_STORAGE  as ts ON ts.table_name = "' || v_table_name || '"' ||
  'WHERE ts.table_schema = "ref_' || v_dataset || '" AND ts.table_name = "' || v_table_name || '" AND  w.process_id = "' || v_process_id || '";';

    -- Execute the query 
    EXECUTE IMMEDIATE v_query;
END;