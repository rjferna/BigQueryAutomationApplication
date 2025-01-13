--***********************************************************************--
--*                                                                     *--
--* Author: rjferna                                                     *-
--* Create Date: 2025-01-08                                             *-
--* Description: Stored Procedure utilized by Data Ingestion Framework  *-
--*         to set Workflow Action Audit Details                        *-
--*                                                                     *-
--**********************************************************************--

CREATE OR REPLACE PROCEDURE `dw-metadata-utilities.metadata_utilities.sp_setWorkflowAuditDetails`(
  param1 STRING, 
  param2 STRING, 
  param3 STRING, 
  param4 STRING, 
  param5 STRING, 
  param6 STRING
  )
BEGIN 
  --* Declare local variables *--
  DECLARE v_process_id INT64 DEFAULT CAST(param1 as INT64);
  DECLARE v_project_id STRING DEFAULT lower(param2);
  DECLARE v_connection_name STRING DEFAULT lower(param3);
  DECLARE v_dataset STRING DEFAULT lower(param4);
  DECLARE v_table_name STRING DEFAULT lower(param5);
  DECLARE v_execution_start_datetime DATETIME DEFAULT CAST(param6 AS DATETIME);

  DECLARE v_insert_query STRING;
  DECLARE v_update_query STRING;

  -- Build Query 
  SET v_insert_query =
  'INSERT INTO `dw-metadata-utilities.metadata_utilities.workflow_audit_details` (' ||
  'audit_id, ' || 
  'process_id, ' ||
  'connection_name, ' ||
  'dataset, ' ||
  'table_name, ' ||
  'execution_start_datetime, ' ||
  'execution_end_datetime, ' ||
  'duration, ' ||
  'record_cnt_before, ' ||
  'record_cnt_after, ' ||
  'record_cnt_delta, ' ||
  'record_cnt_avg, ' ||
  'record_cnt_variance,' || 
  'record_cnt_std_deviation ) ' ||
  'VALUES ((SELECT IFNULL(MAX(audit_id), 0) + 1 FROM `dw-metadata-utilities.metadata_utilities.workflow_audit_details`), ' ||
  v_process_id  || ', ' || 
  'UPPER("' ||v_connection_name || '"), ' || 
  'UPPER("' ||v_dataset || '"), ' || 
  'UPPER("' || v_table_name || '"), ' || 
  'CAST("' || v_execution_start_datetime || '" AS DATETIME), ' || 
  'NULL, ' ||
  'NULL, ' ||
  'NULL, ' ||
  'NULL, ' || 
  'NULL, ' || 
  'NULL, ' || 
  'NULL, ' || 
  'NULL);';

    -- Execute the query 
    EXECUTE IMMEDIATE v_insert_query;



    SET v_update_query = 
  'Update `dw-metadata-utilities.metadata_utilities.workflow_audit_details`' ||
  'SET record_cnt_before = (SELECT COUNT(1) FROM `' || v_project_id || '.ref_' || v_dataset || '.' || v_table_name || '`) ' ||
  'WHERE process_id = ' || v_process_id || ';';

  -- Execute the query 
    EXECUTE IMMEDIATE v_update_query;
END;