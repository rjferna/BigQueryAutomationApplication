--***********************************************************************--
--*                                                                     *--
--* Author: rjferna                                                     *-
--* Create Date: 2025-01-08                                             *-
--* Description: Stored Procedure utilized by Data Ingestion Framework  *-
--*         to update Workflow Action Audit Details                      *-
--*                                                                     *-
--**********************************************************************--

CREATE OR REPLACE PROCEDURE `dw-metadata-utilities.metadata_utilities.sp_updateWorkflowAuditDetails`(param1 STRING, param2 STRING, param3 STRING, param4 STRING)
BEGIN 
  --* Declare local variables *--
  DECLARE v_process_id INT64 DEFAULT CAST(param1 as INT64);
  DECLARE v_project_id STRING DEFAULT lower(param2);
  DECLARE v_dataset STRING DEFAULT lower(param3);
  DECLARE v_table_name STRING DEFAULT lower(param4);


  DECLARE v_query STRING;

  -- Build Query 
  SET v_query = 'UPDATE `dw-metadata-utilities.metadata_utilities.workflow_audit_details` ' ||
                'SET execution_end_datetime = sub.execution_end_datetime, ' ||
                'duration = sub.execution_duration, ' ||
                'total_rows_after = sub.total_rows_after, ' ||
                'row_delta = sub.row_delta, ' ||
                'total_partitions_after = sub.total_partitions_after, ' ||
                'total_logical_bytes_after = sub.total_logical_bytes_after, ' ||
                'logical_bytes_delta = sub.logical_bytes_delta, ' ||
                'total_physical_bytes_after = sub.total_physical_bytes_after, ' ||
                'physical_bytes_delta = sub.physical_bytes_delta ' ||
                'FROM (SELECT CURRENT_DATETIME() AS execution_end_datetime, ' ||
                'TIMESTAMP_DIFF(CURRENT_DATETIME(), ad.execution_start_datetime, MINUTE) AS execution_duration, ' ||
                'ts.total_rows AS total_rows_after, ' ||
                'ts.total_rows - ad.total_rows_before AS row_delta, ' ||
                'ts.total_partitions AS total_partitions_after, ' ||
                'ts.total_logical_bytes AS total_logical_bytes_after, ' ||
                'ts.total_logical_bytes - ad.total_logical_bytes_before AS logical_bytes_delta, ' ||
                'ts.total_physical_bytes AS total_physical_bytes_after, ' ||
                'ts.total_physical_bytes - ad.total_physical_bytes_before AS total_physical_bytes_delta ' ||
                'FROM `dw-metadata-utilities.metadata_utilities.workflow_audit_details` AS ad ' ||
                'LEFT JOIN `' || v_project_id || '.' || v_dataset || '`.INFORMATION_SCHEMA.TABLE_STORAGE AS ts ' ||
                'ON ts.table_name = LOWER(ad.table_name) ' ||
                'WHERE ts.table_schema = "ref_' || v_dataset || '" ' ||
                'AND ts.table_name = "' || v_table_name || '" ' ||
                'AND ad.process_id = ' || v_process_id || ') AS sub ' ||
                'WHERE process_id = ' || v_process_id || ';';


    -- Execute the query 
    EXECUTE IMMEDIATE v_query;
END;
