--***********************************************************************--
--*                                                                     *--
--* Author: rjferna                                                     *-
--* Create Date: 2025-01-08                                             *-
--* Description: Stored Procedure utilized by Data Ingestion Framework  *-
--*         to update Workflow Action Audit Details                      *-
--*                                                                     *-
--**********************************************************************--

CREATE OR REPLACE PROCEDURE `dw-metadata-utilities.metadata_utilities.sp_updateWorkflowAuditDetails`(param1 STRING, param2 STRING, param3 STRING, param4 STRING, param5 STRING, param6 STRING)
BEGIN 
  --* Declare local variables *--
  DECLARE v_process_id INT64 DEFAULT CAST(param1 as INT64);
  DECLARE v_project_id STRING DEFAULT lower(param2);
  DECLARE v_connection_name STRING DEFAULT lower(param3);
  DECLARE v_dataset STRING DEFAULT lower(param4);
  DECLARE v_table_name STRING DEFAULT lower(param5);


  --DECLARE v_query STRING;

  --Update Execution End Date
  EXECUTE IMMEDIATE
  'Update `dw-metadata-utilities.metadata_utilities.workflow_audit_details`' ||
  'SET execution_end_datetime = (SELECT execution_end_datetime FROM `dw-metadata-utilities.metadata_utilities.workflow_action_history` WHERE process_id = ' || v_process_id || ') ' ||
  'WHERE process_id = ' || v_process_id || ';';

  --Update Duration
  EXECUTE IMMEDIATE
  'Update `dw-metadata-utilities.metadata_utilities.workflow_audit_details` ' ||
  'SET duration = TIMESTAMP_DIFF(execution_end_datetime, execution_start_datetime, SECOND) ' ||
  'WHERE process_id = ' || v_process_id || ';';

  --Update Record Count After
  EXECUTE IMMEDIATE
  'Update `dw-metadata-utilities.metadata_utilities.workflow_audit_details`' ||
  'SET record_cnt_after = (SELECT COUNT(1) FROM `' || v_project_id || '.ref_' || v_dataset || '.' || v_table_name || '`) ' ||
  'WHERE process_id = ' || v_process_id || ';';

  --Update Record Count Delta
  EXECUTE IMMEDIATE
  'Update `dw-metadata-utilities.metadata_utilities.workflow_audit_details`' ||
  'SET record_cnt_delta = (record_cnt_after - record_cnt_before)' ||
  'WHERE process_id = ' || v_process_id || ';';

  --Update Variance & Standard Deviation Points
  EXECUTE IMMEDIATE
  'UPDATE `dw-metadata-utilities.metadata_utilities.workflow_audit_details` ' ||
  'SET record_cnt_avg = a.avrg, record_cnt_variance = a.var, record_cnt_std_deviation = a.standard_deviation ' ||
  'FROM ( ' ||
  'WITH vals as ( ' ||
  'SELECT ' ||
  'audit_id,' ||
  'IFNULL(record_cnt_delta, 0.0) record_cnt_delta ' ||
  'FROM `dw-metadata-utilities.metadata_utilities.workflow_audit_details` ' ||
  'WHERE table_name = UPPER("' || v_table_name || '") ' ||
  'ORDER BY audit_id desc ' ||
  'limit 7 ' ||
  '), v_avg as ( ' ||
  'SELECT ' ||
  'SUM(record_cnt_delta) / COUNT(audit_id) as avrg ' ||
  'FROM vals ' ||
  '), v_var_stddev as ( ' ||
  'SELECT ' ||
  'SUM(SQRT(ABS(record_cnt_delta - avrg)))  / COUNT(audit_id) as var, ' ||
  'CASE WHEN COUNT(audit_id) - 1 = 0 THEN SUM(SQRT(ABS(record_cnt_delta - avrg)))  / (COUNT(audit_id)) ELSE SUM(SQRT(ABS(record_cnt_delta - avrg)))  / (COUNT(audit_id) - 1) END as standard_deviation ' ||
  'FROM vals ' ||
  'CROSS JOIN v_avg ' ||
  ') ' ||
  'SELECT ' ||
  'avrg, ' ||
  'var, ' ||
  'standard_deviation ' ||
  'FROM v_avg ' ||
  'CROSS JOIN v_var_stddev ) as a '
  'WHERE process_id = ' || v_process_id || ';';

END;
