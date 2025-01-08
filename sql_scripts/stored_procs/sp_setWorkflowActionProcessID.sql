--***********************************************************************--
--*                                                                     *--
--* Author: rjferna                                                     *-
--* Create Date: 2025-01-08                                             *-
--* Description: Stored Procedure utilized by Data Ingestion Framework  *-
--*         to set new Workflow Action History record                   *-
--*                                                                     *-
--**********************************************************************--

CREATE OR REPLACE PROCEDURE `metadata_utilities`.sp_setWorkflowActionProcessID(
  param1 STRING, 
  param2 STRING,
  param3 STRING,
  param4 STRING,
  param5 STRING,
  param6 STRING
  ) 
BEGIN 
  --* Declare local variables *--
  DECLARE v_process_id INT64 DEFAULT CAST(param1 AS INT64);
  DECLARE v_connection_name STRING DEFAULT param2;
  DECLARE v_dataset STRING DEFAULT param3;
  DECLARE v_table_name STRING DEFAULT param4;
  DECLARE v_execution_start_datetime DATETIME DEFAULT CAST(param5 AS DATETIME);
  DECLARE v_execution_status INT64 DEFAULT CAST(param6 AS INT64);

  --* Query
  INSERT INTO `dw-metadata-utilities.metadata_utilities.workflow_action_history`
  (process_id,connection_name,dataset,table_name,executed_by,execution_start_datetime,execution_end_datetime,execution_status) 
  VALUES(v_process_id, v_connection_name, v_dataset, v_table_name, 'ADMIN', v_execution_start_datetime, NULL, v_execution_status);
END;