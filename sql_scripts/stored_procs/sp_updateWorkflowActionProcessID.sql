--***********************************************************************--
--*                                                                     *--
--* Author: rjferna                                                     *-
--* Create Date: 2025-01-08                                             *-
--* Description: Stored Procedure utilized by Data Ingestion Framework  *-
--*         to update Workflow Action History record by setting status  *-
--*                                                                     *-
--**********************************************************************--

CREATE OR REPLACE PROCEDURE `metadata_utilities`.sp_updateWorkflowActionProcessID(
  param1 STRING, 
  param2 STRING
  ) 
BEGIN 
  --* Declare local variables *--
  DECLARE v_process_id INT64 DEFAULT CAST(param1 AS INT64);
  DECLARE v_execution_status INT64 DEFAULT CAST(param2 AS INT64);
  
  --* Query
  UPDATE `dw-metadata-utilities.metadata_utilities.workflow_action_history`
  SET execution_end_datetime = CURRENT_DATETIME, execution_status = v_execution_status
  WHERE process_id = v_process_id;
END;