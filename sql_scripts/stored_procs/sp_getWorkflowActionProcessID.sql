--***********************************************************************--
--*                                                                     *--
--* Author: rjferna                                                     *-
--* Create Date: 2025-01-08                                             *-
--* Description: Stored Procedure utilized by Data Ingestion Framework  *-
--*         to generate new Workflow Action History Process ID          *-
--*                                                                     *-
--**********************************************************************--

CREATE OR REPLACE PROCEDURE `metadata_utilities`.sp_getWorkflowActionProcessID()
BEGIN 
  --* Query
  SELECT MAX(process_id) + 1 as process_id FROM `dw-metadata-utilities.metadata_utilities.workflow_action_history`;
END;