--***********************************************************************--
--*                                                                     *--
--* Author: rjferna                                                     *-
--* Create Date: 2025-01-08                                             *-
--* Description: Stored Procedure utilized by Data Ingestion Framework  *-
--*         to get the max date for incremental data load               *-
--*                                                                     *-
--**********************************************************************--

CREATE OR REPLACE PROCEDURE `metadata_utilities`.sp_getIncrementalDate(
  param1 STRING,
  param2 STRING,
  param3 STRING,
  param4 STRING
)
BEGIN 
  --* Declare local variables *--
  DECLARE v_project_id STRING DEFAULT lower(param1);
  DECLARE v_dataset STRING DEFAULT lower(param2);
  DECLARE v_table_name STRING DEFAULT lower(param3);
  DECLARE v_column_name STRING DEFAULT lower(param4);
  DECLARE v_query STRING;

  -- Build Query 
    SET v_query = 'SELECT MAX(' || v_column_name || ') as MX_DATE FROM `' || v_project_id || '.' || v_dataset || '.' || v_table_name || '`;';

    -- Execute the query 
  EXECUTE IMMEDIATE v_query;
END;