--***********************************************************************--
--*                                                                     *--
--* Author: rjferna                                                     *-
--* Create Date: 2025-01-08                                             *-
--* Description: Stored Procedure utilized by Data Ingestion Framework  *-
--*         to create external table                                    *-
--*                                                                     *-
--**********************************************************************--

CREATE OR REPLACE PROCEDURE `metadata_utilities`.sp_CreateExternalTable(
  param1 STRING,
  param2 STRING,
  param3 STRING,
  param4 STRING,
  param5 STRING
)
BEGIN 
  --* Declare local variables *--
  DECLARE v_project_id STRING DEFAULT lower(param1);
  DECLARE v_dataset STRING DEFAULT lower(param2);
  DECLARE v_table_name STRING DEFAULT lower(param3);
  DECLARE v_file_format STRING DEFAULT param4;
  DECLARE v_bucket_name STRING DEFAULT param5;
  DECLARE v_query STRING;

  -- Build Query 
    SET v_query = 'CREATE OR REPLACE EXTERNAL TABLE `' || v_project_id || '.' || v_dataset || '.' || v_table_name || '` OPTIONS (format = "' || v_file_format || '", uris = ["gs://' || v_bucket_name || '"]);';


    -- Execute the query 
  EXECUTE IMMEDIATE v_query;
END;