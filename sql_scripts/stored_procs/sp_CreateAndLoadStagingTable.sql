--***********************************************************************--
--*                                                                     *--
--* Author: rjferna                                                     *-
--* Create Date: 2025-01-08                                             *-
--* Description: Stored Procedure utilized by Data Ingestion Framework  *-
--*         to create & load staging table                              *-
--*                                                                     *-
--**********************************************************************--

CREATE OR REPLACE PROCEDURE `metadata_utilities`.sp_CreateAndLoadStagingTable(
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
  DECLARE v_stg_and_ref_create_table STRING DEFAULT param4;
  DECLARE v_source_to_stg_conversion STRING DEFAULT param5;
 

  -- Drop Table if exists    
    EXECUTE IMMEDIATE 
    'DROP TABLE IF EXISTS `' || v_project_id || '.stg_' || v_dataset || '.' || v_table_name || '`;';
  
  -- Create new staging table
    EXECUTE IMMEDIATE
    'CREATE TABLE IF NOT EXISTS `' || v_project_id || '.stg_' || v_dataset || '.' || v_table_name || '` ('
     || v_stg_and_ref_create_table || ', IS_DELETED BOOL, IS_HARD_DELETE BOOL, DW_CREATE_DATETIME DATETIME, DW_LOAD_DATETIME DATETIME);'; 

  -- Load data into staging
    EXECUTE IMMEDIATE
    'INSERT INTO `' || v_project_id || '.stg_' || v_dataset || '.' || v_table_name || '` '
    || 'SELECT ' || v_source_to_stg_conversion || ', FALSE AS IS_DELETED, FALSE AS IS_HARD_DELETE, CURRENT_DATETIME AS DW_CREATE_DATETIME, CURRENT_DATETIME AS DW_LOAD_DATETIME FROM `' || v_project_id || '.external_' || v_dataset || '.' 
    || v_table_name || '`;';

END;