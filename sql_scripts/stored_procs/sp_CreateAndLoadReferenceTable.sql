--***********************************************************************--
--*                                                                     *--
--* Author: rjferna                                                     *-
--* Create Date: 2025-01-08                                             *-
--* Description: Stored Procedure utilized by Data Ingestion Framework  *-
--*         to Create & load Reference table                            *-
--*                                                                     *-
--**********************************************************************--

CREATE OR REPLACE PROCEDURE `metadata_utilities`.sp_CreateAndLoadReferenceTable(
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
  DECLARE v_mapping_stg_ref_create_table STRING DEFAULT param4;
  DECLARE v_mapping_stg_to_ref_conversion STRING DEFAULT param5;
 

  -- Drop Reference Table
    EXECUTE IMMEDIATE 
    'DROP TABLE IF EXISTS `' || v_project_id || '.ref_' || v_dataset || '.' || v_table_name || '`;';
  
  -- Create Reference Table
    EXECUTE IMMEDIATE
    'CREATE TABLE IF NOT EXISTS `' || v_project_id || '.ref_' || v_dataset || '.' || v_table_name || '` ('
     || v_mapping_stg_ref_create_table || ', IS_DELETED BOOL, IS_HARD_DELETE BOOL, DW_CREATE_DATETIME DATETIME, DW_LOAD_DATETIME DATETIME);'; 

  -- Load Staging Data to Reference Table
    EXECUTE IMMEDIATE
    'INSERT INTO `' || v_project_id || '.ref_' || v_dataset || '.' || v_table_name || '` '
    || 'SELECT ' || v_mapping_stg_to_ref_conversion || ', FALSE AS IS_DELETED, FALSE AS IS_HARD_DELETE, CURRENT_DATETIME AS DW_CREATE_DATETIME, CURRENT_DATETIME AS DW_LOAD_DATETIME FROM `' || v_project_id || '.stg_' || v_dataset || '.' 
    || v_table_name || '`;';

END;