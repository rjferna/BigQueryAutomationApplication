--***********************************************************************--
--*                                                                     *--
--* Author: rjferna                                                     *-
--* Create Date: 2025-01-08                                             *-
--* Description: Stored Procedure utilized by Data Ingestion Framework  *-
--*         to get table column details                                 *-
--*                                                                     *-
--**********************************************************************--

CREATE OR REPLACE PROCEDURE `metadata_utilities`.sp_getColumnDetails(
  param1 STRING, 
  param2 STRING,
  param3 STRING
  ) 
BEGIN 
  --* Declare local variables *--
  DECLARE v_project_id STRING DEFAULT param1;
  DECLARE v_dataset STRING DEFAULT param2;
  DECLARE v_table_name STRING DEFAULT param3;

  --* Query
  SELECT 
    table_name,
    STRING_AGG(CONCAT(lower(mapping_column), ' ', (datatype)), ', ') AS stg_ref_create_table_column_details,  
    STRING_AGG(CONCAT('SAFE_CAST(' ,lower(column_name),' AS ',(datatype),') AS ',lower(mapping_column)), ',') AS source_to_stg_conversion_column_details,
    STRING_AGG(lower(column_name)) AS source_to_stg_column_query,
    STRING_AGG(lower(mapping_column)) AS mapping_stg_to_ref_column_query
  FROM `dw-metadata-utilities.metadata_utilities.ingestion_column_details`
  WHERE 
    project_id = v_project_id
  AND
    dataset = v_dataset
  AND
    table_name = v_table_name
  GROUP BY 
    table_name
  ;  
END;