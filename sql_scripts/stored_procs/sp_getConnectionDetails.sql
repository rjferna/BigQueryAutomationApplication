--***********************************************************************--
--*                                                                     *--
--* Author: rjferna                                                     *-
--* Create Date: 2025-01-08                                             *-
--* Description: Stored Procedure utilized by Data Ingestion Framework  *-
--*         to get connection information and  ingestion configuration  *-
--*                                                                     *-
--**********************************************************************--

CREATE OR REPLACE PROCEDURE `metadata_utilities`.sp_getConnectionDetails(
  param1 STRING, 
  param2 STRING
  ) 
BEGIN 
  --* Declare local variables *--
  DECLARE v_connection_name STRING DEFAULT param1;
  DECLARE v_table_name STRING DEFAULT param2;

  --* Query
  SELECT DISTINCT 
    a.connection_name, 
    a.connection_url, 
    a.user_name, 
    a.password_encrypted, 
    a.security_token, 
    b.ingestion_type,
    b.source_schema_table_name,
    b.dataset,
    b.primary_key_column,
    b.incremental_date_column,
    b.load_type,
    b.extra_parameters,
    b.project_id,
    b.dataset,
    b.file_format,
    b.header,
    b.delimiter,            
    b.quote_characters,
    b.escape_characters,
    b.accepted_encoding,
    b.is_parquet,
    b.to_parquet,
    b.bucket,
    b.bucket_destination,
    b.archive_destination
  FROM `dw-metadata-utilities.metadata_utilities.ingestion_connection_info` as a
  INNER JOIN `dw-metadata-utilities.metadata_utilities.ingestion_config` as b on a.connection_name = b.connection_name
  WHERE 
    a.connection_name = v_connection_name
  AND
    b.table_name = v_table_name
  ;  
END;