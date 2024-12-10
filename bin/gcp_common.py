from google.cloud import storage, bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta

def list_files_in_bucket(bucket_name, bucket_destination,keyfile_path):
    # Initialize a client with the service account keyfile
    storage_client = storage.Client.from_service_account_json(keyfile_path)
    
    # Get the bucket
    bucket = storage_client.bucket(bucket_name)
    
    # List all objects in the bucket
    blobs = bucket.list_blobs()

    bucket_items = []
    files = []

    #print(f"Files in bucket {bucket_name}:")
    for blob in blobs:
        bucket_items.append(blob.name)
    
    # Clean Name
    for val in range(0, len(bucket_items)):
        if  len(bucket_items[val]) >= 21:
            #files.append(bucket_items[val].split('input/nhl-game-data/')[1].split('.')[0]) # Removes bucket path and file type extension
            files.append(bucket_items[val].split(bucket_destination)[1])

    return files

def upload_to_bucket(bucket_name, source_file_name, destination_blob_name, keyfile_path): 
    try: 
        # Initialize a client with the service account keyfile 
        storage_client = storage.Client.from_service_account_json(keyfile_path) 
    
        # Get the bucket 
        bucket = storage_client.bucket(bucket_name) 
    
        # Create a blob and upload the file to the bucket 
        blob = bucket.blob(destination_blob_name) 
        blob.upload_from_filename(source_file_name) 
    
        #print(f"File {source_file_name} uploaded to {destination_blob_name}.")
        return "SUCCESS"
    except Exception as e:
        return f"Error: {e}."

def archive_file(source_bucket_name, source_file_name, archive_bucket_name, archive_destination,archive_file_name, keyfile_path):
    try:
        # Initialize a client with the service account keyfile 
        storage_client = storage.Client.from_service_account_json(keyfile_path)

        # Get the source bucket
        source_bucket = storage_client.bucket(source_bucket_name)
        source_file = source_bucket.blob(source_file_name)

        # Get the destination bucket
        archive_destination_bucket = storage_client.bucket(archive_bucket_name)

        # Generate the new filename with current date-time
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        new_name = f'{timestamp}_{archive_file_name}' # archive_file_name.format(timestamp=timestamp)

        # Copy the blob to the new location with the new name
        source_bucket.copy_blob(
            source_file,
            archive_destination_bucket,
            archive_destination + new_name
        )

        # Delete the original blob from the source bucket
        source_file.delete()

        return 'SUCCESS'
    except Exception as e:
        return f'Error: {e}'


def query_dataset(query, keyfile_path):
    try: 
        keyfile = keyfile_path

        # Create credentials & Initialize client
        credentials = service_account.Credentials.from_service_account_file(keyfile)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        # Execute the query
        query_job = client.query(query)

        # Fetch the results
        results = query_job.result()

        # Store results in a dictionary 
        result_dict = {} 
        for row in results: 
            row_dict = {key: row[key] for key in row.keys()} 
            result_dict[row[0]] = row_dict # Using the first column's value as the dictionary key # Print the results

        return result_dict
    except Exception as e:
        return f"Error: {e}"

def get_incremental_date(date, project_id,dataset,table_name, keyfile_path):
    try: 
        keyfile = keyfile_path

        # Create credentials & Initialize Client
        credentials = service_account.Credentials.from_service_account_file(keyfile)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        query=f'''
                SELECT MAX({date.lower()}) as MX_DATE FROM `{project_id.lower()}.{dataset.lower()}.{table_name.lower()}`
                '''

        # Execute the query
        query_job = client.query(query)

        # Fetch the results
        results = query_job.result()

        # Store results in a dictionary 
        result_dict = {} 
        for row in results: 
            row_dict = {key: row[key] for key in row.keys()} 
            result_dict[row[0]] = row_dict # Using the first column's value as the dictionary key # Print the results

        # Identify Data Ingestion Workflow
        for key, value in result_dict.items():
             date_value = value['MX_DATE']

        return date_value
    except Exception as e:
        return (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%dT%H:%M:%SZ')

def get_table_exists(project_id, dataset, table_name, keyfile_path):
    try:
        keyfile = keyfile_path

        # Create credentials & Initialize Client
        credentials = service_account.Credentials.from_service_account_file(keyfile)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        query = f'''
                SELECT COUNT(1) as flag FROM `{project_id}.{dataset}.__TABLES_SUMMARY__` WHERE table_id = '{table_name}';
                ''' 

        query_job = client.query(query)

        # Fetch the results
        results = query_job.result()

        # Store results in a dictionary 
        result_dict = {} 
        for row in results: 
            row_dict = {key: row[key] for key in row.keys()} 
            result_dict[row[0]] = row_dict # Using the first column's value as the dictionary key # Print the results
    
        # Identify Data Ingestion Workflow
        for key, value in result_dict.items(): 
            flag= value['flag']

        return flag
    except Exception as e:
        return f"Error: {e}"    

def get_connection_details(connection_name, table_name, keyfile_path):
    try:
        keyfile = keyfile_path

        # Create credentials & Initialze Client
        credentials = service_account.Credentials.from_service_account_file(keyfile)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        query = f'''
            SELECT DISTINCT 
            a.connection_name, 
            a.connection_url, 
            a.user_name, 
            a.password_encrypted, 
            a.security_token, 
            b.ingestion_type,
            b.dataset,
            b.primary_key_column,
            b.incremental_date_column,
            b.load_type,
            b.extra_parameters,
            b.project_id,
            b.dataset,
            b.delimiter,
            b.file_format,
            b.quote_characters,
            b.is_parquet,
            b.is_external,
            b.bucket,
            b.bucket_destination,
            b.archive_destination,
            b.accepted_encoding
            FROM `dw-metadata-utilities.metadata_utilities.ingestion_connection_info` as a
            INNER JOIN `dw-metadata-utilities.metadata_utilities.ingestion_config` as b on a.connection_name = b.connection_name
            WHERE 
                a.connection_name = '{connection_name}'
            AND
                b.table_name = '{table_name}'
            ; 
            ''' 

        # Execute the query
        query_job = client.query(query)

        # Fetch the results
        results = query_job.result()

        # Store results in a dictionary 
        result_dict = {} 
        for row in results: 
            row_dict = {key: row[key] for key in row.keys()} 
            result_dict[row[0]] = row_dict # Using the first column's value as the dictionary key # Print the results

        return result_dict
    except Exception as e:
        return f"Error: {e}"    
    
def get_column_details(project_id, dataset, table_name, keyfile_path):
    try:
        keyfile = keyfile_path

        # Create credentials & Initialze Client
        credentials = service_account.Credentials.from_service_account_file(keyfile)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        query = f'''
            SELECT 
            table_name,
            STRING_AGG(CONCAT(lower(mapping_column), ' ', (datatype)), ', ') AS stg_ref_create_table_column_details,  
            STRING_AGG(CONCAT('SAFE_CAST(' ,lower(column_name),' AS ',(datatype),') AS ',lower(mapping_column)), ',') AS source_to_stg_conversion_column_details,
            STRING_AGG(lower(column_name)) AS source_to_stg_column_query,
            STRING_AGG(lower(mapping_column)) AS mapping_stg_to_ref_column_query
            FROM `dw-metadata-utilities.metadata_utilities.ingestion_column_details`
            WHERE 
                project_id = '{project_id}'
            AND
                dataset = '{dataset}'
            AND
                table_name = '{table_name}'
            GROUP BY table_name
            ; 
            ''' 

        # Execute the query
        query_job = client.query(query)

        # Fetch the results
        results = query_job.result()

        # Store results in a dictionary 
        result_dict = {} 
        for row in results: 
            row_dict = {key: row[key] for key in row.keys()} 
            result_dict[row[0]] = row_dict # Using the first column's value as the dictionary key # Print the result

        return result_dict
    except Exception as e:
        return f"Error: {e}"     

def get_workflow_action_process_id(keyfile_path):
    try:
        keyfile = keyfile_path

        # Create credentials & Initialize Client
        credentials = service_account.Credentials.from_service_account_file(keyfile)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        query = '''
                SELECT MAX(process_id) + 1 as process_id FROM `dw-metadata-utilities.metadata_utilities.workflow_action_history`;
                ''' 

        query_job = client.query(query)

        # Fetch the results
        results = query_job.result()

        # Store results in a dictionary 
        result_dict = {} 
        for row in results: 
            row_dict = {key: row[key] for key in row.keys()} 
            result_dict[row[0]] = row_dict # Using the first column's value as the dictionary key # Print the results
    
        # Identify Data Ingestion Workflow
        for key, value in result_dict.items(): 
            process_id= value['process_id']

        return process_id
    except Exception as e:
        return f"Error: {e}"

def set_workflow_action_process_id(process_id, connection_name, dataset, table_name, execution_status,keyfile_path):
    try:
        keyfile = keyfile_path

        # Create credentials & Initialize Client
        credentials = service_account.Credentials.from_service_account_file(keyfile)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        query = f'''
            INSERT INTO `dw-metadata-utilities.metadata_utilities.workflow_action_history`
            (process_id,connection_name,dataset,table_name,executed_by,execution_start_datetime,execution_end_datetime,execution_status) 
            VALUES({process_id},'{connection_name}','{dataset}','{table_name}', 'ADMIN',CURRENT_DATETIME,NULL,{execution_status});
            ''' 

        # Execute the query
        query_job = client.query(query)

        # Wait for the query to complete
        query_job.result()

        return "SUCCESS"
    except Exception as e:
        return f"Error: {e}"

def update_workflow_action_process_id(process_id, execution_status, keyfile_path):
    try:
        keyfile = keyfile_path

        # Create credentials & Initialize Client
        credentials = service_account.Credentials.from_service_account_file(keyfile)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        query = f'''
            UPDATE `dw-metadata-utilities.metadata_utilities.workflow_action_history`
            SET execution_end_datetime = CURRENT_DATETIME, execution_status = {execution_status}
            WHERE process_id = {process_id}
            ''' 

        # Execute the query
        query_job = client.query(query)

        # Fetch the results
        results = query_job.result()

        return "SUCCESS"
    except Exception as e:
        return f"Error: {e}"
    
def create_external_table(project_id, dataset, table_name, bucket_destination_name, file_format, keyfile_path):
    try:
        keyfile = keyfile_path

        # Create credentials & Initialize Client
        credentials = service_account.Credentials.from_service_account_file(keyfile)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        query = f'''
                CREATE OR REPLACE EXTERNAL TABLE `{project_id.lower()}.external_{dataset.lower()}.{table_name.lower()}`
                OPTIONS (
                format = '{file_format}',
                uris = ['gs://{bucket_destination_name}']
                );
                ''' 

        query_job = client.query(query)

        # Fetch the results
        results = query_job.result()

        return "SUCCESS"
    except Exception as e:
        return f"Error: {e}"

def create_and_load_staging_table(project_id, dataset, table_name, stg_and_ref_create_table, source_to_stg_conversion, keyfile_path):
    try:
        keyfile = keyfile_path

        # Create credentials & Initialize Client
        credentials = service_account.Credentials.from_service_account_file(keyfile)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        query = f'''
                DROP TABLE IF EXISTS `{project_id.lower()}.stg_{dataset.lower()}.{table_name.lower()}`;
                CREATE TABLE IF NOT EXISTS `{project_id.lower()}.stg_{dataset.lower()}.{table_name.lower()}` (
                {stg_and_ref_create_table}
                );

                INSERT INTO `{project_id.lower()}.stg_{dataset.lower()}.{table_name.lower()}`
                SELECT {source_to_stg_conversion} FROM `{project_id.lower()}.external_{dataset.lower()}.{table_name.lower()}`;                
                ''' 
        
        query_job = client.query(query)

        # Fetch the results
        results = query_job.result()

        return "SUCCESS"
    except Exception as e:
        return f"Error: {e}"            