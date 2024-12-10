import os
import sys 
import shutil
from datetime import datetime, timedelta

from config import Config
from parse_args import parse_args
from logger import set_logger
from gcp_common import get_connection_details, get_column_details, get_workflow_action_process_id, set_workflow_action_process_id, update_workflow_action_process_id, get_incremental_date, upload_to_bucket, create_external_table, archive_file, create_and_load_staging_table, get_table_exists, create_and_load_reference_table
from requests_common import get_request, get_request_payload
from encryption_decryption_common import Prpcrypt
from file_common import response_to_parquet

def main():
    args = parse_args('Data Ingestion Controller', 'Controller script for data ingestion modules.')
    
    # Read the Configuration file
    config_parser = Config(args.get('config_file'))
    config_var = config_parser.get(args['section'])
    
    # Logging level setup
    log_domain = 'LOG_{}'.format(args['section'] + '_' + args.get('asset'))
    logger = set_logger(args.get('log_level'), log_domain, print_log=args['print_log'], log_path = config_var.get('log_path'))
    logger.debug("args: {}".format(args))

    try:     
        # Makes data directory if not exists
        if os.path.exists(config_var.get('file_path')):
            if not os.listdir(config_var.get('file_path')):
                logger.info(f"No files in directory: {config_var.get('file_path')}")
            elif len(os.listdir(config_var.get('file_path'))) >= 10:
                shutil.make_archive(config_var.get('archive_path') + str(datetime.now().strftime("%Y%m%d%H%M%S")),
                                    'zip',
                                    config_var.get('file_path')
                                    )
                logger.info(f"Archived files in directory: {config_var.get('file_path')}")
                shutil.rmtree(config_var.get('file_path'))
        os.makedirs(config_var.get('file_path'), exist_ok=True)

        # Get Connection Details from GCP
        logger.info(f"Querying GCP for Data Ingestion Details: ({args.get('section')}, {args.get('asset')})")
        results = get_connection_details(connection_name=args.get('section'), table_name=args.get('asset'), keyfile_path=config_var.get('gcp_creds'))


        # Assign Data Ingestion Details
        for key, value in results.items(): 
            #print(f"Key: {key}, Value: {value}")
            connection_name= value['connection_name']
            connection_url= value['connection_url'] 
            user_name= value['user_name']
            password_encrypted= value['password_encrypted'] 
            security_token= value['security_token']
            ingestion_type= value['ingestion_type']
            project_id= value['project_id']
            dataset= value['dataset']
            primary_key_column= value['primary_key_column']
            incremental_date_column= value['incremental_date_column']
            load_type= value['load_type']
            extra_parameters= value['extra_parameters']
            delimiter= value['delimiter']
            file_format= value['file_format']
            quote_characters= value['quote_characters']
            is_parquet= value['is_parquet']
            is_external= value['is_external']
            bucket= value['bucket']
            bucket_destination=value['bucket_destination']
            archive_destination=value['archive_destination']
            accepted_encoding=value['accepted_encoding']



        # Get Process_Id from GCP workflow_action_history 
        logger.info(f"Get Process_ID From GCP Worflow Action History.")
        process_id = get_workflow_action_process_id(keyfile_path=config_var.get('gcp_creds'))
        logger.info(f"Output: ({process_id}).")



        # Decrypt Credentials
        logger.info("Obtaining Connection Credentials")
        pc = Prpcrypt(security_token)



        # Identify Data Ingestion Workflow type
        if ingestion_type == 'REQUEST':
            logger.info("Set Workflow Action History Execution Record")
            workflow_result = set_workflow_action_process_id(process_id=process_id, 
                                                             connection_name=connection_name, 
                                                             dataset=dataset, 
                                                             table_name=args.get('asset'), 
                                                             execution_status=0,
                                                             keyfile_path=config_var.get('gcp_creds')
                                                             )
            logger.info(f"Workflow Action Result: {workflow_result}")

            response = get_request(key=pc.decrypt(password_encrypted),
                                        url=connection_url,
                                        encoding=accepted_encoding
                                        )
        if ingestion_type == 'REQUEST_PAYLOAD':
            logger.info("Set Workflow Action History Execution Record")
            workflow_result = set_workflow_action_process_id(process_id=process_id, 
                                                             connection_name=connection_name, 
                                                             dataset=dataset, 
                                                             table_name=args.get('asset'), 
                                                             execution_status=0,
                                                             keyfile_path=config_var.get('gcp_creds')
                                                             )
            logger.info(f"Workflow Action Result: {workflow_result}")

            logger.info('Get last incremental loadtime')
            incr_result = get_incremental_date(date=incremental_date_column, 
                                               project_id=project_id,
                                               dataset=dataset,
                                               table_name=args.get('asset'), 
                                               keyfile_path=config_var.get('gcp_creds')
                                               )
            logger.info(f'Data collection start datetime: {incr_result}')

            response = get_request_payload(key=pc.decrypt(password_encrypted),
                                                url=connection_url,
                                                encoding=accepted_encoding,
                                                start=incr_result,
                                                end=datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
                                                interval=extra_parameters
                                                )
        
        # Check Response Data & Write to Parquet  
        if "Error:" in response:
            logger.info(f"{response}")
            update_result = update_workflow_action_process_id(process_id=process_id, execution_status=-1, keyfile_path=config_var.get('gcp_creds'))
            logger.info(f'Error: Updating Workflow Action Record: {update_result}')
        elif response is None:
            logger.info('Error: None Object returned')
            update_result = update_workflow_action_process_id(process_id=process_id, execution_status=-1, keyfile_path=config_var.get('gcp_creds'))
            logger.info(f'Error: Updating Workflow Action Record: {update_result}')
        else:
            logger.info(f'Response Type: {type(response)}')
            response_file = response_to_parquet(response_data=response, 
                                                parquet_filename=config_var.get('file_path') + args.get('asset')
                                                )  #+ config_var.get('file_name'))
            logger.info(f'Writing response data to flat file')



        # Upload Data to GCP Bucket
        logger.info(f'Checking to see if file exists: {bucket_destination + args.get('asset') + '.' + file_format.lower()}') 
        archive_response = archive_file(source_bucket_name=bucket, 
                                        source_file_name=bucket_destination + args.get('asset') + '.' + file_format.lower(), 
                                        archive_bucket_name=bucket,
                                        archive_destination=archive_destination, 
                                        archive_file_name=args.get('asset') + '.' + file_format.lower(), 
                                        keyfile_path=config_var.get('gcp_creds')
                                        )
        if "Error:" in archive_response:
            logger.info(f'Error: {archive_response}')
        else:
            logger.info(f'Archive Status: {archive_response}')

        logger.info(f'Uploading Data to Bucket Path: {bucket_destination}{args.get('asset')}.{file_format.lower()}')
        upload_data = upload_to_bucket(bucket_name=bucket, 
                                       source_file_name=response_file, 
                                       destination_blob_name=bucket_destination + args.get('asset') + '.' + file_format.lower(), 
                                       keyfile_path=config_var.get('gcp_creds')
                                       )
        if "Error:" in upload_data:
            logger.info(f'{upload_data}')
            update_workflow_action_process_id(process_id=process_id, execution_status=-1, keyfile_path=config_var.get('gcp_creds'))
            logger.info('Data Ingestion Completed with Errors.' + '\n' + 'Execution END.')

            logfilepath = config_var.get('log_path') + '{}_{:%Y_%m_%d}.log'.format(log_domain, datetime.now())
            logfile = config_var.get('log_bucket_workflow_execution_destination') + '{}_{:%Y_%m_%d}.log'.format(log_domain, datetime.now())
     
            # Upload Workflow Execution Log File to GCP Bucket
            upload_to_bucket(bucket_name=config_var.get('log_bucket'), 
                         source_file_name=logfilepath,
                         destination_blob_name=logfile,
                         keyfile_path=config_var.get('gcp_creds') )
            sys.exit(1)

        print(bucket + bucket_destination + args.get('asset') + '.' + file_format.lower())

        # Create External Table
        logger.info(f"Creating External Table: {project_id}.{dataset}.{args.get('asset').lower()}")
        create_external = create_external_table(
                                        project_id=project_id, 
                                        dataset=dataset, 
                                        table_name=args.get('asset'), 
                                        bucket_destination_name=bucket + '/' + bucket_destination + args.get('asset') + '.' + file_format.lower(), 
                                        file_format=file_format, 
                                        keyfile_path=config_var.get('gcp_creds')
                                        )

        if "Error:" in create_external:
            logger.info(f'{create_external}')
            update_workflow_action_process_id(process_id=process_id, execution_status=-1, keyfile_path=config_var.get('gcp_creds'))                
            logger.info('Data Ingestion Completed with Errors.' + '\n' + 'Execution END.')

            logfilepath = config_var.get('log_path') + '{}_{:%Y_%m_%d}.log'.format(log_domain, datetime.now())
            logfile = config_var.get('log_bucket_workflow_execution_destination') + '{}_{:%Y_%m_%d}.log'.format(log_domain, datetime.now())

            # Upload Workflow Execution Log File to GCP Bucket
            upload_to_bucket(bucket_name=config_var.get('log_bucket'), 
                             source_file_name=logfilepath,
                            destination_blob_name=logfile,
                            keyfile_path=config_var.get('gcp_creds'))
            sys.exit(1)



        # Get Column Details from GCP
        logger.info(f"Querying GCP for Column Details.")
        column_results = get_column_details(project_id=project_id, dataset=dataset,table_name=args.get('asset'), keyfile_path=config_var.get('gcp_creds'))

        if "Error:" in column_results:
            logger.info(f'{column_results}')
            update_workflow_action_process_id(process_id=process_id, execution_status=-1, keyfile_path=config_var.get('gcp_creds'))                
            logger.info('Data Ingestion Completed with Errors.' + '\n' + 'Execution END.')

            logfilepath = config_var.get('log_path') + '{}_{:%Y_%m_%d}.log'.format(log_domain, datetime.now())
            logfile = config_var.get('log_bucket_workflow_execution_destination') + '{}_{:%Y_%m_%d}.log'.format(log_domain, datetime.now())

            # Upload Workflow Execution Log File to GCP Bucket
            upload_to_bucket(bucket_name=config_var.get('log_bucket'), 
                             source_file_name=logfilepath,
                            destination_blob_name=logfile,
                            keyfile_path=config_var.get('gcp_creds'))
            sys.exit(1)



        # Assign Data Ingestion Details
        for key, value in column_results.items():
            #print(f"Key: {key}, Value: {value}")
            stg_and_ref_create_table=value['stg_ref_create_table_column_details']  
            source_to_stg_conversion=value['source_to_stg_conversion_column_details']
            source_to_stg_column_query=value['source_to_stg_column_query']
            mapping_stg_to_ref_column_query=value['mapping_stg_to_ref_column_query']


        # Drop & Create Staging Table
        logger.info(f"Creating and loading Staging Table: {project_id}.stg_{dataset}.{args.get('asset').lower()}")
        create_load_staging = create_and_load_staging_table(
                                        project_id=project_id, 
                                        dataset=dataset, 
                                        table_name=args.get('asset'), 
                                        stg_and_ref_create_table=stg_and_ref_create_table, 
                                        source_to_stg_conversion=source_to_stg_conversion, 
                                        keyfile_path=config_var.get('gcp_creds')
                                        )

        if "Error:" in create_load_staging:
            logger.info(f'{create_load_staging}')
            update_workflow_action_process_id(process_id=process_id, execution_status=-1, keyfile_path=config_var.get('gcp_creds'))                
            logger.info('Data Ingestion Completed with Errors.' + '\n' + 'Execution END.')

            logfilepath = config_var.get('log_path') + '{}_{:%Y_%m_%d}.log'.format(log_domain, datetime.now())
            logfile = config_var.get('log_bucket_workflow_execution_destination') + '{}_{:%Y_%m_%d}.log'.format(log_domain, datetime.now())

            # Upload Workflow Execution Log File to GCP Bucket
            upload_to_bucket(bucket_name=config_var.get('log_bucket'), 
                             source_file_name=logfilepath,
                            destination_blob_name=logfile,
                            keyfile_path=config_var.get('gcp_creds'))
            sys.exit(1)
        


        # Check if Reference table Exists if not Create Reference table
        logger.info(f"Checking if Reference Table Exists: {project_id}.ref_{dataset}.{args.get('asset').lower()}")
        ref_exists = get_table_exists(project_id=project_id, 
                                      dataset=f'ref_{dataset}', 
                                      table_name=args.get('asset'), 
                                      keyfile_path=config_var.get('gcp_creds')
        )
        if  int(ref_exists)== 0 or "Error:" in ref_exists:
            logger.info(f'Reference Table Flag: {ref_exists}, Table Does not exists. Creating Reference table.')
            create_ref = create_and_load_reference_table(flag=0, 
                                                         project_id=project_id, 
                                                         dataset=dataset, 
                                                         table_name=args.get('asset'), 
                                                         stg_and_ref_create_table=stg_and_ref_create_table, 
                                                         mapping_stg_to_ref_query=mapping_stg_to_ref_column_query, 
                                                         keyfile_path=config_var.get('gcp_creds')
                                                         )                
            logger.info('Create Reference table & Load Data Completed.' + '\n' + 'Execution END.')
        elif int(ref_exists) == 1 and load_type == 'FULL':
            logger.info(f'Reference Table Flag: {ref_exists}, Table exists. Identified as FULL dataload.')
            create_ref = create_and_load_reference_table(flag=1, 
                                                         project_id=project_id, 
                                                         dataset=dataset, 
                                                         table_name=args.get('asset'), 
                                                         stg_and_ref_create_table=stg_and_ref_create_table, 
                                                         mapping_stg_to_ref_query=mapping_stg_to_ref_column_query, 
                                                         keyfile_path=config_var.get('gcp_creds')
                                                         )                
            logger.info('Drop & Create Reference table, Full dataload Completed.' + '\n' + 'Execution END.')

        
        
        logfilepath = config_var.get('log_path') + '{}_{:%Y_%m_%d}.log'.format(log_domain, datetime.now())
        logfile = config_var.get('log_bucket_workflow_execution_destination') + '{}_{:%Y_%m_%d}.log'.format(log_domain, datetime.now())
     
        # Upload Workflow Execution Log File to GCP Bucket
        upload_to_bucket(bucket_name=config_var.get('log_bucket'), 
                         source_file_name=logfilepath,
                         destination_blob_name=logfile,
                         keyfile_path=config_var.get('gcp_creds') )        
        
        sys.exit(0)

    except Exception as e:
        logger.info(f"Error: {str(e)}")    
        sys.exit(1)

if __name__ == '__main__':
    main()