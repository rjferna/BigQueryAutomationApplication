import os
import sys 
import shutil
from datetime import datetime, timedelta

from config import Config
from parse_args import parse_args
from logger import set_logger
from gcp_common import get_connection_details, get_workflow_action_process_id, set_workflow_action_process_id, update_workflow_action_process_id, get_incremental_date
from requests_common import get_request, get_request_payload
from encryption_decryption_common import Prpcrypt
from file_common import dict_to_json

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
        ###logger.info(f"Output: ({results}).")

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


        # Get Process_Id from GCP workflow_action_history 
        logger.info(f"Get Process_ID From GCP Worflow Action History.")
        process_id = get_workflow_action_process_id(keyfile_path=config_var.get('gcp_creds'))
        logger.info(f"Output: ({process_id}).")
        
        # Decrypt Credentials
        logger.info("Obtaining Connection Credentials")
        pc = Prpcrypt(security_token)
        

        if ingestion_type == 'REQUEST':
            logger.info("Set Workflow Action History Execution Record")
            workflow_result = set_workflow_action_process_id(process_id=process_id, connection_name=connection_name, dataset=dataset, table_name=args.get('asset'), execution_status=0,keyfile_path=config_var.get('gcp_creds'))
            logger.info(f"Workflow Action Result: {workflow_result}")

            response_json = get_request(key=pc.decrypt(password_encrypted),
                                        url=connection_url,
                                        encoding=config_var.get('accepted_encoding')
                                        )
        if ingestion_type == 'REQUEST_PAYLOAD':
            logger.info("Set Workflow Action History Execution Record")
            workflow_result = set_workflow_action_process_id(process_id=process_id, connection_name=connection_name, dataset=dataset, table_name=args.get('asset'), execution_status=0,keyfile_path=config_var.get('gcp_creds'))
            logger.info(f"Workflow Action Result: {workflow_result}")

            logger.info('Get last incremental loadtime')
            incr_result = get_incremental_date(date=incremental_date_column, project_id=project_id,dataset=dataset,table_name=args.get('asset'), keyfile_path=config_var.get('gcp_creds'))
            logger.info(f'Data collection start datetime: {incr_result}')

            response_json = get_request_payload(key=pc.decrypt(password_encrypted),
                                                url=connection_url,
                                                encoding=config_var.get('accepted_encoding'),
                                                start=incr_result,
                                                end=datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
                                                interval=extra_parameters
                                                )  
        if "Error:" in response_json:
            logger.info(f"{response_json}")
            update_result = update_workflow_action_process_id(process_id=process_id, execution_status=-1, keyfile_path=config_var.get('gcp_creds'))
            logger.info(f'Error: Updating Workflow Action Record: {update_result}')
        elif response_json is None:
            logger.info('Error: None Object returned')
            update_result = update_workflow_action_process_id(process_id=process_id, execution_status=-1, keyfile_path=config_var.get('gcp_creds'))
            logger.info(f'Error: Updating Workflow Action Record: {update_result}')
        else:
            dict_to_json(response_json, config_var.get('file_path') + args.get('asset'))  #+ config_var.get('file_name'))
            update_result = update_workflow_action_process_id(process_id=process_id, execution_status=1, keyfile_path=config_var.get('gcp_creds'))
            logger.info(f'Updating Workflow Action Record: {update_result}')
        
        logger.info('Data Ingestion Complete.' + '\n' + 'Execution END.')

        sys.exit(0)

    except Exception as e:
        logger.info(f"Error: {str(e)}")    
        sys.exit(1)

if __name__ == '__main__':
    main()