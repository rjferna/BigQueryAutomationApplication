import argparse
import os
import sys
from datetime import datetime, timedelta

def parse_args(prog, description):
    parser = argparse.ArgumentParser(description=description, prog=prog, epilog='The data will be first loaded to a Flat File')
    parser.add_argument('-s', '--section',required=True, type=str, help='The section name of configuration file, which will be used to get the object information.')
    parser.add_argument('-a', '--asset', required=True, type=str, help='The asset name which will be used to get the object information.')                
    parser.add_argument('-l', required=False, default='info', metavar='LOG_LEVEL', dest='log_level', choices=['info', 'debug', 'warning', 'error'], help='Logging level, "info" by default.')
    parser.add_argument('-p', required=False, metavar='CONFIG_FILE', dest='config_file', default='config.ini', help='The configuration file to be used. If not specified, the program will try to find it with "./config.ini"')
    parser.add_argument('--print_log', required=False, dest='print_log', action='store_true', help='Whether print the log to console. False by default')

    
    args = vars(parser.parse_args())
     
    # Check the if the configuration file exists
    file_name = args.get('config_file')
    dirname = os.path.dirname(file_name)
    
    if not dirname:
        abslute_path = os.path.abspath(sys.argv[0])
        args['config_file'] = os.path.join(os.path.dirname(abslute_path), 'config.ini')
    elif not os.path.isdir(dirname): 
        raise FileNotFoundError('No such directory {}'.format(dirname))
    elif not os.path.isfile(file_name):
        raise FileNotFoundError('No such file {}'.format(file_name))
    
    if not os.path.isfile(args['config_file']):
        raise FileNotFoundError(f'Cannot find config file {file_name}')
    
    return args


if __name__ == '__main__':
    print(parse_args('test', 'This is a test'))
