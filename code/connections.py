#imports
import json
import snowflake.connector as sf
import logging
import datetime
from datetime import datetime

def read_configs(config_file_name = "config"):
    '''
    Reading the config values that are required for processing

    Parameters
    -----------
    Config values
    '''

    with open("config.json", 'r') as c:
            config_json = json.load(c)
    c.close()
    
    return(config_json)


def snowflake_connection(config,logger):
    '''
    Establish a connection to snowflake

    Parameters
    ----------
    Snowflake Connection Object
    '''
    try:
        print("Connecting....")
        sf_conn = sf.connect(
        account = config['sf_account'],
        user = config['sf_user'],
        password = config['sf_password'],
        #database = config['sf_database'],
        #schema = config['sf_src_schema'],
        warehouse = config['sf_warehouse'],
        role = config['sf_role']
        )
        logger.info("Snowflake connection successful")
        return(sf_conn)
    
    except Exception as e:
        logger.error("Unable to connect snowflake")
        logger.error(e)


def get_timestamp():
    '''
    Get the current timestamp

    Parameters
    ----------
    None

    Returns
    -------
    Current Timestamp
    '''
    x = datetime.now()
    return x.strftime("%Y-%m-%d %H:%M:%S")


def set_logging(lname):
    '''
    Establish log configuration

    Parameters
    ----------
    Log file name

    Returns
    -------
    Logger class object
    '''
    logfilename = lname
    logging.basicConfig(filename= logfilename+'.log', 
                    filemode = 'w',
                    level = 'INFO',
                    format = '%(asctime)s %(name)s %(levelname)s %(message)s',
                    datefmt = '%H:%M:%S' )
    logger = logging.getLogger(logfilename)
    return(logger)