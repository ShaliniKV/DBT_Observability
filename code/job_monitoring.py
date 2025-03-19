#--------------------------------------------------------------------------#
        #Script Name : job_monitoring.py
        #Description : To monitor the execution of models in a project
        #Version : 1.0
        #Author : Shalini
        #Last Update Date : 20/11/2024
#--------------------------------------------------------------------------#

import pandas as pd
import datetime
from datetime import date
import connections as cf
import subprocess
import shlex
import json

def main():
    
    #call to set logs
    logger = cf.set_logging('JOB_MONITORING_'+str(date.today()))

    logger.info(f"Process started at {cf.get_timestamp()}")
    logger.info("Reading config.json file")

    #call to read configurations
    config_json = cf.read_configs()
    logger.info(config_json)
    
    #call to establish snowflake connection
    logger.info("Connecting to Snowflake")
    sf_conn = cf.snowflake_connection(config_json,logger)
    sf_cur = sf_conn.cursor()
    """
    try:
        logger.info("Executing command")
        cmd = 'dbt docs generate'
        logger.info(cmd)
        res = subprocess.run(shlex.split(cmd))

        logger.info(f"Returncode : {res.returncode}")
        if(res.returncode == 0):
            logger.info("Reading manifest.json")
            
            model_list = []
            with open(str('../target/manifest.json'), 'r') as f:
                json_manifest = json.loads(f.read())
                key_list = list(json_manifest['nodes'].keys())
                model_list = [i.split('.')[2] for i in key_list if i.split('.')[0] == 'model' and i.split('.')[1] == 'DBT_Observability']
                logger.info("List of models found in this project")
                logger.info(model_list)
                if(len(model_list) == 0):
                    logger.error('No models found in this project')
                    raise Exception

                logger.info("Reading run_results log table")
                for i in model_list :
                    query = f'''with cte as (
                                select *,
                                row_number() over (partition by model_name order by loaded_at desc) as row_id
                                from marts.dbt_results_log  )
                            select distinct invocation_id,model_name,status,message,loaded_at 
                            from cte 
                            where date(loaded_at) = current_date 
                            and model_name like '{i}' 
                            and row_id = 1;'''
                    logger.info(query)
                    sf_cur.execute(query)
                    df = pd.DataFrame(sf_cur.fetchall())
                    if(len(df) == 0):
                        ins_query = f'''insert into marts.model_execution_tracker values 
                                (current_timestamp,0,'{i}','Missed','Not executed for today');'''
                        logger.info(ins_query)
                        sf_cur.execute(ins_query)
                    else:
                        column_list = []
                        for c in sf_cur.description:
                            column_list.append(c[0])
                        df.columns = column_list
                        message = df['MESSAGE'][0].replace("'","''")
                        ins_query = f'''insert into marts.model_execution_tracker values 
                                (current_timestamp,'{df['INVOCATION_ID'][0]}','{i}','{df['STATUS'][0]}','{message}');'''

                        logger.info(ins_query)
                        sf_cur.execute(ins_query)
                logger.info("Insertion Successful")

        else:
            logger.error("Unable to generate docs, please check the command")
            raise

    except Exception as e:
        logger.error(e)
    """
    logger.info(f"Process ended at {cf.get_timestamp()}")
    sf_conn.close()

if __name__== "__main__":
    main() 