import RequestProcesserS3 as req
import logging
import json
import os
loglevel =int(os.environ['LOG_LEVEL'])
logger = logging.getLogger('Lambda_Handlers_S3')
logger.setLevel(loglevel)

# This Lambda Handler is used to Parse WIP RO Data 
def ProcessLargeData(event, context):
    try:
        logger.info("Inside ProcessLargeData function...")
        records = event.get("Records")   
        #if loglevel==logging.DEBUG:  
           #logger.debug("records: JSON:"+json.dumps(records, indent = 1))   
        aws_account_id = context.invoked_function_arn.split(":")[4]       
        kr=req.RequestProcesserS3()
        response=kr.Process_S3Record(records,aws_account_id)
        operation_status=response.get('operation_status')
        logger.info("operation_status:"+str(operation_status))        
    except Exception as e:
           logger.error("Error Occure in ProcessLargeData......." , exc_info=True) 

 