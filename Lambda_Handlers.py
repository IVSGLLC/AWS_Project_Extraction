import RequestProcesser as req
import logging
import json
import os
loglevel =int(os.environ['LOG_LEVEL'])
logger = logging.getLogger('Lambda_Handlers')
logger.setLevel(loglevel)

# This Lambda Handler is used to Parse WIP RO Data 
def Process_CaptureData(event, context):
    try:
        logger.info("Inside Process_CaptureData function...")
        records = event.get("Records")   
        #if loglevel==logging.DEBUG:  
        #logger.debug("records:"+str(records))   
        aws_account_id = context.invoked_function_arn.split(":")[4]      
       
        kr=req.RequestProcesser()
        response=kr.Process_KinesStreamRecord(records,aws_account_id)
        if response is not None and len(response)>0:
            operation_status=response[0].get('operation_status')
            logger.info("operation_status:"+str(operation_status))
        
    except Exception as e:
           logger.error("Error Occure in Process_CaptureData......." , exc_info=True) 

 