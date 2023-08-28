
from datetime import datetime
import json
import sys
import pytz
from EPDE_CloudWatchLog import CloudWatchLogger
from EPDE_Deposits import DepositsManager
from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient
from EPDE_PostPayment import PostManger
from EPDE_Deposits import DepositsManager
from EPDE_PostAccounting import AccountingManager
from pathlib import Path
#from EPDE_SQS import SQSManager
logger=LogManger()
err_handler=ErrorHandler()
def lambda_handler(event, context):
    res_json=""
    resHandler=ResponseHandler()
    cwLogger= CloudWatchLogger()
    api=""
    fileId=""
    store_code=""
    log_api=""
    log_fileId=""
    log_status="ERROR"
    log_dealerCode=""
    log_msg=""
    uploadStart=''
    try:
            eastern=pytz.timezone('US/Eastern')  
            uploadStart = datetime.now().astimezone(eastern)
            app_client=AppClient()
            config=app_client.GetConfiguration(event)            
            region=config['REGION']
            tablePerStorePayment=1
            post_manager=PostManger(region,tablePerStorePayment)

            tablePerStoreDeposit=1
            deposit_manager=DepositsManager(region,tablePerStoreDeposit)

            table_Per_Store_Post_Accounting=1
            postaccounting_manager=AccountingManager(region,table_Per_Store_Post_Accounting)

            _moduleNM="EPDE_Lambda"
            _functionNM="lambda_handler"
            err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)
            maintenanace_resp=app_client.isMaintenanceTime(config)
            if maintenanace_resp['status']:  
                res_json= maintenanace_resp['resjson'] 
                logger.debug("Maintenance Time is running API call not allowed...")
                return resHandler.GetAPIResponse(res_json) 
            
            access_token=None
            try:
                Authorization=event['headers']['Authorization']         
                auth_token=Authorization.split(" ")
                access_token=auth_token[1] 
            except:
                access_token=None
            if access_token is not None:
                   request_body=event['body']
                   logger.debug("UPLOAD RESPONSE request_body="+str(request_body))
                   request_dict=json.loads(request_body)
                   fileName=request_dict['fileName']
                   fileNameList=fileName.split("_")
                   api=""
                   fileId=""
                   if len(fileNameList)>2:
                       api=fileNameList[1] 
                       fileId= fileNameList[2]  #GCP0003_postpaymentsResponse_NYJM3RHUHS.json
                       fileId=Path(fileId).stem

                   data=request_dict['data']
                   store_code=request_dict['dealerCode']

                   #msg_count=int(config['REQUEST_QUEUE_MAX_MSG_COUNT'])
                   #wait_time=int(config['REQUEST_QUEUE_MSG_WAIT_TIME_IN_SECOND'])
                   region=config['REGION']
                   #stage=event['requestContext']['stage']
                   #queueNm=config['RESPONSE_QUEUE_NAME']
                   #if stage == 'test':
                      #queueNm="TEST_"+ queueNm
                   #sqs= SQSManager(queueNm,region,msg_count,wait_time) 
                    
                   postResponse= json.loads(data)
                   try:
                        if "fileId" in postResponse:   
                            postResponse["fileId"]=fileId
                        else:
                            att={"fileId": fileId}
                            postResponse.update(att)

                        if "dealerCode" in postResponse:   
                            postResponse["dealerCode"]=store_code
                        else:
                            att={"dealerCode": store_code}
                            postResponse.update(att)

                        if "api" in postResponse: 
                            postResponse["api"]=api                           
                            if api == 'postpaymentsResponse':
                                postResponse["api"]='postpayments'
                            if api == 'postdepositsResponse':
                                postResponse["api"]='postdeposits' 
                            if api == 'postaccountingResponse':
                                postResponse["api"]='postaccounting' 
                           
                        else:
                            att={"api": api}
                            if api == 'postpaymentsResponse':
                                att={"api": 'postpayments'}                                
                            if api == 'postdepositsResponse':
                                att={"api": 'postdeposits'} 
                            if api == 'postaccountingResponse':
                                att={"api": 'postaccounting'} 
                            postResponse.update(att)
                        
                   except:
                        ""
                   logger.debug("postresponse ="+str(postResponse))         
                   #sqs_resp= sqs.send_message(postResponse,store_code,fileId) will send response to sqs not in use
                                  
                   
                   #api=postResponse['api']
                   client_id=None
                   if api == 'postpaymentsResponse':
                        """ if tablePerStorePayment==1:
                            store_resp=app_client.GetStoreDetail(store_code,region)
                            if store_resp['status']:
                                client_id=store_resp['item']['client_id'] """                            
                        res_json=post_manager.UpdatePostPayment(store_code,fileId,"RESPONSE_UPLOADED",postResponse)
                        log_status="SUCCESS"
                        log_fileId=fileId
                        log_dealerCode=store_code
                        log_msg="RESPONSE_UPLOADED"
                        if postResponse is not None and  'api' in postResponse:
                           log_api=postResponse['api']  

                   elif api == 'postdepositsResponse':                         
                        res_json=deposit_manager.UpdatePostDeposits(store_code,fileId,"RESPONSE_UPLOADED",postResponse)
                        log_status="SUCCESS"
                        log_fileId=fileId
                        log_dealerCode=store_code
                        log_msg="RESPONSE_UPLOADED"
                        if postResponse is not None and  'api' in postResponse:
                           log_api=postResponse['api']  
                   elif api == 'postaccountingResponse':                         
                        res_json=postaccounting_manager.UpdatePostAccounting(store_code,fileId,"RESPONSE_UPLOADED",postResponse)
                        log_status="SUCCESS"
                        log_fileId=fileId
                        log_dealerCode=store_code
                        log_msg="RESPONSE_UPLOADED"
                        if postResponse is not None and  'api' in postResponse:
                           log_api=postResponse['api']  
                   else:
                        log_status="ERROR"                       
                        res_json= resHandler.GetErrorResponseJSON(344,None)  
                        log_msg=res_json['errorList'][0]['message']  
                    #else:
                    #   res_json=sqs_resp 
            else:
                 logger.debug("API Key not found...")
                 log_status='ERROR'
                 res_json= resHandler.GetErrorResponseJSON(331,None)  
                 log_msg=res_json['errorList'][0]['message']            
            #return resHandler.GetAPIResponse(res_json)
    except Exception as e:
            ex_type, ex_value, ex_traceback = sys.exc_info()
            log_status='ERROR'             
            logger.error("error occured in lambda_handler",True)
            log_msg=str(ex_value)
            res_json= resHandler.GetErrorResponseJSON(313,None)
    aws_account_id = context.invoked_function_arn.split(":")[4]  
    time_difference=datetime.now().astimezone(eastern)-uploadStart
    time_difference_in_milliseconds = int(time_difference.total_seconds() * 1000) 
    logEvent={
        
                "instance" :str(aws_account_id),
                "sourceIP":event["requestContext"]["identity"]["sourceIp"],
                "api" :log_api,
                "dealerCode":log_dealerCode,
                "fileId":log_fileId,
                "stage":event['requestContext']['stage'],
                "status": log_status,
                "uploadMessageDateTime":uploadStart.strftime("%Y-%m-%dT%H:%M:%S%z") ,
                "uploadMessageDurationMS": time_difference_in_milliseconds,                
                "message": log_msg,
               
            }     
    cwLogger.DoCloudWatchLog('epde/uploadLog',logEvent)
    return resHandler.GetAPIResponse(res_json)