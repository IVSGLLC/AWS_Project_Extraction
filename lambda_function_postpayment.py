
import datetime
from decimal import Decimal
import json
import boto3
import pytz
from EPDE_CloudWatchLog import CloudWatchLogger

from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient
from EPDE_PostPayment import PostManger
from EPDE_SQS import SQSManager
from EPDE_Validator import RequestValidator
logger=LogManger()
err_handler=ErrorHandler()
class fakefloat(float):
    def __init__(self, value):
        self._value = value
    def __repr__(self):
        return str(self._value)
def defaultencode(o):
    if isinstance(o, Decimal):
        # Subclass float with custom repr?
        return fakefloat(o)
    raise TypeError(repr(o) + " is not JSON serializable")

def lambda_handler(event, context):
    
    resHandler=ResponseHandler()        
    Is_WaitForResponse=False
    tableperstore=1
    tableperstore_post=1
    auth_json=None
    file_id=None  
    message="NOT_POSTED"
    eastern=pytz.timezone('US/Eastern')  
    requestStart = datetime.now().astimezone(eastern)
    res_json=""
    store_code=""
    status="ERROR"    
    queueNM=""
    pay_obj=None
    payment_request=None
    validateResp=None
    total_amount_due=None
    ro_number=None
    ro_status=None
    try:
            
            ct = datetime.datetime.now()
            req_ts = ct.timestamp()            
            requestBody_str=event['body'] 
            payment_request = json.loads(requestBody_str)
            validator=RequestValidator(resHandler)     
            res_json=validator.Validate()
            if res_json["responseCode"]  == -1:
                res_json=res_json["res_json"]
                store_code=res_json['store_code']
                region=res_json['REGION']
                SaveErrorInDB(res_json,payment_request,req_ts,req_ts,store_code,region,tableperstore_post,Is_WaitForResponse) 
                return resHandler.GetAPIResponse(res_json)     

            auth_json=res_json['auth_json']
            store_code=res_json['store_code']
            region=res_json['REGION']
            config=res_json['config']
            queueNM=config['REQUEST_QUEUE_NAME']            
            stage=event['requestContext']['stage']
            if stage == 'test':
                queueNM="TEST_"+ queueNM
            msg_count=int(config['REQUEST_QUEUE_MAX_MSG_COUNT'])
            wait_time=int(config['REQUEST_QUEUE_MSG_WAIT_TIME_IN_SECOND'])

            post_manager= PostManger(region,tableperstore_post)                
            validateResp=post_manager.Validate_PostPayment_inputs(payment_request,auth_json,store_code,tableperstore)
            if validateResp['responseCode']== -1:
                logger.debug("Invalid Payment Request...")
                validateResp1={}
                validateResp1['payment_response']=validateResp
                validateResp1['payment_request']=payment_request
                pay_obj= post_manager.buildPaymentObject(validateResp1,store_code,"NOT_POSTED",req_ts,req_ts,Is_WaitForResponse=Is_WaitForResponse)
                save_resp=post_manager.SavePostPayment(pay_obj) 
                if save_resp['status']== True: 
                    file_id=save_resp['file_id'] 
                    att={"fileId": file_id} 
                    validateResp.update(att)                             
                res_json= validateResp
                return resHandler.GetAPIResponse(res_json)     
            ro_number=payment_request["documentId"]
            ro_status=payment_request["status"]
            store_detail=validateResp['store_detail']  
            pay_obj= post_manager.buildPaymentObject(validateResp,store_code,"IN_QUEUE",req_ts,req_ts,Is_WaitForResponse)         
            save_resp=post_manager.SavePostPayment(pay_obj)               
            if save_resp['status']== False: 
                logger.debug("Error while saving Payment Request in DB...")
                error_code=save_resp['error_code']
                res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)
                return resHandler.GetAPIResponse(res_json) 
            if 'total_amount_due' in pay_obj:
                total_amount_due=pay_obj['total_amount_due']
           
            #//request created...
            file_id=save_resp['file_id']                        
            sqs= SQSManager(QueueName= queueNM,region=region,MaxMsgCount=msg_count,waitTimeInSecond=wait_time)   
            logger.debug("SQS msg_body :"+str(save_resp['payment_request']))
            sqs_resp=sqs.send_message(msg_body=save_resp['payment_request'],MessageGroupId=store_code,MessageDeduplicationId=file_id)
            if sqs_resp['status'] == False:
                logger.debug("Error while sending Payment Request in SQS...")
                error_code=sqs_resp['error_code']
                res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)
                post_manager.UpdatePostPayment(store_code,save_resp['file_id'],"NOT_POSTED",res_json)    
                return resHandler.GetAPIResponse(res_json) 
            message="IN_QUEUE"
            #//request sent to queue and not waiting for response.                                                            
            res_json=validateResp['payment_response'] 
            att={"fileId": file_id} 
            res_json.update(att)
            logger.debug("Response JSON:"+str(res_json))
            ro_detail=validateResp['ro_detail'] 
            logger.debug("store_detail JSON:"+str(store_detail))
            logger.debug("ro_detail JSON:"+str(ro_detail))

            
            if  post_manager.isRetryAllow(store_detail,ro_detail)== False:
                logger.info("Retry rejected: Request timeout/ Retry not activated..Store-Code:"+store_detail['store_code']+",file_id:"+file_id)   
            
            close_all_RO_flag=store_detail['close_all_RO_flag']
            retry_wait_time_in_min=int(store_detail['retry_wait_interval_min'])
            MaxRetryCount=int(store_detail['retry_max_count'])
            # call step function
            input={"request_queue_ms_wait_time_in_second":wait_time,"request_queue_max_msg_count":msg_count,"request_queue_name":queueNM,"close_all_RO_flag":close_all_RO_flag,"MaxRetryCount":MaxRetryCount,"isRetryActive":True,"fileId":save_resp['file_id'],"store_code": store_detail['store_code'],"document_id":ro_detail['document_id'],"retry_wait_time_in_min":(retry_wait_time_in_min*60),"table_per_store_post_payment":tableperstore_post,"table_per_store_ro_detail":tableperstore,"region":region,"stage":stage}           
            client = boto3.client('stepfunctions')
            executionname="PostPayment_Retry_StateMachine_"+str(save_resp['file_id'])
            step_arn='arn:aws:states:us-east-1:188506897258:stateMachine:PostPayment_Retry_StateMachine'
            if stage == 'test':
                executionname="TEST_PostPayment_Retry_StateMachine_"+str(save_resp['file_id'])
                step_arn='arn:aws:states:us-east-1:188506897258:stateMachine:TEST_PostPayment_Retry_StateMachine'
            client.start_execution( stateMachineArn=step_arn,
                                    name=executionname,
                                    input= json.dumps(input,default=defaultencode)
                                )  
            logger.debug("after start stepfunction:")
            status="SUCCESS" 
            return resHandler.GetAPIResponse(res_json)
    except Exception as e:
             #save not posted request 
            logger.error("error occured in lambda_handler",True)
            res_json= resHandler.GetErrorResponseJSON(313,auth_json)
            if file_id is None:
                SaveErrorInDB(res_json,payment_request,req_ts,req_ts,store_code,region,tableperstore_post,Is_WaitForResponse)
                res_json=validateResp['payment_response']
            else:
                post_manager.UpdatePostPayment(store_code,save_resp['file_id'],"NOT_POSTED",res_json)                   
            return resHandler.GetAPIResponse(res_json)
    finally: 
          try:        
               aws_account_id = context.invoked_function_arn.split(":")[4] 
               time_difference=datetime.now().astimezone(eastern)-requestStart
               time_difference_in_milliseconds = int(time_difference.total_seconds() * 1000) 
               responseCode= 0
               errorMessage=message
               errorCode=0
               apiParameters=None
               if status=="ERROR":
                    responseCode= res_json["responseCode"]
                    errorList= res_json["errorList"]               
                    errorMessage=errorList["code"]
                    errorCode=errorList["message"]
               #apiParameters={"payment_request":payment_request,"payment_response":validateResp,"queueName":queueNM, "fileId":file_id}  
               apiParameters={"ro_number":ro_number,"ro_status":ro_status,"totalAmountDue":total_amount_due,"queueName":queueNM, "fileId":file_id}          
               logEvent={
                    "instance" :str(aws_account_id),
                    "app":"EPDE-API-GATEWAY",
                    "sourceIP":event["requestContext"]["identity"]["sourceIp"],
                    "apiId":event['requestContext']['apiId'],   
                    "stage":event['requestContext']['stage'],            
                    "resourcePath" :event['requestContext']['resourcePath'],
                    "apiName" :"PostPayments",
                    "httpMethod" :event['requestContext']['httpMethod'],                   
                    "storeCode":store_code,          
                    "status": status,
                    "requestTime":requestStart.strftime("%Y-%m-%dT%H:%M:%S%z"),  
                    "totalProcessingTimeMS": time_difference_in_milliseconds,                
                    "message": errorMessage,
                    "code": errorCode,
                    "responseCode":responseCode,                    
                    "responseSize": len(res_json.encode('utf-8')) ,
                    "apiType" :"public",                     
                    "extraInfo" :apiParameters           
               } 
               cwLogger= CloudWatchLogger()    
               cwLogger.DoCloudWatchLog('epde/apiLog',logEvent)
          except:
               logger.error("error occured DoCloudWatchLog in lambda_handler",True)
 
def  SaveErrorInDB(res_json,payment_request,req_ts,res_ts,store_code,region,tableperstore_post,Is_WaitForResponse):
                post_manager= PostManger(region,tableperstore_post)   
                validateResp={}
                validateResp['payment_response']=res_json
                validateResp['payment_request']=payment_request
                pay_obj= post_manager.buildPaymentObject(validateResp,store_code,"NOT_POSTED",req_ts,res_ts,Is_WaitForResponse)         
                post_manager.SavePostPayment(pay_obj)  
            