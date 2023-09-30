
import datetime
from decimal import Decimal
import json
import boto3
import pytz
from EPDE_CloudWatchLog import CloudWatchLogger
from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_RO import RepairOrder
from EPDE_Response import ResponseHandler
from EPDE_PostPayment import PostManger
from EPDE_SQS import SQSManager
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
    res_json=""
    resHandler=ResponseHandler()
    message="NOT_POSTED"
    eastern=pytz.timezone('US/Eastern')  
    requestStart = datetime.now().astimezone(eastern)
    res_json=""
    store_code=""
    status="ERROR"    
    queueNM=""
    payment_request=None
    validateResp=None
    tableperstore=1
    tableperstore_post=1
    fileId=None
    document_id=None
    parent_file_id=""
    latest_ro_status=""
    total_amount_due=""
    try:
        logger.debug("PostPaymentRetry>> event="+str(event))
        close_all_RO_flag=event['close_all_RO_flag']
        retry_wait_time_in_min=int(event['retry_wait_time_in_min'])
        MaxRetryCount= int(event['MaxRetryCount'])
        isRetryActive=event['isRetryActive']
        fileId=event['fileId']        
        store_code=event['store_code']     
        document_id= event['document_id'] 
        region=event['region']
        step_arn=None
        queueNm=event['request_queue_name']
        stage='prod'
        if 'stage' in event:
            stage=event['stage']

        msg_count=int(event['request_queue_max_msg_count'])
        wait_time=int(event['request_queue_ms_wait_time_in_second'])
       
        rOrder=RepairOrder(region,tableperstore)
        roDetailResp=rOrder.GetRODetail(store_code,document_id,'RO')  
        if roDetailResp['status']:
            item =roDetailResp['item']
            warranty_due=item['warranty_due']
            spc_ins=item['spc_ins']
            try:
                warranty_due_ft=float(warranty_due)
            except:
                warranty_due_ft=0.0
            latest_ro_status=item['status']
            logger.debug("latest_ro_status="+latest_ro_status)
            post_manager= PostManger(region,tableperstore_post)             
            if (close_all_RO_flag  or warranty_due_ft==0) and latest_ro_status.upper() =='C97':                
                ct = datetime.datetime.now()
                req_ts = ct.timestamp()
                postpayentry=post_manager.GetPostPaymentEntry(store_code,fileId)
                if postpayentry['status']:
                    paymentEntry=postpayentry['item']
                    if 'total_amount_due' in paymentEntry:
                        total_amount_due=paymentEntry['total_amount_due']
                    #if paymentEntry['request_status'] =='REQUEST_DOWNLOADED' or paymentEntry['request_status'] =='RESPONSE_UPLOADED' :
                    if True:
                        request_status=paymentEntry['request_status']
                        paymentEntry['retry_count']=paymentEntry['retry_count']+1
                        if request_status=='IN_QUEUE':
                            response_json=paymentEntry['response_json']
                            if response_json != None:
                                try:                       
                                    errList=[
                                                {
                                                "code": "365",
                                                "message": "Request Timeout!,DMS is not available.",
                                                }
                                            ]
                                    if "errorList" in response_json:
                                        response_json["errorList"]= errList
                                    else:
                                        att={"errorList": errList}
                                        response_json.update(att)
                                except Exception as e:
                                    pass
                            post_manager.UpdatePostPaymentRetryCountRequestStatusTimeOut(store_code,fileId,paymentEntry['retry_count'],latest_ro_status,"TIMEOUT",response_json)
                                                  
                        else:                           
                            post_manager.UpdatePostPaymentRetryCount(store_code,fileId,paymentEntry['retry_count'],latest_ro_status)
                        ct = datetime.datetime.now()
                        req_ts = ct.timestamp()
                        retry_paymentEntry=paymentEntry
                        retry_paymentEntry['ro_status']=str(latest_ro_status)
                        retry_paymentEntry['request_status']='IN_QUEUE'
                        retry_paymentEntry['response_json']=None
                        retry_paymentEntry['req_time']=str(req_ts)
                        retry_paymentEntry['ro_status_on']=str(req_ts)                        
                        retry_paymentEntry['res_time']=str(req_ts)
                        retry_paymentEntry['parent_file_id']=fileId
                        retry_paymentEntry['file_id']=None
                        parent_file_id=fileId
                        
                        try:                       
                            retry_paymentEntry['is_spc_ins']="N"
                            if spc_ins != None and len(spc_ins)>0:         
                               retry_paymentEntry['is_spc_ins']="Y"           
                        except:
                            retry_paymentEntry['is_spc_ins']="N"

                        save_resp= post_manager.SavePostPayment(retry_paymentEntry)
                        if save_resp['status']== True: 
                                file_id=save_resp['file_id']       
                                sqs= SQSManager(queueNm,region,msg_count,wait_time)   
                                payment_request=save_resp['payment_request']
                                logger.debug("SQS msg_body :"+str(save_resp['payment_request']))  

                                sqs_resp=sqs.send_message(msg_body=save_resp['payment_request'],MessageGroupId=store_code,MessageDeduplicationId=file_id)
                                if sqs_resp['status'] == True:
                                    status="SUCCESS"
                                    if isRetryActive and (paymentEntry['retry_count']< MaxRetryCount):
                                        # call step function
                                        input={"request_queue_ms_wait_time_in_second":wait_time,"request_queue_max_msg_count":msg_count,"request_queue_name":queueNm,"close_all_RO_flag":close_all_RO_flag,"MaxRetryCount":MaxRetryCount,"isRetryActive":True,"fileId":fileId,"store_code": store_code,"document_id":document_id,"retry_wait_time_in_min":(retry_wait_time_in_min),"table_per_store_post_payment":tableperstore_post,"table_per_store_ro_detail":tableperstore,"region":region,"stage":stage}           
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
                                    else:
                                        logger.info("Retry rejected:Max Retry count reached.")
                                else:
                                    logger.info("Retry rejected: Error while sending Payment Request in SQS...")
                                    error_code=sqs_resp['error_code']
                                    res_json= resHandler.GetErrorResponseJSON(error_code,None)  
                                    post_manager.UpdatePostPayment(store_code,save_resp['file_id'],"NOT_POSTED",res_json)                   
                                    
                        else:
                            logger.info("Error while saving retry Post Payment Request in DB...") 
                            res_json= resHandler.GetErrorResponseJSON(save_resp['error_code'],None)
                    else:
                        logger.info("Retry rejected: Request timeout/ In_Queue status...file_id:"+fileId)              
                else:
                    logger.info("Post Payment Request Not exist in DB...file_id:"+fileId) 
                    res_json= resHandler.GetErrorResponseJSON(postpayentry['error_code'],None)                   
            else:
               logger.info("Retry rejected: RO not ready or Retry not required or retry criteria not matched...")  
               postpayentry=post_manager.GetPostPaymentEntry(store_code,fileId)
               if postpayentry['status']:
                    paymentEntry=postpayentry['item']
                    request_status=paymentEntry['request_status']
                    if request_status=='IN_QUEUE':
                        response_json=paymentEntry['response_json']
                        if response_json != None:
                            try:                       
                                errList=[
                                                {
                                                "code": "365",
                                                "message": "Request Timeout!,DMS is not available.",
                                                }
                                            ]
                                if "errorList" in response_json:
                                    response_json["errorList"]= errList
                                else:
                                    att={"errorList": errList}
                                    response_json.update(att)
                            except Exception as e:
                                pass
                        post_manager.UpdatePostPaymentRetryCountRequestStatusTimeOut(store_code,fileId,paymentEntry['retry_count'],latest_ro_status,"TIMEOUT",response_json)
                    
                    else:   
                        post_manager.UpdatePostPaymentRetryCount(store_code,fileId,paymentEntry['retry_count'],latest_ro_status)
               res_json= resHandler.GetErrorResponseJSON(342,None)                 
        else:
           logger.info("RO not Found in DB......")  
           res_json= resHandler.GetErrorResponseJSON(roDetailResp['error_code'],None)

    except Exception as e:
            #save not posted request 
            logger.error("error occured in retry_lambda_handler",True)
            res_json= resHandler.GetErrorResponseJSON(313,None)
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
               #apiParameters={"payment_request":payment_request,"payment_response":validateResp,"queueName":queueNM, "fileId":file_id,"parent_file_id":parent_file_id}                          
               apiParameters={"ro_number":document_id,"ro_status":latest_ro_status,"parent_file_id":parent_file_id,"queueName":queueNM,"total_amount_due":total_amount_due, "fileId":file_id}   
               #apiParameters=resHandler.ConvertJsonToString(resjson=param_json)
               resourcePath="PostPayment_Retry_StateMachine"
               if stage == 'test':
                 resourcePath="TEST_PostPayment_Retry_StateMachine"  
               logEvent={
                    "instance" :str(aws_account_id),
                    "app":"EPDE-STATE-MACHINE",
                    "resourcePath":resourcePath,   
                    "stage":stage,            
                    "apiName" :"PostPayment_Retry",                          
                    "storeCode":store_code,          
                    "status": status,
                    "requestTime":requestStart.strftime("%Y-%m-%dT%H:%M:%S%z"),  
                    "totalProcessingTimeMS": time_difference_in_milliseconds,                
                    "message": errorMessage,
                    "code": errorCode,
                    "responseCode":responseCode,                    
                    "responseSize": len(res_json.encode('utf-8')) ,
                    "apiType" :"Internal",                     
                    "extraInfo" :apiParameters           
               } 
               cwLogger= CloudWatchLogger()    
               cwLogger.DoCloudWatchLog('epde/stateMachineLog',logEvent)
          except:
               logger.error("error occured DoCloudWatchLog in lambda_handler",True)
    return resHandler.GetAPIResponse(res_json)        


            