
import datetime
from decimal import Decimal
import json
import boto3
from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_RO import RepairOrder
from EPDE_Response import ResponseHandler
from EPDE_BatchCloseRO import BatchCloseRO
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
    try:
        logger.debug("BatchCloseRORetry>> event="+str(event))
        close_all_RO_flag=event['close_all_RO_flag']
        retry_wait_time_in_min=int(event['retry_wait_time_in_min'])
        MaxRetryCount= int(event['MaxRetryCount'])
        isRetryActive=event['isRetryActive']
        fileId=event['fileId']        
        store_code=event['store_code']     
        document_id= event['document_id'] 
        region=event['region']
        tableperstore=1
        tableperstore_batchclosero=1
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
            post_manager= BatchCloseRO(region,tableperstore_batchclosero)             
            if (close_all_RO_flag  or warranty_due_ft==0) and latest_ro_status.upper() =='C97':                
                ct = datetime.datetime.now()
                req_ts = ct.timestamp()
                batchcloseroentry=post_manager.GetBatchCloseROEntry(store_code,fileId)
                if batchcloseroentry['status']:
                    bcroEntry=batchcloseroentry['item']
                    #if paymentEntry['request_status'] =='REQUEST_DOWNLOADED' or paymentEntry['request_status'] =='RESPONSE_UPLOADED' :
                    if True:
                        request_status=bcroEntry['request_status']
                        bcroEntry['retry_count']=bcroEntry['retry_count']+1
                        if request_status=='IN_QUEUE':
                            response_json=bcroEntry['response_json']
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
                            post_manager.UpdateBatchCloseRORetryCountRequestStatusTimeOut(store_code,fileId,bcroEntry['retry_count'],latest_ro_status,"TIMEOUT",response_json)
                                                  
                        else:                           
                            post_manager.UpdateBatchCloseRORetryCount(store_code,fileId,bcroEntry['retry_count'],latest_ro_status)
                        ct = datetime.datetime.now()
                        req_ts = ct.timestamp()
                        retry_paymentEntry=bcroEntry
                        retry_paymentEntry['ro_status']=str(latest_ro_status)
                        retry_paymentEntry['request_status']='IN_QUEUE'
                        retry_paymentEntry['response_json']=None
                        retry_paymentEntry['req_time']=str(req_ts)
                        retry_paymentEntry['ro_status_on']=str(req_ts)                        
                        retry_paymentEntry['res_time']=str(req_ts)
                        retry_paymentEntry['parent_file_id']=fileId
                        retry_paymentEntry['file_id']=None
                       
                        
                        try:                       
                            retry_paymentEntry['is_spc_ins']="N"
                            if spc_ins != None and len(spc_ins)>0:         
                               retry_paymentEntry['is_spc_ins']="Y"           
                        except:
                            retry_paymentEntry['is_spc_ins']="N"

                        save_resp= post_manager.SaveBatchCloseRO(retry_paymentEntry)
                        if save_resp['status']== True: 
                                
                                sqs= SQSManager(queueNm,region,msg_count,wait_time)   
                                logger.debug("SQS msg_body :"+str(save_resp['batchclosero_request']))               
                                sqs_resp=sqs.send_message(msg_body=save_resp['batchclosero_request'],MessageGroupId=store_code)
                                if sqs_resp['status'] == True:
                                    
                                    if isRetryActive and (bcroEntry['retry_count']< MaxRetryCount):
                                        # call step function
                                        input={"request_queue_ms_wait_time_in_second":wait_time,"request_queue_max_msg_count":msg_count,"request_queue_name":queueNm,"close_all_RO_flag":close_all_RO_flag,"MaxRetryCount":MaxRetryCount,"isRetryActive":True,"fileId":fileId,"store_code": store_code,"document_id":document_id,"retry_wait_time_in_min":(retry_wait_time_in_min),"table_per_store_batchclosero":tableperstore_batchclosero,"table_per_store_ro_detail":tableperstore,"region":region,"stage":stage}           
                                        client = boto3.client('stepfunctions')
                                        executionname="BatchCloseRO_Retry_StateMachine_"+str(save_resp['file_id'])
                                        step_arn='arn:aws:states:us-east-1:188506897258:stateMachine:BatchCloseRO_Retry_StateMachine'
                                        if stage == 'test':
                                                executionname="TEST_BatchCloseRO_Retry_StateMachine_"+str(save_resp['file_id'])
                                                step_arn='arn:aws:states:us-east-1:188506897258:stateMachine:TEST_BatchCloseRO_Retry_StateMachine'
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
                                    post_manager.UpdateBatchCloseRO(store_code,save_resp['file_id'],"NOT_POSTED",res_json)                   
                                    
                        else:
                            logger.info("Error while saving retry Post Payment Request in DB...") 
                            res_json= resHandler.GetErrorResponseJSON(save_resp['error_code'],None)
                    else:
                        logger.info("Retry rejected: Request timeout/ In_Queue status...file_id:"+fileId)              
                else:
                    logger.info("Post Payment Request Not exist in DB...file_id:"+fileId) 
                    res_json= resHandler.GetErrorResponseJSON(batchcloseroentry['error_code'],None)                   
            else:
               logger.info("Retry rejected: RO not ready or Retry not required or retry criteria not matched...")  
               batchcloseroentry=post_manager.GetBatchCloseROEntry(store_code,fileId)
               if batchcloseroentry['status']:
                    bcroEntry=batchcloseroentry['item']
                    request_status=bcroEntry['request_status']
                    if request_status=='IN_QUEUE':
                        response_json=bcroEntry['response_json']
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
                        post_manager.UpdateBatchCloseRORetryCountRequestStatusTimeOut(store_code,fileId,bcroEntry['retry_count'],latest_ro_status,"TIMEOUT",response_json)
                    
                    else:   
                        post_manager.UpdateBatchCloseRORetryCount(store_code,fileId,bcroEntry['retry_count'],latest_ro_status)
               res_json= resHandler.GetErrorResponseJSON(342,None)                 
        else:
           logger.info("RO not Found in DB......")  
           res_json= resHandler.GetErrorResponseJSON(roDetailResp['error_code'],None)

    except Exception as e:
            #save not posted request 
            logger.error("error occured in retry_lambda_handler",True)
            res_json= resHandler.GetErrorResponseJSON(313,None)
    
    return resHandler.GetAPIResponse(res_json)        


            