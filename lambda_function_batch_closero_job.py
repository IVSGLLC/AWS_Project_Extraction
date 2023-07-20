
import datetime
from decimal import Decimal
import json
import boto3

from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient
from EPDE_BatchCloseRO import BatchCloseRO
from EPDE_SQS import SQSManager
from EPDE_PostPayment import PostManger
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
    #client_id=None
    store_code=""    
    try:
            ct = datetime.datetime.now()
            req_ts = ct.timestamp()
            _moduleNM="EPDE_Lambda"
            _functionNM="lambda_handler"
            err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)            
            app_client= AppClient()
            auth_json=None
            file_id=None              
            region=event['REGION']
            payment_request={"batchCloseRORequests":[],"autoCloseRO":True}   
            queueNM=event['REQUEST_QUEUE_NAME']
            #res_queueNM=event['RESPONSE_QUEUE_NAME']
            #requestWaitTImeOutInMin=int(event['REQUEST_TIMEOUT_IN_SECOND'] ) 
            msg_count=int(event['REQUEST_QUEUE_MAX_MSG_COUNT'])
            wait_time=int(event['REQUEST_QUEUE_MSG_WAIT_TIME_IN_SECOND'])
            #res_wait_time=int(event['RESPONSE_QUEUE_WAIT_TIME_IN_SECOND'])
            #res_msg_count=int(event['RESPONSE_QUEUE_MAX_MSG_COUNT'])
            storeCode=None
            if 'STORE_CODE' in event:
                storeCode=event['STORE_CODE']
            is_Test=False
            stage="prod"
            if "IS_TEST" in event:   
                test=str(event["IS_TEST"])
                if test.lower()=='true':
                    is_Test=True   
                    queueNM="TEST_"+ queueNM
                    #res_queueNM="TEST_"+res_queueNM 
                    stage="test"      
            tableperstore=1
            tableperstore_post=1           
            Is_WaitForResponse=False            
            batchclose_manager= BatchCloseRO(region,tableperstore_post) 
            post_manager=PostManger(region,tableperstore_post)   
            store_grp_resp=app_client.GetStoreGroups(region)
            if store_grp_resp['status']:
                store_groups=store_grp_resp['items']
                for group in store_groups:
                    group_id=group['group_id']
                    group_name=group['group_name']
                    stores_resp=None
                    alerts=group['alerts']
                    if not 'DAILY_BATCH_CLOSERO' in alerts:   
                          logger.debug("DAILY_BATCH_CLOSERO Alert Not configured for group_id:"+group_id)
                    else:
                        if not is_Test:
                            stores_resp=app_client.GetStores(group_id,region)
                             
                        else:
                            stores_resp=app_client.GetTestStores(group_id,region)    
                            
                        if stores_resp!=None and  stores_resp['status']== True:
                            if is_Test==True:
                                logger.debug("DAILY_BATCH_CLOSERO Alert start process for Test Stores")
                            else:
                                logger.debug("DAILY_BATCH_CLOSERO Alert start process for Prod Stores")
                           
                            stores=stores_resp['items']                             
                            for store in stores:
                                store_code=store['store_code']  
                                if storeCode is not None: 
                                    if store_code!=storeCode:                                        
                                        continue
                                if 'batchCloseRO' in store and store['batchCloseRO']==True:
                                    validateResp=batchclose_manager.Validate_BatchCloseRO_inputs(payment_request,auth_json,store_code,tableperstore)
                                    logger.debug("validateResp="+str(validateResp))
                                    if validateResp['responseCode']== 0:
                                        store_detail=validateResp['store_detail']  
                                        ro_detail_list=validateResp['ro_detail_list']
                                        validateResp['ro_detail'] =None
                                        pay_obj= batchclose_manager.buildBatchCloseROObject(validateResp,store_code,"IN_QUEUE",req_ts,req_ts,Is_WaitForResponse)         
                                        save_resp=batchclose_manager.SaveBatchCloseRO(pay_obj)               
                                        if save_resp['status']== True: 
                                            time_out=False
                                            #//request created...
                                            file_id=save_resp['file_id'] 

                                            #Loop each request in queue:
                                            sqs= SQSManager(QueueName= queueNM,region=region,MaxMsgCount=msg_count,waitTimeInSecond=wait_time)   
                                            
                                            batchCloseRORequestList=save_resp['batchclosero_request']
                                            #batchCloseRORequestList=batchCloseRORequestJSON['batchCloseRORequest']                        
                                            
                                            counter=0
                                            
                                            batchclosero_response_json=validateResp['batchclosero_response']
                                            batchCloseROResponseList=batchclosero_response_json['batchCloseROResponses']
                                            root_file_id=file_id
                                            res_json=batchclosero_response_json
                                            att={"fileId": root_file_id} 
                                            res_json.update(att)
                                            #for Loop 
                                            
                                            for batchCloseRORequest in batchCloseRORequestList:
                                                res_json_inner=""
                                                validateRespInner={'root_file_id':root_file_id}
                                                validateRespInner['payment_request']=batchCloseRORequest
                                                validateRespInner['payment_response']=batchCloseROResponseList[counter]
                                                validateRespInner['ro_detail']=ro_detail_list[counter]
                                                validateRespInner['store_detail']=store_detail
                                                counter=counter+1                           
                                                pay_obj= post_manager.buildPaymentObject(validateRespInner,store_code,"IN_QUEUE",req_ts,req_ts,Is_WaitForResponse)         
                                                save_resp_inner=post_manager.SavePostPayment(pay_obj)               
                                                if save_resp_inner['status']== True: 
                                                    time_out=False
                                                    #//request created...
                                                    file_id=save_resp_inner['file_id']                        
                                                    sqs= SQSManager(QueueName= queueNM,region=region,MaxMsgCount=msg_count,waitTimeInSecond=wait_time)   
                                                    logger.debug("SQS msg_body :"+str(save_resp_inner['payment_request']))
                                                    sqs_resp=sqs.send_message(msg_body=save_resp_inner['payment_request'],MessageGroupId=store_code,MessageDeduplicationId=file_id)
                                                    if sqs_resp['status'] == True:
                                                        
                                                        #//request sent to queue and not waiting for response.                                                            
                                                        res_json_inner=validateRespInner['payment_response'] 
                                                        att={"fileId": file_id} 
                                                        res_json_inner.update(att)
                                                        logger.debug("RESPONSE FOUND ...Response JSON:"+str(res_json_inner))
                                                        ro_detail=validateRespInner['ro_detail'] 
                                                        logger.debug("store_detail JSON:"+str(store_detail))
                                                        logger.debug("ro_detail JSON:"+str(ro_detail))
                                                        logger.debug("time_out:"+str(time_out))
                                                        if not time_out and post_manager.isRetryAllow(store_detail,ro_detail):
                                                            close_all_RO_flag=store_detail['close_all_RO_flag']
                                                            retry_wait_time_in_min=int(store_detail['retry_wait_interval_min'])
                                                            MaxRetryCount=int(store_detail['retry_max_count'])
                                                            # call step function
                                                            input={"request_queue_ms_wait_time_in_second":wait_time,"request_queue_max_msg_count":msg_count,"request_queue_name":queueNM,"close_all_RO_flag":close_all_RO_flag,"MaxRetryCount":MaxRetryCount,"isRetryActive":True,"fileId":save_resp_inner['file_id'],"store_code": store_detail['store_code'],"document_id":ro_detail['document_id'],"retry_wait_time_in_min":(retry_wait_time_in_min*60),"table_per_store_post_payment":tableperstore_post,"table_per_store_ro_detail":tableperstore,"region":region,"stage":stage}           
                                                            
                                                            client = boto3.client('stepfunctions')
                                                            executionname="PostPayment_Retry_StateMachine_"+str(save_resp_inner['file_id'])
                                                            step_arn='arn:aws:states:us-east-1:188506897258:stateMachine:PostPayment_Retry_StateMachine'
                                                            if stage == 'test':
                                                                executionname="TEST_PostPayment_Retry_StateMachine_"+str(save_resp_inner['file_id'])
                                                                step_arn='arn:aws:states:us-east-1:188506897258:stateMachine:TEST_PostPayment_Retry_StateMachine'
                                                            client.start_execution( stateMachineArn=step_arn,
                                                                                    name=executionname,
                                                                                    input= json.dumps(input,default=defaultencode)
                                                                                )  
                                                            logger.debug("after start stepfunction:")
                                                        else:
                                                            logger.info("Retry rejected: Request timeout/ Retry not activated..Store-Code:"+store_detail['store_code']+",file_id:"+file_id)   
                                                            #logger.info("Retry not activated for Store-Code:"+store_detail['store_code'])    
                                                    else:
                                                        logger.debug("Error while sending Payment Request in SQS...")
                                                        error_code=sqs_resp['error_code']
                                                        res_json_inner= resHandler.GetErrorResponseJSON(error_code,auth_json)
                                                        post_manager.UpdatePostPayment(store_code,save_resp_inner['file_id'],"NOT_POSTED",res_json_inner)                   
                                                        logger.error("Error while sending Payment Request in SQS...res_json_inner="+str(res_json_inner))    
                                                else:
                                                    logger.debug("Error while saving Payment Request in DB...")
                                                    error_code=save_resp_inner['error_code']
                                                    res_json_inner= resHandler.GetErrorResponseJSON(error_code,auth_json)
                                                    logger.error("Error while saving Payment Request in DB...res_json_inner="+str(res_json_inner))
                                            #End For Loop
                                        else:
                                            logger.debug("Error while saving BatchCloseRO Request in DB...")
                                            error_code=save_resp['error_code']
                                            res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)

                                    else:
                                        logger.debug("Invalid BatchCloseRO Request...")
                                        validateResp1={}
                                        validateResp1['batchclosero_response']=validateResp
                                        validateResp1['batchclosero_request']=payment_request
                                        pay_obj= batchclose_manager.buildBatchCloseROObject(validateResp1,store_code,"NOT_POSTED",req_ts,req_ts,Is_WaitForResponse=Is_WaitForResponse)
                                        save_resp=batchclose_manager.SaveBatchCloseRO(pay_obj) 
                                        if save_resp['status']== True: 
                                            file_id=save_resp['file_id'] 
                                            att={"fileId": file_id} 
                                            validateResp1.update(att)                             
                                        res_json= validateResp1
                                       
                                else:
                                     logger.debug("DAILY_BATCH_CLOSERO Job Not configured for storeCode:"+store_code)
                            logger.debug("res_json="+str(res_json))
                        else:
                           logger.debug("DAILY_BATCH_CLOSERO Job No Store found to process for group_id:"+group_id)
            #return resHandler.GetAPIResponse(res_json)
    except Exception as e:
             #save not posted request 
            logger.error("error occured in lambda_handler",True)
            res_json= resHandler.GetErrorResponseJSON(313,auth_json)
            if file_id is None:
                SaveErrorInDB(res_json,payment_request,req_ts,req_ts,store_code,region,tableperstore_post,Is_WaitForResponse)
                res_json=validateResp['batchclosero_response']
            else:
                batchclose_manager.UpdateBatchCloseRO(store_code,save_resp['file_id'],"NOT_POSTED",res_json)                   
            #return resHandler.GetAPIResponse(res_json)

def  SaveErrorInDB(res_json,payment_request,req_ts,res_ts,store_code,region,tableperstore_post,Is_WaitForResponse):
                batchCloseROMgr= BatchCloseRO(region,tableperstore_post)   
                validateResp={}
                validateResp['batchclosero_response']=res_json
                validateResp['batchclosero_request']=payment_request
                pay_obj= batchCloseROMgr.buildBatchCloseROObject(validateResp,store_code,"NOT_POSTED",req_ts,res_ts,Is_WaitForResponse)         
                batchCloseROMgr.SaveBatchCloseRO(pay_obj)  
            