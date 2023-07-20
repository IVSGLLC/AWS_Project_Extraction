
import datetime
from decimal import Decimal
import json
import boto3

from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient
from EPDE_BatchCloseRO import BatchCloseRO
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
    #client_id=None
    store_code=""
    Is_WaitForResponse=False
    try:
            ct = datetime.datetime.now()
            req_ts = ct.timestamp()
            _moduleNM="EPDE_Lambda"
            _functionNM="lambda_handler"
            err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)            
            app_client= AppClient()
            auth_json=None
            file_id=None              
            config=app_client.GetConfiguration(event)
            requestBody_str=event['body'] 
            payment_request = json.loads(requestBody_str)           
            queueNM=config['REQUEST_QUEUE_NAME']
            #res_queueNM=config['RESPONSE_QUEUE_NAME']
            stage=event['requestContext']['stage']

            if stage == 'test':
                queueNM="TEST_"+ queueNM
                #res_queueNM="TEST_"+res_queueNM
           
            region=config['REGION']
            tableperstore=1
            tableperstore_post=1           
            Is_WaitForResponse=False
             
            #requestWaitTImeOutInMin=int(config['REQUEST_TIMEOUT_IN_SECOND'] ) 
            msg_count=int(config['REQUEST_QUEUE_MAX_MSG_COUNT'])
            wait_time=int(config['REQUEST_QUEUE_MSG_WAIT_TIME_IN_SECOND'])
            #res_wait_time=int(config['RESPONSE_QUEUE_WAIT_TIME_IN_SECOND'])
            #res_msg_count=int(config['RESPONSE_QUEUE_MAX_MSG_COUNT'])


            batchclose_manager= BatchCloseRO(region,tableperstore_post)  
            post_manager=PostManger(region,tableperstore_post)  
            auth_detail_resp=app_client.GetAuthenticationDetail(event) 
            if auth_detail_resp['status']== False:
            
                logger.debug("Auth detail not found...")
                auth_json=auth_detail_resp['auth_json']
                error_code=auth_detail_resp['error_code']
                res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)
                SaveErrorInDB(res_json,payment_request,req_ts,req_ts,store_code,region,tableperstore_post,Is_WaitForResponse)       
            else:
               
                auth_json=auth_detail_resp['auth_json'] 
                validate_resp=app_client.ValidateStoreDetail(region,event)
                if validate_resp['status']==False: 
                        logger.debug("STORE NOT FOUND...")
                        error_code=validate_resp['error_code']
                        res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)   
                        return resHandler.GetAPIResponse(res_json) 
                else:
                        if 'store_code' in validate_resp:
                            store_code=validate_resp['store_code']   
                #client_id=auth_detail_resp['client_id']  
                maintenanace_resp=app_client.isMaintenanceTime(config,auth_json)
                if maintenanace_resp['status']:  
                    res_json= maintenanace_resp['resjson'] 
                    logger.debug("Maintenance Time is running Posting not allowed...")

                    SaveErrorInDB(res_json,payment_request,req_ts,req_ts,store_code,region,tableperstore_post,Is_WaitForResponse)       
                    return resHandler.GetAPIResponse(res_json) 
            
                               
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
                
            return resHandler.GetAPIResponse(res_json)
    except Exception as e:
             #save not posted request 
            logger.error("error occured in lambda_handler",True)
            res_json= resHandler.GetErrorResponseJSON(313,auth_json)
            if file_id is None:
                SaveErrorInDB(res_json,payment_request,req_ts,req_ts,store_code,region,tableperstore_post,Is_WaitForResponse)
                res_json=validateResp['batchclosero_response']
            else:
                batchclose_manager.UpdateBatchCloseRO(store_code,save_resp['file_id'],"NOT_POSTED",res_json)                   
            return resHandler.GetAPIResponse(res_json)

def  SaveErrorInDB(res_json,payment_request,req_ts,res_ts,store_code,region,tableperstore_post,Is_WaitForResponse):
                batchCloseROMgr= BatchCloseRO(region,tableperstore_post)   
                validateResp={}
                validateResp['batchclosero_response']=res_json
                validateResp['batchclosero_request']=payment_request
                pay_obj= batchCloseROMgr.buildBatchCloseROObject(validateResp,store_code,"NOT_POSTED",req_ts,res_ts,Is_WaitForResponse)         
                batchCloseROMgr.SaveBatchCloseRO(pay_obj)  
            