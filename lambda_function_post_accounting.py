
import datetime
from decimal import Decimal
import json
import boto3
from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient
from EPDE_PostAccounting import AccountingManager
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
            _moduleNM="EPDE_Lambda_PostAccounting"
            _functionNM="lambda_handler"
            err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)
            requestBody_str=event['body'] 
            accounting_request = json.loads(requestBody_str)
            app_client= AppClient()
            auth_json=None
            file_id=None    
            config=app_client.GetConfiguration(event)
            queueNM=config['REQUEST_QUEUE_NAME']
            #queueNM='EPDE_PostAccounting_Request.fifo'
            #res_queueNM=config['RESPONSE_QUEUE_NAME']
            stage=event['requestContext']['stage']
            if stage == 'test':
                queueNM="TEST_"+ queueNM
                #res_queueNM="TEST_"+res_queueNM
            region=config['REGION']
            tableperstore_post_accounting=1
            logger.debug("tableperstore_post_accounting..."+str(tableperstore_post_accounting))
            #wForResp=str(config['WAIT_FOR_ACCOUNTING_RESPONSE']  )
            Is_WaitForResponse=False
            #if wForResp.lower()=='true':
                #Is_WaitForResponse=True
            logger.debug("Is_WaitForResponse="+str(Is_WaitForResponse)) 
            #requestWaitTImeOutInMin=int(config['REQUEST_TIMEOUT_IN_SECOND'] ) 
            msg_count=int(config['REQUEST_QUEUE_MAX_MSG_COUNT'])
            wait_time=int(config['REQUEST_QUEUE_MSG_WAIT_TIME_IN_SECOND'])
            #res_wait_time=int(config['RESPONSE_QUEUE_WAIT_TIME_IN_SECOND'])
            #res_msg_count=int(config['RESPONSE_QUEUE_MAX_MSG_COUNT'])
            post_manager= AccountingManager(region=region,table_Per_Store=tableperstore_post_accounting)       
            auth_detail_resp=app_client.GetAuthenticationDetail(event) 
            if auth_detail_resp['status']== True:
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
                  SaveErrorInDB(res_json,accounting_request,req_ts,req_ts,store_code,region,tableperstore_post_accounting,Is_WaitForResponse)       
                  return resHandler.GetAPIResponse(res_json) 
                                 
                               
               validateResp=post_manager.Validate_PostAccounting_inputs(accounting_request,auth_json,store_code)
               if validateResp['responseCode']== 0:
                    store_detail=validateResp['store_detail']  
                    accounting_obj= post_manager.buildAccountingObject(validateResp,store_code,"IN_QUEUE",req_ts,req_ts,Is_WaitForResponse)         
                    
                    save_resp=post_manager.SavePostAccounting(accounting_obj)
                    if save_resp['status']== True: 
                        time_out=False
                        file_id=save_resp['file_id']                        
                        sqs= SQSManager(QueueName= queueNM,region=region,MaxMsgCount=msg_count,waitTimeInSecond=wait_time)   
                        sqs_resp=sqs.send_message(msg_body=save_resp['accounting_request'],MessageGroupId=store_code,MessageDeduplicationId=file_id)
                        if sqs_resp['status'] == True:
                            
                            """ if Is_WaitForResponse:  
                                pollres_json=post_manager.PollPostAccountingResponse(store_code,save_resp['file_id'],auth_json,requestWaitTImeOutInMin,res_queueNM,region,res_msg_count,res_wait_time)  
                                                            
                                #pollres_json=post_manager.PollPostAccountingResponse(store_code,save_resp['file_id'],auth_json,requestWaitTImeOutInMin) 
                                res_json=pollres_json['response_json']  
                                att={"fileId": file_id} 
                                res_json.update(att)
                                #update response if DMS timeout or error
                                if pollres_json['request_status']!="RESPONSE_UPLOADED":
                                   post_manager.UpdatePostDepositsTimeOut(store_code,save_resp['file_id'],pollres_json['request_status'],res_json)   
                                if pollres_json['request_status'] == "TIMEOUT":
                                   time_out=True                             
                            else:      """                                                          
                            res_json=validateResp['accounting_response'] 
                            att={"fileId": file_id} 
                            res_json.update(att)
                            logger.debug("RESPONSE FOUND ...Response JSON:"+str(res_json))
                            logger.debug("store_detail JSON:"+str(store_detail))
                            logger.debug("time_out:"+str(time_out))
                            if not time_out and post_manager.isRetryAllow(store_detail):
                                retry_wait_time_in_min=int(store_detail['retry_wait_interval_min_accounting'])
                                MaxRetryCount=int(store_detail['retry_max_count_accounting'])
                                # call step function
                                input={"request_queue_ms_wait_time_in_second":wait_time,"request_queue_max_msg_count":msg_count,"request_queue_name":queueNM,"MaxRetryCount":MaxRetryCount,"isRetryActive":True,"fileId":save_resp['file_id'],"client_id": store_detail['client_id'],"retry_wait_time_in_min":(retry_wait_time_in_min*60),"table_per_store_post_accounting":tableperstore_post_accounting,"region":region}           
                                
                                client = boto3.client('stepfunctions')
                                executionname="PostAccounting_Retry_StateMachine_"+str(save_resp['file_id'])
                                client.start_execution( stateMachineArn='arn:aws:states:us-east-1:188506897258:stateMachine:PostAccounting_Retry_StateMachine',
                                                        name=executionname,
                                                        input= json.dumps(input,default=defaultencode)
                                                    )  
                            else:

                                logger.debug("Post Accounting Retry not activated for Store-Code:"+store_detail['store_code'])    
                        else:
                            logger.debug("Error while sending Post Accounting Request in SQS...")
                            error_code=sqs_resp['error_code']
                            res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)
                            post_manager.UpdatePostAccounting(store_code,save_resp['file_id'],"NOT_POSTED",res_json)                   
                                 
                    else:
                        logger.debug("Error while saving Post Accounting Request in DB...")
                        error_code=save_resp['error_code']
                        res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)

               else:
                    logger.debug("Invalid Post Accounting Request...")
                    validateResp1={}
                    validateResp1['accounting_response']=validateResp
                    validateResp1['accounting_request']=accounting_request
                    accounting_obj= post_manager.buildAccountingObject(validateResp1,store_code,"NOT_POSTED",req_ts,req_ts,Is_WaitForResponse=Is_WaitForResponse)
                    save_resp=post_manager.SavePostAccounting(accounting_obj)  
                    if save_resp['status']== True: 
                        file_id=save_resp['file_id'] 
                        att={"fileId": file_id} 
                        validateResp.update(att)                             
                    res_json= validateResp                        
                     
            else:
                logger.debug("Auth detail not found...")
                auth_json=auth_detail_resp['auth_json']
                error_code=auth_detail_resp['error_code']
                res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)
                SaveErrorInDB(res_json,accounting_request,req_ts,req_ts,store_code,region,tableperstore_post_accounting,Is_WaitForResponse)       
                
            return resHandler.GetAPIResponse(res_json)
    except Exception as e:
             #save not posted request 
            logger.error("error occured in lambda_handler_postaccounting",True)
            res_json= resHandler.GetErrorResponseJSON(313,auth_json)
            if file_id is None:
                SaveErrorInDB(res_json,accounting_request,req_ts,req_ts,store_code,region,tableperstore_post_accounting,Is_WaitForResponse)
                res_json=validateResp['deposit_response']
            else:
                post_manager.UpdatePostDeposits(store_code,save_resp['file_id'],"NOT_POSTED",res_json)                   
            return resHandler.GetAPIResponse(res_json)

def  SaveErrorInDB(res_json,accounting_request,req_ts,res_ts,store_code,region,tableperstore_post_accounting,Is_WaitForResponse):
                post_manager= AccountingManager(region,tableperstore_post_accounting)   
                validateResp={}
                validateResp['accounting_response']=res_json
                validateResp['accounting_request']=accounting_request
                accouting_obj= post_manager.buildAccountingObject(validateResp,store_code,"NOT_POSTED",req_ts,res_ts,Is_WaitForResponse)         
                post_manager.SavePostAccounting(accouting_obj)  
            