
import datetime
from decimal import Decimal
import json
import boto3
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
    try:
       logger.debug("PostPaymentRetry>> event="+str(event))
       close_all_RO_flag=event['close_all_RO_flag']
       retry_wait_time_in_min=int(event['retry_wait_time_in_min'])
       MaxRetryCount= int(event['MaxRetryCount'])
       isRetryActive=event['isRetryActive']
       fileId=event['fileId']        
       client_id=event['client_id']     
       document_id= event['document_id'] 
       region=event['region']
       tableperstore=int(event['table_per_store_ro_detail'])
       tableperstore_post=int(event['table_per_store_post_payment'])
       queueNm=event['request_queue_name']
       msg_count=int(event['request_queue_max_msg_count'])
       wait_time=int(event['request_queue_ms_wait_time_in_second'])
       rOrder=RepairOrder(region,tableperstore)
       roDetailResp=rOrder.GetRODetail(client_id,document_id,'RO')  
       if roDetailResp['status']:
          item =roDetailResp['item']
          warranty_due=item['warranty_due']
          try:
              warranty_due_ft=float(warranty_due)
          except:
              warranty_due_ft=0.0
          latest_ro_status=item['status']
          post_manager= PostManger(region,tableperstore_post)             
          if (close_all_RO_flag  or warranty_due_ft==0) and latest_ro_status.upper() =='C97':                
             ct = datetime.datetime.now()
             req_ts = ct.timestamp()
             postpayentry=post_manager.GetPostPaymentEntry(client_id,fileId)
             if postpayentry['status']:
                paymentEntry=postpayentry['item']
                if paymentEntry['request_status'] =='REQUEST_DOWNLOADED':
                    paymentEntry['retry_count']=paymentEntry['retry_count']+1
                    post_manager.UpdatePostPaymentRetryCount(client_id,fileId,paymentEntry['retry_count'],latest_ro_status)
                    ct = datetime.datetime.now()
                    req_ts = ct.timestamp()
                    retry_paymentEntry=paymentEntry
                    retry_paymentEntry['ro_status']=str(latest_ro_status)
                    retry_paymentEntry['response_json']=None
                    retry_paymentEntry['req_ts']=str(req_ts)
                    retry_paymentEntry['res_ts']=str(req_ts)
                    retry_paymentEntry['parent_file_id']=fileId
                    retry_paymentEntry['file_id']=None
                    save_resp= post_manager.SavePostPayment(retry_paymentEntry)
                    if save_resp['status']== True: 
                            
                            sqs= SQSManager(queueNm,region,msg_count,wait_time)                  
                            sqs_resp=sqs.send_message(msg_body=save_resp['payment_request'],MessageGroupId=client_id)
                            if sqs_resp['status'] == True:
                                
                                if isRetryActive and (paymentEntry['retry_count']< MaxRetryCount):
                                    # call step function
                                    input={"request_queue_ms_wait_time_in_second":wait_time,"request_queue_max_msg_count":msg_count,"request_queue_name":queueNm,"close_all_RO_flag":close_all_RO_flag,"MaxRetryCount":MaxRetryCount,"isRetryActive":True,"fileId":save_resp['file_id'],"client_id": client_id,"document_id":document_id,"retry_wait_time_in_min":(retry_wait_time_in_min),"table_per_store_post_payment":tableperstore_post,"table_per_store_ro_detail":tableperstore,"region":region}           
                                    client = boto3.client('stepfunctions')
                                    executionname="PostPayment_Retry_StateMachine_"+str(save_resp['file_id'])
                                    client.start_execution( stateMachineArn='arn:aws:states:us-east-1:188506897258:stateMachine:PostPayment_Retry_StateMachine',
                                                            name=executionname,
                                                            input= json.dumps(input,default=defaultencode)
                                                        ) 
                                else:
                                    logger.debug("Max Retry count reached.")
                            else:
                                logger.debug("Error while sending Payment Request in SQS...")
                                error_code=sqs_resp['error_code']
                                res_json= resHandler.GetErrorResponseJSON(error_code,None)  
                                post_manager.UpdatePostPayment(client_id,save_resp['file_id'],"NOT_POSTED",res_json)                   
                                
                    else:
                        logger.debug("Error while saving retry Post Payment Request in DB...") 
                        res_json= resHandler.GetErrorResponseJSON(save_resp['error_code'],None)  
                else:
                    # call step function
                    input={"request_queue_ms_wait_time_in_second":wait_time,"request_queue_max_msg_count":msg_count,"request_queue_name":queueNm,"close_all_RO_flag":close_all_RO_flag,"MaxRetryCount":MaxRetryCount,"isRetryActive":True,"fileId":fileId,"client_id": client_id,"document_id":document_id,"retry_wait_time_in_min":(retry_wait_time_in_min*60),"table_per_store_post_payment":tableperstore_post,"table_per_store_ro_detail":tableperstore,"region":region}           
                    client = boto3.client('stepfunctions')
                    executionname="PostPayment_Retry_StateMachine_"+str(fileId)
                    client.start_execution( stateMachineArn='arn:aws:states:us-east-1:188506897258:stateMachine:PostPayment_Retry_StateMachine',
                                                        name=executionname,
                                                        input= json.dumps(input,default=defaultencode)
                                                    )  
                    logger.debug("Request still in IN_QUEUE,re-start stepfunction:")
                                  
             else:
                logger.debug("Post Payment Request Not exist in DB...file_id:"+fileId) 
                res_json= resHandler.GetErrorResponseJSON(postpayentry['error_code'],None)                   
          else:
               logger.debug("RO not ready or Retry not required or retry criteria not matched...")  
               res_json= resHandler.GetErrorResponseJSON(342,None)                 
       else:
           logger.debug("RO not Found in DB......")  
           res_json= resHandler.GetErrorResponseJSON(roDetailResp['error_code'],None)

    except Exception as e:
            #save not posted request 
            logger.error("error occured in retry_lambda_handler",True)
            res_json= resHandler.GetErrorResponseJSON(313,None)
    
    return resHandler.GetAPIResponse(res_json)        


            