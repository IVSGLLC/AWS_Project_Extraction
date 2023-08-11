import datetime
import json
import sys
import pytz
from EPDE_CloudWatchLog import CloudWatchLogger
from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient
from EPDE_PostPayment import PostManger
from EPDE_SQS import SQSManager
from EPDE_Deposits import DepositsManager
from EPDE_PostAccounting import AccountingManager
logger=LogManger()
err_handler=ErrorHandler()
def lambda_handler(event, context):
    res_json=""
    resHandler=ResponseHandler()
    cwLogger= CloudWatchLogger()
    dealers=[]
    apiList=[]
    groups=[]
    dealerCode=''
    reqAPI=''
    queue=''
    dealerGroup=''
    status='SUCCESS'
    msg=''
    allowMsgList=[]
    queueNm=''
    totalMessageCount=0
    time_difference_in_milliseconds=0
    try:      
            eastern=pytz.timezone('US/Eastern')           
            _moduleNM="EPDE_Lambda"
            _functionNM="lambda_handler"
            err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)  
            try:
                if 'queryStringParameters' in event:
                    queryStringParameters=event['queryStringParameters']
                    if 'dealerCode' in queryStringParameters:
                        dealerCode=queryStringParameters['dealerCode']
                        logger.debug("dealerCode="+str(dealerCode))
                    if 'api' in queryStringParameters:
                        reqAPI=queryStringParameters['api']
                        logger.debug("reqAPI="+str(reqAPI))
                    if 'queue' in queryStringParameters:
                        queue=queryStringParameters['queue']
                        logger.debug("queue="+str(queue))
                    if 'dealerGroup' in queryStringParameters:
                        dealerGroup=queryStringParameters['dealerGroup']
                        logger.debug("dealerGroup="+str(dealerGroup))

            except Exception as e:
                dealerCode=''
                reqAPI=''
                dealerGroup=''
                status='ERROR'
                msg='queryStringParameters error'
          

            if len(dealerCode)>0 and  dealerCode.__contains__(","):
                dealers=dealerCode.split(',')
            else:
                 if len(dealerCode)>0 :
                    dealers.append(dealerCode)
            if len(reqAPI)>0 and  reqAPI.__contains__(","):
                apiList=reqAPI.split(',')
            else:
                 if len(reqAPI)>0 :
                    apiList.append(reqAPI)
            if len(dealerGroup)>0 and  dealerGroup.__contains__(","):
                groups=dealerGroup.split(',')
            else:
                 if len(dealerGroup)>0 :
                    groups.append(dealerGroup)
            access_token=None
            try:
                Authorization=event['headers']['Authorization']         
                auth_token=Authorization.split(" ")
                token_type=auth_token[0]  
                if str(token_type).lower() !="bearer":
                    access_token=None
                else:
                    access_token=auth_token[1] 
            except:
                access_token=None
                status='ERROR'
                msg='access_token error'
            if access_token is not None:  
                    app_client=AppClient()     
                    config=app_client.GetConfiguration(event)
                    maintenanace_resp=app_client.isMaintenanceTime(config)
                    if maintenanace_resp['status']:  
                        res_json= maintenanace_resp['resjson'] 
                        logger.info("Maintenance Time is running API call not allowed...")
                        return resHandler.GetAPIResponse(res_json)
                    queueNm=config['REQUEST_QUEUE_NAME']
                    if queue is not None and len(queue )>0:
                        if queue=='deposit':
                           queueNm='EPDE_PostDeposit_Request.fifo'
                        elif queue=='accounting':
                           queueNm='EPDE_PostAccounting_Request.fifo'
                        elif queue=='extract':
                            queueNm='EPDE_Extract_Response.fifo'
                        elif queue=='autoupdate':
                            queueNm='EPDE_Autoupdate.fifo'                        
                    stage=event['requestContext']['stage']
                    if stage == 'test':
                       queueNm="TEST_"+ queueNm
                    if queue=='extract':
                       queueNm='EPDE_Extract_Response.fifo'

                    logger.debug("queueNm="+str(queueNm))
                    msg_count=int(config['REQUEST_QUEUE_MAX_MSG_COUNT'])
                    wait_time=int(config['REQUEST_QUEUE_MSG_WAIT_TIME_IN_SECOND'])
                    region=config['REGION']
                    table_Per_Store_Post_Payment=1
                    post_manager=PostManger(region,table_Per_Store_Post_Payment=table_Per_Store_Post_Payment)
                    table_Per_Store_Post_Deposit=1
                    postdeposit_manager=DepositsManager(region,table_Per_Store_Post_Deposit)
                    table_Per_Store_Post_Accounting=1
                    postaccounting_manager=AccountingManager(region,table_Per_Store_Post_Accounting)
                    downloadStart = datetime.now().astimezone(eastern)
                    sqs= SQSManager(queueNm,region,msg_count,wait_time)                  
                    sqs_resp= sqs.receive_message()                              
                    time_difference=datetime.now().astimezone(eastern)-downloadStart
                    time_difference_in_milliseconds = int(time_difference.total_seconds() * 1000)
                    if sqs_resp['status']:
                        messages=sqs_resp['messages']
                        logger.debug("messages="+str(messages))
                        totalMessageCount=len(messages)
                        for message in messages:
                            logger.debug("message="+str(message))
                            message_body = message["Body"]
                            #store_code=message['Attributes']['MessageGroupId']
                            logger.debug("message_body="+str(message_body))
                            receipt_handle = message['ReceiptHandle']
                            request_json=json.loads(message_body)
                            store_code=request_json['dealerCode']
                            fileId=request_json['fileId']
                            api=request_json['api']
                            grp=''
                            if 'group' in request_json and request_json['group'] is not None :
                                grp=request_json['group']
                            AllowRead=False
                            if  len(groups)>0:
                                logger.debug("groups="+str(groups))
                                if groups.__contains__(grp): 
                                    if len(dealers)>0:                                   
                                        if dealers.__contains__(store_code):
                                            if len(apiList)>0:
                                                logger.debug("apiList="+str(apiList))
                                                if apiList.__contains__(api): 
                                                    AllowRead=True
                                            else:
                                                AllowRead=True
                                    else:
                                        AllowRead=True
                            elif len(dealers)>0:
                                logger.debug("dealers="+str(dealers))
                                if dealers.__contains__(store_code):
                                    if len(apiList)>0:
                                        logger.debug("apiList="+str(apiList))
                                        if apiList.__contains__(api): 
                                            AllowRead=True
                                    else:
                                        AllowRead=True
                            elif len(apiList)>0:
                                logger.debug("apiList="+str(apiList))
                                if apiList.__contains__(api): 
                                    AllowRead=True                             
                            else:
                                AllowRead=True
                            logger.debug("AllowRead="+str(AllowRead))                         
                            if AllowRead : 
                                if api=='postpayments':
                                   fileName=store_code+"_postpaymentsRequest_"+fileId+".json" 
                                   FileDetail={"dealerCode":store_code,"data":message_body,"fileName":fileName}
                                   allowMsgList.append(FileDetail)
                                   sqs.delete_message(receipt_handle)                                
                                   post_manager.UpdatePostPaymentRequestStatus(store_code,fileId,"REQUEST_DOWNLOADED")
               
                                elif api=='postdeposits':   
                                   fileName=store_code+"_postdepositsRequest_"+fileId+".json"  
                                   FileDetail={"dealerCode":store_code,"data":message_body,"fileName":fileName}
                                   allowMsgList.append(FileDetail)
                                   sqs.delete_message(receipt_handle)                                
                                   postdeposit_manager.UpdatePostDepositsRequestStatus(store_code,fileId,"REQUEST_DOWNLOADED")
               
                                elif api=='postaccounting':   
                                   fileName=store_code+"_postaccountingRequest_"+fileId+".json"  
                                   FileDetail={"dealerCode":store_code,"data":message_body,"fileName":fileName}
                                   allowMsgList.append(FileDetail)
                                   sqs.delete_message(receipt_handle)                                
                                   postaccounting_manager.UpdatePostAccountingRequestStatus(store_code,fileId,"REQUEST_DOWNLOADED")                        
                                elif api=='dataextraction':                                     
                                    fileName=fileId
                                    FileDetail={"dealerCode":store_code,"data":message_body,"fileName":fileName}
                                    allowMsgList.append(FileDetail) 
                                    sqs.delete_message(receipt_handle)
                                elif api=='autoupdate':                                    
                                    fileName=fileId
                                    FileDetail={"dealerCode":store_code,"data":message_body,"fileName":fileName}
                                    allowMsgList.append(FileDetail) 
                                    sqs.delete_message(receipt_handle)
                    else:
                        status='ERROR'
                        msg='receiving queue message error'
                        #error in receiving queue message
                        ""
                    if len(allowMsgList)>0:
                        status='SUCCESS'
                        msg=str(len(allowMsgList))+" message download"
                        if msg_count == 1:
                             res_json=allowMsgList[0]
                        else:                            
                            res_json=allowMsgList
                    else:
                        if status!='ERROR':
                            msg="0 message download"
                        res_json=""

                    
            else:
                 logger.debug("API Key not found...")  
                 status='ERROR'
                 msg='API Key error'
                 res_json= resHandler.GetErrorResponseJSON(331,None)
            logger.debug("Download response="+str(res_json))            
    except Exception as e:
            ex_type, ex_value, ex_traceback = sys.exc_info()
            status='ERROR'
            logger.error("error occured in lambda_handler",True)
            msg=str(ex_value)
            res_json= resHandler.GetErrorResponseJSON(313,None)
    aws_account_id = context.invoked_function_arn.split(":")[4]        
    logEvent={
        
                "instance" :str(aws_account_id),
                "sourceIP":event["identity"]["sourceIp"],
                "reqApi" :reqAPI,
                "reqDealerCode":dealerCode,
                "reqDealerGroup":dealerGroup,
                "reqQueue": queue,
                "queueName": queueNm,
                "stage":event['requestContext']['stage'],
                "totalMessageCount": totalMessageCount,
                "status": status,
                "receiveMessageDateTime":downloadStart.strftime("%Y-%m-%dT%H:%M:%S%z") ,
                "receiveMessageDurationMS": time_difference_in_milliseconds,
                "downloadedMessageCount":len(allowMsgList),
                "message": msg  
            }     
    cwLogger.DoCloudWatchLog('epde/downloadLog',logEvent)       
    return resHandler.GetAPIResponse(res_json)
        
