import datetime
import json
import time

import pytz
from EPDE_CloudWatchLog import CloudWatchLogger
from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient
import boto3

from EPDE_S3 import S3Manager 
logger=LogManger()
err_handler=ErrorHandler()
def chunks(lst, n):       
        for i in range(0, len(lst), n):
            yield lst[i:i + n]              
def lambda_handler(event, context):
    res_json=""
    resHandler=ResponseHandler()
    status="ERROR"  
    eastern=pytz.timezone('US/Eastern')  
    requestStart = datetime.now().astimezone(eastern)
    summary_list=[]
    try:
        purgeDetail=[]  
        app_client= AppClient()
        request_queue=event['REQUEST_QUEUE_NAME']
        region=event['REGION']
        grp_resp=app_client.GetStoreGroups(region)
        client_1 = boto3.client('lambda')   
        if grp_resp['status']:
           groups=grp_resp['items']          
           for group in groups:
                if 'alerts' in group:
                    alerts=group['alerts']
                    if 'DAILY_PURGE' in alerts:                         
                        stores_resp=app_client.GetStores(group['group_id'],region)        
                        if stores_resp['status']== True:  
                            stores=stores_resp['items']
                            if len(stores)>0:
                                batches = chunks(stores,5)                
                                for batch in batches: 
                                    delete_batch(client_1,batch,event) 
                                    logger.info("prod store submitted for purge  in Batch:") 
                                    time.sleep(5)  
                                purgeDetail.append({"stage":"prod","group_id":group["group_id"],"group_name":group['group_name'],"storeCount":len(stores),"message":"Stores submitted for purge in batch.","code":0})                    
                            else:
                                logger.info("no prod store to purge...") 
                                purgeDetail.append({"stage":"prod","group_id":group["group_id"],"group_name":group['group_name'],"storeCount":len(stores),"message":"no active store to purge.","code":0})
                        else:
                            error_code=stores_resp['error_code']
                            res_json= resHandler.GetErrorResponseJSON(error_code,None)
                            logger.info("Error found while get prod stores res_json="+str(res_json))  
                            error_message=res_json["errorList"]["message"]
                            purgeDetail.append({"stage":"prod","group_id":group["group_id"],"group_name":group['group_name'],"storeCount":0,"message":error_message,"code":error_code})   

                        stores_resp=app_client.GetTestStores(group['group_id'],region)        
                        if stores_resp['status']== True:           
                            stores=stores_resp['items']
                            if len(stores)>0:
                                batches = chunks(stores,5)                
                                for batch in batches: 
                                    delete_batch(client_1,batch,event) 
                                    logger.info("test store submitted for purge in Batch :")  
                                    time.sleep(5)  
                                purgeDetail.append({"stage":"test","group_id":group["group_id"],"group_name":group['group_name'],"storeCount":len(stores),"message":"Stores submitted for purge in batch.","code":0})   
                            else:
                                logger.info("no test store to purge...") 
                                purgeDetail.append({"stage":"test","group_id":group["group_id"],"group_name":group['group_name'],"storeCount":len(stores),"message":"no active store to purge.","code":0 })   
                        else:
                            error_code=stores_resp['error_code']
                            res_json= resHandler.GetErrorResponseJSON(error_code,None)                            
                            logger.info("Error found while get test stores res_json="+str(res_json)) 
                            error_message=res_json["errorList"]["message"]
                            purgeDetail.append({"stage":"test","group_id":group["group_id"],"group_name":group['group_name'],"storeCount":0,"message":error_message,"code":error_code })   
                    else:
                        logger.info("SKIP DAILY_PURGE not set for Group_id :"+str(group['group_id']))  
                        purgeDetail.append({"group_id":group["group_id"],"group_name":group['group_name'],"message":"DAILY_PURGE not Setup","code":-1 })       
           
           
        else:           
            
            error_code=grp_resp['error_code']
            res_json= resHandler.GetErrorResponseJSON(error_code,None) 
            logger.info("Error found while get groups res_json="+str(res_json))  
            error_message=res_json["errorList"]["message"]
            purgeDetail.append({"message":error_message,"code":error_code, })  

        
        purge_table_data={"purge_table_summary":purgeDetail,"operation_status":status}    
        summary_list.append(purge_table_data)   
        purgeQueueDetail=[] 
        status="SUCCESS"
        sqs_resp=purge_queue(request_queue,region)
        if sqs_resp['status']== True:
              logger.info("Queue: "+str(request_queue)+" Purge completed, wait for next 5 seconds")
              time.sleep(5)
        else:
              logger.info("Queue: "+str(request_queue)+" Not Purge sqs_resp:"+str(sqs_resp))
              status="ERROR"
        purgeQueueDetail.append({"queue":request_queue,"purge_data":sqs_resp})
        request_queue="TEST_"+request_queue       
        sqs_resp=purge_queue(request_queue,region)
        if sqs_resp['status']== True:
              logger.info("Queue: "+str(request_queue)+" Purge completed, wait for next 5 seconds")
               
              time.sleep(5)
        else:
              logger.info("Queue: "+str(request_queue)+" Not Purge sqs_resp:"+str(sqs_resp))
              status="ERROR"
        purgeQueueDetail.append({"queue":request_queue,"purge_data":sqs_resp})
        
        """ request_queue=event['RESPONSE_QUEUE_NAME']        
        sqs_resp=purge_queue(request_queue,region)
        if sqs_resp['status']== True:
              logger.info("Queue: "+str(request_queue)+" Purge completed,wait for next 5 seconds")
              time.sleep(5)
        else:
              logger.info("Queue: "+str(request_queue)+" Not Purge sqs_resp:"+str(sqs_resp))
        purgeQueueDetail.append({"queue":request_queue,"purge_data":sqs_resp})        
       
        request_queue="TEST_"+request_queue
        sqs_resp=purge_queue(request_queue,region)
        if sqs_resp['status']== True:
              logger.info("Queue: "+str(request_queue)+" Purge completed..")
        else:
              logger.info("Queue: "+str(request_queue)+" Not Purge sqs_resp:"+str(sqs_resp))
        purgeQueueDetail.append({"queue":request_queue,"purge_data":sqs_resp})   """

        
        purge_queue_data={"purge_queue_summary":purgeQueueDetail,"operation_status":status}   
        summary_list.append(purge_queue_data)   
        s3=S3Manager(region)
        s3.delete_all_objects_from_s3_folder(bucket_name='epde-data-files',folderPrefix="data_files/")        
        res_json={
                    "responseCode": -1,"errorList": [
                        {"message":"Purge Process completed.",
                        "code":0
                        }
                    ]
                }
        purge_s3_data={"purge_s3_data_file_summary":{"message":"Purge files at S3::epde-data-files/data_files/ ","code":0},"operation_status":status} 
        summary_list.append(purge_s3_data)  
        status="SUCCESS" 
        return resHandler.GetAPIResponse(res_json) 
    except Exception as e:
            status="ERROR" 
            logger.error("error occured in lambda_handler",True)
            res_json= resHandler.GetErrorResponseJSON(313,None)
            return resHandler.GetAPIResponse(res_json) 
    finally:
        LogErrorInCloudWatch(context=context,requestStart=requestStart,res_json=res_json,status=status)
         
def LogErrorInCloudWatch(context,requestStart,res_json,status):
    try:        
        aws_account_id = context.invoked_function_arn.split(":")[4]            
        logEvent={
                "instance" :str(aws_account_id),
                "app":"EPDE-JOB",
                "jobName" :"Purge_Job",
                "status": status,
                "requestTime":requestStart.strftime("%Y-%m-%dT%H:%M:%S%z"),                        
                "message": res_json["errorList"]["message"],
                "code": res_json["errorList"]["code"],
                "responseCode":-1                
        } 
        cwLogger= CloudWatchLogger()    
        cwLogger.DoCloudWatchLog('epde/epdejobs',logEvent)
    except:
        logger.error("error occured DoCloudWatchLog in lambda_handler",True)      
   
def purge_queue(QueueName,region):        
        try:
            # Purge all SQS messages            
            sqs_client = boto3.client("sqs", region_name=region)
            sqs_queue_url = sqs_client.get_queue_url(QueueName=QueueName)['QueueUrl']          
            sqs_client.purge_queue(QueueUrl=sqs_queue_url)        
            return {"status":True }  
        except Exception as e:
            return err_handler.HandleGeneralError(moduleNM="purgedata",functionNM="purge_queue")              
def delete_batch(client_1,stores,event):         
            try:             
                # Define the input parameters that will be passed
                # on to the child function   
                payments_days=15                
                if 'RETAIN_DAYS_POST_PAYMENT' in event:
                    payments_days=int(event['RETAIN_DAYS_POST_PAYMENT'])

                deposits_days=15
                if 'RETAIN_DAYS_POST_DEPOSIT' in event:
                    deposits_days=int(event['RETAIN_DAYS_POST_DEPOSIT']) 

                batchclosero_days=15
                if 'RETAIN_DAYS_BATCH_CLOSERO' in event:
                    batchclosero_days=int(event['RETAIN_DAYS_BATCH_CLOSERO']) 

                extract_log_days=3
                if 'RETAIN_DAYS_EXTRACT_LOG' in event:
                    extract_log_days=int(event['RETAIN_DAYS_EXTRACT_LOG'])  
               
                accounting_days=15
                if 'RETAIN_DAYS_POST_ACCOUNTING' in event:
                    accounting_days=int(event['RETAIN_DAYS_POST_ACCOUNTING']) 

                region='us-east-1'
                if 'REGION' in event: 
                    region=event['REGION']  
                store_code_lst=[]
                for store in stores:
                    store_code_lst.append(store['store_code'])
                store_code= ','.join(store_code_lst)   
                inputParams = {
                    "REGION":str(region),
                    "store_code"   : str(store_code) ,
                    "payments_days":(payments_days),
                    "deposits_days" : (deposits_days),  
                    "batchclosero_days": (batchclosero_days),
                    "extractlog_days" :  (extract_log_days) ,
                    "accounting_days" :  accounting_days           
                }             
                response = client_1.invoke(
                    FunctionName = 'arn:aws:lambda:us-east-1:188506897258:function:PurgeDataHandler',
                    InvocationType = 'Event',
                    Payload = json.dumps(inputParams)
                )
            except Exception :
                 logger.error("Error found delete_batch",True)
 