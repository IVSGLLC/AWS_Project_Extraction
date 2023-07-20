import json
import time
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

    try:
        _moduleNM="EPDE_Lambda"
        _functionNM="lambda_handler"
        err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)             
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
                            else:
                                logger.info("no prod store to purge...") 
                        else:
                            error_code=stores_resp['error_code']
                            res_json= resHandler.GetErrorResponseJSON(error_code,None)
                            logger.info("Error found while get prod stores res_json="+str(res_json))   

                        stores_resp=app_client.GetTestStores(group['group_id'],region)        
                        if stores_resp['status']== True:           
                            stores=stores_resp['items']
                            if len(stores)>0:
                                batches = chunks(stores,5)                
                                for batch in batches: 
                                    delete_batch(client_1,batch,event) 
                                    logger.info("test store submitted for purge in Batch :")  
                                    time.sleep(5)  
                            else:
                                logger.info("no test store to purge...") 
                        else:
                            error_code=stores_resp['error_code']
                            res_json= resHandler.GetErrorResponseJSON(error_code,None)
                            logger.info("Error found while get test stores res_json="+str(res_json)) 
                    else:
                        logger.info("SKIP DAILY_PURGE not set for Group_id :"+str(group['group_id']))         
                 
        else:
            error_code=grp_resp['error_code']
            res_json= resHandler.GetErrorResponseJSON(error_code,None) 
            logger.info("Error found while get groups res_json="+str(res_json))    

         
        sqs_resp=purge_queue(request_queue,region)
        if sqs_resp['status']== True:
              logger.info("Queue: "+str(request_queue)+" Purge completed, wait for next 5 seconds")
              time.sleep(5)
        else:
              logger.info("Queue: "+str(request_queue)+" Not Purge sqs_resp:"+str(sqs_resp))
        
        request_queue="TEST_"+request_queue       
        sqs_resp=purge_queue(request_queue,region)
        if sqs_resp['status']== True:
              logger.info("Queue: "+str(request_queue)+" Purge completed, wait for next 5 seconds")
              time.sleep(5)
        else:
              logger.info("Queue: "+str(request_queue)+" Not Purge sqs_resp:"+str(sqs_resp))
        
        request_queue=event['RESPONSE_QUEUE_NAME']        
        sqs_resp=purge_queue(request_queue,region)
        if sqs_resp['status']== True:
              logger.info("Queue: "+str(request_queue)+" Purge completed,wait for next 5 seconds")
              time.sleep(5)
        else:
              logger.info("Queue: "+str(request_queue)+" Not Purge sqs_resp:"+str(sqs_resp))
       
        request_queue="TEST_"+request_queue
        sqs_resp=purge_queue(request_queue,region)
        if sqs_resp['status']== True:
              logger.info("Queue: "+str(request_queue)+" Purge completed..")
        else:
              logger.info("Queue: "+str(request_queue)+" Not Purge sqs_resp:"+str(sqs_resp))

        s3=S3Manager(region)
        s3.delete_all_objects_from_s3_folder(bucket_name='epde-data-files',folderPrefix="data_files/")  
        res_json={"message":"Purge Process completed...."}

        return resHandler.GetAPIResponse(res_json) 
    except Exception as e:
            logger.error("error occured in lambda_handler",True)
            res_json= resHandler.GetErrorResponseJSON(313,None)
            return resHandler.GetAPIResponse(res_json) 
   
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
 