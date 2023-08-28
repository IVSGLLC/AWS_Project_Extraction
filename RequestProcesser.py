import base64
from datetime import datetime
import gzip
import json
import logging
import os
import sys

import pytz
from DBHelper import DBHelper
import DynamoDBDAO as DAO
from EPDE_CloudWatchLog import CloudWatchLogger
#from EPDE_SQS import SQSManager
from EPDE_S3 import S3Manager
import ParseData as PU
loglevel =int(os.environ['LOG_LEVEL'])
logger = logging.getLogger('RequestProcesser')
logger.setLevel(loglevel)

class RequestProcesser():
     
    @classmethod 
    def Process_KinesStreamRecord(self,records,aws_account_id):
        logger.debug("Inside Process_KinesStreamRecord function...")
        #if(loglevel==logging.DEBUG):
            #logger.debug("records: JSON:"+json.dumps(records, indent = 1)) 
            #   
        eastern=pytz.timezone('US/Eastern')   
        st = datetime.now()
        processDateTime = datetime.now().astimezone(eastern)
        logger.info("KinesStreamRecord Count:"+str(len(records)))
        reponse_list=[]
        cwLogger= CloudWatchLogger()
        for record in records:
            st = datetime.now()
            processDateTime = datetime.now().astimezone(eastern)
            partitionKey=None
            store_code=''
            file_type=''
            extractType=''  
            df_final={}  
            totalProcessDuration=0 
            try:
                # Process your record
                partitionKey=record['kinesis']['partitionKey']
                logger.info("partitionKey:"+partitionKey)
                base64_byte=record['kinesis']['data']
                #logger.debug("record['kinesis']['data'] base64_byte:"+base64_byte)
                base64_string=""
                if isinstance(base64_byte, (bytes, bytearray)):
                    base64_string=base64_byte.decode()
                else:
                    base64_string=base64_byte
                #if(loglevel==logging.DEBUG):
                    #logger.debug("base64_string:"+base64_string)            
                b64decode=base64.b64decode(base64_string)            
                if isinstance(b64decode, (bytes, bytearray)):
                    base64_string=b64decode.decode('utf-8')
                else:
                    base64_string=b64decode
                
                payload=json.loads(base64_string)
                
                #if(loglevel==logging.DEBUG):
                #    logger.debug("data - payload="+str(payload))  
                
                store_code=payload.get('store_code')
                #logger.info("store_code:"+store_code)
                
                client_id=payload.get('client_id')
                #logger.info("client_id:"+client_id)
                
                file_type=payload.get('file_type')
                logger.info("store_code:"+store_code+",file_type:"+file_type+",client_id:"+client_id)

                file_data_b64=payload.get('file_data')
                try:
                    is_compressed=payload.get('is_compressed')
                except Exception as e:
                    is_compressed=False
                logger.debug("is_compressed:"+is_compressed)               
                if is_compressed :
                    compressed_file_bytes= base64.b64decode(file_data_b64)
                    file_data=gzip.decompress(compressed_file_bytes).decode()                              
                    if isinstance(file_data, (bytes, bytearray)):
                        file_data=file_data.decode()   
                       
                else:
                    if isinstance(file_data_b64, (bytes, bytearray)):
                            base64_string=file_data_b64.decode()
                    else:
                            base64_string=file_data_b64
                    #if loglevel==logging.DEBUG:        
                        #logger.debug("base64_string (file_data_b64):"+base64_string)                
                    file_data=base64.b64decode(base64_string)
                    
                    if isinstance(file_data, (bytes, bytearray)):
                            file_data=file_data.decode('utf-8')
                #if loglevel==logging.DEBUG:              
                #logger.debug("file data as Plain Text:"+file_data) 
                ParseData1= PU.ParseData()
                if file_type=='WIP':
                    extractType='WIP'
                    df_final=ParseData1.parse_DMS_WIP_File(file_data)
                elif file_type=='SALESCUST':
                    extractType='SALESCUST'
                    df_final=ParseData1.parse_DMS_SalesCust_File(file_data)
                elif file_type=='INVOICE':
                    extractType='INVOICE'
                    df_final=ParseData1.parse_DMS_Invoice_File(file_data)
                elif file_type=='PARTS':
                    extractType='PARTS'
                    df_final=ParseData1.parse_DMS_Parts_File(file_data)
                elif file_type=='INVENTORY':
                    extractType='INVENTORY'
                    df_final=ParseData1.parse_DMS_Inventory_File(file_data)
                elif file_type=='DEAL':
                     extractType='DEAL'
                     df_final=ParseData1.parse_DMS_Deal_File(file_data)
                elif file_type=='ROHIST':
                     extractType='ROHIST'
                     df_final=ParseData1.parse_DMS_ROHistory_File(file_data)
                elif file_type=='ROHISTV1' or file_type=='ROHISTV2':
                    extractType='ROHIST'
                    df_final=ParseData1.parse_DMS_ROHistory_FileV1(file_data)
                elif file_type=='TKWIP':
                    extractType='WIP'
                    df_final=ParseData1.parse_TKION_WIP_File(file_data)
                elif file_type=='TKPARTS':
                    extractType='PARTS'
                    df_final=ParseData1.parse_TEKION_Parts_File(file_data)
                elif file_type=='TKSALESCUST':
                    extractType='SALESCUST'
                    df_final=ParseData1.parse_TEKION_SalesCust_File(file_data)
                elif file_type=='MRWIP':
                    extractType='WIP'
                    df_final=ParseData1.parse_MR_WIP_File(file_data)
                elif file_type=='MRPARTS':
                    extractType='PARTS'
                    df_final=ParseData1.parse_MR_Parts_File(file_data)
                elif file_type=='MRSALESCUST':
                    extractType='SALESCUST'
                    df_final=ParseData1.parse_MR_SalesCust_File(file_data)
                elif file_type=='ULIST' :
                    extractType='ULIST'    
                    try:               
                        region=os.environ['REGION']
                        s3=S3Manager(region)
                        parsedFile_folder= 'session_files' 
                        bucketName='epde-data-files'                     
                        fileName=parsedFile_folder+"/"+ str(partitionKey)+".txt"   
                        s3.uploadFile(bucketName=bucketName,fileName=fileName,content=file_data)
                    except:
                        logger.error("Error Occure in Process_KinesisStreamRecord......." , exc_info=True)   
                elif file_type=='LOGFAIL' :
                    extractType='LOGFAIL'    
                    try:               
                        region=os.environ['REGION']
                        s3=S3Manager(region)
                        parsedFile_folder= 'loginfail_files' 
                        bucketName='epde-data-files'                     
                        fileName=parsedFile_folder+"/"+ str(partitionKey)+".txt"   
                        s3.uploadFile(bucketName=bucketName,fileName=fileName,content=file_data)
                    except:
                        logger.error("Error Occure in Process_KinesisStreamRecord......." , exc_info=True) 
                dao_Obj=DAO.DynamoDBDAO()
                
                response= { "operation_status":"FAILED","error_code":-1 ,"error_message":"Unrecognised file found."} 
                if file_type=='WIP':
                    response= dao_Obj.Save(store_code,df_final,client_id)
                elif file_type=='SALESCUST':
                    response= dao_Obj.SaveSalesCust(store_code,df_final,client_id)
                elif file_type=='INVOICE':
                    response= dao_Obj.SaveInvoice(store_code,df_final,client_id)
                elif file_type=='PARTS':
                    response= dao_Obj.SaveParts(store_code,df_final,client_id)
                elif file_type=='INVENTORY':
                    response= dao_Obj.SaveInventory(store_code,df_final,client_id)
                elif file_type=='DEAL':
                    response= dao_Obj.SaveDeal(store_code,df_final,client_id)
                elif file_type=='ROHIST' or file_type=='ROHISTV1':
                     response= dao_Obj.SaveROHistory(store_code,df_final,client_id)
                elif file_type=='ROHISTV2':
                     response= dao_Obj.SaveROHistoryNew(store_code,df_final,client_id)
                elif file_type=='TKWIP':
                    response= dao_Obj.SaveTKRO(store_code,df_final,client_id)
                elif file_type=='TKPARTS':
                    response= dao_Obj.SavePartsTK(store_code,df_final,client_id)   
                elif file_type=='TKSALESCUST':
                    response= dao_Obj.SaveSalesCustTK(store_code,df_final,client_id)
                elif file_type=='MRWIP':
                    response= dao_Obj.SaveMRRO(store_code,df_final,client_id)
                elif file_type=='MRPARTS':
                    response= dao_Obj.SavePartsMR(store_code,df_final,client_id)   
                elif file_type=='MRSALESCUST':
                    response= dao_Obj.SaveSalesCustMR(store_code,df_final,client_id)
                elif file_type=='ULIST' or file_type=='LOGFAIL':
                    response= { "operation_status":"SUCCESS","total_item_count":str(0),"total_item_parsed":str(0),"items":[]} 
                
                if response['operation_status']=="SUCCESS":
                                       
                    keep_raw=str(os.environ['KEEP_DMS_RAW_FILES_IN_S3'])
                    keep_json=str(os.environ['KEEP_PARSED_FILES_IN_S3'])
                    logger.debug("keep_raw :"+str(keep_raw))
                    if keep_raw.lower() == 'true' or keep_json.lower() == 'true':
                        region=os.environ['REGION']
                        parsedFile_folder= 'json_files' 
                        rawFile_folder= 'raw_files' 
                        bucketName='epde' 
                        s3=S3Manager(region)
                        issued = datetime.now()
                        folder_suffix = issued.strftime("%Y/%m/%d")                         
                        region=os.environ['REGION']                        
                        allowStore=True
                        if 'TRACE_STORE_ID' in os.environ:
                            TRACE_STORE_ID_STR= str(os.environ['TRACE_STORE_ID']) 
                                                
                            if len(TRACE_STORE_ID_STR.strip())>0:
                                TRACE_FILE_TYPE_ARR=[]
                                if 'TRACE_FILE_TYPE' in os.environ:
                                  TRACE_FILE_TYPE_STR= str(os.environ['TRACE_FILE_TYPE'])  
                                  TRACE_FILE_TYPE_ARR=TRACE_FILE_TYPE_STR.split(',')
                                
                                TRACE_STORE_ID_ARR=TRACE_STORE_ID_STR.split(',')
                                if TRACE_STORE_ID_ARR.__contains__(store_code) and TRACE_FILE_TYPE_ARR.__contains__(file_type):
                                    allowStore=True
                                else:
                                    allowStore=False
                        logger.debug("allowStore :"+str(allowStore))        
                        if keep_raw.lower() == 'true' and allowStore: 
                            fileName=rawFile_folder+"/"+folder_suffix+"/"+ str(partitionKey)+".txt"
                            s3.uploadFile(bucketName=bucketName,fileName=fileName,content=file_data)
                            logger.info("Uploaded RAW Data to S3 fileName:"+str(fileName))
                        
                        logger.debug("keep_json :"+str(keep_json))        
                        if keep_json.lower() == 'true'  and allowStore:  
                            data=response['items']
                            result = json.dumps(data)
                            fileName=parsedFile_folder+"/"+folder_suffix+"/"+ str(partitionKey)+".json"
                            s3.uploadFile(bucketName=bucketName,fileName=fileName,content=result)
                            logger.info("Uploaded JSON Data to S3 fileName:"+str(fileName))
                et = datetime.now()
                delta=et-st
                totalProcessDuration=delta.total_seconds()*1000
                logger.debug("TOTAL PROCESSING TIME="+str(delta.total_seconds()))
                dbhelper=DBHelper()
                region=os.environ['REGION']
                dbhelper.Audit_ExtractDetail(region=region,store_code=store_code,client_id=client_id,partitionKey=partitionKey,fileType=file_type,response=response,start_time=st,end_time=et)
                #return response
            except Exception as e:
                   logger.error("Error Occure in Process_KinesStreamRecord......." , exc_info=True)
                   ex_type, ex_value, ex_traceback = sys.exc_info()
                   response= { "operation_status":"FAILED","error_code":-1 ,"error_message":ex_value} 
            reponse_list.append(response)  
            status='ERROR'            
            error_message=""
            total_item_count=0
            total_item_parsed=0
            if response['operation_status']=='SUCCESS':
                status='SUCCESS'  
                if  "total_item_count" in response:
                    total_item_count=response["total_item_count" ]
                if  "total_item_parsed" in response:
                    total_item_parsed=response["total_item_parsed" ]    
                    
            else:
                if 'error_message'in response and response['operation_status']=='FAILED':
                    error_message=response['error_message']
           
               
            logEvent= {
                "instance" :str(aws_account_id),
                "fileId":partitionKey,
                "app": "ETL_KINESIS",
                "storeCode": store_code,
                "extractType": extractType,	
                "status": status,                  
                "message":error_message,
                "insertedRecordCount":total_item_parsed,
                "totalRecordCount":total_item_count,
                "processDateTime":processDateTime.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "processDurationMS"	:totalProcessDuration
            }                  
            cwLogger.DoCloudWatchLog('epde/etlLog',logEvent)           
        return reponse_list
        