from decimal import Decimal
from datetime import datetime, timedelta
import gzip
from io import BytesIO
import json
import logging
import boto3
import os
import concurrent.futures

import pytz
from EPDE_Client import AppClient
from EPDE_S3 import S3Manager
from EPDE_SQS import SQSManager
loglevel = int(os.environ['LOG_LEVEL'])
logger = logging.getLogger('DBHelper')
logger.setLevel(loglevel)

class DBHelper():

    @classmethod
    def chunks(self,lst, n):       
        for i in range(0, len(lst), n):
            yield lst[i:i + n]       
    @classmethod         
    def load_batch(self,dynamodb,table_name,pkeys, batch_list):
        table = dynamodb.Table(table_name)
        with table.batch_writer(overwrite_by_pkeys=pkeys) as batch:
            for item in batch_list:
                batch.put_item(
                    Item=item
                )
    @classmethod
    def is_number(self,n):
        is_number = True
        try:
            num = Decimal(n)
            # check for "nan" floats
            is_number = num == num   # or use `math.isnan(num)`
        except :
            is_number = False
        return is_number
    @classmethod
    def CovertToNumericString(self,val) :
            strval=str(val)
            if loglevel==logging.DEBUG:
                        logger.debug("CovertToNumericString:"+strval)
            if strval.isnumeric() or strval.isdecimal():
                return strval
            else:
                return "0.0"
    @classmethod
    def CovertToString(self,val) :
        strval=str(val)
        if strval.isnumeric() or strval.isdecimal() :
            return strval
        else:
            if strval.lower()=="nan" or strval.lower()=="none":
                return ""               
        return strval    
    @classmethod
    def CovertToDecimalNumericString(self,val) : 
        try:
            strval=str(val)
           # if loglevel==logging.DEBUG:
                  #  logger.debug("CovertToDecimalNumericString:"+strval)
            if strval.isnumeric() or strval.isdecimal() :
                length=len(strval)
                if(length>2):
                    length=len(strval)-2
                    strval=strval[0:length]+"."+strval[-2]+strval[-1]
                else:
                    if(length==2):
                        strval="0."+strval[-2]+strval[-1]
                    else:
                        strval="0."+strval+"0"
                return strval
            else:
                return "0.0" 
        except Exception as e:
            logger.error("Error Occure in CovertToDecimalNumericString ..." , exc_info=True)
            return ""                 
    @classmethod
    def RemoveNewLineCarrigeReturn(self,rawLine):
         rawLine=str(rawLine)
         rawLine= rawLine.replace('\n',' ') 
         rawLine= rawLine.replace('\r',' ') 
         rawLine= rawLine.replace('\r\n',' ')
         rawLine= ' '.join(filter(None,rawLine.split(' ')))
         rawLine=rawLine.strip()
         return rawLine
    @classmethod
    def getBatchList(self,items) :
        st = datetime.now()
        LAMBDA_BATCH_SIZE=100
        batch_items=[]
        new_items=[]
        for item in items:
            new_items.append(item) 
            json_string = json.dumps(new_items)
            byte_ = json_string.encode("utf-8")
            size_in_bytes = len(byte_)
            if size_in_bytes>=240000 or len(new_items)>= LAMBDA_BATCH_SIZE:
                batch_items.append(new_items)
                new_items=[]
                
        if(len(new_items)>0):
            batch_items.append(new_items)  
        et = datetime.now()
        delta=et-st
        logger.debug("Build getBatchList total_seconds="+str(delta.total_seconds()))
        return batch_items  
    @classmethod
    def submitSaveData(self,TableName,items,region='us-east-1',overwrite_by_pkeys=['document_type', 'document_id'],checkForUpdateExisting=False,MAX_PROC=4,BATCH_SIZE=25):
            # Define the client to interact with AWS Lambda
            
            
            client_1 = boto3.client('lambda')
            batches =items  #self.chunks(items,lambda_batch_size)                        
            for batch in batches:
                # Define the input parameters that will be passed
                # on to the child function
                 
                inputParams = {
                    "tableName"   : TableName,
                    "overwrite_by_pkeys"      : overwrite_by_pkeys,
                    "MAX_PROC"     : MAX_PROC,
                    "BATCH_SIZE"     : BATCH_SIZE,
                    "region":region,
                    "items":batch,
                    "checkForUpdateExisting":checkForUpdateExisting
                }             
                response = client_1.invoke(
                    FunctionName = 'arn:aws:lambda:us-east-1:188506897258:function:SaveDataHandler',
                    InvocationType = 'Event',
                    Payload = json.dumps(inputParams)
                )
                logger.info("responseFrom SaveDataHandler Lambda="+str(response))   
    @classmethod
    def submitSaveDataLocal(self,TableName,items,region='us-east-1',overwrite_by_pkeys=['document_type', 'document_id'],checkForUpdateExisting=False,MAX_PROC=4,BATCH_SIZE=25):
            # Define the client to interact with AWS Lambda
           # logger.debug("Started.submitSaveDataLocal TableName="+str(TableName))
            results=[]
            futures=[]
            dynamodb = boto3.resource('dynamodb', region_name=region)
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_PROC) as executor:
                batches = self.chunks(items,BATCH_SIZE)                
                for batch in batches:
                    #logger.debug("batch added size="+str(len(batch)))
                    futures.append(executor.submit(self.load_batch,dynamodb, TableName,overwrite_by_pkeys, batch))
                for future in concurrent.futures.as_completed(futures):                     
                    results.append(future.result())  
    @classmethod
    def GetLatestStatus(self,status):
        try:
            if status is None or status.strip()=='':
                return status
            else:
                if "," in status:
                    if "C98" in status:
                        return "C98"
                    elif "C97" in status:
                        return "C97"
                    elif "C94" in status:
                        return "C94"
                    elif "I98" in status:
                        return "I98"
                    elif "I91" in status:
                        return "I91"  
                    else:
                        sts= status.split(",") 
                        space_to_empty = [x.strip() for x in sts]
                        space_clean_list = [x for x in space_to_empty if x]
                        if len(space_clean_list)>0:
                           return space_clean_list[0] 
                        else:
                           return ""
        except:
            logger.error("error found in GetLatestStatus",True)
            pass
        return status 
    @classmethod      
    def decompressBytesToString(self,inputBytes):
        """
        decompress the given byte array (which must be valid 
        compressed gzip data) and return the decoded text (utf-8).
        """
        bio = BytesIO()
        stream = BytesIO(inputBytes)
        decompressor = gzip.GzipFile(fileobj=stream, mode='r')
        while True:  # until EOF
            chunk = decompressor.read(8192)
            if not chunk:
                decompressor.close()
                bio.seek(0)
                return bio.read().decode("utf-8")
            bio.write(chunk)
        return None
    @classmethod
    def compressStringToBytes(self,inputString):
        """
        read the given string, encode it in utf-8,
        compress the data and return it as a byte array.
        """
        bio = BytesIO()
        bio.write(inputString.encode("utf-8"))
        bio.seek(0)
        stream = BytesIO()
        compressor = gzip.GzipFile(fileobj=stream, mode='w')
        while True:  # until EOF
            chunk = bio.read(8192)
            if not chunk:  # EOF?
                compressor.close()
                return stream.getvalue()
            compressor.write(chunk) 
    @classmethod
    def GetGroup(self,store_code,region):
        app_client=AppClient()   
        group=''     
        store_resp=app_client.GetStoreDetail(store_code,region=region)
        if store_resp['status']:
            item=store_resp['item']
            if 'group_id' in item:
                group=item['group_id']
        return group
    @classmethod
    def ValidateAccounts(self,store_code,account_lines,region,accCodeList):
        status=False
        if accCodeList is not None and  account_lines is not None and len(account_lines)>0:
            try:  
                accounts=self.GetAccounts(store_code=store_code,region=region) 
                if accounts is not None:
                    accNameList=[]
                    for s_acc in accCodeList: 
                         if s_acc in accounts:
                           
                            accNameList.append( ","+str(accounts[s_acc])+")")                
                    if len(accNameList) >0:
                        status=True                 
                        for acc_line in account_lines:  
                            logger.debug("2.loop acc_line :"+str(acc_line))                   
                            if len(acc_line)>0 and  (list(filter(acc_line.endswith, accNameList)) == []) :
                               return False            
            except Exception as e:
                logger.error("Error Occure in GetAccounts.." , exc_info=True)
        return status
    @classmethod
    def GetAccounts(self,store_code,region):
        try: 
            app_client=AppClient()   
            accounts=[]     
            store_resp=app_client.GetStoreDetail(store_code,region=region)
            if store_resp['status']:
                item=store_resp['item']
                if 'accounts' in item:
                    return item['accounts']
        except Exception as e:
                logger.error("Error Occure in GetAccounts.." , exc_info=True)
        return None
    @classmethod
    def GetAccountByCode(self,store_code,region,code):
        try:  
            app_client=AppClient()   
            accounts=[]     
            store_resp=app_client.GetStoreDetail(store_code,region=region)
            if store_resp['status']:
                item=store_resp['item']
                if 'accounts' in item:
                    accounts=item['accounts']
                    if accounts is not None and code in accounts:
                        return accounts[code]
        except Exception as e:
                logger.error("Error Occure in GetAccountByCode.." , exc_info=True)
        return None
    @classmethod
    def Audit_ExtractDetail(self,region,store_code,client_id,partitionKey,fileType,response,start_time,end_time):
        TableName=store_code+"_EXTRACT_LOG"
        try:                     
            logger.debug("Audit_ExtractDetail >> partitionKey="+str(partitionKey)) 
            isAudit=False
            
            if 'AUDIT' in os.environ and os.environ['AUDIT'] is not None:
                isAuditStr= str(os.environ['AUDIT'])  
                if isAuditStr.lower()=='true':
                    isAudit=True
                    logger.debug("Audit_ExtractDetail >> isAudit="+str(isAudit)) 
            

            if partitionKey is not None and len(partitionKey)>0:
                ct = datetime.now()
                create_date = ct.strftime("%Y-%m-%d %I:%M:%S %p")
                create_date_only = ct.strftime("%Y-%m-%d")
                delta = end_time-start_time
                total_seconds=delta.total_seconds()
                start_time_stamp=start_time.timestamp()
                end_time_stamp = end_time.timestamp()              
                operation_status=''
                total_item_count=-1            
                total_item_parsed=-1
                error_message=''
                error_code=-1
                if response is not None :
                    if 'operation_status' in response and response['operation_status'] is not None:
                        operation_status=str(response['operation_status'])
                    if 'total_item_count' in response and response['total_item_count'] is not None:
                        total_item_count=int(response['total_item_count'])
                    #if 'total_item_parsed' in response and response['total_item_parsed'] is not None:
                    #    total_item_parsed=int(response['total_item_parsed'])
                    if 'error_code' in response and response['error_code'] is not None:
                        error_code=int(response['error_code'])
                    if 'error_message' in response and response['error_message'] is not None:
                        error_message=str(response['error_message'])
                
                if 'SYNC' in os.environ and os.environ['SYNC'] is not None:
                    isSYNCStr= str(os.environ['SYNC'])  
                    if isSYNCStr.lower()=='true':
                        group=self.GetGroup(region=region,store_code=store_code)
                        item={
                                "partitionKey": str(partitionKey),
                                "fileId": str(partitionKey),
                                "dealerCode": str(store_code),
                                "group":str(group),
                                "api":"dataextraction"                                                                                                 
                            }
                        logger.debug("SQS msg_body :"+str(item))
                        sqs= SQSManager(QueueName= "EPDE_Extract_Response.fifo",region=region,MaxMsgCount=20,waitTimeInSecond=10)   
                        sqs_resp=sqs.send_message(msg_body=(item),MessageGroupId=str(store_code),MessageDeduplicationId=str(partitionKey))
                        
                if isAudit:
                    dynamodb = boto3.resource('dynamodb', region_name=region)
                    #log_id=str(uuid.uuid4())               
                    table = dynamodb.Table(TableName) 
                    with table.batch_writer(overwrite_by_pkeys=[ 'file_id']) as batch:
                            batch.put_item (
                                            Item={
                        "file_id": str(partitionKey),
                        "fileId": str(partitionKey),
                        "dealerCode": str(store_code),
                        "api":"dataextraction",
                        "client_id": str(client_id),
                        "file_type": str(fileType),
                        "operation_status": str(operation_status), 
                        "total_item_count": str(total_item_count),      
                        "error_code":str(error_code),
                        "error_message":str(error_message), 
                        "start_time":str(start_time_stamp),                             
                        "end_time":str(end_time_stamp),
                        "total_seconds": str(total_seconds),
                        "create_date": str(create_date_only),
                        "create_date_time":str(create_date)                                                                               
                                    }
                                        ) 
                    logger.info("Audit Log Created partitionKey:"+str(partitionKey)) 
            else:
                logger.info("Audit Log Not Created, partitionKey is None or isAudit is false,partitionKey:"+str(partitionKey) )       
        except Exception as e:
                logger.error("Error Occure in Audit_ExtractDetail data to table ["+TableName+"] in DB..." , exc_info=True)
    @classmethod
    def compare_dict_listtype_json_objects(self,obj1, obj2,key, attr):
        if key in attr:
            parts_attributes= attr[key]
            parts_List1=obj1.get(key)
            parts_List2=obj2.get(key) 
            filtered_parts_list = self.filter_changed_objects( list1=parts_List1,list2= parts_List2,attributes= parts_attributes)
            if filtered_parts_list is not None and len(filtered_parts_list)>0:
                return False
        return True
    @classmethod
    def compare_dict_dicttype_json_objects(self,obj1, obj2,key, attr):
        if isinstance(obj1, dict):
            if type(obj1) != type(obj2):
                return False 
            attributes= attr[key]
            obj11=obj1.get(key)
            obj22=obj2.get(key)
            if self.compare_json_objects(obj1=obj11, obj2=obj22,attributes= attributes) != True:
               return False
        return True
     
 
    @classmethod
    def compare_json_objects(self,obj1, obj2, attributes):
        for attr in attributes:
            if isinstance(attr, dict):
                if "partsList" in attr:
                    if self.compare_dict_listtype_json_objects(obj1=obj1,obj2= obj2, key="partsList",attr=attr) ==False:
                        return False
                if "punchTimes" in attr:
                    if self.compare_dict_listtype_json_objects(obj1=obj1,obj2= obj2, key="punchTimes",attr=attr) ==False:
                        return False     
                if "parts" in attr:
                    if self.compare_dict_listtype_json_objects(obj1=obj1,obj2= obj2, key="parts",attr=attr) ==False:
                        return False         
                if "roOperations" in attr:
                    if self.compare_dict_listtype_json_objects(obj1=obj1,obj2= obj2, key="roOperations",attr=attr) ==False:
                        return False  
                if "roParts" in attr:
                    if self.compare_dict_listtype_json_objects(obj1=obj1,obj2= obj2, key="roParts",attr=attr) ==False:
                        return False     
                if "serviceDetails" in attr:
                    if self.compare_dict_listtype_json_objects(obj1=obj1,obj2= obj2, key="serviceDetails",attr=attr) ==False:
                        return False                   
                if "contacts" in attr:
                    if self.compare_dict_listtype_json_objects(obj1=obj1,obj2= obj2, key="contacts",attr=attr) ==False:
                        return False 
                if "operations" in attr:
                    if self.compare_dict_listtype_json_objects(obj1=obj1,obj2= obj2, key="operations",attr=attr) ==False:
                        return False   
                if "addresses" in attr:
                    if self.compare_dict_listtype_json_objects(obj1=obj1,obj2= obj2, key="addresses",attr=attr) ==False:
                        return False             
                
                if "soldTo" in attr:
                    if self.compare_dict_dicttype_json_objects(obj1=obj1,obj2= obj2, key="soldTo",attr=attr) ==False:
                       return False       
                if "shipTo" in attr:
                    if self.compare_dict_dicttype_json_objects(obj1=obj1,obj2= obj2, key="shipTo",attr=attr) ==False:
                       return False        
                if "vehicle" in attr:
                    if self.compare_dict_dicttype_json_objects(obj1=obj1,obj2= obj2, key="vehicle",attr=attr) ==False:
                        return False                          
                if "customer" in attr:
                    if self.compare_dict_dicttype_json_objects(obj1=obj1,obj2= obj2, key="customer",attr=attr) ==False:
                        return False                
                if "techStory" in attr:
                    if self.compare_dict_dicttype_json_objects(obj1=obj1,obj2= obj2, key="techStory",attr=attr) ==False:
                        return False
                if "employee" in attr:
                    if self.compare_dict_dicttype_json_objects(obj1=obj1,obj2= obj2, key="employee",attr=attr) ==False:
                        return False                                  
            else: 
                if isinstance(obj1, dict):
                    if type(obj1) != type(obj2):
                        return False                    
                    if obj1 is not None and obj2 is not None:                        
                        if obj1.get(attr) != obj2.get(attr):
                            return False  
                        
                if isinstance(obj1, list):
                    if len(obj1) != len(obj2):
                        return False 
                    if obj1 is not None and obj2 is not None:                        
                        if obj1.get(attr) != obj2.get(attr):
                            return False   
                else: 
                    if obj1 is not None and obj2 is not None:                
                        if obj1.get(attr) != obj2.get(attr):
                            return False
                    if obj1 is None and obj2 is not None: 
                        return False
                    if obj1 is not None and obj2 is  None: 
                        return False
                    
        return True
    
    @classmethod
    def filter_similar_objects(self,list1, list2, attributes):
        similar_objects = []
        for obj1 in list1:
            for obj2 in list2:
                if self.compare_json_objects(obj1=obj1,obj2= obj2, attributes=attributes):
                    similar_objects.append(obj1)
        return similar_objects
    @classmethod
    def filter_changed_objects(self,list1, list2, attributes):
        filtered_list = []
        for obj1 in list1:
            found_similar = False
            for obj2 in list2:
                if self.compare_json_objects(obj1=obj1, obj2=obj2, attributes=attributes):
                    found_similar = True
                    break
            if not found_similar:
                filtered_list.append(obj1)
        return filtered_list
    @classmethod
    def GetChangedItems(self,region, tableName,items, attributes_to_compare):
        if 'applyFilter' in os.environ and str(os.environ['applyFilter'] )=="True":
            filtered_list=[]
            isFiltered=False
            if items is not None and len(items)>0:
                isFiltered=False
                try:
                    parsedFile_folder= 'data_files' 
                    bucketName='epde-data-files' 
                    s3=S3Manager(region)
                    #issued = datetime.now()
                    #folder_suffix = issued.strftime("%Y/%m/%d")          
                    fileName=parsedFile_folder+"/"+ str(tableName)+".json"          
                    oldFileData=s3.read_s3_file(bucketName=bucketName,fileName=fileName)
                    if oldFileData is not None and len(oldFileData)>0:
                        oldItems=None
                        try:
                            oldItems=json.loads(oldFileData)
                        except :
                            logger.error("Error in convert old  data to  JSON Data fileName:"+str(fileName),True)
                        if oldItems is not None and len(oldItems)>0:
                            filtered_list = self.filter_changed_objects(  list1=items,list2= oldItems,attributes= attributes_to_compare)
                            isFiltered=True
                            if filtered_list is not None and len(filtered_list)>0:
                                try:
                                    result = json.dumps(items)
                                    #s3.deleteFile(bucketName=bucketName,fileName=fileName)
                                    s3.uploadFile(bucketName=bucketName,fileName=fileName,content=result)
                                    logger.debug("Uploaded Latest JSON Data to S3 fileName:"+str(fileName))
                                except:
                                    logger.error("Error in Upload Latest JSON Data to S3 fileName:"+str(fileName),True)
                    else:
                        try:
                            result = json.dumps(items)
                            #s3.deleteFile(bucketName=bucketName,fileName=fileName)
                            s3.uploadFile(bucketName=bucketName,fileName=fileName,content=result)
                            logger.debug("First time Uploaded JSON Data to S3 fileName:"+str(fileName))
                        except:
                            logger.error("Error in First time Upload JSON Data to S3 fileName:"+str(fileName),True)           
                    
                except:
                    logger.error("Error in read  JSON Data from S3 fileName:"+str(fileName),True)                 
                
            if isFiltered==False:
                filtered_list=items
            logger.debug("GetChangedItems isFiltered:"+str(isFiltered))  
            return   filtered_list
        else:
             return   items

    @classmethod    
    def get_time_to_live(self):
        target_hour=3
        target_minute=45
        DATA_EXP_TIME=""
        try:
            if 'DATA_EXP_TIME' in os.environ:
                DATA_EXP_TIME=  str(os.environ['DATA_EXP_TIME']) 
                timeArray= DATA_EXP_TIME.split(":")
                try:
                    target_hour=int(timeArray[0])
                except:
                    target_hour=3
                  
                try:
                    target_minute=int(timeArray[1])
                except:                   
                    target_minute=45        

              
        except:
            target_hour=3
            target_minute=45  
        epoch_time=0           
        try:    
            current_time = datetime.now()
            eastern=pytz.timezone('US/Eastern')       
            current_time_eastern = current_time.astimezone(eastern)        
            target_time_eastern = current_time_eastern.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)        
            # If the target time has already passed for today, calculate for tomorrow        
            if current_time_eastern > target_time_eastern:
                target_time_eastern = target_time_eastern + timedelta(days=1)        
            utc=pytz.utc        
            target_time_utc=target_time_eastern.astimezone(utc)
            epoch_time = int(target_time_utc.timestamp())
        except:
            ""
        return epoch_time
       