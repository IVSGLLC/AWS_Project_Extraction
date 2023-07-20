import datetime
import gzip
import time
import random
import boto3
from boto3.dynamodb.conditions import Attr
from EPDE_Client import AppClient
from EPDE_Logging import LogManger
from decimal import Decimal
from EPDE_Error import ErrorHandler
import string
from EPDE_Response import ResponseHandler
from EPDE_SQS import SQSManager

#This Class  is responsible to handle Post payment operation
class AccountingManager(object):
    logger=LogManger()
    err_handler=ErrorHandler()
    region='us-east-1'
    table_Per_Store_Post_Accounting=1
  
    def __init__(self,region,table_Per_Store=1):  
        AccountingManager.region=region
        AccountingManager.table_Per_Store_Post_Accounting=table_Per_Store
    def __get_string_size_in_bytes(self, message_body):
       return len(message_body.encode('utf-8'))
    
    def __is_large(self, message):     
        msg_body_size = self.__get_string_size_in_bytes(self,message_body=str(message))
        return (msg_body_size >= 400000)
    @classmethod
    def writeLargeData(self,data):
        if data is None or len(data)==0 :
            return data
        if self.__is_large(self,message=str(data)):
           byte_data = str.encode(str(data))  
           compressed_data = gzip.compress(byte_data)  
           payLoad= {"EPDELargePayload":compressed_data}
           return payLoad          
        else:
            return data     
          
    @classmethod
    def readLargeData(self,data):
        if data is None or len(data)==0 :
            return data
        if isinstance(data, (str)):   
            return data               
        large_pay_load_attribute_value = data.get('EPDELargePayload',None)
        if large_pay_load_attribute_value:
            compressed_data= data.get('EPDELargePayload',None)
            decompressed_data = gzip.decompress(compressed_data)  
            if isinstance(decompressed_data, (bytes, bytearray)):
                decompressed_data=decompressed_data.decode()   
        else:
            decompressed_data=data
        return decompressed_data       
    @classmethod     
    def GetTableName(self,store_code):
        TableName=store_code+"_POST_ACCOUNTING"      
       
        return TableName
    @classmethod
    def GetPostAccountingEntry(self,store_code,fileId):
        try:
            _moduleNM="AccountingManager"
            _functionNM="GetPostAccountingEntry"
            self.logger.debug("GetPostAccountingEntry>> store_code="+str(store_code)+",fileId="+str(fileId))
           
            dynamodb = boto3.resource('dynamodb', region_name=AccountingManager.region)
            TableName=self.GetTableName(store_code)
            table = dynamodb.Table(TableName)
            response = table.get_item(Key={'file_id': fileId},ConsistentRead=True)
            try:
                item = response['Item']
                if 'request_json' in item:
                    item['request_json']=self.readLargeData(data=item['request_json'])
                if 'response_json' in item:
                    item['response_json']=self.readLargeData(data=item['response_json'])
                return { "status":True,"item": item } 
            except KeyError as kerr:         
               return self.err_handler.HandleAppError(356,moduleNM=_moduleNM,functionNM=_functionNM)   
        except Exception as e:
               return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
    @classmethod
    def GetProcessLogs(self,store_code,fileId):
        process_logs=[]
        try:
            _moduleNM="AccountingManager"
            _functionNM="GetProcessLogs"
            self.logger.debug("GetProcessLogs>> store_code="+str(store_code)+",fileId="+str(fileId))            
            dynamodb = boto3.resource('dynamodb', region_name=AccountingManager.region)
            TableName=self.GetTableName(store_code)
            table = dynamodb.Table(TableName)
            response = table.get_item(Key={'file_id': fileId},ConsistentRead=True)
            try:
                item = response['Item'] 
                if 'process_logs'  in item: 
                    process_logs=self.readLargeData(item['process_logs'])
                return process_logs
            except KeyError as kerr:         
               return process_logs  
        except Exception as e:
               return process_logs     
    @classmethod
    def SavePostAccounting(self,post_dict):  
        _moduleNM="AccountingManager"
        _functionNM="SavePostAccounting"
        try:  
            isUpdate=False      
            self.logger.debug("SavePostAccounting >table_Per_Store_Post_Accounting="+str(AccountingManager.table_Per_Store_Post_Accounting)) 
           
            self.logger.debug("SavePostAccounting >> post_dict="+str(post_dict))
            if post_dict['file_id'] is None:
                fileId=self.GetFileId()            
                res=self.GetAccountingRequest(fileId,post_dict['request_json'],post_dict['store_code'])
                if res['status']==False:
                   return res
                requestJSON=res['request_json']  
                issued = datetime.datetime.now()
                create_date = issued.strftime("%Y-%m-%d")
                 
            else:
              fileId = post_dict['file_id']
              requestJSON=post_dict['request_json']  
              isUpdate=True 
              try:
                create_date= post_dict['create_date']     
              except:
                issued = datetime.datetime.now()
                create_date = issued.strftime("%Y-%m-%d")

          
            is_retry_request=False
            retry_count=0
            try:
                if not (post_dict['parent_file_id'] is None) and  post_dict['parent_file_id'] != "":
                    try:
                        retry_count=post_dict['retry_count']
                    except:
                        retry_count=0
                    is_retry_request=True
                else:
                    is_retry_request=False
            except :
                is_retry_request=False
            
            is_timeout=False
            try:
                is_timeout=post_dict['is_timeout']
            except:
                is_timeout=False

            store_code=post_dict['store_code']

             #Added 11/29/2022 for keep Processing History for each Batch Close RO Request    
            lt = datetime.datetime.now()
            log_time = lt.timestamp()
            process_logs=[]
            process_log={"log_time":str(log_time), "request_status":post_dict['request_status'],"response_json":post_dict['response_json'],"comments":"Request Created"}
            process_logs.append(process_log)

            # Get the service resource.             
            dynamodb = boto3.resource('dynamodb', region_name=AccountingManager.region)          
            TableName=self.GetTableName(store_code)
            table = dynamodb.Table(TableName)            
            with table.batch_writer(overwrite_by_pkeys=[ 'file_id']) as batch:
                batch.put_item(
                                Item={
                                "file_id": str(fileId),
                                "store_code": str(store_code),                               
                                "accounting_ref_number": str(post_dict['accountingReferenceNumber']),
                                "total_accounting_amount": str(post_dict['totalAccountingAmount']),
                                "request_status": post_dict['request_status'],                               
                                "request_json": self.writeLargeData(data=requestJSON),
                                "response_json":self.writeLargeData(data= post_dict['response_json']),
                                "api_key":post_dict['api_key'],
                                "req_time": str(post_dict['req_time']) ,
                                "res_time": str(post_dict['res_time']) ,
                                "create_date":str(create_date),
                                "is_timeout": is_timeout,      
                                "is_retry_request":is_retry_request,
                                "retry_count":Decimal(retry_count),
                                "parent_file_id": post_dict['parent_file_id'], 
                                "process_logs":self.writeLargeData(data= process_logs)                                                      
                                }
                            )
            if isUpdate==False:
                self.logger.debug("Post Accounting Request Created file_id:"+fileId+",request_status:"+post_dict['request_status'])
            else:
                self.logger.debug("Post Accounting Request Updated file_id:"+fileId+",request_status:"+post_dict['request_status'])
            return { "status":True,"file_id": fileId,"accounting_request":requestJSON }          
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
    @classmethod
    def GetFileId(self):
        # initializing size of string 
        N = 10        
        # using random.choices()
        # generating random strings 
        res = ''.join(random.choices(string.ascii_uppercase +
                                    string.digits, k = N))
        return res
        
    @classmethod
    def GetAccountingRequest(self,fileId,accounting,store_code):
        _moduleNM="AccountingManager"
        _functionNM="GetAccountingRequest"
        try:
            self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)    
            self.logger.debug("Accounting:"+str(accounting))
            
            if "dealerCode" in accounting:
                accounting["dealerCode"]=store_code
            else:
                att={"dealerCode": store_code}
                accounting.update(att)

            if "fileId" in accounting:   
                accounting["fileId"]=fileId
            else:
                att={"fileId": fileId}
                accounting.update(att)  

            if "api" in accounting:   
                accounting["api"]="postaccounting"
            else:
                att={"api": "postaccounting"}
                accounting.update(att)  
            
            #self.logger.debug("PaymentsUpdated:"+str(Payments));
            return {"status":True,"request_json": accounting}
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
       
    @classmethod
    def UpdatePostAccounting(self,store_code,fileId,req_status,response_json):
        _moduleNM="AccountingManager"
        _functionNM="UpdatePostAccounting"
        ct = datetime.datetime.now()
        res_ts = ct.timestamp()
        try:
            self.logger.debug("UpdatePostAccounting>> store_code="+str(store_code)+",req_status:"+req_status+",fileId:"+fileId)
            #Added 11/30/2022 for keep Processing History for each UpdatePostDeposits Request    
            process_logs=self.GetProcessLogs(store_code=store_code,fileId=fileId)
            comments="Update request_status="+str(req_status)
            process_log={"log_time":str(res_ts), "request_status":str(req_status),"response_json":response_json,"comments":comments}
            process_logs.append(process_log)

            dynamodb = boto3.resource('dynamodb', region_name=AccountingManager.region)
            TableName=self.GetTableName(store_code)
            table = dynamodb.Table(TableName)
            response = table.update_item(
                            Key={
                                'file_id': fileId
                            },
                            UpdateExpression="set res_time=:ut,request_status=:ros,response_json=:rspj,process_logs=:plogs",
                            ExpressionAttributeValues={
                                ':ut': str(res_ts),
                                ':ros': str(req_status),
                                ':rspj':self.writeLargeData(data=response_json),
                                ':plogs':self.writeLargeData(data= process_logs),   
                                
                            },
                            ReturnValues="UPDATED_NEW"         
                        )
            #self.logger.debug("response:"+str(response))
            return { "status":True } 
            
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
    
    @classmethod
    def UpdatePostAccountingTimeOut(self,store_code,fileId,req_status,response_json):
        _moduleNM="AccountingManager"
        _functionNM="UpdatePostAccountingTimeOut"
        ct = datetime.datetime.now()
        res_ts = ct.timestamp()
        try:
            self.logger.debug("UpdatePostAccountingTimeOut>> store_code="+str(store_code)+",req_status:"+req_status+",fileId:"+fileId)
             #Added 11/30/2022 for keep Processing History for each UpdatePostDeposits Request    
            process_logs=self.GetProcessLogs(store_code=store_code,fileId=fileId)
            comments="update timeOut=True,request_status="+str(req_status)
            process_log={"log_time":str(res_ts), "request_status":str(req_status),"response_json":response_json,"comments":comments}
            process_logs.append(process_log)

            dynamodb = boto3.resource('dynamodb', region_name=AccountingManager.region)
            TableName=self.GetTableName(store_code)
            table = dynamodb.Table(TableName)
            response = table.update_item(
                            Key={
                                'file_id': fileId
                            },
                            UpdateExpression="set res_time=:ut,request_status=:ros,response_json=:rspj,is_timeout=:tt,process_logs=:plogs",
                            ExpressionAttributeValues={
                                ':ut': str(res_ts),
                                ':ros': str(req_status),
                                ':rspj':self.writeLargeData(data=response_json),
                                ':tt':True,
                                ':plogs':self.writeLargeData(data= process_logs),  
                            },
                            ReturnValues="UPDATED_NEW"         
                        )
            self.logger.debug("response:"+str(response))
            return { "status":True }             
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
    @classmethod
    def UpdatePostAccountingRequestStatus(self,store_code,fileId,req_status):
        _moduleNM="AccountingManager"
        _functionNM="UpdatePostAccountingRequestStatus"
        ct = datetime.datetime.now()
        res_ts = ct.timestamp()
        try:
            self.logger.debug("UpdatePostAccountingRequestStatus>> store_code="+store_code+",req_status:"+req_status+",fileId:"+fileId)
             #Added 11/30/2022 for keep Processing History for each UpdatePostDeposits Request    
            process_logs=self.GetProcessLogs(store_code=store_code,fileId=fileId)
            comments="update request_status ="+str(req_status)
            process_log={"log_time":str(res_ts), "request_status":str(req_status),"response_json":"","comments":comments}
            process_logs.append(process_log)
            dynamodb = boto3.resource('dynamodb', region_name=AccountingManager.region)
            TableName=self.GetTableName(store_code)
            table = dynamodb.Table(TableName)
            response = table.update_item(
                            Key={
                                'file_id': fileId
                            },
                            UpdateExpression="set res_time=:ut,request_status=:ros,process_logs=:plogs",
                            ExpressionAttributeValues={
                                ':ut': str(res_ts),
                                ':ros': str(req_status),
                                ':plogs':self.writeLargeData(data= process_logs),  
                             
                            },
                            ReturnValues="UPDATED_NEW"         
                        )
            self.logger.debug("response:"+str(response))
            return { "status":True }             
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
    
    @classmethod
    def UpdatePostAccountingRetryCount(self,store_code,file_id,retry_count):
        _moduleNM="AccountingManager"
        _functionNM="UpdatePostAccountingRetryCount"
        try:
            self.logger.debug("UpdatePostAccountingRetryCount file_id:"+str(file_id))            
            ct = datetime.datetime.now()
            res_ts = ct.timestamp()
            #Added 11/30/2022 for keep Processing History for each Deposit Request    
            process_logs=self.GetProcessLogs(store_code=store_code,fileId=file_id)
            comments="update retry_count ="+str(retry_count)
            process_log={"log_time":str(res_ts),"request_status":"","response_json":"","comments":comments}
            process_logs.append(process_log)            
            dynamodb = boto3.resource('dynamodb', region_name=AccountingManager.region)
            TableName=self.GetTableName(store_code)
            table = dynamodb.Table(TableName)
            response = table.update_item(
                            Key={
                                'file_id': file_id                                
                            },
                            UpdateExpression="set retry_count=:rc,res_ts=:ut,process_logs=:plogs",
                            ExpressionAttributeValues={                                
                                ':rc': retry_count,
                                ':ut': str(res_ts),                             
                                ':plogs':self.writeLargeData(data= process_logs),
                            },
                            ReturnValues="UPDATED_NEW"         
                    )
            return { "status":True,"response": response }           
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
    
    @classmethod
    def Validate_PostAccounting_inputs(self,_accounting,auth_json,store_code):
        _moduleNM="AccountingManager"
        _functionNM="Validate_PostAccounting_inputs"
        accounting=_accounting
        res_handler=ResponseHandler()
        try:
            self.logger.debug("Validate_PostAccounting_inputs >>store_code:"+str(store_code)+",_Accounting="+str(_accounting))
            if (accounting is None ):       
                return res_handler.GetErrorResponseJSON(code=308,auth_json=auth_json)
            try:    
                accountingReferenceNumber= accounting['accountingReferenceNumber']
            except KeyError:
                accountingReferenceNumber=None
            if (accountingReferenceNumber is None ) or accountingReferenceNumber == "" or len(accountingReferenceNumber.strip())==0:
                return res_handler.GetErrorResponseJSON(code=310,auth_json=auth_json)
            try:    
                totalAccountingAmount= accounting['totalAccountingAmount']
            except KeyError:
                totalAccountingAmount=None    
         
            #if (totalAccountingAmount is None ) or totalAccountingAmount == "" or len(totalAccountingAmount.strip())==0:
            #    return res_handler.GetErrorResponseJSON(code=320,auth_json=auth_json)  
            try:                     
                accountingDetails=accounting['accountingDetails']
            except KeyError:
                accountingDetails=None
            if accountingDetails is None or len(accountingDetails)==0:
                return res_handler.GetErrorResponseJSON(code=327,auth_json=auth_json) 
            
            responseCode=0 
            totalAmount=0.0
            accountingresponse=[]
            store_detail=None
            for detail in accountingDetails:
                is_referenceNumberStr_Null= False
                referenceNumberStr = str(detail['referenceNumber'])
                if (referenceNumberStr is None ) or referenceNumberStr == "" or len(referenceNumberStr.strip())==0:
                   is_referenceNumberStr_Null=True

                is_amountStr_Null= False
                is_amount_NAN=False
                amountStr = str(detail['amount'])
                if (amountStr is None ) or amountStr == "" or len(amountStr.strip())==0:
                    is_amountStr_Null=True   
                else:
                    amount = 0.0
                    try : 
                        amount= float(amountStr)
                        totalAmount=totalAmount+amount
                        is_amount_NAN=False
                    except :
                        is_amount_NAN = True

                is_department_Null= False
                department = str(detail['department'])
                if (department is None ) or department == "" or len(department.strip())==0:
                   is_department_Null=True
                errorCode=0

                #if is_referenceNumberStr_Null or is_amountStr_Null or is_department_Null or is_amount_NAN:
                if is_referenceNumberStr_Null or  is_department_Null:    
                   responseCode=-1
                   errorCode=345

                pRes={
                            "referenceNumber":detail['referenceNumber'],
                            "amount": detail['amount'],
                            "department": detail['department'],
                            "errorCode": errorCode
                        }
                accountingresponse.append(pRes)

            if responseCode==-1:
                resjson={
                        "responseCode": responseCode,
                        "accountingReferenceNumber": accountingReferenceNumber,
                        "totalAccountingAmount": totalAccountingAmount,
                        "accountingResponse": accountingresponse, 
                        "errorList": [
                                {
                                "code": errorCode,
                                "message": self.err_handler.GetErrorMessage(errorCode)
                                }
                            ],                 
                        "auth_token": auth_json 
                }
                return resjson
            else:    
                resjson={
                        "responseCode": responseCode,
                        "accountingReferenceNumber": accountingReferenceNumber,
                        "totalAccountingAmount": totalAccountingAmount,
                        "accountingResponse": accountingresponse,                    
                        "auth_token": auth_json 
                }
           
            #if totalAccountingAmount is not None:
            if False:
                try : 
                    amountdue=float(totalAccountingAmount )
                    if amountdue != totalAmount:
                        list=[str(totalAmount),str(amountdue)]
                        return res_handler.GetFormattedErrorResponseJSON(code=334,auth_json=auth_json,args=list)
                        
                except :
                        c=""
        
            app_client=AppClient()
            store_detail_resp=app_client.GetStoreDetail(store_code,AccountingManager.region)  
            if store_detail_resp['status']:  
               store_detail=store_detail_resp['item']
            return { "responseCode":0,"accounting_request": accounting,"accounting_response":resjson,"store_detail":store_detail } 
        except Exception as e:            
            res= self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
            return res_handler.GetErrorResponseJSON(code=res['error_code'],auth_json=auth_json) 
    
    @classmethod
    def PollPostAccountingResponseOld(self,store_code,file_id,auth_json,RequestTimeOutInSecond=20):
        res_handler=ResponseHandler()
        timeOut=RequestTimeOutInSecond
        time_c=1
        while time_c<=timeOut:
            entry=self.GetPostAccountingEntry(store_code,file_id)
            if entry['status'] :
                request= entry['item']
                if  request['request_status']=="RESPONSE_UPLOADED":
                    return {"request_status":"RESPONSE_UPLOADED","response_json":request['response_json']} 
                time_c=time_c+1
                time.sleep(1)
            else:
                res=res_handler.GetErrorResponseJSON(code=312,auth_json=auth_json)
                return {"request_status":"ERROR","response_json":res} 
                         
        res=res_handler.GetErrorResponseJSON(code=312,auth_json=auth_json)
        return {"request_status":"TIMEOUT","response_json":res}  
    @classmethod
    def PollPostAccountingResponse(self,store_code,file_id,auth_json,RequestTimeOutInSecond=20,ResponseQueueName="EPDE_PostPayment_Response.fifo",region="us-east-1",MaxMsgCount=1,waitTimeInSecond=10):
       #_moduleNM="AccountingManager"
       # _functionNM="PollPostAccountingResponse"
       # self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM) 
        res_handler=ResponseHandler()
        timeOut=RequestTimeOutInSecond
        time_c=1
        sqs= SQSManager(QueueName= ResponseQueueName,region=region,MaxMsgCount=MaxMsgCount,waitTimeInSecond=waitTimeInSecond)   
        while time_c<=timeOut:
            if False:    
                entry=self.GetPostAccountingEntry(store_code,file_id)
                if entry['status'] :
                    request= entry['item']
                    if  request['request_status']=="RESPONSE_UPLOADED":
                        return {"request_status":"RESPONSE_UPLOADED","response_json":request['response_json']} 
                    time_c=time_c+1
                    time.sleep(1)
                else:
                    res=res_handler.GetErrorResponseJSON(code=312,auth_json=auth_json)
                    return {"request_status":"ERROR","response_json":res} 
            else:
                time_st=datetime.datetime.now()
                response_json=sqs.fetch_response("postAccounting",file_id)
                time_ed=datetime.datetime.now()
                time_c1=(time_ed-time_st).total_seconds()               
                if response_json is not None :
                    return {"request_status":"RESPONSE_UPLOADED","response_json":response_json} 
                time_c=time_c+time_c1+1                 
                time.sleep(1)

        res=res_handler.GetErrorResponseJSON(code=312,auth_json=auth_json)
        return {"request_status":"TIMEOUT","response_json":res}     
    @classmethod
    def buildAccountingObject(self,validate_resp,store_code,request_status,req_time,res_time):
        post_dict={}    
        post_dict['file_id']=None     
        post_dict['req_time']=req_time   
        post_dict['request_status']=request_status   
        post_dict['res_time']=res_time
        post_dict['parent_file_id']=None
        post_dict['retry_count']=0
        post_dict['store_code']=store_code

        if 'store_detail' in validate_resp:
            store_detail=validate_resp['store_detail']
        else:
            store_detail=None

        if 'accounting_response' in validate_resp:
            response_json =  validate_resp['accounting_response']
            post_dict['response_json']=response_json
        else:
             post_dict['response_json']=None
        
        if 'accounting_request' in validate_resp:
            request_json =  validate_resp['accounting_request']
            post_dict['request_json']=request_json
            if 'totalAccountingAmount' in request_json:
                post_dict['totalAccountingAmount']=request_json['totalAccountingAmount']
            else:
                  post_dict['totalAccountingAmount']=""
            if 'accountingReferenceNumber' in request_json:
                post_dict['accountingReferenceNumber']=request_json['accountingReferenceNumber']
            else:
                   post_dict['accountingReferenceNumber']=""
        else:
            post_dict['request_json']=""
            post_dict['accountingReferenceNumber']=""
            post_dict['totalAccountingAmount']=""

        if 'api_key' in store_detail:
            post_dict['api_key']=store_detail['api_key']    
        else:
             post_dict['api_key']=""
        
        return post_dict
    @classmethod         
    def isRetryAllow(self,store_detail):
        initiateRetry=False
        try:
            isRetryActive= store_detail['is_retry_active_accounting']
            MaxRetryCount=int(store_detail['retry_max_count_accounting'])
            if isRetryActive and MaxRetryCount>0:            
                    initiateRetry=True
        except Exception as e:    
            initiateRetry=False
        return initiateRetry  
    @classmethod
    def GetPostAccountingRequestList(self,store_code,create_date):
        _moduleNM="AccountingManager"
        _functionNM="GetPostAccountingRequestList"
        try:
            self.logger.debug("GetPostAccountingRequestList>> store_code="+str(store_code)+",create_date="+str(create_date))           
            dynamodb = boto3.resource('dynamodb', region_name=AccountingManager.region)
            TableName=self.GetTableName(store_code)
            table = dynamodb.Table(TableName)
            response = table.scan(
                FilterExpression= Attr('store_code').eq(store_code) & Attr('create_date').eq(create_date),
                ConsistentRead=True)
            try:
                    items = response['Items']                
                    while 'LastEvaluatedKey' in response:
                        response = table.scan(
                            FilterExpression= Attr('store_code').eq(store_code) & Attr('create_date').eq(create_date),
                            ConsistentRead=True,               
                            ExclusiveStartKey=response['LastEvaluatedKey']
                            )
                        try:
                            items.update(response['Items'])
                        except:
                            ""
                    if items is not None:
                        for item in items:
                            if 'request_json' in item:
                                item['request_json']=self.readLargeData(data=item['request_json'])
                            if 'response_json' in item:
                                item['response_json']=self.readLargeData(data=item['response_json'])
                    return { "status":True,"items": items }
            except:
                    return { "status":True,"items": [] }  
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
    
  