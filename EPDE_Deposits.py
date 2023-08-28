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
class DepositsManager(object):
    logger=LogManger()
    err_handler=ErrorHandler()
    region='us-east-1'
    table_Per_Store_Post_Deposit=1
  
    def __init__(self,region,table_Per_Store_Post_Deposit1=1):  
        DepositsManager.region=region
        DepositsManager.table_Per_Store_Post_Deposit=table_Per_Store_Post_Deposit1
    def __get_string_size_in_bytes(self, message_body):
       return len(message_body.encode('utf-8'))
    
    def __is_large(self, message):     
        msg_body_size = self.__get_string_size_in_bytes(self,message_body=str(message))
        return (msg_body_size >= 400000)

    @classmethod     
    def GetTableName(self,store_code):
        TableName=store_code+"_POST_DEPOSIT"      
       
        return TableName
    @classmethod
    def GetDepositsEntry(self,store_code,fileId):
        try:
            _moduleNM="DepositsManager"
            _functionNM="GetDepositsEntry"
            self.logger.debug("GetDepositsEntry>> store_code="+str(store_code)+",fileId="+str(fileId))           
            dynamodb = boto3.resource('dynamodb', region_name=DepositsManager.region)
            TableName=self.GetTableName(store_code)
            table = dynamodb.Table(TableName)
            response = table.get_item(Key={'file_id': fileId},ConsistentRead=True)
            try:
                item = response['Item']
                if 'request_json' in item:
                    item['request_json']=self.readLargeData(data=item['request_json'])
                if 'response_json' in item:
                    item['response_json']=self.readLargeData(data=item['response_json'])  
                if 'process_logs'  in item: 
                    item['process_logs']= self.readLargeData(data=item['process_logs'])                    
                return { "status":True,"item": item } 
            except KeyError as kerr:         
               return self.err_handler.HandleAppError(356,moduleNM=_moduleNM,functionNM=_functionNM)   
        except Exception as e:
               return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
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
        #self.logger.debug("GetProcessLogs >> data="+str(data))
        if data is None or len(data)==0 :
            return data
        if isinstance(data, (str)):   
            return data    
        if 'EPDELargePayload' in data :           
            large_pay_load_attribute_value = data.get('EPDELargePayload',None)
            #self.logger.debug("GetProcessLogs >> large_pay_load_attribute_value="+str(large_pay_load_attribute_value))
            if large_pay_load_attribute_value:
                compressed_data= data.get('EPDELargePayload',None)
                decompressed_data = gzip.decompress(compressed_data)  
                if isinstance(decompressed_data, (bytes, bytearray)):
                    decompressed_data=decompressed_data.decode()   
            else:
                decompressed_data=data
        else:
             decompressed_data=data
        return decompressed_data 

    @classmethod
    def GetProcessLogs(self,store_code,fileId):
        process_logs=[]
        try:
            _moduleNM="DepositsManager"
            _functionNM="GetProcessLogs"
            self.logger.debug("GetProcessLogs>> store_code="+str(store_code)+",fileId="+str(fileId))            
            dynamodb = boto3.resource('dynamodb', region_name=DepositsManager.region)
            TableName=self.GetTableName(store_code)
            table = dynamodb.Table(TableName)
            response = table.get_item(Key={'file_id': fileId},ConsistentRead=True)
            #self.logger.debug("GetProcessLogs >> response="+str(response))
            try:
                item = response['Item']                 
                if 'process_logs'  in item: 
                    process_logs=self.readLargeData(data=item['process_logs'])                 
                return process_logs
            except KeyError as kerr:                     
               return process_logs  
        except Exception as e:                    
               return process_logs    
    @classmethod
    def SavePostDeposits(self,post_dict):  
        _moduleNM="DepositsManager"
        _functionNM="SavePostDeposits"
        try:  
            isUpdate=False      
            self.logger.debug("SavePostDeposits >table_Per_Store_Post_Deposit="+str(DepositsManager.table_Per_Store_Post_Deposit)) 
           
            self.logger.debug("SavePostDeposits >> post_dict="+str(post_dict))
            if post_dict['file_id'] is None:
                fileId=self.GetFileId()            
                res=self.GetDepositsRequest(fileId,post_dict['request_json'],post_dict['store_code'])
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
            # Get the service resource. 
            # 
            #Added 11/29/2022 for keep Processing History for each Batch Close RO Request    
            lt = datetime.datetime.now()
            log_time = lt.timestamp()
            process_logs=[]
            process_log={"log_time":str(log_time), "request_status":post_dict['request_status'],"comments":"Request Created"}
            process_logs.append(process_log)                       
            dynamodb = boto3.resource('dynamodb', region_name=DepositsManager.region)
            store_code=post_dict['store_code']
            TableName=self.GetTableName(store_code)
            table = dynamodb.Table(TableName)            
            with table.batch_writer(overwrite_by_pkeys=[ 'file_id']) as batch:
                batch.put_item(
                                Item={
                                "file_id": str(fileId),
                                "store_code": str(store_code),                               
                                "deposit_ref_number": str(post_dict['depositReferenceNumber']),
                                "total_deposit_amount": str(post_dict['totalDepositAmount']),
                                "request_status": post_dict['request_status'],                               
                                "request_json": self.writeLargeData(data=requestJSON),
                                "response_json":self.writeLargeData(data=post_dict['response_json']), 
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
                self.logger.debug("Post Deposits Request Created file_id:"+fileId+",request_status:"+post_dict['request_status'])
            else:
                self.logger.debug("Post Deposits Request Updated file_id:"+fileId+",request_status:"+post_dict['request_status'])
            return { "status":True,"file_id": fileId,"deposit_request":requestJSON }          
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
    def GetDepositsRequest(self,fileId,Deposits,store_code):
        _moduleNM="DepositsManager"
        _functionNM="GetDepositsRequest"
        try:
            self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)    
            self.logger.debug("Deposits:"+str(Deposits))
            att={"dealerCode": store_code}
            Deposits.update(att)
            att={"fileId": fileId}
            Deposits.update(att)
            att={"api": "postdeposits"}
            Deposits.update(att)
            #self.logger.debug("PaymentsUpdated:"+str(Payments));
            return {"status":True,"request_json": Deposits}
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
     
    @classmethod
    def UpdatePostDeposits(self,store_code,fileId,req_status,response_json):
        _moduleNM="DepositsManager"
        _functionNM="UpdatePostDeposits"
        ct = datetime.datetime.now()
        res_ts = ct.timestamp()
        try:
            self.logger.debug("UpdatePostDeposits>> store_code="+str(store_code)+",req_status:"+req_status+",fileId:"+fileId)
            #Added 11/30/2022 for keep Processing History for each UpdatePostDeposits Request    
            process_logs=self.GetProcessLogs(store_code=store_code,fileId=fileId)
            comments="Update request_status="+str(req_status)
            process_log={"log_time":str(res_ts), "request_status":str(req_status),"comments":comments}
            process_logs.append(process_log)
            dynamodb = boto3.resource('dynamodb', region_name=DepositsManager.region)
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
                                ':rspj':self.writeLargeData(response_json),
                                ':plogs':self.writeLargeData(data= process_logs),   
                                
                            },
                            ReturnValues="UPDATED_NEW"         
                        )
            #self.logger.debug("response:"+str(response))
            return { "status":True }             
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
    
    @classmethod
    def UpdatePostDepositsTimeOut(self,store_code,fileId,req_status,response_json):
        _moduleNM="DepositsManager"
        _functionNM="UpdatePostDepositsTimeOut"
        ct = datetime.datetime.now()
        res_ts = ct.timestamp()
        try:
            self.logger.debug("UpdatePostDepositsTimeOut>> store_code="+str(store_code)+",req_status:"+req_status+",fileId:"+fileId)
            #Added 11/30/2022 for keep Processing History for each UpdatePostDeposits Request    
            process_logs=self.GetProcessLogs(store_code=store_code,fileId=fileId)
            comments="update timeOut=True,request_status="+str(req_status)
            process_log={"log_time":str(res_ts), "request_status":str(req_status),"comments":comments}
            process_logs.append(process_log)            
            dynamodb = boto3.resource('dynamodb', region_name=DepositsManager.region)
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
                                ':rspj':self.writeLargeData(response_json),
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
    def UpdatePostDepositsRequestStatus(self,store_code,fileId,req_status):
        _moduleNM="DepositsManager"
        _functionNM="UpdatePostDepositsRequestStatus"
        ct = datetime.datetime.now()
        res_ts = ct.timestamp()
        try:
            self.logger.debug("UpdatePostDepositsRequestStatus>> store_code="+store_code+",req_status:"+req_status+",fileId:"+fileId)
            #Added 11/30/2022 for keep Processing History for each UpdatePostDeposits Request    
            process_logs=self.GetProcessLogs(store_code=store_code,fileId=fileId)
            comments="update request_status ="+str(req_status)
            process_log={"log_time":str(res_ts),"request_status":str(req_status),"comments":comments}
            process_logs.append(process_log)
            dynamodb = boto3.resource('dynamodb', region_name=DepositsManager.region)
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
    def UpdatePostDepositsRetryCount(self,store_code,file_id,retry_count):
        _moduleNM="DepositsManager"
        _functionNM="UpdatePostDepositsRetryCount"
        try:
            self.logger.debug("UpdatePostDepositsRetryCount file_id:"+str(file_id))            
            ct = datetime.datetime.now()
            res_ts = ct.timestamp()
            #Added 11/30/2022 for keep Processing History for each Deposit Request    
            process_logs=self.GetProcessLogs(store_code=store_code,fileId=file_id)
            comments="update retry_count ="+str(retry_count)
            request_status=''
            if len(process_logs)>0:
               request_status=process_logs[len(process_logs)-1]["request_status"]                
            process_log={"log_time":str(res_ts),"request_status":request_status,"comments":comments}
            process_logs.append(process_log)
            dynamodb = boto3.resource('dynamodb', region_name=DepositsManager.region)
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
    def Validate_PostDeposits_inputs(self,_deposits,auth_json,store_code):
        _moduleNM="DepositsManager"
        _functionNM="Validate_PostDeposits_inputs"
        deposits=_deposits
        res_handler=ResponseHandler()
        try:
            self.logger.debug("Validate_PostDeposits_inputs >>store_code:"+str(store_code)+",_deposits="+str(_deposits))
            if (deposits is None ):       
                return res_handler.GetErrorResponseJSON(code=308,auth_json=auth_json)
            try:    
                depositReferenceNumber= deposits['depositReferenceNumber']
            except KeyError:
                depositReferenceNumber=None
            if (depositReferenceNumber is None ) or depositReferenceNumber == "" or len(depositReferenceNumber.strip())==0:
                return res_handler.GetErrorResponseJSON(code=310,auth_json=auth_json)
            try:    
                totalDepositAmount= deposits['totalDepositAmount']
            except KeyError:
                totalDepositAmount=None    
         
            #if (totalDepositAmount is None ) or totalDepositAmount == "" or len(totalDepositAmount.strip())==0:
            #    return res_handler.GetErrorResponseJSON(code=320,auth_json=auth_json)  
            try:                     
                depositDetails=deposits['depositDetails']
            except KeyError:
                depositDetails=None
            if depositDetails is None or len(depositDetails)==0:
                return res_handler.GetErrorResponseJSON(code=327,auth_json=auth_json) 
            
            responseCode=0 
            totalAmount=0.0
            depositresponse=[]
            store_detail=None
            for detail in depositDetails:
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
                depositresponse.append(pRes)

            if responseCode==-1:
                resjson={
                        "responseCode": responseCode,
                        "depositReferenceNumber": depositReferenceNumber,
                        "totalDepositAmount": totalDepositAmount,
                        "depositResponse": depositresponse, 
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
                        "depositReferenceNumber": depositReferenceNumber,
                        "totalDepositAmount": totalDepositAmount,
                        "depositResponse": depositresponse,                    
                        "auth_token": auth_json 
                }
           
            #if totalDepositAmount is not None:
            if False:
                try : 
                    amountdue=float(totalDepositAmount )
                    if amountdue != totalAmount:
                        list=[str(totalAmount),str(amountdue)]
                        return res_handler.GetFormattedErrorResponseJSON(code=334,auth_json=auth_json,args=list)
                        
                except :
                        c=""
        
            app_client=AppClient()
            store_detail_resp=app_client.GetStoreDetail(store_code,DepositsManager.region)  
            if store_detail_resp['status']:  
               store_detail=store_detail_resp['item']
            return { "responseCode":0,"deposit_request": deposits,"deposit_response":resjson,"store_detail":store_detail } 
        except Exception as e:            
            res= self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
            return res_handler.GetErrorResponseJSON(code=res['error_code'],auth_json=auth_json) 
      
    @classmethod
    def PollPostDepositResponse(self,store_code,file_id,auth_json,RequestTimeOutInSecond=20,ResponseQueueName="EPDE_PostPayment_Response.fifo",region="us-east-1",MaxMsgCount=1,waitTimeInSecond=10):
       #_moduleNM="DepositManager"
       # _functionNM="PollPostDepositResponse"
       # self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM) 
        res_handler=ResponseHandler()
        timeOut=RequestTimeOutInSecond
        time_c=1
        sqs= SQSManager(QueueName= ResponseQueueName,region=region,MaxMsgCount=MaxMsgCount,waitTimeInSecond=waitTimeInSecond)   
        while time_c<=timeOut:
            if False:    
                entry=self.GetDepositsEntry(store_code,file_id)
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
                response_json=sqs.fetch_response("postdeposits",file_id)
                time_ed=datetime.datetime.now()
                time_c1=(time_ed-time_st).total_seconds()               
                if response_json is not None :
                    return {"request_status":"RESPONSE_UPLOADED","response_json":response_json} 
                time_c=time_c+time_c1+1                 
                time.sleep(1)

        res=res_handler.GetErrorResponseJSON(code=312,auth_json=auth_json)
        return {"request_status":"TIMEOUT","response_json":res}     
    @classmethod
    def buildDepositObject(self,validate_resp,store_code,request_status,req_time,res_time,Is_WaitForResponse):
        post_dict={}    
        post_dict['file_id']=None     
        post_dict['req_time']=req_time   
        post_dict['request_status']=request_status   
        post_dict['res_time']=res_time
        post_dict['parent_file_id']=None
        post_dict['retry_count']=0
        post_dict['store_code']=store_code
        store_detail=None
        try:
            store_detail=validate_resp['store_detail']
        except:  
            store_detail=None
            

         
        try:
            response_json =  validate_resp['deposit_response']  
            post_dict['response_json']=response_json
        except:  
            post_dict['response_json']=None
      
        try:
            request_json=validate_resp['deposit_request']
            post_dict['request_json']=request_json
            post_dict['totalDepositAmount']=request_json['totalDepositAmount']
            post_dict['depositReferenceNumber']=request_json['depositReferenceNumber']
        except:   
            post_dict['request_json']=""
            post_dict['depositReferenceNumber']=""
            post_dict['totalDepositAmount']=""
            
        try:
            post_dict['api_key']=store_detail['api_key']
            
           
        except:
            post_dict['api_key']=""
            
        """ try:
          
            post_dict['store_code']=store_detail['store_code']
           
        except:
           
            post_dict['store_code']="" """
        return post_dict
    @classmethod         
    def isRetryAllow(self,store_detail):
        #_moduleNM="DepositManager"
        #_functionNM="isRetryAllow"
        #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM) 
        initiateRetry=False
        try:
            isRetryActive= store_detail['is_retry_active_deposit']
            MaxRetryCount=int(store_detail['retry_max_count_deposit'])
            if isRetryActive and MaxRetryCount>0:            
                    initiateRetry=True
        except Exception as e:    
            initiateRetry=False
        return initiateRetry  
    @classmethod
    def GetPostDepositRequestList(self,store_code,create_date):
        _moduleNM="DepositManager"
        _functionNM="GetPostDepositRequestList"
        try:
            self.logger.debug("GetPostDepositRequestList>> store_code="+str(store_code)+",create_date="+str(create_date))           
            dynamodb = boto3.resource('dynamodb', region_name=DepositsManager.region)
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
                                if 'process_logs'  in item: 
                                    item['process_logs']= self.readLargeData(data=item['process_logs'])      
                                 
                    return { "status":True,"items": items }
            except:
                    return { "status":True,"items": [] }  
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
    
  