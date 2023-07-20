import datetime
import time
import gzip
import random
import boto3
from boto3.dynamodb.conditions import Attr
from EPDE_Client import AppClient
from EPDE_Logging import LogManger
from decimal import Decimal
from EPDE_Error import ErrorHandler
import string
from EPDE_RO import RepairOrder
from EPDE_Response import ResponseHandler
from EPDE_SQS import SQSManager
#This Class  is responsible to handle Post payment operation
class PostManger(object):
    logger=LogManger()
    err_handler=ErrorHandler()
    region='us-east-1'
    table_Per_Store_Post_Payment=1
    def __init__(self):        
        PostManger.region="us-east-1"
        PostManger.table_Per_Store_Post_Payment=1
        
    def __init__(self,region,table_Per_Store_Post_Payment=1):  
        PostManger.region=region
        PostManger.table_Per_Store_Post_Payment=table_Per_Store_Post_Payment
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
        TableName=store_code+"_POST_PAYMENT"
        return TableName
    @classmethod
    def GetPostPaymentEntry(self,store_code,fileId):
        try:
            _moduleNM="PostManger"
            _functionNM="GetPostPaymentEntry"
            self.logger.debug("GetPostPaymentEntry>> store_code="+str(store_code)+",fileId="+str(fileId))            
            dynamodb = boto3.resource('dynamodb', region_name=PostManger.region)
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
    def GetPostPaymentProcessLogs(self,store_code,fileId):
        process_logs=[]
        try:
            _moduleNM="PostManger"
            _functionNM="GetPostPaymentProcessLogs"
            self.logger.debug("GetPostPaymentProcessLogs>> store_code="+str(store_code)+",fileId="+str(fileId))            
            dynamodb = boto3.resource('dynamodb', region_name=PostManger.region)
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
    def SavePostPayment(self,post_dict):  
        _moduleNM="PostManger"
        _functionNM="SavePostPayment"
        try:  
            isUpdate=False         
            self.logger.debug("SavePostPayment >> post_dict="+str(post_dict))
            if post_dict['file_id'] is None:
                fileId=self.GetFileId()            
                res=self.GetPaymentRequest(fileId,post_dict['request_json'],post_dict['store_code'],post_dict['is_spc_ins'])
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

            try:
                if (float(post_dict['warranty_due']) > 0 ):
                    is_warranty_due=True
                else:
                    is_warranty_due=False
            except :
                    is_warranty_due=False
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
            
            root_file_id=None    
            if 'root_file_id' in post_dict:
               root_file_id= post_dict['root_file_id']
            
            store_code=post_dict['store_code']
            item={
                                "file_id": str(fileId),
                                "store_code": str(store_code),                                
                                "document_id": str(post_dict['document_id']),
                                "warranty_due": str(post_dict['warranty_due']),
                                "total_amount_due": str(post_dict['total_amount_due']),
                                "ro_status":str(post_dict['ro_status']),
                                "request_status": post_dict['request_status'],                                
                                "is_retry_request":is_retry_request,
                                "retry_count":Decimal(retry_count),
                                "parent_file_id": post_dict['parent_file_id'],
                                "request_json": self.writeLargeData(data=requestJSON),
                                "response_json":self.writeLargeData(data= post_dict['response_json']), 
                                "is_warranty_due":is_warranty_due,
                                "api_key":post_dict['api_key'],
                                "req_time": str(post_dict['req_time']) ,
                                "res_time": str(post_dict['res_time']) ,
                                "ro_status_on": str(post_dict['ro_status_on']) ,
                                "create_date":str(create_date),
                                "is_timeout": is_timeout                                                                           
                                }
            if root_file_id is not None:
                item['root_file_id']=root_file_id

            #Added 11/29/2022 for keep Processing History for each Payment Request    
            lt = datetime.datetime.now()
            log_time = lt.timestamp()
            process_logs=[]
            process_log={"log_time":str(log_time), "request_status":post_dict['request_status'],"response_json":post_dict['response_json'],"comments":"Request Created"}
            process_logs.append(process_log)
            item['process_logs']=self.writeLargeData(data=process_logs)
            

            # Get the service resource.             
            dynamodb = boto3.resource('dynamodb', region_name=PostManger.region)
            store_code=post_dict['store_code']
            TableName=self.GetTableName(store_code)
            table = dynamodb.Table(TableName)            
            with table.batch_writer(overwrite_by_pkeys=[ 'file_id']) as batch:
                batch.put_item(
                                Item=item
                            )
            if isUpdate==False:
                self.logger.debug("Post Request Created file_id:"+fileId+",request_status:"+post_dict['request_status'])
            else:
                self.logger.debug("Post Request Updated file_id:"+fileId+",request_status:"+post_dict['request_status'])
           
            #json.dumps({'b': 1, 'a': 2})
            return { "status":True,"file_id": fileId,"payment_request":requestJSON }          
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
    def GetPaymentRequest(self,fileId,Payments,store_code,is_spc_ins):
        _moduleNM="PostManger"
        _functionNM="GetPaymentRequest"
        try:
            if "dealerCode" in Payments:
                Payments["dealerCode"]=store_code
            else:
                att={"dealerCode": store_code}
                Payments.update(att)
            
            if "fileId" in Payments:   
                Payments["fileId"]=fileId
            else:
                att={"fileId": fileId}
                Payments.update(att)

            if "api" in Payments:   
                Payments["api"]="postpayments"
            else:
                att={"api": "postpayments"}
                Payments.update(att)  

            if "isSpcIns" in Payments:   
                Payments["isSpcIns"]=is_spc_ins
            else:
                att={"isSpcIns": is_spc_ins}
                Payments.update(att)     
                   
            return {"status":True,"request_json": Payments}
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)    
     
    
    @classmethod
    def UpdatePostPayment(self,store_code,fileId,req_status,response_json):
        _moduleNM="PostManger"
        _functionNM="UpdatePostPayment"
        ct = datetime.datetime.now()
        res_ts = ct.timestamp()
        try:
            #Added 11/29/2022 for keep Processing History for each Payment Request            
            process_logs=self.GetPostPaymentProcessLogs(store_code=store_code,fileId=fileId)
            comments="Update request_status="+str(req_status)
            process_log={"log_time":str(res_ts), "request_status":str(req_status),"response_json":response_json,"comments":str(comments)}
            process_logs.append(process_log)
           
            self.logger.debug("UpdatePostPayment>> store_code="+str(store_code)+",req_status:"+req_status+",fileId:"+fileId)            
            dynamodb = boto3.resource('dynamodb', region_name=PostManger.region)
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
                                ':plogs':self.writeLargeData(data=process_logs),                                
                            },
                            ReturnValues="UPDATED_NEW"         
                        )
            self.logger.debug("response:"+str(response))
            return { "status":True }             
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
    
    @classmethod
    def UpdatePostPaymentTimeOut(self,store_code,fileId,req_status,response_json):
        _moduleNM="PostManger"
        _functionNM="UpdatePostPaymentTimeOut"
        ct = datetime.datetime.now()
        res_ts = ct.timestamp()
        try:
           
            
            self.logger.debug("UpdatePostPaymentTimeOut>> store_code="+str(store_code)+",req_status:"+req_status+",fileId:"+fileId)            
           
            #Added 11/29/2022 for keep Processing History for each Payment Request            
            process_logs=self.GetPostPaymentProcessLogs(store_code=store_code,fileId=fileId)
            comments="update timeOut=True, Request_status="+str(req_status)
            process_log={"log_time":str(res_ts), "request_status":str(req_status),"response_json":response_json,"comments":str(comments)}
            process_logs.append(process_log)

            dynamodb = boto3.resource('dynamodb', region_name=PostManger.region)
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
                                ':plogs':self.writeLargeData(data=process_logs),   
                            },
                            ReturnValues="UPDATED_NEW"         
                        )
            self.logger.debug("response:"+str(response))
            return { "status":True }             
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
    @classmethod
    def UpdatePostPaymentRequestStatus(self,store_code,fileId,req_status):
        _moduleNM="PostManger"
        _functionNM="UpdatePostPaymentRequestStatus"
        ct = datetime.datetime.now()
        res_ts = ct.timestamp()
        try:
            self.logger.debug("UpdatePostPaymentRequestStatus>> store_code="+store_code+",req_status:"+req_status+",fileId:"+fileId)             
            #Added 11/29/2022 for keep Processing History for each Payment Request            
            process_logs=self.GetPostPaymentProcessLogs(store_code=store_code,fileId=fileId)
            comments="update request_status ="+str(req_status)
            process_log={"log_time":str(res_ts), "request_status":str(req_status),"response_json":"","comments":str(comments)}
            process_logs.append(process_log)

            dynamodb = boto3.resource('dynamodb', region_name=PostManger.region)
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
                                ':plogs':self.writeLargeData(data=process_logs),   
                             
                            },
                            ReturnValues="UPDATED_NEW"         
                        )
            self.logger.debug("response:"+str(response))
            return { "status":True } 
            
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
    
    @classmethod
    def UpdatePostPaymentRetryCount(self,store_code,file_id,retry_count,ro_status):
        _moduleNM="PostManger"
        _functionNM="UpdatePostPaymentRetryCount"
        try:
            self.logger.debug("UpdatePostPaymentRetryCount file_id:"+str(file_id))            
            ct = datetime.datetime.now()
            res_ts = ct.timestamp()

            #Added 11/29/2022 for keep Processing History for each Payment Request            
            process_logs=self.GetPostPaymentProcessLogs(store_code=store_code,fileId=file_id)
            comments="update retry_count ="+str(retry_count)+",ro_status="+str(ro_status)
            process_log={"log_time":str(res_ts), "request_status":"","response_json":"","comments":str(comments)}
            process_logs.append(process_log)

            dynamodb = boto3.resource('dynamodb', region_name=PostManger.region)
            TableName=self.GetTableName(store_code)
            table = dynamodb.Table(TableName)
            response = table.update_item(
                            Key={
                                'file_id': file_id                                
                            },
                            UpdateExpression="set retry_count=:rc,ro_status_on=:ut,ro_status=:ros,process_logs=:plogs",
                            ExpressionAttributeValues={                                
                                ':rc': retry_count,
                                ':ut': str(res_ts),                             
                                ':ros': ro_status,
                                ':plogs':self.writeLargeData(data=process_logs),  
                            },
                            ReturnValues="UPDATED_NEW"         
                    )
            return { "status":True,"response": response } 
          
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
    @classmethod
    def UpdatePostPaymentRetryCountRequestStatusTimeOut(self,store_code,file_id,retry_count,ro_status,req_sts,response_json):
        _moduleNM="PostManger"
        _functionNM="UpdatePostPaymentRetryCountRequestStatusTimeOut"
        try:
            self.logger.debug("UpdatePostPaymentRetryCountRequestStatusTimeOut file_id:"+str(file_id))            
            ct = datetime.datetime.now()
            res_ts = ct.timestamp()

            #Added 11/29/2022 for keep Processing History for each Payment Request            
            process_logs=self.GetPostPaymentProcessLogs(store_code=store_code,fileId=file_id)
            comments="update timeOut =True,retryCount="+str(retry_count)+"ro_status="+str(ro_status)
            process_log={"log_time":str(res_ts), "request_status":str(req_sts),"response_json":response_json,"comments":str(comments)}
            process_logs.append(process_log)

            dynamodb = boto3.resource('dynamodb', region_name=PostManger.region)
            TableName=self.GetTableName(store_code)
            table = dynamodb.Table(TableName)
            response = table.update_item(
                            Key={
                                'file_id': file_id                                
                            },
                            UpdateExpression="set retry_count=:rc,ro_status_on=:ut,ro_status=:ros,request_status=:rs,is_timeout=:tt,response_json=:rspj,process_logs=:plogs",
                            ExpressionAttributeValues={                                
                                ':rc': retry_count,
                                ':ut': str(res_ts),                             
                                ':ros': ro_status,
                                ':rs': req_sts,
                                ':tt':True,
                                ':rspj':self.writeLargeData(data=response_json),
                                ':plogs':self.writeLargeData(data=process_logs),  
                            },
                            ReturnValues="UPDATED_NEW"         
                    )
            return { "status":True,"response": response } 
          
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
    
    @classmethod
    def Validate_PostPayment_inputs(self,_payments,auth_json,store_code,tablePerStoreRODetail=1):
        _moduleNM="PostManger"
        _functionNM="Validate_PostPayment_inputs"
        payments=_payments
        res_handler=ResponseHandler()
        try:
            self.logger.debug("Validate_PostPayment_inputs >>store_code:"+str(store_code)+",_payments="+str(_payments))
            
            if (payments is None ):       
                return res_handler.GetErrorResponseJSON(code=308,auth_json=auth_json)
            try:    
                document_id= payments['documentId']
            except KeyError:
                document_id=None
            if (document_id is None ) or document_id == "" or len(document_id.strip())==0:
                return res_handler.GetErrorResponseJSON(code=310,auth_json=auth_json)
            try:    
                employee_id= payments['employeeId']
            except KeyError:
                employee_id=None    
         
            if (employee_id is None ) or employee_id == "" or len(employee_id.strip())==0:
                return res_handler.GetErrorResponseJSON(code=320,auth_json=auth_json)  
            try:    
                 status= payments['status']
            except KeyError:
                status=None       
           
            if (status is None ) or status == "" or len(status.strip())==0:
                return res_handler.GetErrorResponseJSON(code=315,auth_json=auth_json)    
            if  status.lower() != "c97" :
                list=[ str(status)]
                return res_handler.GetFormattedErrorResponseJSON(code=316,auth_json=auth_json,args=list)
            try:                     
                paymentDetails=payments['paymentDetails']
            except KeyError:
                paymentDetails=None
            if paymentDetails is None or len(paymentDetails)==0:
                return res_handler.GetErrorResponseJSON(code=327,auth_json=auth_json) 
            
            rOrder=RepairOrder(PostManger.region,tablePerStoreRODetail)
            roDetailResp=rOrder.GetRODetail(store_code,document_id,'RO')  
            if roDetailResp['status']== False:  
                #save not posted request                
                error_code=roDetailResp['error_code']
                return res_handler.GetErrorResponseJSON(error_code,auth_json)
            
            ro_detail=roDetailResp['item'] 
            self.logger.info("ro_detail="+str(ro_detail))
            dms_status=ro_detail["status"] 
             
            if  dms_status.lower() != "c97" :
                list=[ str(status),str(dms_status)]
                return res_handler.GetFormattedErrorResponseJSON(code=362,auth_json=auth_json,args=list)
           
            employee_no=ro_detail["employee_id"] 
            if (employee_no is not None ) and employee_id != employee_no :
                list=[employee_id]
                return res_handler.GetFormattedErrorResponseJSON(code=314,auth_json=auth_json,args=list)

            warrantyDue=ro_detail['warranty_due'] 
            amount_due=ro_detail['amount_due'] 
            totalAmount=0.0
            paymentresponse=[]
            store_detail=None
            ICount=0
            paymentDetailsList=[]
            for detail in paymentDetails:
                amountStr = str(detail['amount'])
                if (amountStr is None ) or amountStr == "" or len(amountStr.strip())==0:
                    return res_handler.GetErrorResponseJSON(code=319,auth_json=auth_json)     
                amount = 0.0
                try : 
                    amount= float(amountStr)
                    totalAmount=totalAmount+amount
                    res = True
                except :
                    res = False
                if not res:
                        list=[amountStr]
                        return res_handler.GetFormattedErrorResponseJSON(code=309,auth_json=auth_json,args=list)
                warrantyDue_amount=0.0
                try :                        
                    warrantyDue_amount= float(warrantyDue)                        
                except :
                    warrantyDue_amount=0.0 
                try :    
                   warrantyDueStr = str(detail['warrantyDue'])
                except :
                    warrantyDueStr=""
                if (warrantyDueStr is not None ) and  warrantyDueStr != "" and  len(warrantyDueStr.strip())>0:
                    
                    try : 
                        float(warrantyDueStr)                     
                        res = True
                    except :
                        res = False
                    if not res: 
                        detail["warrantyDue"] = str(warrantyDue_amount)
                        warrantyDueStr=""
                else:
                    
                    detail["warrantyDue"] = str(warrantyDue_amount)                
                 
                if store_detail is None:
                    app_client=AppClient()
                    store_detail_resp=app_client.GetStoreDetail(store_code,PostManger.region)  
                    if store_detail_resp['status']:  
                       store_detail=store_detail_resp['item']
                
                if not store_detail is None:
                            
                    store_detail=store_detail_resp['item']
                    posPayType=None
                    if store_detail['is_paytype_mapping_activated'] == True:
                            posPayType=None
                            try:
                                posPayType=str(detail['posPayType'])
                            except KeyError as ke:
                                posPayType=None                            
                            if (posPayType is not None ) and  posPayType != "" and  len(posPayType.strip())>0:
                                try:
                                    source= store_detail['pay_type_mapping'][posPayType]
                                except KeyError:
                                    source=None
                                if (source is not None ) and  source != "" and  len(source.strip())>0:
                                    detail["transactionSource"] = source
                                else:
                                    list=[posPayType]
                                    return res_handler.GetFormattedErrorResponseJSON(code=318,auth_json=auth_json,args=list)
                                       
                    transactionsource=detail["transactionSource"]
                    if (transactionsource is None ) or transactionsource == "" or len(transactionsource.strip())==0:
                       return res_handler.GetErrorResponseJSON(code=317,auth_json=auth_json)                                      
                    if store_detail['is_transactionsource_mapping_activated'] == True:
                      
                        try:
                            tsource= store_detail['transaction_source_mapping'][transactionsource.upper()]
                        except KeyError:
                            tsource=None
                        if (tsource is not None ) and  tsource != "" and  len(tsource.strip())>0:
                            detail["transactionSource"] = tsource
                        else:
                            list=[transactionsource]
                            return res_handler.GetFormattedErrorResponseJSON(code=328,auth_json=auth_json,args=list)
                    
                pRes={
                            "amount": detail['amount'],
                            "warrantyDue": detail['warrantyDue'],
                            "paymentStatus": 0
                        }
                paymentresponse.append(pRes)
                if 'posPayType' not in detail:
                    detail['posPayType']=None
                 
                paymentDetailsList.append(detail)
            #loop end
            payments['paymentDetails']=paymentDetailsList
            try : 
                amountdue=float(amount_due )
                if amountdue != totalAmount:
                    list=[str(totalAmount),str(amountdue)]
                    return res_handler.GetFormattedErrorResponseJSON(code=334,auth_json=auth_json,args=list)
           
           
            except :
                     c=""
            resjson={
                    "responseCode": 0,
                    "documentId": payments['documentId'],
                    "paymentResponse": paymentresponse,
                    "auth_token": auth_json 
            }
            return { "responseCode":0,"payment_request": payments,"ro_detail":ro_detail,"payment_response":resjson,"store_detail":store_detail } 
        except Exception as e:            
            res= self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
            return res_handler.GetErrorResponseJSON(code=res['error_code'],auth_json=auth_json) 
   
    @classmethod
    def PollPostPaymentResponse(self,store_code,file_id,auth_json,RequestTimeOutInSecond=20,ResponseQueueName="EPDE_PostPayment_Response.fifo",region="us-east-1",MaxMsgCount=1,waitTimeInSecond=20):
       # _moduleNM="PostManger"
       # _functionNM="PollPostPaymentResponse"
       # self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM) 
        res_handler=ResponseHandler()
        timeOut=RequestTimeOutInSecond
        time_c=1
        sqs= SQSManager(QueueName= ResponseQueueName,region=region,MaxMsgCount=MaxMsgCount,waitTimeInSecond=waitTimeInSecond)   
        self.logger.debug("PollPostPaymentResponse time out:"+str(timeOut))
        while time_c<=timeOut:
            if False:    
                entry=self.GetPostPaymentEntry(store_code,file_id)
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
                response_json=sqs.fetch_response("postpayments",file_id)
                time_ed=datetime.datetime.now()
                time_c1=(time_ed-time_st).total_seconds()
                
                if response_json is not None :
                    return {"request_status":"RESPONSE_UPLOADED","response_json":response_json} 
                time_c=time_c+time_c1+1
                
                time.sleep(1)
        
       
        res=res_handler.GetErrorResponseJSON(code=312,auth_json=auth_json)
        self.logger.debug("timeout res:"+str(res))
        return {"request_status":"TIMEOUT","response_json":res}  
              
    @classmethod
    def buildPaymentObject(self,validate_resp,store_code,request_status,req_time,res_time,Is_WaitForResponse):
        post_dict={}    
        post_dict['file_id']=None     
        post_dict['req_time']=req_time   
        post_dict['request_status']=request_status   
        post_dict['res_time']=res_time
        post_dict['ro_status_on']=req_time
        post_dict['parent_file_id']=None
        post_dict['retry_count']=0
        post_dict['store_code']=store_code
        store_detail=None
        if 'root_file_id' in validate_resp:
            post_dict['root_file_id']=validate_resp['root_file_id']
        else:
            post_dict['root_file_id']=None
        try:
            store_detail=validate_resp['store_detail']
        except:  
            store_detail=None
        
        try:
            response_json =  validate_resp['payment_response']  
            post_dict['response_json']=response_json
        except:  
            post_dict['response_json']=None
       
        
        try:
            request_json=validate_resp['payment_request']
            post_dict['request_json']=request_json
            post_dict['ro_status']=request_json['status']
            post_dict['document_id']=request_json['documentId']
        except:   
            post_dict['request_json']=""
            post_dict['ro_status']=""
            post_dict['document_id']=""
            
        try:
            post_dict['api_key']=store_detail['api_key']
            
           
        except:
            post_dict['api_key']=""
           
        """ try:            
            post_dict['store_code']=store_detail['store_code']           
        except:           
            post_dict['store_code']="" """
        try:
            ro_detail= validate_resp['ro_detail']  

            post_dict['warranty_due']=ro_detail['warranty_due']
            post_dict['total_amount_due']=ro_detail['amount_due']
        except:
            post_dict['warranty_due']=0.0
            post_dict['total_amount_due']=0.0

        try:
            ro_detail= validate_resp['ro_detail'] 
            spcIns=ro_detail['spc_ins'] 
            post_dict['is_spc_ins']="N"
            if spcIns != None and len(spcIns)>0:         
               post_dict['is_spc_ins']="Y"           
        except:
           post_dict['is_spc_ins']="N"
           
        return post_dict
    @classmethod         
    def isRetryAllow(self,store_detail,ro_detail):
        #_moduleNM="PostManger"
        #_functionNM="isRetryAllow"
        #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM) 
        initiateRetry=False
        isRetryActive= store_detail['is_retry_active']
        MaxRetryCount=int(store_detail['retry_max_count'])
        if isRetryActive and MaxRetryCount>0:
            isAllowCloseAllRO=store_detail['close_all_RO_flag']
            if isAllowCloseAllRO:
                initiateRetry=True
            else:
                try : 
                    warrantyDue_amount= float(ro_detail['warranty_due'])  
                    if warrantyDue_amount==0:
                        initiateRetry=True    
                except :
                        initiateRetry=True
        #self.logger.debug("isRetryAllow:"+str(initiateRetry))
        return initiateRetry  
    @classmethod
    def GetPostPaymentRequestList(self,store_code,create_date):
        _moduleNM="PostManger"
        _functionNM="GetPostPaymentRequestList"
        try:
            self.logger.debug("GetPostPaymentRequestList>> store_code="+str(store_code)+",create_date="+str(create_date))
            
            dynamodb = boto3.resource('dynamodb', region_name=PostManger.region)
            TableName=self.GetTableName(store_code)
            table = dynamodb.Table(TableName)
            response = table.scan(
                            FilterExpression= Attr('store_code').eq(store_code) & Attr('req_time').gte(create_date),
                            ConsistentRead=True)
            try:
                    items = response['Items']                
                    while 'LastEvaluatedKey' in response:
                        response = table.scan(
                            FilterExpression= Attr('store_code').eq(store_code) & Attr('req_time').gte(create_date),
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
    @classmethod
    def GetBatchCloseRO_PostPaymentRequestList(self,store_code,create_date):
        _moduleNM="PostManger"
        _functionNM="GetBatchCloseRO_PostPaymentRequestList"
        try:
            self.logger.debug("GetBatchCloseRO_PostPaymentRequestList>> store_code="+str(store_code)+",create_date="+str(create_date))
            
            dynamodb = boto3.resource('dynamodb', region_name=PostManger.region)
            TableName=self.GetTableName(store_code)
            table = dynamodb.Table(TableName)
            response = table.scan(
                            FilterExpression= Attr('store_code').eq(store_code) & Attr('req_time').gte(create_date) & Attr('root_file_id').exists(),
                            ConsistentRead=True)
            try:
                    items = response['Items']                
                    while 'LastEvaluatedKey' in response:
                        response = table.scan(
                            FilterExpression= Attr('store_code').eq(store_code) & Attr('req_time').gte(create_date) & Attr('root_file_id').exists(),
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
    
    @classmethod
    def GetPostPaymentRereconciliationReport(self,store_code,create_date,tablePerStoreRoDetail=1):
        _moduleNM="PostManager"
        _functionNM="GetPostPaymentRereconciliationReport"
        try:           
            self.logger.debug("GetPostPaymentRereconciliationReport>> store_code:"+str(store_code)+",create_date="+create_date)
            app_client=AppClient()
            store_resp=app_client.GetStoreDetail(store_code,PostManger.region)
            if store_resp['status']==False:
               self.logger.debug("GetStoreDetailByClientId status false:") 
               return store_resp
            rOrder=RepairOrder(PostManger.region,tablePerStoreRoDetail)
            #create_date=''
            post_resp=self.GetPostPaymentRequestList(store_code,create_date)
            store_resp=store_resp['item']
            StoreName=store_resp['store_name']
            store_code=store_resp['store_code']
            log_on=store_resp['logon']
            close_all_RO_flag=store_resp['close_all_RO_flag']
            TotalRequest=0
            PostedClosedC98=0
            PostedWarrantyDue=0
            PostedNotClosedC97=0
            NotPosted=0	
            InQueue=0 
            total_timeout=0 
            excel_doc=[]       
            if post_resp['status']==False:
                self.logger.debug("GetPostPaymentRequestList status false:") 
                return post_resp
            else:
                items=post_resp['items']               
                parentPostList=[]
                retryPostList=[]
                try:
                    parentPostList = [d for d in items if d['is_retry_request'] is False]
                    retryPostList = [d for d in items if d['is_retry_request'] is True]
                    TotalRequest=len(parentPostList) 
                except:
                    ""
                self.logger.debug("TotalRequest  :"+str(TotalRequest)) 
                self.logger.debug("items  :"+str(items))  
                self.logger.debug("parentPostList  :"+str(parentPostList))
                self.logger.debug("retryPostList  :"+str(retryPostList)) 
                self.logger.debug("Total items :"+str(len(items))) 
                self.logger.debug("Total parentPostList :"+str(len(parentPostList))) 
                self.logger.debug("Total retryPostList :"+str(len(retryPostList)))        
                roListResp=rOrder.GetROList(store_code,'RO',None,-1)
                if roListResp['status']==False:  
                    ro_list_items=[]
                else:     
                    ro_list_items=roListResp['items']  

                for item in parentPostList:                    
                    document_id=item['document_id']
                    store_code=store_code
                    ro_status=item['ro_status']
                    request_status=item['request_status']
                    ro_number=document_id
                    amount_due=item['total_amount_due']
                    warranty_due=item['warranty_due']
                    ro_posted_on=item['req_time']
                    ro_status_on=item['res_time'] 
                    ro_response_on=item['res_time']    
                    if 'ro_status_on' in item:
                       ro_status_on=item['ro_status_on']
                         
                    
                    retry_count=item['retry_count']
                    try:
                        fileId=item['file_id']  
                        retryList = [d for d in retryPostList if d['parent_file_id'] == fileId] 
                        self.logger.debug("document_id="+str(document_id)+",fileId="+str(fileId)+",retryList count :"+str(len(retryList)))   
                        if len(retryList)>0:
                           retry_count=len(retryList)
                           item_retry=retryList[len(retryList)-1] 
                           ro_status=item_retry['ro_status'] 
                           if 'ro_status_on' in item_retry:
                               ro_status_on=item_retry['ro_status_on']
                           if 'res_time' in item_retry:
                                ro_response_on=item_retry['res_time']
                           request_status=item_retry['request_status']
                    except: 
                        ""  

                    try:   
                        ro_detail = [x for x in ro_list_items if x['document_id'] == document_id]
                        if len(ro_detail)>0: 
                          roData=ro_detail[len(ro_detail)-1]
                          latest_ro_status= roData['status']
                          dt_ro = datetime.datetime.strptime(str(roData['create_date_time'] ),"%Y-%m-%d %I:%M:%S %p") 
                          t=dt_ro.timestamp()
                          latest_ro_status_on= str(t)
                        else:
                           latest_ro_status= ro_status
                           latest_ro_status_on= ro_status_on      
                    except:
                        
                        latest_ro_status= ro_status
                        latest_ro_status_on= ro_status_on   
                                       
                    processing_time_in_min=0
                    processing_st=""
                    try:
                        dt_request = datetime.datetime.fromtimestamp(float(ro_posted_on)) 
                        dt_response = datetime.datetime.fromtimestamp(float(ro_response_on)) 
                        c=dt_response-dt_request
                        processing_time_in_min = c.total_seconds() / 60
                        processing_time_in_min=(round(processing_time_in_min, 3))
                        secs = c.total_seconds()
                        hours = int(secs // 3600)
                        minutes = int((secs % 3600) // 60)
                        seconds = int(secs % 60)                        
                        processing_st='{}:{}:{}'.format(hours,minutes,seconds)
                        self.logger.debug("processing_st="+str(processing_st)) 
                    except:
                         ""
                    api_status=""

                    pedning_since_in_min=0  
                    pending_st="" 
                    try:               
                        dt_request_1 = datetime.datetime.fromtimestamp(float(ro_posted_on)) 
                        dt_response_1 = datetime.datetime.now()
                        c1=dt_response_1-dt_request_1
                        pedning_since_in_min = c1.total_seconds() / 60
                        pedning_since_in_min=(round(pedning_since_in_min, 3)) 
                        secs = c1.total_seconds()
                        hours = int(secs // 3600)
                        minutes = int((secs % 3600) // 60)
                        seconds = int(secs % 60)                        
                        pending_st='{}:{}:{}'.format(hours,minutes,seconds)
                        self.logger.debug("pending_st="+str(pending_st)) 
                    except:
                         ""
                    if request_status=='NOT_POSTED':
                       NotPosted=NotPosted+1                       
                       code=""
                       msg=""
                       try:
                        response_json=item['response_json'] 
                        if response_json!= None:
                            errorList= response_json['errorList']
                            if errorList != None and len(errorList)>0:
                                error=errorList[0]
                                code=error['code']
                                msg=error['message']
                       except:
                           self.logger.debug("response_json is None") 
                       api_status="RO Not Posted,ErrorCode="+str(code)+",ErrorMessage="+msg
                       #processing_time_in_min
                       pedning_since_in_min=0 
                       pending_st=''
                    elif request_status=='TIMEOUT':
                       total_timeout=total_timeout+1                       
                       code=""
                       msg=""
                       try:
                        response_json=item['response_json']   
                        if response_json!= None:
                            errorList= response_json['errorList']
                            if errorList != None and len(errorList)>0:
                                error=errorList[0]
                                code=error['code']
                                msg=error['message']
                       except:
                           self.logger.debug("response_json is None") 
                       api_status="RO Posted but not processed due to dms service unavailability,ErrorCode="+str(code)+",ErrorMessage="+msg
                       #processing_time_in_min
                       pedning_since_in_min=0   
                       pending_st=''  
                    elif request_status =='IN_QUEUE':
                        InQueue=InQueue+1
                        api_status="RO in Queue"                      
                        processing_time_in_min=0  
                        processing_st=''                    
                    else:                         
                        is_warranty_due= item['is_warranty_due']
                        if not is_warranty_due or close_all_RO_flag:

                            if latest_ro_status=='C98':
                                PostedClosedC98=PostedClosedC98+1
                                api_status="RO Posted Successfully, Now C98"
                                #processing_time_in_min
                                pedning_since_in_min=0 
                                pending_st=''
                            else:
                                PostedNotClosedC97=PostedNotClosedC97+1
                                api_status="RO Posted Successfully but still not closed,Now "+latest_ro_status                                   
                                #pedning_since_in_min=0 calculate minute
                                processing_time_in_min=0  
                                processing_st=''
                        else:
                            PostedWarrantyDue=PostedWarrantyDue+1
                            if latest_ro_status=='C98':
                                api_status="RO with Warranty due Posted Successfully and Closed By Clerk,Now C98"
                                pedning_since_in_min=0 
                                pending_st=''
                            else:
                                api_status="RO with Warranty due Posted Successfully and Stil Not Closed By Clerk,Now "+latest_ro_status 
                                #pedning_since_in_min=0 calculate minute
                                  
                    excel_row={
                        "StoreCode":store_code,
                        "StoreName":StoreName,
                        "CloseAllPaidROs":close_all_RO_flag,
                        "RONumber":ro_number,
                        "AmountDue":amount_due,
                        "WarrantyDue":warranty_due,
                        "ROPostedOn":self.TimeToDateString(ro_posted_on),
                        "ROStatus":ro_status,
                        "ROStatusOn":self.TimeToDateString(ro_status_on),
                        "Latest_ROStatus":latest_ro_status,
                        "Latest_ROStatusOn":self.TimeToDateString(latest_ro_status_on),
                        "TotalProcessingTime":processing_st,
                        "ApiStatus":api_status,
                        "PedningSince":pending_st,
                        "TotalRetryCount":retry_count,
                    } 
                    excel_doc.append(excel_row) 

            report_summary= {  
                         "store_code":store_code,
                         "store_name":StoreName,
                         "close_all_paid_ros":close_all_RO_flag,
                         "log_on":log_on,
                         "total_request":TotalRequest,
                         "total_posted_closed_C98":PostedClosedC98,
                         "total_posted_warranty_due":PostedWarrantyDue,
                         "total_not_posted":NotPosted,
                         "total_in_queue":InQueue,
                         "total_posted_not_closed_C97":PostedNotClosedC97,
                         "total_timeout":total_timeout, 
                         }
            excelName=store_code+"_PostPaymentCallLog_"+".xlsx"
            return {"status":True,"report_summary":report_summary,"excel_doc":excel_doc ,"excel_name":excelName}            
           
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 
    @classmethod
    def GetBatchCloseROReport(self,store_code,create_date,tablePerStoreRoDetail=1):
        _moduleNM="PostManager"
        _functionNM="GetBatchCloseROReport"
        try:           
            self.logger.debug("GetBatchCloseROReport>> store_code:"+str(store_code)+",create_date="+create_date)
            app_client=AppClient()
            store_resp=app_client.GetStoreDetail(store_code,PostManger.region)
            if store_resp['status']==False:
               self.logger.debug("GetStoreDetailByClientId status false:") 
               return store_resp
            rOrder=RepairOrder(PostManger.region,tablePerStoreRoDetail)
            #create_date=''
            post_resp=self.GetBatchCloseRO_PostPaymentRequestList(store_code,create_date)
            store_resp=store_resp['item']
            StoreName=store_resp['store_name']
            store_code=store_resp['store_code']
            log_on=store_resp['logon']           
            TotalRequest=0
            PostedClosedC98=0            
            PostedNotClosedC97=0            
            InQueue=0 
            total_timeout=0 
            excel_doc=[]       
            if post_resp['status']==False:
                self.logger.debug("GetBatchCloseRO_PostPaymentRequestList status false:") 
                return post_resp
            else:
                items=post_resp['items']               
                parentPostList=[]
                retryPostList=[]
                try:
                    parentPostList = [d for d in items if d['is_retry_request'] is False]
                    retryPostList = [d for d in items if d['is_retry_request'] is True]
                    TotalRequest=len(parentPostList) 
                except:
                    ""
                self.logger.debug("TotalRequest  :"+str(TotalRequest)) 
                self.logger.debug("items  :"+str(items))  
                self.logger.debug("parentPostList  :"+str(parentPostList))
                self.logger.debug("retryPostList  :"+str(retryPostList)) 
                self.logger.debug("Total items :"+str(len(items))) 
                self.logger.debug("Total parentPostList :"+str(len(parentPostList))) 
                self.logger.debug("Total retryPostList :"+str(len(retryPostList)))        
                roListResp=rOrder.GetROList(store_code,'RO',None,-1)
                if roListResp['status']==False:  
                    ro_list_items=[]
                else:     
                    ro_list_items=roListResp['items']  

                for item in parentPostList:                    
                    document_id=item['document_id']
                    store_code=store_code
                    ro_status=item['ro_status']
                    request_status=item['request_status']
                    ro_number=document_id
                    amount_due=item['total_amount_due']
                    warranty_due=item['warranty_due']
                    ro_posted_on=item['req_time']
                    ro_status_on=item['res_time'] 
                    ro_response_on=item['res_time']    
                    if 'ro_status_on' in item:
                       ro_status_on=item['ro_status_on']

                                    
                    
                    retry_count=item['retry_count']
                    try:
                        fileId=item['file_id']  
                        retryList = [d for d in retryPostList if d['parent_file_id'] == fileId] 
                        self.logger.debug("document_id="+str(document_id)+",fileId="+str(fileId)+",retryList count :"+str(len(retryList)))   
                        if len(retryList)>0:
                           retry_count=len(retryList)
                           item_retry=retryList[len(retryList)-1] 
                           ro_status=item_retry['ro_status'] 
                           if 'ro_status_on' in item_retry:
                               ro_status_on=item_retry['ro_status_on']
                           if 'res_time' in item_retry:
                                ro_response_on=item_retry['res_time']
                           request_status=item_retry['request_status']
                    except: 
                        ""  

                    try:   
                        ro_detail = [x for x in ro_list_items if x['document_id'] == document_id]
                        if len(ro_detail)>0: 
                          roData=ro_detail[len(ro_detail)-1]
                          latest_ro_status= roData['status']
                          dt_ro = datetime.datetime.strptime(str(roData['create_date_time'] ),"%Y-%m-%d %I:%M:%S %p") 
                          t=dt_ro.timestamp()
                          latest_ro_status_on= str(t)
                        else:
                           latest_ro_status= ro_status
                           latest_ro_status_on= ro_status_on      
                    except:
                        
                        latest_ro_status= ro_status
                        latest_ro_status_on= ro_status_on   
                                       
                    processing_time_in_min=0
                    processing_st=""
                    try:
                        dt_request = datetime.datetime.fromtimestamp(float(ro_posted_on)) 
                        dt_response = datetime.datetime.fromtimestamp(float(ro_response_on)) 
                        c=dt_response-dt_request
                        processing_time_in_min = c.total_seconds() / 60
                        processing_time_in_min=(round(processing_time_in_min, 3))
                        secs = c.total_seconds()
                        hours = int(secs // 3600)
                        minutes = int((secs % 3600) // 60)
                        seconds = int(secs % 60)                        
                        processing_st='{}:{}:{}'.format(hours,minutes,seconds)
                        self.logger.debug("processing_st="+str(processing_st)) 
                    except:
                         ""
                    api_status=""

                    pedning_since_in_min=0  
                    pending_st="" 
                    try:               
                        dt_request_1 = datetime.datetime.fromtimestamp(float(ro_posted_on)) 
                        dt_response_1 = datetime.datetime.now()
                        c1=dt_response_1-dt_request_1
                        pedning_since_in_min = c1.total_seconds() / 60
                        pedning_since_in_min=(round(pedning_since_in_min, 3)) 
                        secs = c1.total_seconds()
                        hours = int(secs // 3600)
                        minutes = int((secs % 3600) // 60)
                        seconds = int(secs % 60)                        
                        pending_st='{}:{}:{}'.format(hours,minutes,seconds)
                        self.logger.debug("pending_st="+str(pending_st)) 
                    except:
                         ""
                    if request_status=='TIMEOUT':
                       total_timeout=total_timeout+1                       
                       code=""
                       msg=""
                       try:
                        response_json=item['response_json']  
                        if response_json!= None:
                            errorList= response_json['errorList']
                            if errorList != None and len(errorList)>0:
                                error=errorList[0]
                                code=error['code']
                                msg=error['message']
                       except:
                           self.logger.debug("response_json is None") 
                       api_status="RO Posted for Auto Close but not processed due to dms service unavailability,ErrorCode="+str(code)+",ErrorMessage="+msg
                       #processing_time_in_min
                       pedning_since_in_min=0   
                       pending_st=''  
                    elif request_status =='IN_QUEUE':
                        InQueue=InQueue+1
                        api_status="RO in Queue"                      
                        processing_time_in_min=0  
                        processing_st=''                    
                    else: 
                        if latest_ro_status=='C98':
                            PostedClosedC98=PostedClosedC98+1
                            api_status="RO Posted for Auto Close Successfully, Now C98"
                            #processing_time_in_min
                            pedning_since_in_min=0 
                            pending_st=''
                        else:
                            PostedNotClosedC97=PostedNotClosedC97+1
                            api_status="RO Posted for Auto Close Successfully but still not closed,Now "+latest_ro_status                                   
                            #pedning_since_in_min=0 calculate minute
                            processing_time_in_min=0  
                            processing_st=''
                        
                    excel_row={
                        "StoreCode":store_code,
                        "StoreName":StoreName,
                        "RONumber":ro_number,
                        "AmountDue":amount_due,
                        "WarrantyDue":warranty_due,
                        "ROPostedOn":self.TimeToDateString(ro_posted_on),
                        "ROStatus":ro_status,
                        "ROStatusOn":self.TimeToDateString(ro_status_on),
                        "Latest_ROStatus":latest_ro_status,
                        "Latest_ROStatusOn":self.TimeToDateString(latest_ro_status_on),
                        "TotalProcessingTime":processing_st,
                        "ApiStatus":api_status,
                        "PedningSince":pending_st,
                        "TotalRetryCount":retry_count
                    } 
                    excel_doc.append(excel_row) 

            report_summary= {  
                         "store_code":store_code,
                         "store_name":StoreName,                       
                         "log_on":log_on,
                         "total_request":TotalRequest,
                         "total_posted_closed_C98":PostedClosedC98,
                         "total_in_queue":InQueue,
                         "total_posted_not_closed_C97":PostedNotClosedC97,
                         "total_timeout":total_timeout, 
                         }
            excelName=store_code+"_BatchCloseROCallLog_"+".xlsx"
            return {"status":True,"report_summary":report_summary,"excel_doc":excel_doc ,"excel_name":excelName}            
           
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 
    @classmethod
    def TimeToDateString(self,time_string):
        try:
            dt_request = datetime.datetime.fromtimestamp(float(time_string)) 
            return str(dt_request.strftime("%m-%d-%Y %I:%M:%S %p"))
        except:
               return time_string

    @classmethod
    def GetPostPaymentRereconciliationReportPendingOnly(self,store_code,create_date,C97Pending_Since_inSecond=15,tablePerStoreRoDetail=1):
        _moduleNM="PostManager"
        _functionNM="GetPostPaymentRereconciliationReportPendingOnly"
        try:           
            self.logger.debug("GetPostPaymentRereconciliationReportPendingOnly>> store_code:"+str(store_code)+",create_date="+create_date+",C97Pending_Since_inSecond="+str(C97Pending_Since_inSecond))
            app_client=AppClient()
            store_resp=app_client.GetStoreDetail(store_code,PostManger.region)
            
            if store_resp['status']==False:
               self.logger.debug("GetStoreDetailByClientId status false:") 
               return store_resp
            rOrder=RepairOrder(PostManger.region,tablePerStoreRoDetail)
            #create_date=''
            post_resp=self.GetPostPaymentRequestList(store_code,create_date)
            
            store_resp=store_resp['item']
            StoreName=store_resp['store_name']
            store_code=store_resp['store_code']
            log_on=store_resp['logon']
            close_all_RO_flag=store_resp['close_all_RO_flag']          
            PostedNotClosedC97=0            
            excel_doc=[]  
            response_txt_list=[]    
            if post_resp['status']==False:
                self.logger.debug("GetPostPaymentRequestList status false:") 
                return post_resp
            else:
                items=post_resp['items'] 
                parentPostList=[]
                retryPostList=[]
                try:
                    parentPostList = [d for d in items if d['is_retry_request'] is False]
                    retryPostList = [d for d in items if d['is_retry_request'] is True]
                     
                except:
                    ""
                self.logger.debug("items  :"+str(items))  
                self.logger.debug("parentPostList  :"+str(parentPostList))
                self.logger.debug("retryPostList  :"+str(retryPostList)) 
                self.logger.debug("Total items :"+str(len(items))) 
                self.logger.debug("Total parentPostList :"+str(len(parentPostList))) 
                self.logger.debug("Total retryPostList :"+str(len(retryPostList)))         
               

                roListResp=rOrder.GetROList(store_code,'RO',None,-1)
                if roListResp['status']==False:  
                    self.logger.debug("GetROList  status false:")  
                    ro_list_items=[]
                else:     
                    ro_list_items=roListResp['items']  

                for item in parentPostList: 
                           
                    document_id=item['document_id']
                    store_code=item['store_code']                   
                    request_status=item['request_status']
                    ro_number=document_id
                    amount_due=item['total_amount_due']
                    warranty_due=item['warranty_due']
                    ro_posted_on=item['req_time']
                    ro_status_on=item['res_time']
                    ro_status=item['ro_status']
                    if 'ro_status_on' in item:
                       ro_status_on=item['ro_status_on']
                    retry_count=item['retry_count']  
                    is_warranty_due= item['is_warranty_due']
                    try:
                        fileId=item['file_id']  
                        retryList = [d for d in retryPostList if d['parent_file_id'] == fileId] 
                        self.logger.debug("document_id="+str(document_id)+",fileId="+str(fileId)+",retryList count :"+str(len(retryList)))   
                        if len(retryList)>0:
                           retry_count=len(retryList)
                           item_retry=retryList[len(retryList)-1] 
                           ro_status=item_retry['ro_status'] 
                           if 'ro_status_on' in item_retry:
                                ro_status_on=item_retry['ro_status_on']
                           request_status=item_retry['request_status']
                    except: 
                        ""                   
                    try:   
                        ro_detail = [x for x in ro_list_items if x['document_id'] == document_id]
                        if len(ro_detail)>0: 
                          roData=ro_detail[len(ro_detail)-1]
                          latest_ro_status= roData['status']
                          #latest_ro_status_on= roData['created_ts'] 
                          dt_ro = datetime.datetime.strptime(str(roData['create_date_time'] ),"%Y-%m-%d %I:%M:%S %p") 
                          t=dt_ro.timestamp()
                          latest_ro_status_on= str(t)
                        else:
                           latest_ro_status= ro_status
                           latest_ro_status_on= ro_status_on     
                    except:
                        
                        latest_ro_status= ro_status
                        latest_ro_status_on= ro_status_on   
                                    
                    pedning_since_in_min=0
                    st=""
                    try:
                        dt_object = datetime.datetime.fromtimestamp(float(ro_posted_on)) 
                        self.logger.debug("dt_object="+str(dt_object))
                        dt_object_now = datetime.datetime.now() 
                        self.logger.debug("dt_object_now="+str(dt_object_now))
                        c=dt_object_now-dt_object 
                        self.logger.debug("c="+str(c))
                        self.logger.debug("total seconds="+str(c.total_seconds() ))
                        pedning_since_in_min = c.total_seconds() / 60
                        
                        secs = c.total_seconds()
                        hours = int(secs / 3600)
                        minutes = int(secs / 60) % 60 
                        st='{} hour{} {} minute{},'.format(
                                                            hours, 's' if hours != 1 else '',minutes, 's' if minutes != 1 else '')
                        self.logger.debug("st="+str(st))
                    except:
                        pedning_since_in_min=0
                        st=""

                    api_status=""
                   
                    
                    pflag=pedning_since_in_min>C97Pending_Since_inSecond
                    self.logger.debug("RO#:"+str(document_id)+",request_status="+request_status+",is_warranty_due="+str(is_warranty_due)+",latest_ro_status="+latest_ro_status+",processing_time_in_min="+str( round(pedning_since_in_min, 3))+",pflag="+str(pflag))                                                   
                    
                    if request_status!='NOT_POSTED':
                       
                        if not is_warranty_due or close_all_RO_flag:
                            if latest_ro_status=='C97' and pedning_since_in_min>C97Pending_Since_inSecond:
                                PostedNotClosedC97=PostedNotClosedC97+1
                                if request_status=='TIMEOUT':
                                    api_status="RO Posted but not processed due to dms service unavailability, RO is still not closed,Now "+latest_ro_status    
                                    response_txt= "RO#"+str(ro_number)+"( WarrantyDue="+str(warranty_due)+",Status="+str(latest_ro_status)+",Pending since Last "+str(st)+",RO Posted but not processed due to DMS service unavailability!)"
                                
                                else: 
                                    api_status="RO Posted Successfully but still not closed,Now "+latest_ro_status                                   
                                #pedning_since_in_min=0 calculate minute
                                    response_txt= "RO#"+str(ro_number)+"( WarrantyDue="+str(warranty_due)+",Status="+str(latest_ro_status)+",Pending since Last "+str(st)+" )"
                                excel_row={
                                "StoreCode":store_code,
                                "StoreName":StoreName,
                                "logon":log_on,
                                "CloseAllPaidROs":close_all_RO_flag,
                                "RONumber":ro_number,
                                "AmountDue":amount_due,
                                "WarrantyDue":warranty_due,
                                "ROPostedOn":self.TimeToDateString(ro_posted_on),
                                "ROStatus":ro_status,
                                "ROStatusOn":self.TimeToDateString(ro_status_on),
                                "Latest_ROStatus":latest_ro_status,
                                "Latest_ROStatusOn":self.TimeToDateString(latest_ro_status_on),
                                "TotalProcessingTimeInMin": "",
                                "ApiStatus":api_status,
                                "PedningSinceInMin": round(pedning_since_in_min, 3),
                                "TotalRetryCount":retry_count,                         
                                } 
                                excel_doc.append(excel_row) 
                                response_txt_list.append(response_txt)
                excelName=store_code+"_PostPaymentPendingC97CallLog_"+".xlsx"
            return {"status":True,"store_code":store_code,"store":(store_code+"-"+StoreName),"logon":log_on,"not_closed_c97_list":response_txt_list,"excel_doc":excel_doc ,"excel_name":excelName,"PostedNotClosedC97":PostedNotClosedC97}           
           
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 
   