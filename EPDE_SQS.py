from EPDE_Logging import LogManger
import boto3
from EPDE_Error import ErrorHandler
from decimal import Decimal
import json
from SQSClientExtended import SQSClientExtended
#This Class  is responsible to handle EPDE AWS SQS Operation
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

class SQSManager(object):
    QueueName="EPDE_PostPayment_Request.fifo"
    region="us-east-1"
    MaxMsgCount=1
    waitTimeInSecond=20
    logger=LogManger()   
    err_handler=ErrorHandler() 
     
    def __init__(self,QueueName="EPDE_PostPayment_Request.fifo",region="us-east-1",MaxMsgCount=1,waitTimeInSecond=20):
        SQSManager.QueueName=QueueName
        SQSManager.region=region
        SQSManager.MaxMsgCount=MaxMsgCount
        SQSManager.waitTimeInSecond=waitTimeInSecond    
    def send_message_old(self, msg_body,MessageGroupId):
        # Send the SQS message
        _moduleNM="SQSManager"
        _functionNM="send_message"
        try:
           
            sqs_client = boto3.client("sqs", region_name=SQSManager.region)
            sqs_queue_url = sqs_client.get_queue_url(QueueName=SQSManager.QueueName)['QueueUrl']              
            msgbdy_json=json.dumps(msg_body,sort_keys=True)  
            msg = sqs_client.send_message(QueueUrl=sqs_queue_url,MessageBody=msgbdy_json,MessageGroupId=str(MessageGroupId)) 
            self.logger.debug("send_message Sucessfully:msg:"+str(msg))
            return {"status":True,"msg":msg }
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)     
    
    def send_message(self, msg_body,MessageGroupId,MessageDeduplicationId):
        # Send the SQS message
        _moduleNM="SQSManager"
        _functionNM="send_message_large"
        try:           
            sqs = boto3.client("sqs", region_name=SQSManager.region)
            sqs_queue_url = sqs.get_queue_url(QueueName=SQSManager.QueueName)['QueueUrl']  
            sqs_client = SQSClientExtended(aws_region_name= SQSManager.region,s3_bucket_name= 'epde-sqs-data')
            sqs_client.set_always_through_s3(False)
            
            msg = sqs_client.send_message( queue_url=sqs_queue_url,message=msg_body,message_group_id=MessageGroupId,message_deduplication_id=MessageDeduplicationId,message_attributes={})
            self.logger.debug("send_message Sucessfully:msg:"+str(msg))
            return {"status":True,"msg":msg }
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 

    def send_message_att(self, msg_body,MessageGroupId,MessageAttributes,MessageDeduplicationId):
        # Send the SQS message
        _moduleNM="SQSManager"
        _functionNM="send_message_att"
        try:
            sqs_client = boto3.client("sqs", region_name=SQSManager.region)
            sqs_queue_url = sqs_client.get_queue_url(QueueName=SQSManager.QueueName)['QueueUrl']  
            msgbdy_json=json.dumps(msg_body)  
            msg = sqs_client.send_message(QueueUrl=sqs_queue_url,MessageBody=msgbdy_json,MessageGroupId=str(MessageGroupId),MessageAttributes=MessageAttributes,MessageDeduplicationId=MessageDeduplicationId) 
            self.logger.debug("send_message with attributes Sucessfully:msg:"+str(msg))
            return {"status":True,"msg":msg }
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 
     
    def receive_message_old(self):
        # receive the Top N SQS messages    
        message_body_list=[]
        _moduleNM="SQSManager"
        _functionNM="receive_message"
        try:
            sqs_client = boto3.client("sqs", region_name=SQSManager.region)
            sqs_queue_url = sqs_client.get_queue_url(QueueName=SQSManager.QueueName)['QueueUrl'] 
            response = sqs_client.receive_message(
                QueueUrl=sqs_queue_url,
                AttributeNames=['MessageGroupId'],
                MaxNumberOfMessages=SQSManager.MaxMsgCount,
                WaitTimeSeconds=SQSManager.waitTimeInSecond,
               
            )
            #self.logger.debug("receive_message response:"+str(response))
            msgList=response.get("Messages", [])
            self.logger.debug("receive_message Number of messages received:"+str(len(msgList)))
            for message in msgList:
                message_body_list.append(message)
                #self.logger.debug("receive_message message:"+str(message))
            return {"status":True,"messages": message_body_list }          
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 
    
    def receive_message(self):
        # receive the Top N SQS messages    
        message_body_list=[]
        _moduleNM="SQSManager"
        _functionNM="receive_message"
        try:
            sqs = boto3.client("sqs", region_name=SQSManager.region)
            self.logger.debug("SQSManager.QueueName:"+str(SQSManager.QueueName))
            sqs_queue_url = sqs.get_queue_url(QueueName=SQSManager.QueueName)['QueueUrl'] 
            self.logger.debug("sqs_queue_url:"+str(sqs_queue_url))
            sqs_client = SQSClientExtended(aws_region_name= SQSManager.region,s3_bucket_name='epde-sqs-data')
            sqs_client.set_always_through_s3(False)
            #sqs_client.receive_message()
            response = sqs_client.receive_message(queue_url=
                sqs_queue_url,
                max_number_Of_Messages=SQSManager.MaxMsgCount,
                wait_time_seconds=SQSManager.waitTimeInSecond               
            )
            self.logger.debug("receive_message response:"+str(response))
            #msgList=response.get("Messages", [])
            msgList=response
            if msgList is not None:
                self.logger.debug("receive_message Number of messages received:"+str(len(msgList)))
                for message in msgList:
                    message_body_list.append(message)
                    #self.logger.debug("receive_message message:"+str(message))
             
            return {"status":True,"messages": message_body_list }          
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 

    def delete_message_old(self , receipt_handle ):
        _moduleNM="SQSManager"
        _functionNM="delete_message"
        try:
            # Del the Top SQS message            
            sqs_client = boto3.client("sqs", region_name=SQSManager.region)
            sqs_queue_url = sqs_client.get_queue_url(QueueName=SQSManager.QueueName)['QueueUrl']          
            sqs_client.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handle)
            return {"status":True }  
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 
    
    def delete_message(self , receipt_handle ):
        _moduleNM="SQSManager"
        _functionNM="delete_message"
        try:
            # Del the Top SQS message          
            sqs = boto3.client("sqs", region_name=SQSManager.region)
            sqs_queue_url = sqs.get_queue_url(QueueName=SQSManager.QueueName)['QueueUrl'] 
            sqs_client = SQSClientExtended(aws_region_name= SQSManager.region,s3_bucket_name='epde-sqs-data') 
            sqs_client.set_always_through_s3(False)
            sqs_client.delete_message(queue_url=sqs_queue_url, receipt_handle=receipt_handle)
            return {"status":True }  
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 
    
    def purge_queue(self):
        _moduleNM="SQSManager"
        _functionNM="purge_queue"
        try:
            # Purge all SQS messages            
            sqs_client = boto3.client("sqs", region_name=SQSManager.region)
            sqs_queue_url = sqs_client.get_queue_url(QueueName=SQSManager.QueueName)['QueueUrl']          
            sqs_client.purge_queue(QueueUrl=sqs_queue_url)        
            return {"status":True }  
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
    
    @classmethod
    def fetch_response(self,apiName,file_id):
        self.logger.debug("fetch_response file_id="+str(file_id)+",apiName="+apiName)
        res_json=None      
        sqs_resp= self.receive_message(self)
        if sqs_resp['status']:
            messages=sqs_resp['messages']
            for message in messages: 
                            message_body = message["Body"]
                            receipt_handle = message['ReceiptHandle']
                            request_json=json.loads(message_body) 
                            msg_fileId=request_json['fileId']
                            api=request_json['api']                         
                            if msg_fileId == file_id and api == apiName:
                                res_json=request_json
                                try:
                                 self.delete_message(self,receipt_handle)  
                                except Exception :                                   
                                    ""
                                     
        return res_json  