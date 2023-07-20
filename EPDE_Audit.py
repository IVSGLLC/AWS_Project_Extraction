from EPDE_Logging import LogManger
import boto3
from boto3.dynamodb.conditions import Attr
from EPDE_Error import ErrorHandler
from datetime import datetime
import uuid
#This module is responsible to handle EPDE API RepairOrder handling
class AuditLog(object):
    logger=LogManger()   
    err_handler=ErrorHandler()
    region='us-east-1' 
    table_Per_Store_auditlog=1

    def __init__(self):        
        self.region="us-east-1"
        self.table_Per_Store_auditlog=1
        
    def __init__(self,region,table_Per_Store_auditlog=1):  
        self.region=region
        self.table_Per_Store_auditlog=table_Per_Store_auditlog
         
    @classmethod   
    def getTableName(self,client_id):
        TableName=client_id+"_AUDIT_LOG"        
        return TableName
    
    @classmethod
    def LogMessage(self,log_dict):         
        try:                     
            self.logger.debug("LogMessage >> log_dict="+str(log_dict))
            issued = datetime.datetime.now()
            create_date = issued.timestamp()
            dynamodb = boto3.resource('dynamodb', region_name=self.region)
            client_id=log_dict['client_id']
            log_id=str(uuid.uuid4())
            TableName=self.GetTableName(client_id)
            table = dynamodb.Table(TableName)            
            with table.batch_writer(overwrite_by_pkeys=[ 'log_id']) as batch:
                batch.put_item(
                                Item={
                                "audit_id": log_id,
                                "store_code": str(log_dict['store_code']),
                                "client_id": str(client_id),
                                "api_key":log_dict['api_key'],
                                "api": str(log_dict['api']),
                                "file_id": log_dict['file_id'], 
                                "request_status": log_dict['request_status'], 
                                "messsage": log_dict['messsage'],     
                                "request_json": log_dict['response_json'],
                                "response_json": log_dict['response_json'],                               
                                "create_date_time":str(create_date),                                                                               
                                }
                            )
            self.logger.debug("Audit Log Created log_id:"+log_id)
          
        except Exception as e:
                return False
        return True
    @classmethod
    def GetAuditLogs(self,client_id,date_to, date_from):
        _moduleNM="AuditLog"
        _functionNM="GetAuditLogs"
        try:
            self.logger.debug("GetAuditLogs>> client_id="+str(client_id)+", date_to="+str(date_to)+", date_from="+str(date_from))
           
            dynamodb = boto3.resource('dynamodb', region_name=self.region)
            TableName=self.GetTableName(client_id)
            table = dynamodb.Table(TableName)
            response = table.scan(FilterExpression= Attr('client_id').eq(client_id) & Attr('date_from').gte(date_from) &  Attr('date_to').lte(date_to))
            if response['Count'] > 0:
               items = response['Items']
               return { "status":True,"items": items } 
            else:
               return { "status":True,"items": [] } 
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
    