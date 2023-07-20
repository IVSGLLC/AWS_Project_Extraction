from EPDE_Logging import LogManger
import boto3
import json
from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.conditions import Key
from EPDE_Error import ErrorHandler
from datetime import datetime
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient
#This module is responsible to handle EPDE API RepairOrder handling
class RepairOrder(object):
    logger=LogManger()   
    err_handler=ErrorHandler()
    region='us-east-1'
    table_Per_Store_RO_Detail=1
    def __init__(self):        
        RepairOrder.region="us-east-1"
        RepairOrder.table_Per_Store_RO_Detail=1
        
    def __init__(self,region,table_Per_Store_RO_Detail=1):  
        RepairOrder.region=region
        RepairOrder.table_Per_Store_RO_Detail=table_Per_Store_RO_Detail
         
    @classmethod   
    def getTableName(self,store_code):
        TableName=store_code+"_WIP_FILE"          
        return TableName

    @classmethod
    def GetRODetail(self,store_code,document_id,document_type):
        _moduleNM="RepairOrder"
        _functionNM="GetRODetail"
        try:
            self.logger.debug("GetRODetail>>store_code="+str(store_code)+",document_id="+str(document_id)+",document_type="+str(document_type))
            dynamodb = boto3.resource('dynamodb', region_name=RepairOrder.region)
            TableName=self.getTableName(store_code)
            table = dynamodb.Table(TableName)
            response = table.get_item(Key={'document_type': document_type,'document_id': document_id},ConsistentRead=False)
            try:
                item = response['Item']
                return { "status":True,"item": item } 
            except Exception as e:
               return self.err_handler.HandleAppError(341,moduleNM=_moduleNM,functionNM=_functionNM)  
        except Exception as e:
               return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
    
    @classmethod
    def GetROList(self,store_code,document_type,open_date_time="",last_key=None,page_size=-1):
        _moduleNM="RepairOrder"
        _functionNM="GetROList"
        try:           
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)       
            self.logger.debug("GetROList>> store_code:"+str(store_code)+",open_date_time:"+str(open_date_time)+",document_type"+str(document_type))
           
            dynamodb = boto3.resource('dynamodb', region_name=RepairOrder.region)
            TableName=self.getTableName(store_code)
            table = dynamodb.Table(TableName)
            fetchAll=False
            LastEvaluatedKey=None
            if isinstance(open_date_time,datetime):
                #response = table.scan(
                 #    FilterExpression= Attr('open_date_time').gte(open_date_time) & Attr('document_type').eq(document_type),
                  #   ConsistentRead=True)
                if last_key and page_size>0:
                    key1=json.loads(last_key)
                    response = table.scan(
                                FilterExpression= Attr('open_date_time').gte(open_date_time) & Attr('document_type').eq(document_type),
                                ExclusiveStartKey=key1,Limit=page_size,ConsistentRead=False)
                    if 'LastEvaluatedKey' in response:
                        LastEvaluatedKey=response['LastEvaluatedKey']   
                else:
                    if page_size>0:
                        response = table.scan(
                                    FilterExpression= Attr('open_date_time').gte(open_date_time) & Attr('document_type').eq(document_type),
                                    Limit=page_size,ConsistentRead=False)                    
                        if 'LastEvaluatedKey' in response:
                            LastEvaluatedKey=response['LastEvaluatedKey']  
                    else:
                        response = table.scan(
                                    FilterExpression= Attr('open_date_time').gte(open_date_time) & Attr('document_type').eq(document_type),
                                    ConsistentRead=False)
                        if 'LastEvaluatedKey' in response:
                            LastEvaluatedKey=response['LastEvaluatedKey']   
                        fetchAll=True

                try:
                    items = response['Items']  
                    if fetchAll:                   
                        while 'LastEvaluatedKey' in response:
                            table.scan(
                                FilterExpression= Attr('open_date_time').gte(open_date_time) & Attr('document_type').eq(document_type),
                                ConsistentRead=False,               
                                ExclusiveStartKey=response['LastEvaluatedKey']
                                )
                            try:
                                items.update(response['Items'])
                                if 'LastEvaluatedKey' in response:
                                    LastEvaluatedKey=response['LastEvaluatedKey'] 
                            except:
                                ""
                    return { "status":True,"items": items,"LastEvaluatedKey":LastEvaluatedKey }
                except:
                    return { "status":True,"items": [],"LastEvaluatedKey":LastEvaluatedKey } 
            
            else:
                # response = table.query(
                #     KeyConditionExpression= Key('document_type').eq(document_type),
                #    ConsistentRead=True)
                if last_key and page_size>0:
                    key1=json.loads(last_key)
                    response = table.query(
									KeyConditionExpression= Key('document_type').eq(document_type),
									ExclusiveStartKey=key1,Limit=page_size,ConsistentRead=False)                     
                    if 'LastEvaluatedKey' in response:
                        LastEvaluatedKey=response['LastEvaluatedKey']   
                else:
                    if page_size>0:
                        response = table.query(
									KeyConditionExpression= Key('document_type').eq(document_type),
									Limit=page_size,ConsistentRead=False)                                       
                        if 'LastEvaluatedKey' in response:
                            LastEvaluatedKey=response['LastEvaluatedKey']  
                    else:
                        response = table.query(
									KeyConditionExpression= Key('document_type').eq(document_type),
									ConsistentRead=False)
                        if 'LastEvaluatedKey' in response:
                            LastEvaluatedKey=response['LastEvaluatedKey']   
                        fetchAll=True
                try:
                    items = response['Items']    
                    if fetchAll:                 
                        while 'LastEvaluatedKey' in response:
                            response = table.query(
                                KeyConditionExpression= Key('document_type').eq(document_type),
                                ConsistentRead=False,               
                                ExclusiveStartKey=response['LastEvaluatedKey']
                                )
                            try:
                                items.update(response['Items'])
                                if 'LastEvaluatedKey' in response:
                                    LastEvaluatedKey=response['LastEvaluatedKey'] 
                            except:
                                ""
                    return { "status":True,"items": items ,"LastEvaluatedKey":LastEvaluatedKey}
                except:
                    return { "status":True,"items": [] ,"LastEvaluatedKey":LastEvaluatedKey}                
          
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 

    @classmethod
    def prepareDocumentList(self,items):
        _moduleNM="RepairOrder"
        _functionNM="prepareDocumentList"
        try:           
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)  
            documentList=[]
            for item in items :                 
                document_id=item.get('document_id')
                #self.logger.debug("in loop document_id :"+str(document_id))
                document={
                            "documentId": item.get('document_id'),
                            "documentType": item.get('document_type'),
                            "status": item.get('status'),
                            "openDateTime": item.get('open_date_time'),
                            "closeDateTime": item.get('close_date_time'),
                            "amountDue": item.get('amount_due'),
                            "warrantyDue": item.get('warranty_due'),
                            "comments": item.get('comments'),
                            "vehicle": {
                                        "vin": item.get('vehicle_vin'),
                                        "make": item.get('vehicle_make'),
                                        "model": item.get('vehicle_model'),
                                        "year": item.get('vehicle_year'),
                                        },
                            "customer": {
                                "id": item.get('customer_id'),
                                "firstName": item.get('customer_first_name'),
                                "lastName": item.get('customer_last_name'),
                                "email": item.get('customer_email'),
                                "addresses": [
                                    {
                                        "addressLine": [
                                        item.get('customer_addresses_addressline'),
                                        ],
                                        "city": item.get('customer_addresses_city'),
                                        "state": item.get('customer_addresses_state'),
                                        "zip": item.get('customer_addresses_zip'),
                                    }
                                ]                                 
                            },
                            "employee": {
                                "id": item.get('employee_id'),
                                "firstName": item.get('employee_first_name'),
                                "lastName": item.get('employee_last_name'),
                            } ,
                            "hattagnumber": item.get('hat_tag_number'),
                            "contactemail": item.get('contact_email'),
                            "contactphone": item.get('contact_phone') ,
                            "spcInstruction": item.get('spc_ins') 
                                                 

                        }
                documentList.append(document)

            return { "status":True,"documentList": documentList }
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)  
    
    @classmethod
    def prepareDocument(self,item,auth_json):
        _moduleNM="RepairOrder"
        _functionNM="prepareDocument"
        try:           
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)  
            docuemnt= {
                    "responseCode":0,
                    "documentId": item.get('document_id'),
                    "documentType": item.get('document_type'),
                    "status": item.get('status'),
                    "openDateTime": item.get('open_date_time'),
                    "closeDateTime": item.get('close_date_time'),
                    "amountDue": item.get('amount_due'),
                    "warrantyDue": item.get('warranty_due'),
                    "comments": item.get('comments'),
                    "vehicle": {
                                "vin": item.get('vehicle_vin'),
                                "make": item.get('vehicle_make'),
                                "model": item.get('vehicle_model'),
                                "year": item.get('vehicle_year'),
                                },
                    "customer": {
                        "id": item.get('customer_id'),
                        "firstName": item.get('customer_first_name'),
                        "lastName": item.get('customer_last_name'),
                        "email": item.get('customer_email'),
                        "addresses": [
                            {
                                "addressLine": [
                                    item.get('customer_addresses_addressline'),
                                ],
                                "city": item.get('customer_addresses_city'),
                                "state": item.get('customer_addresses_state'),
                                "zip": item.get('customer_addresses_zip'),
                            }
                        ], 
                        "contacts": [
                                        {
                                    "desc": "Contact Number",
                                    "value": item.get('contact_phone')
                                    }
                                ],
                        "company": item.get('company')
                    
                    },
                    "employee": {
                        "id": item.get('employee_id'),
                        "firstName": item.get('employee_first_name'),
                        "lastName": item.get('employee_last_name'),
                    },
                    "hattagnumber": item.get('hat_tag_number'),
                    "contactemail": item.get('contact_email'),
                    "contactphone": item.get('contact_phone'),
                    "spcInstruction": item.get('spc_ins'), 
                    "auth_token":auth_json
                    }            

            return { "status":True,"document": docuemnt }
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 

    @classmethod
    def Validate_ROList_inputs(self,open_date_time,document_type,auth_json):
        res_handler=ResponseHandler()
        if (document_type is None ) or document_type == "" or len(document_type.strip())==0:
           return res_handler.GetErrorResponseJSON(code=322,auth_json=auth_json)
        if document_type != "RO":
            list=[ str(document_type)]
            return res_handler.GetFormattedErrorResponseJSON(code=321,auth_json=auth_json,args=list)
        if (open_date_time is not None ) and len(str(open_date_time))>0:
               if not isinstance(open_date_time,datetime.datetime):
                    list=[ str(open_date_time)]
                    return res_handler.GetFormattedErrorResponseJSON(code=325,auth_json=auth_json,args=list)
        return {"responseCode":0}

    @classmethod
    def Validate_RODetail_inputs(self,document_id,document_type,auth_json):
        res_handler=ResponseHandler()
        if (document_id is None ) or document_id == "" or len(document_id.strip())==0:
            return res_handler.GetErrorResponseJSON(code=310,auth_json=auth_json)     
        if (document_type is None ) or document_type == "" or len(document_type.strip())==0:
            return res_handler.GetErrorResponseJSON(code=322,auth_json=auth_json)  
        if document_type != "RO":
            list=[ str(document_type)]
            return res_handler.GetFormattedErrorResponseJSON(code=321,auth_json=auth_json,args=list)
        return {"responseCode":0}     
    @classmethod
    def GetROStatistics(self,store_code,region):
        _moduleNM="RepairOrder"
        _functionNM="GetROStatistics"
        try:           
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)       
            self.logger.debug("GetROStatistics>> store_code:"+str(store_code))
            app_client=AppClient()
            store_resp=app_client.GetStoreDetail(store_code,region=region)
            if store_resp['status']==False:
               return store_resp
            ro_resp=self.GetROList(store_code,'RO',None,-1)           
            store_detail=store_resp['item']
            store_code=store_detail['store_code']
            StoreName=store_detail['store_name']
            log_on=store_detail['logon']
            Total_RO=0
            WarrantyDue=0	
            InvalidAmount=0	
            InvalidWarrantyDue=0
            C97_sts=0
            C98_sts=0
            I91_sts=0
            I98_sts=0
            C94_sts=0
            blank_sts=0
            Other_sts=0
            random_ro=''
            error_code=0
            error_message=''
            if ro_resp['status']==False:
                error_code=ro_resp['error_code']
                error_message=ro_resp['error_message']
            else:
                items=ro_resp['items']               
                Total_RO=len(items)              
                for item in items:
                    random_ro=item['document_id']
                    try:
                       amt=float(item['warranty_due'])
                       if amt>0:
                           WarrantyDue=WarrantyDue+1
                    except:
                        InvalidWarrantyDue=InvalidWarrantyDue+1

                    try:
                       amt=float(item['amount_due'])
                        
                    except:
                        InvalidAmount=InvalidAmount+1
                   
                    try:
                        status=item['status']
                        if status is None or status=='':
                            blank_sts=blank_sts+1
                        elif status=='C97':
                            C97_sts=C97_sts+1
                        elif status=='C98':
                            C98_sts=C98_sts+1
                        elif status=='C94':
                            C94_sts=C94_sts+1
                        elif status=='I98':
                            I98_sts=I98_sts+1
                        elif status=='I91':
                            I91_sts=I91_sts+1
                        else:
                            Other_sts=Other_sts+1
                    except:
                        blank_sts=blank_sts+1
                    
            ro_sts= {    "store_code":store_code,
                         "store_name":StoreName,
                         "log_on":log_on,
                         "total_ro":Total_RO,
                         "total_warranty_due":WarrantyDue,
                         "total_invalid_amount_due":InvalidAmount,
                         "total_invalid_warranty_due":InvalidWarrantyDue,
                         "status_C97":C97_sts,
                         "status_C98":C98_sts,
                         "status_C94":C94_sts,
                         "status_I98":I98_sts,
                         "status_I91":I91_sts,
                         "status_blank":blank_sts,
                         "status_other":Other_sts,
                         "random_ro":random_ro,
                         "error_code":error_code,
                         "error_message":error_message
                         }
            return {"status":True,"ro_state":ro_sts }            
           
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 