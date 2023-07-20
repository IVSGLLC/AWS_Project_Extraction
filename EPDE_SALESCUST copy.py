import json
from operator import itemgetter
from EPDE_Logging import LogManger
import boto3
from boto3.dynamodb.conditions import Key
from EPDE_Error import ErrorHandler
from EPDE_Response import ResponseHandler
#This module is responsible to handle EPDE API RepairOrder handling
class SalesCustomer(object):
    logger=LogManger()   
    err_handler=ErrorHandler()
    region='us-east-1'
    table_Per_Store_salescust=1
    def __init__(self):        
        SalesCustomer.region="us-east-1"
        SalesCustomer.table_Per_Store_salescust=1
        
    def __init__(self,region,table_Per_Store_salescust_Detail=1):  
        SalesCustomer.region=region
        SalesCustomer.table_Per_Store_salescust=table_Per_Store_salescust_Detail
         
    @classmethod   
    def getTableName(self,store_code):
        TableName=store_code+"_SALESCUST_FILE"          
        return TableName
    
    @classmethod
    def prepareSalesCustomerListResponse(self,items,auth_json):
        _moduleNM="SalesCustomer"
        _functionNM="prepareSalesCustomerListResponse"
        try:           
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)  
            salesCustomerList=[]
            salesDetailList=[]
            customerNumber=''
            customerName=''
            for item in items :                 
                customerNumber= item.get('buyer_number')
                customerName= item.get('buyer_name')
                customerEmail= item.get('buyer_email')
                #customerEmailDesc= item.get('buyer_email_desc')
                res = None
                sales={
                            "dealNumber": item.get('deal_number'),
                            "soldDate": item.get('date_sold'),
                            "soldAge": item.get('sold_age'),
                            "vin": item.get('vin'),    
                            "make": item.get('make'),       
                            "model": item.get('model'),       
                            "year": item.get('year'),       
                            "status": item.get('status'),    
                            "cashDown": item.get('cashDown'),                                 
                        }
                index=0
                for cust in salesCustomerList:
                    if cust['customerNumber'] == customerNumber:
                        res = cust
                        break
                    index=index+1
                if res==None:
                    salesDetailList=[]
                else:
                    salesDetailList=cust['salesDetailList']
                salesDetailList.append(sales)
                salesDetailList=sorted(salesDetailList, key=itemgetter('dealNumber'))  
                salesCustomerDetail={                           
                            "customerNumber": customerNumber,
                            "customerName": customerName,
                            "customerEmail":customerEmail,
                            "salesDetailList":salesDetailList          
                        }
                if res==None:
                    salesCustomerList.append(salesCustomerDetail) 
                else:
                    salesCustomerList[index]= salesCustomerDetail
            salesCustomerList=sorted(salesCustomerList, key=itemgetter('customerNumber'))   
            return { "status":True,"salesCustomerList": salesCustomerList }
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)  
     
    @classmethod
    def GetSalesCustomerList(self,store_code,last_key=None,page_size=-1):
        _moduleNM="SalesCustomer"
        _functionNM="GetSalesCustomerList"
        try:           
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)       
            self.logger.debug("GetSalesCustomerList>> store_code:"+str(store_code))           
            dynamodb = boto3.resource('dynamodb', region_name=SalesCustomer.region)
            TableName=self.getTableName(store_code)
            table = dynamodb.Table(TableName)
            fetchAll=False
            LastEvaluatedKey=None
            if last_key and page_size>0:
                
                key1=json.loads(last_key)
                response = table.scan(ExclusiveStartKey=key1,Limit=page_size,ConsistentRead=False)
                if 'LastEvaluatedKey' in response:
                    LastEvaluatedKey=response['LastEvaluatedKey']   
            else:
                if page_size>0:
                   response = table.scan(Limit=page_size,ConsistentRead=False)
                   if 'LastEvaluatedKey' in response:
                       LastEvaluatedKey=response['LastEvaluatedKey']  
                else:
                   response = table.scan(ConsistentRead=False)
                   if 'LastEvaluatedKey' in response:
                       LastEvaluatedKey=response['LastEvaluatedKey']   
                   fetchAll=True
            try:
                items = response['Items']
                if fetchAll:             
                    while 'LastEvaluatedKey' in response:
                        table.scan(ConsistentRead=False,               
                            ExclusiveStartKey=response['LastEvaluatedKey']
                            )
                        try:
                            items.update(response['Items'])
                            if 'LastEvaluatedKey' in response:
                                LastEvaluatedKey=response['LastEvaluatedKey']   
                        except:
                            ""
                        
                return { "status":True,"items": items,"LastEvaluatedKey": LastEvaluatedKey}
            except:
                return { "status":True,"items": [],"LastEvaluatedKey":LastEvaluatedKey } 
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 

    @classmethod
    def GetSalesCustomerDetail(self,store_code,buyer_number):
        _moduleNM="SalesCustomer"
        _functionNM="GetSalesCustomerDetail"
        try:           
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)       
            self.logger.debug("GetSalesCustomerDetail>> store_code:"+str(store_code)+",buyer_number:"+str(buyer_number))
           
            dynamodb = boto3.resource('dynamodb', region_name=SalesCustomer.region)
            TableName=self.getTableName(store_code)
            table = dynamodb.Table(TableName)
            #response = table.scan(FilterExpression= Attr('buyer_number').eq(buyer_number) ,ConsistentRead=False)
            response = table.query(
                KeyConditionExpression=Key('buyer_number').eq(buyer_number),
                ConsistentRead=False)
            self.logger.debug("response:"+str(response))
            try:
                if response['Count'] > 0:    
                    items = response['Items']                
                    while 'LastEvaluatedKey' in response:
                        response = table.query(
                            KeyConditionExpression=Key('buyer_number').eq(buyer_number),
                            ConsistentRead=False,               
                            ExclusiveStartKey=response['LastEvaluatedKey']
                            )
                        try:
                            items.update(response['Items'])
                        except:
                            ""
                    return { "status":True,"items": items }
                else:
                    return self.err_handler.HandleAppError(343,moduleNM=_moduleNM,functionNM=_functionNM) 
            except:
                   return self.err_handler.HandleAppError(343,moduleNM=_moduleNM,functionNM=_functionNM) 
            
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 

    @classmethod
    def prepareSalesCustomerResponse(self,items,auth_json):
        _moduleNM="SalesCustomer"
        _functionNM="prepareSalesCustomerResponse"
        try:           
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)  
            salesDetailList=[]
            customerNumber=''
            customerName=''
            for item in items :                 
                deal_number=item.get('deal_number')
                customerNumber= item.get('buyer_number')
                customerName= item.get('buyer_name')
                customerEmail=item.get('buyer_email')
                #customerEmailDesc=item.get('buyer_email_desc')
                self.logger.debug("in loop deal_number :"+str(deal_number))
                sales={
                            "dealNumber": item.get('deal_number'),
                            "soldDate": item.get('date_sold'),
                            "soldAge": item.get('sold_age') ,
                            "vin": item.get('vin'),    
                            "make": item.get('make'),       
                            "model": item.get('model'),       
                            "year": item.get('year'),       
                            "status": item.get('status'),
                            "cashDown": item.get('cashDown'),                                            
                        }
                salesDetailList.append(sales)
            
            salesCustomerDetail={
                            "responseCode":0,
                            "customerNumber": customerNumber,
                            "customerName": customerName,
                            "customerEmail":customerEmail,
                            "salesDetailList":salesDetailList,
                            "auth_token":auth_json                      
                        }
            return { "status":True,"salesCustomerDetail": salesCustomerDetail }
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)  
    
    @classmethod
    def Validate_SalesCustDetail_inputs(self,customer_number,auth_json):
        res_handler=ResponseHandler()
        if (customer_number is None ) or customer_number == "" or len(customer_number.strip())==0:
            return res_handler.GetErrorResponseJSON(code=330,auth_json=auth_json)     
        return {"responseCode":0}     
    @classmethod
    def Validate_SalesCustList_inputs(self,auth_json):
        return {"responseCode":0}  