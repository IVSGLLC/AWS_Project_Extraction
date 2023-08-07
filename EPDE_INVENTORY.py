from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.conditions import Key 
import urllib.parse
import json
from EPDE_Client import AppClient 
from EPDE_Logging import LogManger
import boto3
from boto3.dynamodb.conditions import Key
from EPDE_Error import ErrorHandler
from datetime import datetime, timedelta
from EPDE_Response import ResponseHandler
from boto3.dynamodb.types import TypeDeserializer
import Batching 
from decimal import Decimal
#This module is responsible to handle EPDE API Inventory handling
class Inventory(object):
    logger=LogManger()   
    err_handler=ErrorHandler()
    region='us-east-1'
    table_Per_Store=1
    def __init__(self):        
        Inventory.region="us-east-1"
        Inventory.table_Per_Store=1
        
    def __init__(self,region,table_Per_Store_Invoice=1):  
        Inventory.region=region
        Inventory.table_Per_Store=table_Per_Store_Invoice
    @classmethod
    def chunks(self,lst, n):       
        for i in range(0, len(lst), n):
            yield lst[i:i + n]  
                
    @classmethod   
    def getTableName(self,store_code):
        TableName=store_code+"_INVENTORY_FILE"         
        return TableName
    @classmethod
    def GetFilteredListNow(self,store_code,last_key=None,page_size=-1,inventoryNow=None):
        _moduleNM="Inventory"
        _functionNM="GetFilteredListNow"
        try:   
            projectionExpression=None
            starttime = datetime.now()
            self.logger.debug("GetFilteredListNow>> store_code:"+str(store_code)+",last_key="+str(last_key)+",pageSize="+str(page_size)+",inventoryNow="+str(inventoryNow))
            dynamodb = boto3.resource('dynamodb', region_name=Inventory.region)
            TableName=self.getTableName(store_code)
            table = dynamodb.Table(TableName)                   
            items=[]
            fetchAll=False
            LastEvaluatedKey=None
            response=None            
            filterExpression=None
            keyConditionExpression= Key('document_type').eq('INV')           
            if inventoryNow is not None :
                    stockflag_filter_expression=None  
                    stockflagList=['A','I']
                    if str(inventoryNow).upper()=='FALSE': 
                        stockflagList=['A','O']
                    stockflag_filter_expression=Attr("stockFlag").is_in(stockflagList)
                    if filterExpression is None:
                        filterExpression=stockflag_filter_expression
                    else:
                        filterExpression=filterExpression & stockflag_filter_expression
                    projectionExpression='document_id,stocks,stockFlag'
                       
            if filterExpression is not None:
                if last_key and len(last_key)>0 and page_size>0 :                     
                    try:
                            key1=json.loads(last_key)
                    except  Exception as e:
                            sq=urllib.parse.unquote(last_key)
                            key1 = json.loads(str(sq)) 
                    
                    if projectionExpression is None:
                        response = table.query(
                                        KeyConditionExpression= keyConditionExpression,                                 
                                        FilterExpression= filterExpression,
                                        ExclusiveStartKey=key1,Limit=page_size,ConsistentRead=False) 
                    else:
                        response = table.query(
                                        ProjectionExpression=projectionExpression,
                                        KeyConditionExpression= keyConditionExpression,                                 
                                        FilterExpression= filterExpression,
                                        ExclusiveStartKey=key1,Limit=page_size,ConsistentRead=False) 
                 

                else:
                    if last_key and  len(last_key)>0 :                     
                        try:
                            key1=json.loads(last_key)
                        except  Exception as e:
                            sq=urllib.parse.unquote(last_key)
                            key1 = json.loads(str(sq))  
                        if projectionExpression is None:
                            response = table.query(
                                        KeyConditionExpression= keyConditionExpression, 
                                        FilterExpression= filterExpression,
                                        ExclusiveStartKey=key1,ConsistentRead=False)
                        else:
                            response = table.query(
                                        ProjectionExpression=projectionExpression,
                                        KeyConditionExpression= keyConditionExpression, 
                                        FilterExpression= filterExpression,
                                        ExclusiveStartKey=key1,ConsistentRead=False)
                    elif page_size>0:
                        if projectionExpression is None:
                            response = table.query(
                                        KeyConditionExpression= keyConditionExpression, 
                                        FilterExpression= filterExpression,
                                        Limit=page_size,ConsistentRead=False)
                        else:
                            response = table.query(
                                        ProjectionExpression=projectionExpression,
                                        KeyConditionExpression= keyConditionExpression, 
                                        FilterExpression= filterExpression,
                                        Limit=page_size,ConsistentRead=False)
                    else:
                        if projectionExpression is None:
                            response = table.query(
                                        KeyConditionExpression= keyConditionExpression, 
                                        FilterExpression= filterExpression,
                                        ConsistentRead=False)
                        else:
                            response = table.query(
                                        ProjectionExpression=projectionExpression,
                                        KeyConditionExpression= keyConditionExpression, 
                                        FilterExpression= filterExpression,
                                        ConsistentRead=False)

                callTimeOut=10       
                endtime = datetime.now()
                delta=endtime-starttime
                consumedSeconds=delta.total_seconds()  
                self.logger.debug("consumedSeconds="+str(consumedSeconds))         
                self.logger.debug("response="+str(response))
                if 'LastEvaluatedKey' in response:
                        LastEvaluatedKey=response['LastEvaluatedKey'] 
                        self.logger.debug("LastEvaluatedKey="+str(LastEvaluatedKey))
                        if(consumedSeconds+consumedSeconds )<callTimeOut:  
                            fetchAll=True               
                if 'Items' in response:
                    items = response['Items']
                self.logger.debug("Total items="+str(len(items)))
                page_size_new=0
                if page_size>0: 
                    if fetchAll==True and len(items)<page_size:
                        fetchAll=True
                        page_size_new=page_size-len(items)
                    else:                    
                        fetchAll=False
                        self.logger.debug("Total items  is Greater or equal to page size="+str(page_size))        
                
                if  fetchAll ==True:                
                    while 'LastEvaluatedKey' in response:
                        starttime = datetime.now()
                        if page_size>0:
                            if projectionExpression is None:
                                response = table.query(
                                    KeyConditionExpression= keyConditionExpression,
                                    FilterExpression= filterExpression,
                                    ConsistentRead=False,Limit=page_size_new ,            
                                    ExclusiveStartKey=response['LastEvaluatedKey']
                                )
                            else:
                                response = table.query(
                                    ProjectionExpression=projectionExpression,
                                    KeyConditionExpression= keyConditionExpression,
                                    FilterExpression= filterExpression,
                                    ConsistentRead=False,Limit=page_size_new ,               
                                    ExclusiveStartKey=response['LastEvaluatedKey']
                                )
                        else:
                            if projectionExpression is None:
                                response = table.query(
                                    KeyConditionExpression= keyConditionExpression,
                                    FilterExpression= filterExpression,
                                    ConsistentRead=False,               
                                    ExclusiveStartKey=response['LastEvaluatedKey']
                                )
                            else:
                                response = table.query(
                                    ProjectionExpression=projectionExpression,
                                    KeyConditionExpression= keyConditionExpression,
                                    FilterExpression= filterExpression,
                                    ConsistentRead=False,               
                                    ExclusiveStartKey=response['LastEvaluatedKey']
                                )
                        try:
                            items1=response['Items']
                            self.logger.debug("loop inner Total items="+str(len(items1)))
                            items.extend(items1)
                            self.logger.debug("loop after adding final Total items"+str(len(items)))
                            LastEvaluatedKey=None
                            page_size_new=page_size_new-len(items1) 
                            if 'LastEvaluatedKey' in response:
                                LastEvaluatedKey=response['LastEvaluatedKey']   
                                self.logger.debug("Loop LastEvaluatedKey="+str(LastEvaluatedKey))
                            if LastEvaluatedKey is None or (page_size>0 and len(items)>=page_size):
                                #self.logger.debug("Loop LastEvaluatedKey="+str(LastEvaluatedKey))
                                break                               
                        except:
                            self.logger.error(">> error=",True) 
                        endtime = datetime.now()
                        delta=endtime-starttime
                        consumedSeconds=consumedSeconds+delta.total_seconds()
                        #self.logger.debug("loop consumedSeconds="+str(consumedSeconds))                           
                        if(delta.total_seconds()+consumedSeconds )>=callTimeOut:    
                            self.logger.debug("loop exceeded consumedSeconds="+str(consumedSeconds))  
                            break 
                            
                if LastEvaluatedKey is not None :                  
                    LastEvaluatedKey = json.dumps(LastEvaluatedKey) 
                    LastEvaluatedKey=urllib.parse.quote_plus(str(LastEvaluatedKey) ) 
            self.logger.debug("GetFilteredListNow LastEvaluatedKey>>"+str(LastEvaluatedKey))
            return { "status":True,"items": items, "LastEvaluatedKey":LastEvaluatedKey }                              
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)      
    @classmethod
    def GetInventoryList(self,store_code,document_type,last_key=None,page_size=-1):
        _moduleNM="Inventory"
        _functionNM="GetInventoryList"
        try:                 
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)       
            self.logger.debug("GetInventoryList>> store_code:"+str(store_code))           
            dynamodb = boto3.resource('dynamodb', region_name=Inventory.region)
            TableName=self.getTableName(store_code)
            table = dynamodb.Table(TableName)
            LastEvaluatedKey=None
            response=None
            items=[]
            fetchAll=False
            starttime = datetime.now()
            if last_key and page_size>0:                
                try:
                    key1=json.loads(last_key)
                except  Exception as e:
                        sq=urllib.parse.unquote(last_key)
                        key1 = json.loads((sq)) 
                response = table.query(
                     KeyConditionExpression= Key('document_type').eq(document_type),
                     ExclusiveStartKey=key1,ConsistentRead=False,Limit=page_size)
                if 'LastEvaluatedKey' in response:
                    LastEvaluatedKey=response['LastEvaluatedKey']   
            else:
                if last_key:
                    try:
                            key1=json.loads(last_key)
                    except  Exception as e:
                            sq=urllib.parse.unquote(last_key)
                            key1 = json.loads(str(sq)) 

                    response = table.query(
                        KeyConditionExpression= Key('document_type').eq(document_type),
                        ConsistentRead=False, ExclusiveStartKey=key1)
                    if 'LastEvaluatedKey' in response:
                        LastEvaluatedKey=response['LastEvaluatedKey']  
                elif page_size>0:
                   response = table.query(
                     KeyConditionExpression= Key('document_type').eq(document_type),
                     ConsistentRead=False,Limit=page_size)
                   if 'LastEvaluatedKey' in response:
                       LastEvaluatedKey=response['LastEvaluatedKey']  
                else:
                    response = table.query(
                     KeyConditionExpression= Key('document_type').eq(document_type),
                     ConsistentRead=False)
                    if 'LastEvaluatedKey' in response:
                       LastEvaluatedKey=response['LastEvaluatedKey'] 
                       #fetchAll=True  
                    
           
            callTimeOut=5     
            endtime = datetime.now()
            delta=endtime-starttime
            consumedSeconds=delta.total_seconds()  
            self.logger.debug("consumedSeconds="+str(consumedSeconds))         
            self.logger.debug("response="+str(response))
            if 'LastEvaluatedKey' in response:
                    LastEvaluatedKey=response['LastEvaluatedKey'] 
                    self.logger.debug("LastEvaluatedKey="+str(LastEvaluatedKey))
                    if(consumedSeconds+consumedSeconds )<callTimeOut:  
                       fetchAll=True
                       
            if 'Items' in response:
                items = response['Items']  
            self.logger.debug("Total items="+str(len(items)))
            page_size_new=0
            if page_size>0: 
                if fetchAll==True and len(items)<page_size:
                    fetchAll=True
                    page_size_new=page_size-len(items)
                else:                    
                    fetchAll=False
                    self.logger.debug("Total items  is Greater or equal to page size="+str(page_size))     
            if  fetchAll ==True:                
                    while 'LastEvaluatedKey' in response:
                        starttime = datetime.now()
                        if page_size>0:
                                response = table.query(
                                    KeyConditionExpression= Key('document_type').eq(document_type),
                                    ConsistentRead=False,Limit=page_size_new ,              
                                    ExclusiveStartKey=response['LastEvaluatedKey']
                                    )
                        else:
                                response = table.query(
                                    KeyConditionExpression= Key('document_type').eq(document_type),
                                    ConsistentRead=False,               
                                    ExclusiveStartKey=response['LastEvaluatedKey']
                                    )
                         
                        try:
                            items1=response['Items']
                            self.logger.debug("loop inner Total items="+str(len(items1)))
                            items.extend(items1)
                            self.logger.debug("loop after adding final Total items"+str(len(items)))
                            LastEvaluatedKey=None
                            page_size_new=page_size_new-len(items1) 
                            if 'LastEvaluatedKey' in response:
                                LastEvaluatedKey=response['LastEvaluatedKey']   
                                self.logger.debug("Loop LastEvaluatedKey="+str(LastEvaluatedKey))
                           
                            if LastEvaluatedKey is None or (page_size>0 and len(items)>=page_size):
                                break        
                               
                        except:
                            self.logger.error(">> error=",True) 
                        endtime = datetime.now()
                        delta=endtime-starttime
                        consumedSeconds=consumedSeconds+delta.total_seconds()
                        self.logger.debug("loop consumedSeconds="+str(consumedSeconds))                           
                        if(delta.total_seconds()+consumedSeconds )>=callTimeOut:    
                           self.logger.debug("loop exceeded consumedSeconds="+str(consumedSeconds))  
                           break 
                        
            if LastEvaluatedKey is not None :                  
                LastEvaluatedKey = json.dumps(LastEvaluatedKey) 
                LastEvaluatedKey=urllib.parse.quote_plus(str(LastEvaluatedKey) ) 
            self.logger.debug("GetFilteredList LastEvaluatedKey>>"+str(LastEvaluatedKey))
            return { "status":True,"items": items, "LastEvaluatedKey":LastEvaluatedKey }    
                         
             
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 
    @classmethod
    def GetVINSList(self,vinFilters):
        vinList=None         
        if vinFilters is not None:  
            if str(vinFilters) == "ALL":  
              return None
            vinList=str(vinFilters).split(',')
            if len(vinList)>1:
                vinList=list(set(vinList))
        return  vinList  
    @classmethod
    def GetFilteredList(self,store_code,vinList,gpsServicePlan,fromDate,toDate,activeServicePlans,stockflag,fromEntryDate,toEntryDate,last_key=None,page_size=-1):
        _moduleNM="Inventory"
        _functionNM="GetFilteredList"
        try:   
            starttime = datetime.now()
            self.logger.debug("GetFilteredList>> store_code:"+str(store_code)+",vinList="+str(vinList)+",fromDate="+str(fromDate)+",toDate="+str(toDate)+",last_key="+str(last_key)+",pageSize="+str(page_size)+",stockflag="+str(stockflag)+",fromEntryDate="+str(fromEntryDate)+",toEntryDate="+str(toEntryDate))
            dynamodb = boto3.resource('dynamodb', region_name=Inventory.region)
            TableName=self.getTableName(store_code)
            table = dynamodb.Table(TableName)                   
            items=[]
            fetchAll=False
            LastEvaluatedKey=None
            response=None
            vinList=self.GetVINSList(vinList)
            self.logger.debug("GetFilteredList vinList>>"+str(vinList))
            filterExpression=None
            keyConditionExpression= Key('document_type').eq('INV')           
            date_filter_expression=None
            if vinList is not None and not vinList.__contains__('ALL') :
                if len(vinList) ==1:
                    keyConditionExpression=keyConditionExpression & Key('document_id').eq(vinList[0])
                    response = table.query(
                                    KeyConditionExpression= keyConditionExpression, 
                                    ConsistentRead=False)
                    if 'Items' in response:
                        items = response['Items']  
                    return { "status":True,"items": items, "LastEvaluatedKey":None }                  
                else:
                    batch_list=[]
                    for vin in vinList:
                        batch_list.append({"document_type":'INV','document_id':vin})  
                    tab_existingitems=Batching.get_batch_data(dynamodb=dynamodb,tableName=TableName,item_list=batch_list)
                    items=tab_existingitems[TableName]  
                    return { "status":True,"items": items, "LastEvaluatedKey":None } 

            elif gpsServicePlan is not None  and fromDate is not None and toDate is not None:
                    plan_filter_expression=None
                    if gpsServicePlan.upper() == "ALL":                    
                        if len(activeServicePlans)>0:                            
                            for plan in activeServicePlans:
                                if plan_filter_expression ==None:
                                    plan_filter_expression=Attr("servicePlans").contains(plan.upper())                                    
                                else:
                                    plan_filter_expression=plan_filter_expression | Attr("servicePlans").contains(plan.upper())
                    elif gpsServicePlan.upper() == "NONE":
                        plan_filter_expression=Attr("servicePlans").contains('')                          
                    else:
                        plan_filter_expression=Attr("servicePlans").contains(gpsServicePlan.upper()) 
                    
                    date_filter_expression=self.GetDateFilterExpression(fromDtStr=fromDate,toDtStr=toDate,searchFieldName="searchDate",minDateFieldName="dealMinSoldDate",maxDateFieldName="dealMaxSoldDate")
                   
                    if filterExpression is None:
                        filterExpression= (plan_filter_expression)  & (date_filter_expression ) 
                    else:
                        filterExpression=(filterExpression) & (plan_filter_expression)  & (date_filter_expression )    
                 
                    
            elif stockflag is not None  and fromEntryDate is not None and toEntryDate is not None:
                    stockflag_filter_expression=None                  
                    if stockflag=='A':
                       stockflagList=['A','I','O']
                       stockflag_filter_expression=Attr("stockFlag").is_in(stockflagList) 
                    elif stockflag=='I':
                       stockflagList=['A','I']
                       stockflag_filter_expression=Attr("stockFlag").is_in(stockflagList)
                    elif stockflag=='O':
                       stockflagList=['A','O']
                       stockflag_filter_expression=Attr("stockFlag").is_in(stockflagList)                 
                    
                    date_filter_expression=self.GetDateFilterExpression(fromDtStr=fromEntryDate,toDtStr=toEntryDate,searchFieldName="searchEntryDate",minDateFieldName="stockMinEntryDate",maxDateFieldName="stockMaxEntryDate")
                    if filterExpression is None:
                        filterExpression=(stockflag_filter_expression) & (date_filter_expression)
                    else:
                        filterExpression=(filterExpression) & (stockflag_filter_expression) & (date_filter_expression)   
            elif fromDate is not None and toDate is not None:
                    date_filter_expression=self.GetDateFilterExpression(fromDtStr=fromDate,toDtStr=toDate,searchFieldName="searchDate",minDateFieldName="dealMinSoldDate",maxDateFieldName="dealMaxSoldDate")
                    if filterExpression is None:
                        filterExpression=date_filter_expression
                    else:
                        filterExpression=(filterExpression) & (date_filter_expression ) 
                    
            elif fromEntryDate is not None and toEntryDate is not None:
                    date_filter_expression=self.GetDateFilterExpression(fromDtStr=fromEntryDate,toDtStr=toEntryDate,searchFieldName="searchEntryDate",minDateFieldName="stockMinEntryDate",maxDateFieldName="stockMaxEntryDate")
                    if filterExpression is None:
                        filterExpression= date_filter_expression 
                    else:
                        filterExpression= (filterExpression) & ( date_filter_expression)              
            
            if filterExpression is not None:
                if last_key and len(last_key)>0 and page_size>0 :                     
                    try:
                            key1=json.loads(last_key)
                    except  Exception as e:
                            sq=urllib.parse.unquote(last_key)
                            key1 = json.loads(str(sq)) 
                    
                    response = table.query(
                                    KeyConditionExpression= keyConditionExpression,                                 
                                    FilterExpression= filterExpression,
                                    ExclusiveStartKey=key1,Limit=page_size,ConsistentRead=False) 
                else:
                    if last_key and  len(last_key)>0 :                     
                        try:
                            key1=json.loads(last_key)
                        except  Exception as e:
                            sq=urllib.parse.unquote(last_key)
                            key1 = json.loads(str(sq))  
                        
                        response = table.query(
                                        KeyConditionExpression= keyConditionExpression, 
                                        FilterExpression= filterExpression,
                                        ExclusiveStartKey=key1,ConsistentRead=False)
                    elif page_size>0:
                        response = table.query(
                                        KeyConditionExpression= keyConditionExpression, 
                                        FilterExpression= filterExpression,
                                        Limit=page_size,ConsistentRead=False)
                    else:
                        response = table.query(
                                        KeyConditionExpression= keyConditionExpression, 
                                        FilterExpression= filterExpression,
                                        ConsistentRead=False)
                callTimeOut=5       
                endtime = datetime.now()
                delta=endtime-starttime
                consumedSeconds=delta.total_seconds()  
                self.logger.debug("consumedSeconds="+str(consumedSeconds))         
                self.logger.debug("response="+str(response))
                if 'LastEvaluatedKey' in response:
                        LastEvaluatedKey=response['LastEvaluatedKey'] 
                        self.logger.debug("LastEvaluatedKey="+str(LastEvaluatedKey))
                        if(consumedSeconds+consumedSeconds )<callTimeOut:  
                            fetchAll=True
                if 'Items' in response:
                    items = response['Items']  
                self.logger.debug("Total items="+str(len(items)))
                page_size_new=0
                if page_size>0: 
                    if fetchAll==True and len(items)<page_size:
                        fetchAll=True
                        page_size_new=page_size-len(items)
                    else:                    
                        fetchAll=False
                        self.logger.debug("Total items  is Greater or equal to page size="+str(page_size))    

                if  fetchAll ==True:                
                    while 'LastEvaluatedKey' in response:
                        starttime = datetime.now()
                        if page_size>0:
                            response = table.query(
                                    KeyConditionExpression= keyConditionExpression,
                                    FilterExpression= filterExpression,
                                    ConsistentRead=False,Limit=page_size_new ,               
                                    ExclusiveStartKey=response['LastEvaluatedKey']
                            )
                        else:
                            response = table.query(
                                    KeyConditionExpression= keyConditionExpression,
                                    FilterExpression= filterExpression,
                                    ConsistentRead=False,               
                                    ExclusiveStartKey=response['LastEvaluatedKey']
                                    )
                        try:
                            items1=response['Items']
                            self.logger.debug("loop inner Total items="+str(len(items1)))
                            items.extend(items1)
                            self.logger.debug("loop after adding final Total items"+str(len(items)))
                            LastEvaluatedKey=None
                            page_size_new=page_size_new-len(items1) 
                            if 'LastEvaluatedKey' in response:
                                LastEvaluatedKey=response['LastEvaluatedKey']   
                                self.logger.debug("Loop LastEvaluatedKey="+str(LastEvaluatedKey))
                            if LastEvaluatedKey is None or (page_size>0 and len(items)>=page_size):
                                #self.logger.debug("Loop LastEvaluatedKey="+str(LastEvaluatedKey))
                                break                               
                        except:
                            self.logger.error(">> error=",True) 
                        endtime = datetime.now()
                        delta=endtime-starttime
                        consumedSeconds=consumedSeconds+delta.total_seconds()
                        #self.logger.debug("loop consumedSeconds="+str(consumedSeconds))                           
                        if(delta.total_seconds()+consumedSeconds )>=callTimeOut:    
                            self.logger.debug("loop exceeded consumedSeconds="+str(consumedSeconds))  
                            break 
                            
                if LastEvaluatedKey is not None :                  
                    LastEvaluatedKey = json.dumps(LastEvaluatedKey) 
                    LastEvaluatedKey=urllib.parse.quote_plus(str(LastEvaluatedKey) ) 
            self.logger.debug("GetFilteredList LastEvaluatedKey>>"+str(LastEvaluatedKey))
            return { "status":True,"items": items, "LastEvaluatedKey":LastEvaluatedKey }                              
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)      
   
    @classmethod
    def GetDateFilterExpressionOld(self,fromDtStr,toDtStr,searchFieldName,minDateFieldName,maxDateFieldName):
        fromDt=datetime.strptime(fromDtStr, '%d%b%y')
        fromDt=fromDt+ timedelta(days=-1)
        fromDate = fromDt.strftime("%Y-%m-%d")  
        toDt=datetime.strptime(toDtStr, '%d%b%y')
        toDt=toDt+ timedelta(days=1)
        toDate = toDt.strftime("%Y-%m-%d") 
        return  (Attr(searchFieldName).ne("") & Attr(searchFieldName).between(fromDate,toDate)) | (Attr(minDateFieldName).ne("") & Attr(minDateFieldName).between(fromDate,toDate) ) | (Attr(maxDateFieldName).ne("") & Attr(maxDateFieldName).between(fromDate,toDate))
    @classmethod
    def GetDateFilterExpression(self,fromDtStr,toDtStr,searchFieldName,minDateFieldName,maxDateFieldName):
        fromDt=datetime.strptime(fromDtStr, '%d%b%y')
        #fromDt=fromDt+ timedelta(days=-1)
        fromDate = fromDt.strftime("%Y-%m-%d")  
        toDt=datetime.strptime(toDtStr, '%d%b%y')
        #toDt=toDt+ timedelta(days=1)
        toDate = toDt.strftime("%Y-%m-%d") 
        cond1=Attr(minDateFieldName).ne("") &  Attr(minDateFieldName).lte(toDate) 
        cond2=Attr(maxDateFieldName).ne("") & Attr(maxDateFieldName).gte(fromDate) 
        return  (Attr(searchFieldName).ne("") & Attr(searchFieldName).between(fromDate,toDate)) | (cond1 & cond2)
      
    @classmethod
    def GetFilteredDeals(self,deals,plan,activePlan,fromDate,toDate):
        #self.logger.debug("GetFilteredDeals plan="+str(plan)+",fromDate="+str(fromDate)+",toDate="+str(toDate))
        if deals is None or len(deals)<=0 :
            return deals
        if fromDate is  None and toDate  is None and plan is None: 
            return deals
        plans=None
        if plan is not None and plan.upper() =="NONE" :
            plans=[""]
        elif plan is not None and plan.upper() =="ALL":
            plans=activePlan 
        elif plan is not None :
            plans=[plan.upper()]
        soldFromDate=None
        soldToDate=None
        if fromDate is not None and toDate is not None:
            soldFromDate=datetime.strptime(fromDate, '%d%b%y')
            soldToDate=datetime.strptime(toDate, '%d%b%y')
            #self.logger.debug("GetFilteredDeals soldFromDate="+str(soldFromDate))
            #self.logger.debug("GetFilteredDeals soldToDate="+str(soldToDate))
        deals1=[]           
        if deals is not None and len(deals)>0: 
             for deal in deals :
                if soldFromDate is not None and soldToDate is not None :
                    try:
                        dealSoldDate=datetime.strptime(deal["soldDate"], '%d%b%y')
                        #self.logger.debug("loop dealSoldDate="+str(dealSoldDate))
                        if soldFromDate<=dealSoldDate and dealSoldDate<=soldToDate:                           
                            if plans is not None:
                              if str(deal["desc"]).upper() in plans: 
                                 #self.logger.debug("loop date and plan match deal="+str(deal))  
                                 deals1.append(deal)
                            else:
                                #self.logger.debug("loop date match deal="+str(deal))  
                                deals1.append(deal)
                    except:
                        self.logger.debug("error in date match deal="+str(deal))  
                else:
                    if plans is not None:
                     if str(deal["desc"]).upper() in plans: 
                        #self.logger.debug("loop plan match deal="+str(deal))                    
                        deals1.append(deal)
        return  deals1 
    @classmethod
    def GetFilteredDealsForRepairOrders(self,deals,plan,activePlan):
        #self.logger.debug("GetFilteredDealsForRepairOrders plan="+str(plan))
        if deals is None or len(deals)<=0 :
            return deals        
        plans=None
        if plan is not None and plan.upper() =="NONE" :
            plans=[""]
        elif plan is not None and plan.upper() =="ALL":
            plans=activePlan 
        elif plan is not None :
            plans=[plan.upper()]
        deals1=[]           
        if deals is not None and len(deals)>0 and plans is not None: 
             for deal in deals :
                if str(deal["desc"]).upper() in plans: 
                    #self.logger.debug("loop plan match deal="+str(deal))                    
                    deals1.append(deal)
        return  deals1 
    @classmethod
    def GetFilteredROsForRepairOrders(self,roList,fromDate,toDate):
        #self.logger.debug("GetFilteredROs plan="+str(plan)+",fromDate="+str(fromDate)+",toDate="+str(toDate))
        if roList is None or len(roList)<=0 :
            return roList
        if fromDate is  None and toDate  is None: 
            return roList        
        openFromDate=None
        openToDate=None
        if fromDate is not None and toDate is not None:
            openFromDate=datetime.strptime(fromDate, '%d%b%y')
            openToDate=datetime.strptime(toDate, '%d%b%y')
            #self.logger.debug("GetFilteredROs openFromDate="+str(openFromDate))
            #self.logger.debug("GetFilteredROs openToDate="+str(openToDate))
        roList1=[]           
        if roList is not None and len(roList)>0: 
             for ro in roList :
                if openFromDate is not None and openToDate is not None :
                    try:
                        roOpenDate=datetime.strptime(ro["openDate"], '%d%b%y')
                        #self.logger.debug("loop roOpenDate="+str(roOpenDate))
                        if openFromDate<=roOpenDate and roOpenDate<=openToDate:                           
                           roList1.append(ro)
                    except:
                        self.logger.debug("error in date match ro="+str(ro))  
                
        return  roList1 
    @classmethod
    def GetFilteredROList(self,store_code,vinList,gpsServicePlan,fromDate,toDate,activeServicePlans,stockflag,fromEntryDate,toEntryDate,last_key=None,page_size=-1):
        _moduleNM="Inventory"
        _functionNM="GetFilteredROList"
        try:   
            starttime = datetime.now()
            self.logger.debug("GetFilteredROList>> store_code:"+str(store_code)+",vinList="+str(vinList)+",fromDate="+str(fromDate)+",toDate="+str(toDate)+",last_key="+str(last_key)+",pageSize="+str(page_size)+",stockflag="+str(stockflag)+",fromEntryDate="+str(fromEntryDate)+",toEntryDate="+str(toEntryDate))
            dynamodb = boto3.resource('dynamodb', region_name=Inventory.region)
            TableName=self.getTableName(store_code)
            table = dynamodb.Table(TableName)                   
            items=[]
            fetchAll=False
            LastEvaluatedKey=None
            response=None
            vinList=self.GetVINSList(vinList)
            self.logger.debug("GetFilteredROList vinList>>"+str(vinList))
            filterExpression=None
            keyConditionExpression= Key('document_type').eq('INV')           
            date_filter_expression=None
            if vinList is not None and not vinList.__contains__('ALL') :
                if len(vinList) ==1:
                    keyConditionExpression=keyConditionExpression & Key('document_id').eq(vinList[0])
                    response = table.query(
                                    KeyConditionExpression= keyConditionExpression, 
                                    ConsistentRead=False)
                    if 'Items' in response:
                        items = response['Items']  
                    return { "status":True,"items": items, "LastEvaluatedKey":None }                  
                else:
                    batch_list=[]
                    for vin in vinList:
                        batch_list.append({"document_type":'INV','document_id':vin})  
                    tab_existingitems=Batching.get_batch_data(dynamodb=dynamodb,tableName=TableName,item_list=batch_list)
                    items=tab_existingitems[TableName]  
                    return { "status":True,"items": items, "LastEvaluatedKey":None } 

            elif gpsServicePlan is not None  and fromDate is not None and toDate is not None:
                    plan_filter_expression=None
                    if gpsServicePlan.upper() == "ALL":                    
                        if len(activeServicePlans)>0:                            
                            for plan in activeServicePlans:
                                if plan_filter_expression ==None:
                                    plan_filter_expression=Attr("servicePlans").contains(plan.upper())                                    
                                else:
                                    plan_filter_expression=plan_filter_expression | Attr("servicePlans").contains(plan.upper())
                    elif gpsServicePlan.upper() == "NONE":
                        plan_filter_expression=Attr("servicePlans").contains('')                          
                    else:
                        plan_filter_expression=Attr("servicePlans").contains(gpsServicePlan.upper())                     

                    date_filter_expression=self.GetDateFilterExpression(fromDtStr=fromDate,toDtStr=toDate,searchFieldName="searchOpenDate",minDateFieldName="roMinOpenDate",maxDateFieldName="roMaxOpenDate")
                    if filterExpression is None:
                        filterExpression= (plan_filter_expression)  & (date_filter_expression ) 
                    else:
                        filterExpression=(filterExpression) & (plan_filter_expression)  & (date_filter_expression )    
            elif stockflag is not None  and fromEntryDate is not None and toEntryDate is not None:
                    stockflag_filter_expression=None                    
                    if stockflag=='A':
                       stockflagList=['A','I','O']
                       stockflag_filter_expression=Attr("stockFlag").is_in(stockflagList) 
                    elif stockflag=='I':
                       stockflagList=['A','I']
                       stockflag_filter_expression=Attr("stockFlag").is_in(stockflagList)
                    elif stockflag=='O':
                       stockflagList=['A','O']
                       stockflag_filter_expression=Attr("stockFlag").is_in(stockflagList)                   
                    
                    date_filter_expression=self.GetDateFilterExpression(fromDtStr=fromEntryDate,toDtStr=toEntryDate,searchFieldName="searchEntryDate",minDateFieldName="stockMinEntryDate",maxDateFieldName="stockMaxEntryDate")
                    if filterExpression is None:
                        filterExpression=(stockflag_filter_expression) & (date_filter_expression)
                    else:
                        filterExpression=(filterExpression) & (stockflag_filter_expression) & (date_filter_expression)   
            elif fromDate is not None and toDate is not None:
                    date_filter_expression=self.GetDateFilterExpression(fromDtStr=fromDate,toDtStr=toDate,searchFieldName="searchOpenDate",minDateFieldName="roMinOpenDate",maxDateFieldName="roMaxOpenDate")
                    if filterExpression is None:
                        filterExpression= (date_filter_expression ) 
                    else:
                        filterExpression=(filterExpression) & (date_filter_expression )               
            elif fromEntryDate is not None and toEntryDate is not None:
                    date_filter_expression=self.GetDateFilterExpression(fromDtStr=fromEntryDate,toDtStr=toEntryDate,searchFieldName="searchEntryDate",minDateFieldName="stockMinEntryDate",maxDateFieldName="stockMaxEntryDate")
                    if filterExpression is None:
                        filterExpression= (date_filter_expression )
                    else:
                        filterExpression= (filterExpression) & ( date_filter_expression)  

            if filterExpression is not None:
                if last_key and len(last_key)>0 and page_size>0 :                     
                    try:
                            key1=json.loads(last_key)
                    except  Exception as e:
                            sq=urllib.parse.unquote(last_key)
                            key1 = json.loads(str(sq)) 
                    
                    response = table.query(
                                    KeyConditionExpression= keyConditionExpression,                                 
                                    FilterExpression= filterExpression,
                                    ExclusiveStartKey=key1,Limit=page_size,ConsistentRead=False) 
                else:
                    if last_key and  len(last_key)>0 :                     
                        try:
                            key1=json.loads(last_key)
                        except  Exception as e:
                            sq=urllib.parse.unquote(last_key)
                            key1 = json.loads(str(sq))  
                        
                        response = table.query(
                                        KeyConditionExpression= keyConditionExpression, 
                                        FilterExpression= filterExpression,
                                        ExclusiveStartKey=key1,ConsistentRead=False)
                    elif page_size>0:
                        response = table.query(
                                        KeyConditionExpression= keyConditionExpression, 
                                        FilterExpression= filterExpression,
                                        Limit=page_size,ConsistentRead=False)
                    else:
                        response = table.query(
                                        KeyConditionExpression= keyConditionExpression, 
                                        FilterExpression= filterExpression,
                                        ConsistentRead=False)
                callTimeOut=5       
                endtime = datetime.now()
                delta=endtime-starttime
                consumedSeconds=delta.total_seconds()  
                self.logger.debug("consumedSeconds="+str(consumedSeconds))         
                #self.logger.debug("response="+str(response))
                if 'LastEvaluatedKey' in response:
                        LastEvaluatedKey=response['LastEvaluatedKey'] 
                        self.logger.debug("LastEvaluatedKey="+str(LastEvaluatedKey))
                        if(consumedSeconds+consumedSeconds )<callTimeOut:  
                            fetchAll=True
                if 'Items' in response:
                    items = response['Items']  
                self.logger.debug("Total items="+str(len(items)))
                page_size_new=0
                if page_size>0: 
                    if fetchAll==True and len(items)<page_size:
                        fetchAll=True
                        page_size_new=page_size-len(items)
                    else:                    
                        fetchAll=False
                        self.logger.debug("Total items  is Greater or equal to page size="+str(page_size))     
                if  fetchAll ==True:                
                    while 'LastEvaluatedKey' in response:
                        starttime = datetime.now()
                        if page_size>0:
                            response = table.query(
                                KeyConditionExpression= keyConditionExpression,
                                FilterExpression= filterExpression,
                                ConsistentRead=False, Limit=page_size_new ,              
                                ExclusiveStartKey=response['LastEvaluatedKey']
                                )
                        else:
                            response = table.query(
                                KeyConditionExpression= keyConditionExpression,
                                FilterExpression= filterExpression,
                                ConsistentRead=False,               
                                ExclusiveStartKey=response['LastEvaluatedKey']
                                )
                        try:
                            items1=response['Items']
                            #self.logger.debug("loop inner Total items="+str(len(items1)))
                            items.extend(items1)
                            #self.logger.debug("loop after adding final Total items"+str(len(items)))
                            LastEvaluatedKey=None
                            page_size_new=page_size_new-len(items1) 
                            if 'LastEvaluatedKey' in response:
                                LastEvaluatedKey=response['LastEvaluatedKey']   
                                self.logger.debug("Loop LastEvaluatedKey="+str(LastEvaluatedKey))
                            if LastEvaluatedKey is None or (page_size>0 and len(items)>=page_size):
                                #self.logger.debug("Loop LastEvaluatedKey="+str(LastEvaluatedKey))
                                break                               
                        except:
                            self.logger.error(">> error=",True) 
                        endtime = datetime.now()
                        delta=endtime-starttime
                        consumedSeconds=consumedSeconds+delta.total_seconds()
                        #self.logger.debug("loop consumedSeconds="+str(consumedSeconds))                           
                        if(delta.total_seconds()+consumedSeconds )>=callTimeOut:    
                            self.logger.debug("loop exceeded consumedSeconds="+str(consumedSeconds))  
                            break 
                            
                if LastEvaluatedKey is not None :                  
                    LastEvaluatedKey = json.dumps(LastEvaluatedKey) 
                    LastEvaluatedKey=urllib.parse.quote_plus(str(LastEvaluatedKey) ) 
            self.logger.debug("GetFilteredROList LastEvaluatedKey>>"+str(LastEvaluatedKey))
            return { "status":True,"items": items, "LastEvaluatedKey":LastEvaluatedKey }                              
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)      
     
    @classmethod 
    def from_dynamodb_to_json(self,item):
        d = TypeDeserializer()
        return {k: d.deserialize(value=v) for k, v in item.items()}
    @classmethod
    def getInvItem(self,item,isRepairOrders):
        inventory_item={                           
                        "vin":item.get('document_id'),
                        "vinFound":True,                            
                        "make":item.get('make'),
                        "model":item.get('model'),
                        "year":item.get('year'),                       
                        "color":item.get('color'), 
                        "gps":item.get('gps'),
                        "appt":item.get('appt'), 
                        "stocks":item.get('stocks'),     
                        "repairOrders":item.get('repairOrders')                                        
                        }
        if isRepairOrders==False:
                    inventory_item['poOrders']=item.get('poOrders')   
        return inventory_item
    @classmethod
    def getBlankInvItem(self,vin_item,isRepairOrders):
        inventory_item={
                            "vin":vin_item,                                     
                            "vinFound":False,                            
                            "make":"",
                            "model":"",
                            "year":"",                       
                            "color":"", 
                            "gps":False,
                            "appt":False, 
                            "stocks":[],    
                            "repairOrders":[]                                                  
                            }
        if isRepairOrders==False:
            inventory_item['poOrders']=[]  
        return inventory_item
    @classmethod
    def prepareListForVIN(self,items,vinsList,isRepairOrders):
        inventoryList=[]
        vinsList=self.GetVINSList(vinsList)
        for item in items :           
            try: 
                vinsList.remove(item.get('vin'))  
            except:
                ""
            inventoryList.append(self.getInvItem(item,isRepairOrders))
        if  vinsList is not None  and len(vinsList)>0:
                for vin_item in vinsList : 
                    if vin_item!="ALL":
                        inventoryList.append(self.getBlankInvItem(vin_item,isRepairOrders))
        return { "status":True,"items": inventoryList }
    @classmethod
    def prepareListForStockFlag(self,items,isRepairOrders,stockFlag,fromEntryDate,toEntryDate):
        inventoryList=[]
        entryFromDate=datetime.strptime(fromEntryDate, '%d%b%y')
        entryToDate=datetime.strptime(toEntryDate, '%d%b%y')       
        for item in items :           
            stocks=item.get('stocks')
            if stocks is not None:
                stockList=[]
                for stock_item in stocks:
                    try:
                        stockEntryDate=datetime.strptime(stock_item["entryDate"], '%d%b%y')
                        if entryFromDate<=stockEntryDate and stockEntryDate<=entryToDate: 
                            if stockFlag is not None and stockFlag !='A' and item['stockFlag'] =='A':
                                inStock=False
                                if stock_item['inStockFlag']=='I':
                                   inStock=True
                                
                                if  ( stockFlag=='I' and inStock==True) or ( stockFlag=='O' and inStock==False) :
                                    stockList.append(stock_item)
                            else:
                                stockList.append(stock_item)
                    except:
                        self.logger.debug("error in date match stock="+str(stock_item))                             
                stocks=stockList    
            if len(stocks)>0:  
                item['stocks'] =stocks    
                inventoryList.append(self.getInvItem(item,isRepairOrders))
        
        return { "status":True,"items": inventoryList }
    @classmethod
    def prepareListForPlan(self,items,isRepairOrders,plan,activePlanList,fromDate,toDate):
        inventoryList=[]
        #fromDate=datetime.strptime(fromDate, '%d%b%y')
        #toDate=datetime.strptime(toDate, '%d%b%y')  
        if isRepairOrders:
            for item in items :           
                stocks=item.get('stocks')
                if stocks is not None and plan is not None:                
                    stockList=[]                    
                    for stock_item in stocks:                                                       
                        deals=self.GetFilteredDealsForRepairOrders(deals=stock_item.get('deals'),plan=plan,activePlan=activePlanList)
                        if len(deals)>0:
                            stock_item['deals']=deals
                            stockList.append(stock_item)                                        
                    stocks=stockList   

                if stocks is not None and len(stocks)>0:  
                    item['stocks'] =stocks 
                    roList=self.GetFilteredROsForRepairOrders(item['repairOrders'],fromDate=fromDate,toDate=toDate)   
                    if roList is not None and len(roList)>0:
                        item['repairOrders']=roList
                        inventoryList.append(self.getInvItem(item,isRepairOrders))  
        else:                 
            for item in items :           
                stocks=item.get('stocks')
                if stocks is not None:                
                    stockList=[]
                    for stock_item in stocks:                                
                        deals=self.GetFilteredDeals(deals=stock_item.get('deals'),plan=plan,activePlan=activePlanList,fromDate=fromDate,toDate=toDate)
                        if len(deals)>0:
                            stock_item['deals']=deals                                     
                            stockList.append(stock_item)                                        
                    stocks=stockList    
                if len(stocks)>0:  
                    item['stocks'] =stocks    
                    inventoryList.append(self.getInvItem(item,isRepairOrders))  

        return { "status":True,"items": inventoryList }
    @classmethod
    def prepareListForFilterOpenDate(self,items,fromDate,toDate):
        inventoryList=[]        
        for item in items :           
            roList=self.GetFilteredROsForRepairOrders(item['repairOrders'],fromDate=fromDate,toDate=toDate)   
            if roList is not None and len(roList)>0:
                item['repairOrders']=roList
                inventoryList.append(self.getInvItem(item,True))              
        return { "status":True,"items": inventoryList }
    @classmethod
    def prepareListForInventoryNow(self,items,inventoryNow):
        inventoryList=[]  
        stockFlag='I' 
        if  str(inventoryNow).upper() =='FALSE':
            stockFlag='O' 
        for item in items :           
            stocks=item.get('stocks')
            stockList=[]
            if stocks is not None:
                for stock_item in stocks:
                    try:
                        if item['stockFlag'] =='A':
                                inStock=False
                                if stock_item['inStockFlag']=='I':
                                   inStock=True
                                if  ( stockFlag=='I' and inStock==True) or ( stockFlag=='O' and inStock==False) :
                                    st_list = [st for st in stockList if (st['stockNo'] == stock_item['stockNo'])]
                                    if st_list is None or   len(st_list)==0:
                                        stockList.append(self.prepareStockJSON(stock_item))
                        else:
                            st_list = [st for st in stockList if (st['stockNo'] == stock_item['stockNo'])]
                            if st_list is None or   len(st_list)==0:
                               stockList.append(self.prepareStockJSON(stock_item))
                    except:
                        self.logger.debug("error in check inventoryNow stock="+str(stock_item))                             
            item= {                           
                  "vin":item.get('document_id'),
                  "stockNumbers":stockList                                                        
            }   
            inventoryList.append(item)        
        return { "status":True,"items": inventoryList }
    @classmethod
    def prepareStockJSON(self,stock_item):
        pendingBuyerName=None
        pendingContractDate=None
        inventoryType=None
        if 'inventoryType' in stock_item:
            inventoryType=stock_item['inventoryType']
        if 'deals' in stock_item and stock_item['deals'] is not None:
            deals=stock_item['deals']
            if(len(deals)>1):
                deal=deals[len(deals)-1]
                pendingBuyerName=deal['buyerName']
                pendingContractDate=deal['soldDate']
            elif(len(deals)==1):
                deal=deals[0]
                pendingBuyerName=deal['buyerName']
                pendingContractDate=deal['soldDate']
         
        entryDate=stock_item['entryDate']
        if pendingContractDate is not None and len(pendingContractDate)>0 and   entryDate is not None and len(entryDate)>0:
            try:
                entryDate_dt = datetime.strptime(entryDate, "%d%b%y")        
                pendingContractDate_dt = datetime.strptime(pendingContractDate, "%d%b%y") 
                if entryDate_dt>pendingContractDate_dt:
                    pendingBuyerName=None
                    pendingContractDate=None
            except:
                ""
        
        stock_element={
                    "stockNo":stock_item["stockNo"],
					"stockStatus": stock_item["stockStatus"],
					"lienHolderAmount": stock_item["lienHolderAmount"],
					"lienHolderName": stock_item["lienHolderName"],
					"pendingContractDate": pendingContractDate,
					"pendingBuyerName": pendingBuyerName,
                    "inventoryType":inventoryType
                   }
        return stock_element
    
    @classmethod
    def prepareOutputJSONList(self,items,vinsList,isRepairOrderAPI,plan,activePlanList,fromDate,toDate,stockflag,fromEntryDate,toEntryDate):
        _moduleNM="Inventory"
        _functionNM="prepareOutputJSONList"
        try:           
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)  
            if vinsList is not None:
                return self.prepareListForVIN(items,vinsList,isRepairOrderAPI)
            elif plan is not None  and fromDate is not None and toDate is not None:
                 return self.prepareListForPlan(items,isRepairOrderAPI,plan,activePlanList,fromDate,toDate)
            elif stockflag is not None  and fromEntryDate is not None and toEntryDate is not None:
                return self.prepareListForStockFlag(items,isRepairOrderAPI,stockflag,fromEntryDate,toEntryDate)
            elif fromDate is not None and toDate is not None:
                return self.prepareListForPlan(items,isRepairOrderAPI,None,None,fromDate,toDate)
            elif fromEntryDate is not None and toEntryDate is not None:
                return self.prepareListForStockFlag(items,isRepairOrderAPI,None,fromEntryDate,toEntryDate)

            return { "status":True,"items": [] }
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 
    @classmethod
    def prepareOutputJSONListForRepairOrders(self,items,vinsList,isRepairOrderAPI,plan,activePlanList,fromDate,toDate,stockflag,fromEntryDate,toEntryDate):
        _moduleNM="Inventory"
        _functionNM="prepareOutputJSONListForRepairOrders"
        try:           
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)  
            if vinsList is not None:
                return self.prepareListForVIN(items,vinsList,isRepairOrderAPI)
            elif plan is not None  and fromDate is not None and toDate is not None:
                 return self.prepareListForPlan(items,isRepairOrderAPI,plan,activePlanList,fromDate,toDate)
            #elif stockflag is not None  and fromEntryDate is not None and toEntryDate is not None:
                #return self.prepareListForStockFlag(items,isRepairOrderAPI,stockflag,fromEntryDate,toEntryDate)
            elif fromDate is not None and toDate is not None:
                return self.prepareListForFilterOpenDate(items,fromDate,toDate)
            #elif fromEntryDate is not None and toEntryDate is not None:
                #return self.prepareListForStockFlag(items,isRepairOrderAPI,None,fromEntryDate,toEntryDate)

            return { "status":True,"items": [] }
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 

    @classmethod
    def prepareInventoryStatusList(self,items,vinsList,isFiltered,plan,activePlanList,fromDate,toDate,stockflag):
        _moduleNM="Inventory"
        _functionNM="prepareInventoryStatusList"
        try:           
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)  
            inventoryList=[]
            vinsList=self.GetVINSList(vinsList)
            for item in items :                 
                if isFiltered:
                    ""
                    #item=self.from_dynamodb_to_json(item)  
                if vinsList is not None: 
                    try: 
                        vinsList.remove(item.get('vin'))  
                    except:
                       ""
                stocks=item.get('stocks')
                if vinsList is None:
                     
                    if stocks is not None:
                        stockList=[]
                        if (fromDate is not None and toDate  is not None):
                            for stock_item in stocks:                                
                                deals=self.GetFilteredDeals(deals=stock_item.get('deals'),plan=plan,activePlan=activePlanList,fromDate=fromDate,toDate=toDate)
                                self.logger.debug("debug:"+str(deals))
                                if len(deals)>0:
                                    stock_item['deals']=deals                                     
                                    if stockflag is not None and stockflag !='A' and item['stockFlag'] =='A':
                                        inStock=False
                                        if stock_item['inStockFlag']=='I':
                                            inStock=True
                                        if  ( stockflag=='I' and inStock==True) or ( stockflag=='O' and inStock==False) :
                                            stockList.append(stock_item)
                                    else:
                                        stockList.append(stock_item)
                        else:                             
                            for stock_item in stocks:                                  
                                if stockflag is not None and stockflag !='A' and item['stockFlag'] =='A':
                                        inStock=False
                                        if stock_item['inStockFlag']=='I':
                                            inStock=True
                                        
                                        if  ( stockflag=='I' and inStock==True) or ( stockflag=='O' and inStock==False) :
                                            stockList.append(stock_item)
                                else:
                                    stockList.append(stock_item)
                         
                        stocks=stockList    
                else:
                    if stocks is not None:
                        stockList=[]
                        for stock_item in stocks:
                            if stockflag is not None and stockflag !='A' and item['stockFlag'] =='A':
                                inStock=False
                                if stock_item['inStockFlag']=='I':
                                    inStock=True                                        
                                if  ( stockflag=='I' and inStock==True) or ( stockflag=='O' and inStock==False) :
                                        stockList.append(stock_item)
                            else:
                                stockList.append(stock_item)
                        stocks=stockList

                if len(stocks)>0:               
                    inventory_item={                           
                                        "vin":item.get('document_id'),
                                        "vinFound":True,                            
                                        "make":item.get('make'),
                                        "model":item.get('model'),
                                        "year":item.get('year'),                       
                                        "color":item.get('color'), 
                                        "gps":item.get('gps'),
                                        "appt":item.get('appt'), 
                                        "stocks":stocks,    
                                        "repairOrders":item.get('repairOrders'),
                                        "poOrders":item.get('poOrders')
                                        }
                    inventoryList.append(inventory_item)

            if  vinsList is not None  and len(vinsList)>0:
                for item in vinsList : 
                    if item!="ALL":
                        inventory_item={
                                "vin":item,                                     
                                "vinFound":False,                            
                                "make":"",
                                "model":"",
                                "year":"",                       
                                "color":"", 
                                "gps":False,
                                "appt":False, 
                                "stocks":[],    
                                "repairOrders":[],
                                "poOrders":[]                          
                                }
                        inventoryList.append(inventory_item)
            return { "status":True,"items": inventoryList }
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 
    """ @classmethod
    def prepareRepairOrdersList(self,items,vinJSONList,isFiltered,plan,activePlanList,fromDate,toDate,stockflag):
        _moduleNM="Inventory"
        _functionNM="prepareRepairOrdersList"
        try:           
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)  
            inventoryList=[]
            vinsList=self.GetVINSList(vinJSONList)
            for item in items :                 
                if isFiltered:
                    #item=self.from_dynamodb_to_json(item) 
                    "" 
                if vinsList is not None :
                    try: 
                        vinsList.remove(item.get('vin'))  
                    except:
                        ""  
                stocks=item.get('stocks')
                if vinsList is None:
                     
                    if stocks is not None:
                        stockList=[]
                        if (fromDate is not None and toDate  is not None):
                            for stock_item in stocks:                                
                                deals=self.GetFilteredDeals(deals=stock_item.get('deals'),plan=plan,activePlan=activePlanList,fromDate=fromDate,toDate=toDate)
                                self.logger.debug("debug:"+str(deals))
                                if len(deals)>0:
                                    stock_item['deals']=deals                                     
                                    if stockflag is not None and stockflag !='A' and item['stockFlag'] =='A':
                                        inStock=False
                                        if stock_item['inStockFlag']=='I':
                                            inStock=True
                                        
                                        if  ( stockflag=='I' and inStock==True) or ( stockflag=='O' and inStock==False) :
                                            stockList.append(stock_item)
                                    else:
                                        stockList.append(stock_item)
                        else:
                            for stock_item in stocks:                                  
                                if stockflag is not None and stockflag !='A' and item['stockFlag'] =='A':
                                        inStock=False
                                        if stock_item['inStockFlag']=='I':
                                            inStock=True
                                        
                                        if  ( stockflag=='I' and inStock==True) or ( stockflag=='O' and inStock==False) :
                                            stockList.append(stock_item)
                                else:
                                    stockList.append(stock_item)
                         
                        stocks=stockList    
                else:
                    if stocks is not None:
                        stockList=[]
                        for stock_item in stocks:
                            if stockflag is not None and stockflag !='A' and item['stockFlag'] =='A':
                                inStock=False
                                if stock_item['inStockFlag']=='I':
                                    inStock=True
                                        
                                if  ( stockflag=='I' and inStock==True) or ( stockflag=='O' and inStock==False) :
                                        stockList.append(stock_item)
                            else:
                                stockList.append(stock_item)
                        stocks=stockList
                
                if len(stocks)>0:                                             
                    inventory_item={
                                    "vin":item.get('document_id'),
                                    "vinFound":True,                            
                                    "make":item.get('make'),
                                    "model":item.get('model'),
                                    "year":item.get('year'),                       
                                    "color":item.get('color'), 
                                    "gps":item.get('gps'),
                                    "appt":item.get('appt'), 
                                    "stocks":stocks,    
                                    "repairOrders":item.get('repairOrders')                                                         
                                    }
                    inventoryList.append(inventory_item)
            if  vinsList is not None  and len(vinsList)>0:
                for item in vinsList : 
                    if item!="ALL":
                        inventory_item={
                                "vin":item,                                     
                                "vinFound":False,                            
                                "make":"",
                                "model":"",
                                "year":"",                       
                                "color":"", 
                                "gps":False,
                                "appt":False, 
                                "stocks":[],    
                                "repairOrders":[],                       
                                }
                        inventoryList.append(inventory_item)
            return { "status":True,"items": inventoryList }
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)   """
    @classmethod
    def ValidateDate(self,date_string):        
        if date_string is None or date_string=="" or len(date_string)==0:
            return False
        try:
            datetime.strptime(date_string, '%d%b%y')             
        except ValueError:
            return False       
        return True
    @classmethod
    def CompareDate(self,fromDate,toDate):        
        if fromDate is  None or fromDate=="" or len(fromDate)==0:
            return False
        if toDate is  None or toDate=="" or len(toDate)==0:
            return False
        try:
          dtFrom=  datetime.strptime(fromDate, '%d%b%y')
          dtTo=  datetime.strptime(toDate, '%d%b%y')  
          return dtFrom<=dtTo
        except ValueError:
            return False  
    @classmethod
    def CheckDateRange(self,fromDate,toDate,auth_json,fromDateStr,toDateStr):
        res_handler=ResponseHandler()
        isDateFilter=False
        isToDateValid=False
        isFromDateValid=False
        if (fromDate is not None ) and len(str(fromDate))>0:            
            if not self.ValidateDate(fromDate):
                    list1=[ str(fromDateStr),str(fromDate),str("ddMMMYY")]
                    return res_handler.GetFormattedErrorResponseJSON(code=371,auth_json=auth_json,args=list1)
            else:
                isDateFilter=True
                isFromDateValid=True        
        if (toDate is not None ) and len(str(toDate))>0:           
            if not self.ValidateDate(toDate):            
                    list=[ str(toDateStr),str(toDate),str("ddMMMYY")]
                    return res_handler.GetFormattedErrorResponseJSON(code=371,auth_json=auth_json,args=list)
            else:
                isDateFilter=True  
                isToDateValid=True
        if self.ValidateDate(fromDate) and self.ValidateDate(toDate):
            if not self.CompareDate(fromDate=fromDate,toDate=toDate)  :
               list1=[ fromDateStr,toDateStr]
               return res_handler.GetErrorResponseJSON(code=373,auth_json=auth_json)  
        if isDateFilter==True:
            if not isFromDateValid ==True:
                list1=[ fromDateStr]
                return res_handler.GetFormattedErrorResponseJSON(code=369,auth_json=auth_json,args=list1)
                 
            if not isToDateValid==True:
                list1=[ toDateStr]
                return res_handler.GetFormattedErrorResponseJSON(code=370,auth_json=auth_json,args=list1)
                
        return {"responseCode":0}     
    @classmethod
    def Validate_RepairOrder_inputs(self,store_code,vinList,gpsServicePlan,fromDate,toDate,auth_json,stockFlag,fromEntryDate,toEntryDate):
        res_handler=ResponseHandler()
        activeServicePlans=[]
        if vinList is not  None :        
            if ( len(vinList)==0  or str(vinList).upper()=='ALL'  or str(vinList).upper()=='NONE'):
                list1=[ "vin"]
                return res_handler.GetFormattedErrorResponseJSON(code=372,auth_json=auth_json,args=list1)
            return {"responseCode":0,"activeServicePlans":activeServicePlans}  

        elif gpsServicePlan is not None and fromDate is not None and toDate is not None:
            if len(gpsServicePlan)==0 :
                list1=[ "plan"]
                return res_handler.GetFormattedErrorResponseJSON(code=372,auth_json=auth_json,args=list1)
           
            if gpsServicePlan is not  None and  len(str(gpsServicePlan).strip())>0:           
                    filterList=["NONE","ALL"]
                    activeServicePlansDict=self.GetDescriptionFilters(store_code=store_code,region=Inventory.region)
                    uniquePlans=set( activeServicePlansDict.values())
                    activeServicePlans = sorted(uniquePlans)
                    filterList.extend(activeServicePlans)
                    if not gpsServicePlan.upper() in filterList:
                        list1=[ gpsServicePlan]
                        return res_handler.GetFormattedErrorResponseJSON(code=374,auth_json=auth_json,args=list1)
                    chkResp=self.CheckDateRange(fromDate,toDate,auth_json,'fromDate','toDate')
                    if chkResp['responseCode']==-1:
                        return chkResp
            return {"responseCode":0,"activeServicePlans":activeServicePlans} 
        # elif stockFlag is not None and fromEntryDate is not None and toEntryDate is not None:
        #     if stockFlag is not  None   and ( len(stockFlag)==0  or not ['A','I','O'].__contains__(str(stockFlag)) ):
        #         list1=[ "stockFilter"]
        #         return res_handler.GetFormattedErrorResponseJSON(code=372,auth_json=auth_json,args=list1)
        #     chkResp=self.CheckDateRange(fromEntryDate,toEntryDate,auth_json,'fromEntryDate','toEntryDate')
        #     if chkResp['responseCode']==-1:
        #         return chkResp  
        #     return {"responseCode":0,"activeServicePlans":activeServicePlans} 
        elif fromDate is not None and toDate is not None:
            chkResp=self.CheckDateRange(fromDate,toDate,auth_json,'fromDate','toDate')
            if chkResp['responseCode']==-1:
                return chkResp
            return {"responseCode":0,"activeServicePlans":activeServicePlans} 
        # elif fromEntryDate is not None and toEntryDate is not None:
        #     chkResp=self.CheckDateRange(fromEntryDate,toEntryDate,auth_json,'fromEntryDate','toEntryDate')
        #     if chkResp['responseCode']==-1:
        #         return chkResp
        #     return {"responseCode":0,"activeServicePlans":activeServicePlans} 
        
        return res_handler.GetErrorResponseJSON(code=386,auth_json=auth_json)    
    @classmethod
    def Validate_inputs(self,store_code,vinList,gpsServicePlan,fromDate,toDate,auth_json,stockFlag,fromEntryDate,toEntryDate):
        res_handler=ResponseHandler()
        activeServicePlans=[]
        if vinList is not  None :        
            if ( len(vinList)==0  or str(vinList).upper()=='ALL'  or str(vinList).upper()=='NONE'):
                list1=[ "vin"]
                return res_handler.GetFormattedErrorResponseJSON(code=372,auth_json=auth_json,args=list1)
            return {"responseCode":0,"activeServicePlans":activeServicePlans}  

        elif gpsServicePlan is not None and fromDate is not None and toDate is not None:
            if len(gpsServicePlan)==0 :
                list1=[ "plan"]
                return res_handler.GetFormattedErrorResponseJSON(code=372,auth_json=auth_json,args=list1)
           
            if gpsServicePlan is not  None and  len(str(gpsServicePlan).strip())>0:           
                    filterList=["NONE","ALL"]
                    activeServicePlansDict=self.GetDescriptionFilters(store_code=store_code,region=Inventory.region)
                    uniquePlans=set( activeServicePlansDict.values())
                    activeServicePlans = sorted(uniquePlans)
                    filterList.extend(activeServicePlans)
                    if not gpsServicePlan.upper() in filterList:
                        list1=[ gpsServicePlan]
                        return res_handler.GetFormattedErrorResponseJSON(code=374,auth_json=auth_json,args=list1)
                    chkResp=self.CheckDateRange(fromDate,toDate,auth_json,'fromDate','toDate')
                    if chkResp['responseCode']==-1:
                        return chkResp
            return {"responseCode":0,"activeServicePlans":activeServicePlans} 
        elif stockFlag is not None and fromEntryDate is not None and toEntryDate is not None:
            if stockFlag is not  None   and ( len(stockFlag)==0  or not ['A','I','O'].__contains__(str(stockFlag)) ):
                list1=[ "stockFilter"]
                return res_handler.GetFormattedErrorResponseJSON(code=372,auth_json=auth_json,args=list1)
            chkResp=self.CheckDateRange(fromEntryDate,toEntryDate,auth_json,'fromEntryDate','toEntryDate')
            if chkResp['responseCode']==-1:
                return chkResp  
            return {"responseCode":0,"activeServicePlans":activeServicePlans} 
        elif fromDate is not None and toDate is not None:
            chkResp=self.CheckDateRange(fromDate,toDate,auth_json,'fromDate','toDate')
            if chkResp['responseCode']==-1:
                return chkResp
            return {"responseCode":0,"activeServicePlans":activeServicePlans} 
        elif fromEntryDate is not None and toEntryDate is not None:
            chkResp=self.CheckDateRange(fromEntryDate,toEntryDate,auth_json,'fromEntryDate','toEntryDate')
            if chkResp['responseCode']==-1:
                return chkResp
            return {"responseCode":0,"activeServicePlans":activeServicePlans} 
        
        return res_handler.GetErrorResponseJSON(code=378,auth_json=auth_json) 
    @classmethod
    def GetDescriptionFilters(self,store_code,region):
        filters=[]
        app_client=AppClient()
        store_resp=app_client.GetStoreDetail(store_code,region=region)
        if store_resp['status']:
            item=store_resp['item']
            if 'plan_mapping' in item:
                filters=item['plan_mapping']                
        return filters   