from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.conditions import Key 
import urllib.parse
import json

import numpy as np
import pandas as pad
from EPDE_Logging import LogManger
import boto3
from boto3.dynamodb.conditions import Key
from EPDE_Error import ErrorHandler
from datetime import datetime, timedelta
from EPDE_Response import ResponseHandler
from boto3.dynamodb.types import TypeDeserializer

#This module is responsible to handle EPDE API ROHistory handling
class ROHistory(object):
    logger=LogManger()   
    err_handler=ErrorHandler()
    region='us-east-1'
    table_Per_Store=1
    def __init__(self):        
        ROHistory.region="us-east-1"
        ROHistory.table_Per_Store=1
        
    def __init__(self,region,table_Per_Store_Invoice=1):  
        ROHistory.region=region
        ROHistory.table_Per_Store=table_Per_Store_Invoice
    @classmethod
    def chunks(self,lst, n):       
        for i in range(0, len(lst), n):
            yield lst[i:i + n]  
                
    @classmethod   
    def getTableName(self,store_code):
        TableName=store_code+"_ROHISTORY_FILE"         
        return TableName
    
    @classmethod
    def GetROHistoryList(self,store_code,document_type,last_key=None,page_size=-1):
        _moduleNM="ROHistory"
        _functionNM="GetROHistoryList"
        try:                 
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)       
            self.logger.debug("GetROHistoryList>> store_code:"+str(store_code))           
            dynamodb = boto3.resource('dynamodb', region_name=ROHistory.region)
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

            if  fetchAll ==True:                
                    while 'LastEvaluatedKey' in response:
                        starttime = datetime.now()
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
                            if 'LastEvaluatedKey' in response:
                                LastEvaluatedKey=response['LastEvaluatedKey']   
                                self.logger.debug("Loop LastEvaluatedKey="+str(LastEvaluatedKey))
                            if LastEvaluatedKey is None:
                                break
                               
                        except:
                            ""
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
            self.logger.debug("GetROHistoryList LastEvaluatedKey>>"+str(LastEvaluatedKey))
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
    def GetFilteredList(self,store_code,vinList,customerNo,fromDate,toDate,last_key=None,page_size=-1):
        _moduleNM="ROHistory"
        _functionNM="GetFilteredList"
        try:   
            starttime = datetime.now()
            self.logger.debug("GetFilteredList>> store_code:"+str(store_code)+",vinList="+str(vinList)+",customerNo="+str(customerNo)+",fromDate="+str(fromDate)+",toDate="+str(toDate))  
            dynamodb = boto3.resource('dynamodb', region_name=ROHistory.region)
            TableName=self.getTableName(store_code)
            table = dynamodb.Table(TableName)                   
            items=[]
            keyConditionExpression= Key('document_type').eq('ROHIST')     
            fetchAll=False
            LastEvaluatedKey=None
            response=None
            vinList=self.GetVINSList(vinList)
            self.logger.debug("GetFilteredList vinList>>"+str(vinList))
            filterExpression=None
            keyConditionExpression= Key('document_type').eq('ROHIST')           
            if vinList is not None and not vinList.__contains__('ALL')  :
                if customerNo is not None:
                    if len(vinList) ==1:
                        self.logger.debug("1. only vinList len ="+str(len(vinList)))
                        keyConditionExpression=keyConditionExpression & Key('document_id').eq(vinList[0]+"_"+customerNo)
                        response = table.query(
                                    KeyConditionExpression= keyConditionExpression, 
                                    ConsistentRead=False)
                        if 'Items' in response:
                         items = response['Items']  
                        return { "status":True,"items": items, "LastEvaluatedKey":None } 
                    else:
                      self.logger.debug("2. only vinList len ="+str(len(vinList)))
                      filterExpression=Attr("vin").is_in(vinList) & Attr("customerNo").eq(customerNo)
                else:
                    if len(vinList) ==1:
                        self.logger.debug("3 only vin ="+(vinList[0]+"_"))
                        keyConditionExpression=keyConditionExpression & Key('document_id').begins_with(str(vinList[0]+"_"))
                    else:
                      self.logger.debug("4 vinList len ="+str(len(vinList)))
                      filterExpression=Attr("vin").is_in(vinList) 

            elif customerNo is not None:
                    self.logger.debug("5 customerNo="+str(customerNo))
                    filterExpression= Attr("customerNo").eq(customerNo)
            elif fromDate is not None and toDate is not None:
                    self.logger.debug("6 ro close date filters")
                    date_filter_expression=self.GetDateFilterExpression(fromDtStr=fromDate,toDtStr=toDate,searchFieldName="searchCloseDate",minDateFieldName="roMinCloseDate",maxDateFieldName="roMaxCloseDate")
                    if filterExpression is None:
                        filterExpression=(date_filter_expression)
                    else:
                        filterExpression=(filterExpression) & (date_filter_expression ) 
                          
            if filterExpression is not None:

                if last_key and len(last_key)>0 and page_size>0 :                     
                    try:
                            key1=json.loads(last_key)
                    except  Exception as e:
                            sq=urllib.parse.unquote(last_key)
                            key1 = json.loads(str(sq)) 
                    self.logger.debug("query exe....7")
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
                        self.logger.debug("query exe....8")
                        response = table.query(
                                        KeyConditionExpression= keyConditionExpression, 
                                        FilterExpression= filterExpression,
                                        ExclusiveStartKey=key1,ConsistentRead=False)
                    elif page_size>0:
                        self.logger.debug("query exe....9")
                        response = table.query(
                                        KeyConditionExpression= keyConditionExpression, 
                                        FilterExpression= filterExpression,
                                        Limit=page_size,ConsistentRead=False)
                    else:
                        self.logger.debug("query exe....10")
                        response = table.query(
                                        KeyConditionExpression= keyConditionExpression, 
                                        FilterExpression= filterExpression,
                                        ConsistentRead=False)

            else:
                if last_key and len(last_key)>0 and page_size>0 :                     
                    try:
                            key1=json.loads(last_key)
                    except  Exception as e:
                            sq=urllib.parse.unquote(last_key)
                            key1 = json.loads(str(sq)) 
                    self.logger.debug("query exe....11") 
                    response = table.query(
                                    KeyConditionExpression= keyConditionExpression,
                                    ExclusiveStartKey=key1,Limit=page_size,ConsistentRead=False) 
                else:
                    if last_key and  len(last_key)>0 :                     
                        try:
                            key1=json.loads(last_key)
                        except  Exception as e:
                            sq=urllib.parse.unquote(last_key)
                            key1 = json.loads(str(sq))  
                        self.logger.debug("query exe....12") 
                        response = table.query(
                                        KeyConditionExpression= keyConditionExpression,                                         
                                        ExclusiveStartKey=key1,ConsistentRead=False)
                    elif page_size>0:
                        self.logger.debug("query exe....13") 
                        response = table.query(
                                        KeyConditionExpression= keyConditionExpression,                                       
                                        Limit=page_size,ConsistentRead=False)
                    else:
                        self.logger.debug("only vin provided=") 
                        response = table.query(
                                        KeyConditionExpression= keyConditionExpression,                                       
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
            if  fetchAll ==True:                
                while 'LastEvaluatedKey' in response:
                    starttime = datetime.now()
                    if filterExpression is not None:
                        response = table.query(
                            KeyConditionExpression= keyConditionExpression,
                            FilterExpression= filterExpression,
                            ConsistentRead=False,               
                            ExclusiveStartKey=response['LastEvaluatedKey']
                            )
                    else:
                        response = table.query(
                            KeyConditionExpression= keyConditionExpression,
                            ConsistentRead=False,               
                            ExclusiveStartKey=response['LastEvaluatedKey']
                            )
                    try:
                        items1=response['Items']
                        #self.logger.debug("loop inner Total items="+str(len(items1)))
                        items.extend(items1)
                        #self.logger.debug("loop after adding final Total items"+str(len(items)))
                        LastEvaluatedKey=None
                        if 'LastEvaluatedKey' in response:
                            LastEvaluatedKey=response['LastEvaluatedKey']   
                            self.logger.debug("Loop LastEvaluatedKey="+str(LastEvaluatedKey))
                        if LastEvaluatedKey is None:
                            self.logger.debug("Loop LastEvaluatedKey="+str(LastEvaluatedKey))
                            break                               
                    except:
                        ""
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
    def GetFilteredListNew(self,store_code,vinList,customerNo,fromDate,toDate,last_key=None,page_size=-1):
        _moduleNM="ROHistory"
        _functionNM="GetFilteredListNew"
        try:   
            starttime = datetime.now()
            self.logger.debug("GetFilteredListNew>> store_code:"+str(store_code)+",vinList="+str(vinList)+",customerNo="+str(customerNo)+",fromDate="+str(fromDate)+",toDate="+str(toDate))  
            dynamodb = boto3.resource('dynamodb', region_name=ROHistory.region)
            TableName=self.getTableName(store_code)
            table = dynamodb.Table(TableName)                   
            items=[]
            keyConditionExpression= Key('document_type').eq('ROHIST')     
            fetchAll=False
            LastEvaluatedKey=None
            response=None
            vinList=self.GetVINSList(vinList)
            self.logger.debug("GetFilteredListNew vinList>>"+str(vinList))
            filterExpression=None
            keyConditionExpression= Key('document_type').eq('ROHIST')           
            if vinList is not None and not vinList.__contains__('ALL')  :
                if customerNo is not None:
                    if len(vinList) ==1:
                        self.logger.debug("1. only vinList len ="+str(len(vinList)))
                        filterExpression=Attr("vin").eq(vinList[0]) & Attr("customerNo").eq(customerNo)
                    else:
                      self.logger.debug("2. only vinList len ="+str(len(vinList)))
                      filterExpression=Attr("vin").is_in(vinList) & Attr("customerNo").eq(customerNo)
                else:
                    if len(vinList) ==1:
                        self.logger.debug("3 only vin ="+(vinList[0]+"_"))
                        keyConditionExpression=keyConditionExpression & Key('document_id').begins_with(str(vinList[0]+"_"))
                    else:
                      self.logger.debug("4 vinList len ="+str(len(vinList)))
                      filterExpression=Attr("vin").is_in(vinList) 

            elif customerNo is not None:
                    self.logger.debug("5 customerNo="+str(customerNo))
                    filterExpression= Attr("customerNo").eq(customerNo)
            elif fromDate is not None and toDate is not None:
                    self.logger.debug("6 ro close date filters")
                    fromDt=datetime.strptime(fromDate, '%d%b%y')                    
                    fromDate = fromDt.strftime("%Y-%m-%d")  
                    toDt=datetime.strptime(toDate, '%d%b%y')                   
                    toDate = toDt.strftime("%Y-%m-%d") 
                    date_filter_expression=(Attr("searchCloseDate").ne("") & Attr("searchCloseDate").between(fromDate,toDate))
                    #date_filter_expression=self.GetDateFilterExpression(fromDtStr=fromDate,toDtStr=toDate,searchFieldName="searchCloseDate",minDateFieldName="roMinCloseDate",maxDateFieldName="roMaxCloseDate")
                    if filterExpression is None:
                        filterExpression=(date_filter_expression)
                    else:
                        filterExpression=(filterExpression) & (date_filter_expression ) 
                          
            if filterExpression is not None:

                if last_key and len(last_key)>0 and page_size>0 :                     
                    try:
                            key1=json.loads(last_key)
                    except  Exception as e:
                            sq=urllib.parse.unquote(last_key)
                            key1 = json.loads(str(sq)) 
                    self.logger.debug("query exe....7")
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
                        self.logger.debug("query exe....8")
                        response = table.query(
                                        KeyConditionExpression= keyConditionExpression, 
                                        FilterExpression= filterExpression,
                                        ExclusiveStartKey=key1,ConsistentRead=False)
                    elif page_size>0:
                        self.logger.debug("query exe....9")
                        response = table.query(
                                        KeyConditionExpression= keyConditionExpression, 
                                        FilterExpression= filterExpression,
                                        Limit=page_size,ConsistentRead=False)
                    else:
                        self.logger.debug("query exe....10")
                        response = table.query(
                                        KeyConditionExpression= keyConditionExpression, 
                                        FilterExpression= filterExpression,
                                        ConsistentRead=False)

            else:
                if last_key and len(last_key)>0 and page_size>0 :                     
                    try:
                            key1=json.loads(last_key)
                    except  Exception as e:
                            sq=urllib.parse.unquote(last_key)
                            key1 = json.loads(str(sq)) 
                    self.logger.debug("query exe....11") 
                    response = table.query(
                                    KeyConditionExpression= keyConditionExpression,
                                    ExclusiveStartKey=key1,Limit=page_size,ConsistentRead=False) 
                else:
                    if last_key and  len(last_key)>0 :                     
                        try:
                            key1=json.loads(last_key)
                        except  Exception as e:
                            sq=urllib.parse.unquote(last_key)
                            key1 = json.loads(str(sq))  
                        self.logger.debug("query exe....12") 
                        response = table.query(
                                        KeyConditionExpression= keyConditionExpression,                                         
                                        ExclusiveStartKey=key1,ConsistentRead=False)
                    elif page_size>0:
                        self.logger.debug("query exe....13") 
                        response = table.query(
                                        KeyConditionExpression= keyConditionExpression,                                       
                                        Limit=page_size,ConsistentRead=False)
                    else:
                        self.logger.debug("only vin provided=") 
                        response = table.query(
                                        KeyConditionExpression= keyConditionExpression,                                       
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
            if  fetchAll ==True:                
                while 'LastEvaluatedKey' in response:
                    starttime = datetime.now()
                    if filterExpression is not None:
                        response = table.query(
                            KeyConditionExpression= keyConditionExpression,
                            FilterExpression= filterExpression,
                            ConsistentRead=False,               
                            ExclusiveStartKey=response['LastEvaluatedKey']
                            )
                    else:
                        response = table.query(
                            KeyConditionExpression= keyConditionExpression,
                            ConsistentRead=False,               
                            ExclusiveStartKey=response['LastEvaluatedKey']
                            )
                    try:
                        items1=response['Items']
                        #self.logger.debug("loop inner Total items="+str(len(items1)))
                        items.extend(items1)
                        #self.logger.debug("loop after adding final Total items"+str(len(items)))
                        LastEvaluatedKey=None
                        if 'LastEvaluatedKey' in response:
                            LastEvaluatedKey=response['LastEvaluatedKey']   
                            self.logger.debug("Loop LastEvaluatedKey="+str(LastEvaluatedKey))
                        if LastEvaluatedKey is None:
                            self.logger.debug("Loop LastEvaluatedKey="+str(LastEvaluatedKey))
                            break                               
                    except:
                        ""
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
            self.logger.debug("GetFilteredListNew LastEvaluatedKey>>"+str(LastEvaluatedKey))
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
    def from_dynamodb_to_json(self,item):
        d = TypeDeserializer()
        return {k: d.deserialize(value=v) for k, v in item.items()}
    @classmethod
    def getROHistoryItem(self,item):
        rohist_item={ 
                        "id":item.get('document_id'),
                        "vechicle":item.get('vechicle'),                            
                        "customer":item.get('customer'),
                        "roList":item.get('roList')                                                        
                        }
           
        return rohist_item
    @classmethod
    def prepareCloseDateFilterList(self,roList,fromDate,toDate):
        self.logger.debug("prepareCloseDateFilterList fromDate="+str(fromDate)+",toDate="+str(toDate))
        if roList is None or len(roList)<=0 :
            return { "status":True,"items": [] }    
        if fromDate is  None and toDate  is None : 
            roHistList=[]        
            for ro in roList :      
                roHistList.append(ro)
            return { "status":True,"items": roHistList }            
        closeFromDate=None
        closeToDate=None
        if fromDate is not None and toDate is not None:
            closeFromDate=datetime.strptime(fromDate, '%d%b%y')
            closeToDate=datetime.strptime(toDate, '%d%b%y')
            self.logger.debug("prepareCloseDateFilterList closeFromDate="+str(closeFromDate))
            self.logger.debug("prepareCloseDateFilterList closeToDate="+str(closeToDate))
        updatedROList=[]           
        if roList is not None and len(roList)>0: 
             for ro in roList :
                if closeFromDate is not None and closeToDate is not None :
                    try:
                        roCLoseDate=datetime.strptime(str(ro["closeDate"]), '%d%b%y')
                        self.logger.debug("loop roCLoseDate="+str(roCLoseDate))
                        if closeFromDate<=roCLoseDate and roCLoseDate<=closeToDate:                
                            updatedROList.append(ro)
                    except:
                        self.logger.debug("error in date match ro="+str(ro))  
                 
        return { "status":True,"items": updatedROList }  
    @classmethod
    def prepareList(self,items):
        roHistList=[]        
        for item in items : 
            roHistList.append(self.getROHistoryItem(item))        
        return { "status":True,"items": roHistList }   
    @classmethod
    def prepareOutputJSONList(self,items,fromDate,toDate):
        _moduleNM="ROHistory"
        _functionNM="prepareOutputJSONList"
        try:  
            if fromDate is not None and toDate is not None:
                roHistList=[]
                for item in items:
                    roList=item['roList']
                    retResp=self.prepareCloseDateFilterList(fromDate=fromDate,roList=roList,toDate=toDate)
                    if retResp['status']:
                        updatedROList=retResp['items']
                        if len(updatedROList)>0:
                            item['roList']=updatedROList
                            roHistList.append(self.getROHistoryItem(item))
                return { "status":True,"items": roHistList }           
            else:         
                return self.prepareList(items)            
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 
    @classmethod
    def prepareOutputJSONListNew(self,items,fromDate,toDate):
        _moduleNM="ROHistory"
        _functionNM="prepareOutputJSONListNew"
        try:  
           
            roHistList=[]

            groups = {}
            for item in items:
                key = item['vin']
                if key not in groups:
                    groups[key] = []
                groups[key].append(item)
            for VIN, VIN_GROUP in groups.items():
                self.logger.debug("VIN: "+str(VIN))
                vin_records=VIN_GROUP[0] 
                cust_groups = {}                 
                for VIN_GROUP_item in VIN_GROUP:
                    #self.logger.debug("VIN_GROUP Item: "+str(VIN_GROUP_item))
                    key = VIN_GROUP_item['customerNo']
                    if key is None or len(str(key).strip())==0:
                        key='NULL'
                    if key not in cust_groups:
                        cust_groups[key] = []
                    cust_groups[key].append(VIN_GROUP_item)

                for CUSTNO, CUST_GROUP in cust_groups.items():
                    self.logger.debug("CUSTNO: "+str(CUSTNO))
                    cust_records=CUST_GROUP[0] 
                    ro_groups = {} 
                    for CUST_GROUP_item in CUST_GROUP:
                        #self.logger.debug("CUST_GROUP_item: "+str(CUST_GROUP_item))
                        key = CUST_GROUP_item['roNo']
                        if key not in ro_groups:
                            ro_groups[key] = []
                        ro_groups[key].append(CUST_GROUP_item)
                    roList=[]
                    for RO, RO_GROUP in ro_groups.items():
                        self.logger.debug("RONO: "+str(RO))
                        ro_records=RO_GROUP[0] 
                        #self.logger.debug("ro_records: "+str(ro_records)) 
                        roList.append(ro_records['ro'])  
                    if len(roList)>0:
                        item={
                            "id": vin_records["vin"]+"_"+cust_records['customerNo'],
                            "vechicle":vin_records["vechicle"],
                            "customer":cust_records["customer"],
                            "roList":roList,                                       
                    }
                    roHistList.append(item) 
             
            return { "status":True,"items": roHistList }           
                   
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)        
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
               return res_handler.GetFormattedErrorResponseJSON(code=373,auth_json=auth_json,args=list1)  
        if isDateFilter==True:
            if not isFromDateValid ==True:
                list1=[ fromDateStr]
                return res_handler.GetFormattedErrorResponseJSON(code=369,auth_json=auth_json,args=list1)
                 
            if not isToDateValid==True:
                list1=[ toDateStr]
                return res_handler.GetFormattedErrorResponseJSON(code=370,auth_json=auth_json,args=list1)
                
        return {"responseCode":0}     
       
    @classmethod
    def Validate_inputs(self,store_code,vinList,customerNo,fromDate,toDate,auth_json):
        res_handler=ResponseHandler()
       
        if vinList is not  None and customerNo is not  None :        
            if ( len(vinList)==0  or str(vinList).upper()=='ALL'  or str(vinList).upper()=='NONE'):
                list1=[ "vin"]
                return res_handler.GetFormattedErrorResponseJSON(code=372,auth_json=auth_json,args=list1)
            if  len(customerNo)==0 :
                list1=[ "customerNo"]
                return res_handler.GetFormattedErrorResponseJSON(code=372,auth_json=auth_json,args=list1)
            return {"responseCode":0}
        elif vinList is not  None:
            if ( len(vinList)==0  or str(vinList).upper()=='ALL'  or str(vinList).upper()=='NONE'):
                list1=[ "vin"]
                return res_handler.GetFormattedErrorResponseJSON(code=372,auth_json=auth_json,args=list1)
            return {"responseCode":0}
        elif customerNo is not  None :        
            if  len(customerNo)==0 :
                list1=[ "customerNo"]
                return res_handler.GetFormattedErrorResponseJSON(code=372,auth_json=auth_json,args=list1)
            return {"responseCode":0}
        elif fromDate is not None and toDate is not None:
            chkResp=self.CheckDateRange(fromDate,toDate,auth_json,'fromDate','toDate')
            if chkResp['responseCode']==-1:
                return chkResp
            return {"responseCode":0}   
       
        return res_handler.GetErrorResponseJSON(code=380,auth_json=auth_json) 
       
       
    
    
    
     
  