 
import urllib.parse
import json
from EPDE_Client import AppClient 
from EPDE_Logging import LogManger
import boto3
from boto3.dynamodb.conditions import Key
from EPDE_Error import ErrorHandler
from datetime import datetime
from EPDE_Response import ResponseHandler
from boto3.dynamodb.types import TypeDeserializer
from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.conditions import Key 
#This module is responsible to handle EPDE API Deal handling
class Deal(object):
    logger=LogManger()   
    err_handler=ErrorHandler()
    region='us-east-1'
    table_Per_Store=1
    def __init__(self):        
        Deal.region="us-east-1"
        Deal.table_Per_Store=1
        
    def __init__(self,region,table_Per_Store_Invoice=1):  
        Deal.region=region
        Deal.table_Per_Store=table_Per_Store_Invoice
    @classmethod
    def chunks(self,lst, n):       
        for i in range(0, len(lst), n):
            yield lst[i:i + n]  
                
    @classmethod   
    def getTableName(self,store_code):
        TableName=store_code+"_DEAL_FILE"         
        return TableName
    
    @classmethod
    def GetDealList(self,store_code,document_type,last_key=None,page_size=100):
        _moduleNM="Deal"
        _functionNM="GetDealList"
        try:    
            starttime = datetime.now()             
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)       
            self.logger.debug("GetDealList>> store_code:"+str(store_code))           
            dynamodb = boto3.resource('dynamodb', region_name=Deal.region)
            TableName=self.getTableName(store_code)
            table = dynamodb.Table(TableName)
            LastEvaluatedKey=None
            response=None
            items=[]
            fetchAll=False
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
    def GetFilteredList(self,store_code,document_type,vinList,status,plan,_type,fromDate,toDate,activeServicePlans,activeSaleTypes,activeStatus,last_key=None,page_size=-1):
        _moduleNM="Deal"
        _functionNM="GetFilteredList"
        try:   
            starttime = datetime.now()
            self.logger.debug("GetFilteredList>> store_code:"+str(store_code)+",vinList="+str(vinList)+",fromDate="+str(fromDate)+",toDate="+str(toDate)+",last_key="+str(last_key)+",pageSize"+str(page_size))
            dynamodb = boto3.resource('dynamodb', region_name=Deal.region)
            TableName=self.getTableName(store_code)
            table = dynamodb.Table(TableName)                   
            items=[]
            fetchAll=False
            LastEvaluatedKey=None
            response=None
            vinList=self.GetVINSList(vinList)
            self.logger.debug("GetFilteredList vinList>>"+str(vinList))
            filterExpression=None
            keyConditionExpression= Key('document_type').eq(document_type)
            vin_filter_expression=None
            if vinList is not None and not vinList.__contains__('ALL') :
                vin_filter_expression = Attr("vin").is_in(vinList)                
                if filterExpression is None:
                    filterExpression=vin_filter_expression
                else:
                    filterExpression=filterExpression & vin_filter_expression
            else:
                date_filter_expression=None
                if fromDate is not None and toDate is not None:
                    soldDate=datetime.strptime(fromDate, '%d%b%y')
                    fromDate = soldDate.strftime("%Y-%m-%d")  
                    soldDate1=datetime.strptime(toDate, '%d%b%y')
                    toDate = soldDate1.strftime("%Y-%m-%d")   
                    date_filter_expression= Attr("searchDate").between(fromDate,toDate)
                    if filterExpression is None:
                        filterExpression=date_filter_expression
                    else:
                        filterExpression=filterExpression & date_filter_expression
                plan_filter_expression=None
                if plan is not None :
                    plan_filter_expression=None
                    if plan.upper() == "ALL":                     
                        if len(activeServicePlans)>1:
                            plan_filter_expression=Attr("description").is_in(activeServicePlans)  
                        elif len(activeServicePlans)==  1:
                            plan_filter_expression=Attr("description").eq(activeServicePlans[0].upper())  
                    elif plan.upper() == "NONE":
                        plan_filter_expression=Attr("description").eq('') 
                    else:
                        plan_filter_expression=Attr("description").eq(plan.upper()) 
                    if filterExpression is None:
                        filterExpression=plan_filter_expression
                    else:
                        filterExpression=filterExpression & plan_filter_expression    
                saletype_filter_expression=None
                if _type is not None:
                    saletype_filter_expression=None
                    if _type.upper() == "ALL":                     
                        if len(activeSaleTypes)>1:
                            saletype_filter_expression=Attr("r_or_w").is_in(activeSaleTypes)  
                        elif len(activeSaleTypes)==  1:
                            saletype_filter_expression=Attr("r_or_w").eq(activeSaleTypes[0].upper())  
                    elif _type.upper() == "NONE":
                        saletype_filter_expression=Attr("r_or_w").eq('') 
                    else:
                        saletype_filter_expression=Attr("r_or_w").eq(_type.upper()) 
                    if filterExpression is None:
                        filterExpression=saletype_filter_expression
                    else:
                        filterExpression=filterExpression & saletype_filter_expression    
                status_filter_expression=None    
                if status is not None:
                    status_filter_expression=None   
                    if status.upper() == "ALL":
                        if len(activeStatus)>1:
                            status_filter_expression=Attr("status").is_in(activeStatus)                                                      
                        elif len(activeStatus)==  1:
                            status_filter_expression=Attr("status").eq(activeStatus[0].upper()) 
                    elif status.upper() == "NONE":
                        status_filter_expression=Attr("status").eq('')                      
                    else:
                        status_filter_expression=Attr("status").eq(status.upper()) 
                    if filterExpression is None:
                        filterExpression=status_filter_expression
                    else:
                        filterExpression=filterExpression & status_filter_expression                  
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
                    #if 'LastEvaluatedKey' in response:
                    #    fetchAll=True 
                #self.logger.debug("response="+str(response))             
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
                                ConsistentRead=False,  Limit=page_size_new ,             
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
    def GetFilteredDeals(self,deals,plan,activePlan):
        if plan is None:
            return deals

        if plan.upper() =="NONE" :
            plans=[""]
        elif plan.upper() =="ALL":
            plans=activePlan 
        else:
            plans=[plan.upper()]
        deals1=[]  
        
        if deals is not None and len(deals)>0: 
             for deal in deals :  
                 if str(deal["description"]).upper() in plans:
                    deals1.append(deal)
        return  deals1      
   
    @classmethod 
    def from_dynamodb_to_json(self,item):
        d = TypeDeserializer()
        return {k: d.deserialize(value=v) for k, v in item.items()}
   
    @classmethod
    def prepareDealList(self,items,isFiltered):
        _moduleNM="Deal"
        _functionNM="prepareDealList"
        try:           
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)  
            dealList=[]             
            for item in items :                 
                if isFiltered:
                    ""
                    #item=self.from_dynamodb_to_json(item)  
                deal_item={                  
                    "dealNo":item.get('document_id'), 
                    "type":item.get('type'),
                    "status":item.get('status'), 
                    "saleType":item.get('saleType'), 
                    "soldDate":item.get('soldDate'),
                    "description":item.get('description'), 
                    "sale":item.get('sale'), 
                    "cost":item.get('cost'),  
                    "r_or_w":item.get('r_or_w'), 
                    "pOrL":item.get('pOrL'), 
                    "cashDown":item.get('cashDown'), 
                    "cashPrice":item.get('cashPrice'), 
                    "contractTerm":item.get('contractTerm'), 
                    "fiManager":item.get('fiManager'),
                    "financeCompanyName":item.get('financeCompanyName'), 
                    "financeCharge":item.get('financeCharge'), 
                    "financeTotalAmount":item.get('financeTotalAmount'), 
                    "leaseCashDown":item.get('leaseCashDown'), 
                    "leaseContractTerm":item.get('leaseContractTerm'), 
                    "leaseEndValue":item.get('leaseEndValue'),
                    "leaseFinanceAmount":item.get('leaseFinanceAmount'),
                    "leaseFinanceCharge":item.get('leaseFinanceCharge'),
                    "leaseMoneyFactor":item.get('leaseMoneyFactor'),
                    "leaseMSRPO":item.get('leaseMSRPO'),
                    "leasePayment":item.get('leasePayment'),
                    "leaseTotal":item.get('leaseTotal'),
                    "leftInTerm":item.get('leftInTerm'),
                    "trade1SerialNo":item.get('trade1SerialNo'),
                    "trade1StockNo":item.get('trade1StockNo'),
                    "payment":item.get('payment'),
                    "prodDate":item.get('prodDate'), 
                    "trimLevel":item.get('trimLevel'),
                    "shortStyle":item.get('shortStyle'), 
                    "tradeInNet":item.get('tradeInNet'),                  
                    "buyerNumber":item.get('buyerNumber'), 
                    "buyerFullName":item.get('buyerFullName'),  
                    "firstName":item.get('firstName'), 
                    "middleName":item.get('middleName'),
                    "lastName":item.get('lastName'), 
                    "buyerBirth":item.get('buyerBirth'), 
                    "gender":item.get('gender'), 
                    "buyerStreet":item.get('buyerStreet'),
                    "buyerCity":item.get('buyerCity'), 
                    "buyerState":item.get('buyerState'),                     
                    "buyerZip":item.get('buyerZip'), 
                    "buyerPhone1":item.get('buyerPhone1'), 
                    "buyerPhone2":item.get('buyerPhone2'), 
                    "email1":item.get('email1'), 
                    "email2":item.get('email2'),
                    "dhGender":item.get('dhGender'), 
                    "children":item.get('children'), 
                    "isCoBuyer":item.get('isCoBuyer'), 
                    "coBuyerNumber":item.get('coBuyerNumber'), 
                    "coBuyerName":item.get('coBuyerName'), 
                    "coBuyerPhone1":item.get('coBuyerPhone1'), 
                    "coBuyerPhone2":item.get('coBuyerPhone2'), 
                    "vehicleStockNo":item.get('vehicleStockNo'), 
                    "vin":item.get('vin'), 
                    "engine":item.get('engine'), 
                    "vehStyle":item.get('vehStyle'), 
                    "madeIn":item.get('madeIn'), 
                    "make":item.get('make'), 
                    "model":item.get('model'), 
                    "color":item.get('color'), 
                    "year":item.get('year') ,
                    "mileage":item.get('mileage') ,
                    "dateInService":item.get('dateInService') ,
                    "customerNo":item.get('customerNo') ,
                    "apr":item.get('apr') ,
                    "leaseTotalPayments":item.get('leaseTotalPayments'),
                    "saleTax":item.get('saleTax')
                 }
                dealList.append(deal_item)
            return { "status":True,"items": dealList }
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
    def Validate_inputs(self,store_code,vinList,plan,status,_type,fromDate,toDate,document_type,auth_json):
        res_handler=ResponseHandler()
        if vinList is not  None   and ( len(vinList)==0 or str(vinList).upper()=='ALL'  or str(vinList).upper()=='NONE'):
            list=[ "vin"]
            return res_handler.GetFormattedErrorResponseJSON(code=372,auth_json=auth_json,args=list)
            
        if plan is not  None   and  len(plan)==0 :
            list=[ "plan"]
            return res_handler.GetFormattedErrorResponseJSON(code=372,auth_json=auth_json,args=list)
        
        activeServicePlans=[]
        if plan is not  None and  len(str(plan).strip())>0:           
                filterList=["NONE","ALL"]
                activeServicePlansDict=self.GetDescriptionFilters(store_code=store_code,region=Deal.region)
                uniquePlans=set( activeServicePlansDict.values())
                activeServicePlans = sorted(uniquePlans)
                filterList.extend(activeServicePlans)
                if not plan.upper() in filterList:
                    list=[ plan]
                    return res_handler.GetFormattedErrorResponseJSON(code=374,auth_json=auth_json,args=list)
        
        if _type is not  None   and  len(_type)==0 :
            list=[ "type"]
            return res_handler.GetFormattedErrorResponseJSON(code=372,auth_json=auth_json,args=list)
        if _type is not  None and  len(str(_type).strip())>0:           
                validTypes=['RETAIL','WHOLESALE','ALL']                 
                if not _type.upper() in validTypes:
                    list=[ _type]
                    return res_handler.GetFormattedErrorResponseJSON(code=375,auth_json=auth_json,args=list)
        if status is not  None   and  len(status)==0 :
            list=[ "status"]
            return res_handler.GetFormattedErrorResponseJSON(code=372,auth_json=auth_json,args=list)
        
        if status is not  None and  len(str(status).strip())>0:           
                validStatus=['F','B','I','U','ALL']                 
                if not status.upper() in validStatus:
                    list=[ status]
                    return res_handler.GetFormattedErrorResponseJSON(code=376,auth_json=auth_json,args=list)
        
        isDateFilter=False
        isToDateValid=False
        isFromDateValid=False
        if (fromDate is not None ) and len(str(fromDate))>0:            
            if not self.ValidateDate(fromDate):
                    list=[ str("fromDate"),str(fromDate),str("ddMMMYY")]
                    return res_handler.GetFormattedErrorResponseJSON(code=371,auth_json=auth_json,args=list)
            else:
                isDateFilter=True
                isFromDateValid=True        
        if (toDate is not None ) and len(str(toDate))>0:           
            if not self.ValidateDate(toDate):            
                    list=[ str("toDate"),str(toDate),str("ddMMMYY")]
                    return res_handler.GetFormattedErrorResponseJSON(code=371,auth_json=auth_json,args=list)
            else:
                isDateFilter=True  
                isToDateValid=True
        if self.ValidateDate(fromDate) and self.ValidateDate(toDate):
            if not self.CompareDate(fromDate=fromDate,toDate=toDate)  :
               return res_handler.GetErrorResponseJSON(code=373,auth_json=auth_json)  
        if isDateFilter:
            if not isFromDateValid:
                 return res_handler.GetErrorResponseJSON(code=369,auth_json=auth_json)    
            if not isToDateValid:
                 return res_handler.GetErrorResponseJSON(code=370,auth_json=auth_json)     
        
        if vinList is None  and  isDateFilter == False and _type is None and status is None and plan is None:
            return res_handler.GetErrorResponseJSON(code=379,auth_json=auth_json) 
        
        if (plan is not  None or _type is not None or status is not None  ) and  isDateFilter==False:
            return res_handler.GetErrorResponseJSON(code=377,auth_json=auth_json)   
        

        if (document_type is None ) or document_type == "" or len(document_type.strip())==0:
            return res_handler.GetErrorResponseJSON(code=322,auth_json=auth_json)  
        if document_type != "DEAL":
            list=[ str(document_type)]
            return res_handler.GetFormattedErrorResponseJSON(code=321,auth_json=auth_json,args=list)       
 
        return {"responseCode":0,"activeServicePlans":activeServicePlans,"activeSaleTypeList":['RETAIL','WHOLESALE'],"activeStatusList":['F','B','I','U'] } 
          
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
     
    
    
     
  