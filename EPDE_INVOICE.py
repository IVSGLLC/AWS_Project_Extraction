from decimal import Decimal
import itertools
import json
import os
import subprocess
import textwrap
import urllib.parse 
from EPDE_Logging import LogManger
import boto3
from boto3.dynamodb.conditions import Key
from EPDE_Error import ErrorHandler
from datetime import datetime
from EPDE_Response import ResponseHandler
from EPDE_S3 import S3Manager
import concurrent.futures
import itertools
#This module is responsible to handle EPDE API Invoice handling
class Invoice(object):
    logger=LogManger()   
    err_handler=ErrorHandler()
    region='us-east-1'
    table_Per_Store_Invoice=1
    def __init__(self):        
        Invoice.region="us-east-1"
        Invoice.table_Per_Store_Invoice=1
        
    def __init__(self,region,table_Per_Store_Invoice=1):  
        Invoice.region=region
        Invoice.table_Per_Store_Invoice=table_Per_Store_Invoice
    @classmethod
    def chunks(self,lst, n):       
        for i in range(0, len(lst), n):
            yield lst[i:i + n]  
                
    @classmethod   
    def getTableName(self,store_code):
        TableName=store_code+"_INVOICE_FILE"          
        return TableName

    @classmethod
    def GetInvoiceDetail(self,store_code,document_id,document_type):
        _moduleNM="Invoice"
        _functionNM="GetInvoiceDetail"
        try:
            self.logger.debug("GetInvoiceDetail>>store_code="+str(store_code)+",document_id="+str(document_id)+",document_type="+str(document_type))
            dynamodb = boto3.resource('dynamodb', region_name=Invoice.region)
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
    def GetInvoiceList(self,store_code,document_type,last_key=None,page_size=-1):
        _moduleNM="Invoice"
        _functionNM="Invoice"
        try:           
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)       
            self.logger.debug("GetInvoiceList>> store_code:"+str(store_code)+",document_type"+str(document_type))           
            dynamodb = boto3.resource('dynamodb', region_name=Invoice.region)            
            TableName=self.getTableName(store_code)
            table = dynamodb.Table(TableName)       
            fetchAll=False
            LastEvaluatedKey=None
            starttime = datetime.now()
            items=[]
            if last_key and page_size>0:
                try:
                    key1=json.loads(last_key)
                except  Exception as e:
                        sq=urllib.parse.unquote(last_key)
                        key1 = json.loads((sq)) 
                response = table.query(
                     KeyConditionExpression= Key('document_type').eq(document_type),
                     ExclusiveStartKey=key1,Limit=page_size,ConsistentRead=False)
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
            try:
                callTimeOut=20     
                endtime = datetime.now()
                delta=endtime-starttime
                consumedSeconds=delta.total_seconds()  
                self.logger.debug("consumedSeconds="+str(consumedSeconds))         
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
                self.logger.debug("LastEvaluatedKey>>"+str(LastEvaluatedKey))                
                return { "status":True,"items": items ,"LastEvaluatedKey":LastEvaluatedKey}
            except:
                return { "status":True,"items": [] ,"LastEvaluatedKey":LastEvaluatedKey}           
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 

    @classmethod
    def prepareInvoiceList(self,items):
        _moduleNM="Invoice"
        _functionNM="prepareInvoiceList"
        try:           
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)  
            documentList=[]
            for item in items :                 
                document_id=item.get('document_id')                
                document={
                            "documentId": item.get('document_id'),
                            "documentType": item.get('saleType'),                          
                            "status":   item.get('status'),
                            "openDate": item.get('openDate'),
                            "openTime": item.get('openTime'),
                            "closeDate": item.get('closeDate'),                           
                            "closeTime": item.get('closeTime'),
                            "amountDue":item.get('amountDue'),
                            "warrantyDue": item.get('warrantyDue'),
                            "comments": item.get('comments'),
                            "vehicle":  item.get('vehicle'),
                            "customer":  item.get('customer'),
                            "employee": item.get('employee'),
                            "hattagnumber": item.get('hattagnumber'),
                            "contactemail": item.get('contactemail'),
                            "contactphone": item.get('contactphone') ,       
                            "cellphone": item.get('cellphone') ,  
                            "homephone": item.get('homephone') , 
                            "busphone": item.get('busphone') ,                                   
                            "laborTotal":item.get('laborTotal'),
                            "partsTotal":item.get('partsTotal'),
                            "miscTotal":item.get('miscTotal'),
                            "golTotal":item.get('golTotal'), 
                            "salesTaxTotal":item.get('salesTaxTotal'),
                            "deductibleTotal":item.get('deductibleTotal'),
                            "sublTotal":item.get('sublTotal'),
                            "schgTotal":item.get('schgTotal'),
                            "poNumber":item.get('poNumber'),
                            "rate":item.get('rate'),
                            "payment":item.get('payment'),
                            "promisedDate":item.get('promisedDate'),
                            "promisedTime":item.get('promisedTime'),
                            "waiter":item.get('waiter'),
                            "mileageIn":  item.get('mileageIn') ,
                            "mileageOut":  item.get('mileageOut'),
                            "prodDate":item.get('prodDate'),
                            "warExpDate":item.get('warExpDate'),
                            "license":item.get('license'), 
                            "options":item.get('options'),
                            "spcInstruction": item.get('spc_ins') ,
                            "serviceDetails": item.get('serviceDetails') ,
                            "roOperations":item.get('roOperations') ,  
                            "roParts":item.get('roParts') ,
                             "totalEstimate":item.get('totalEstimate')                   

                        }
                documentList.append(document)

            return { "status":True,"invoiceList": documentList }
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)  
    
    @classmethod
    def prepareInvoice(self,item,auth_json):
        _moduleNM="Invoice"
        _functionNM="prepareInvoice"
        try:           
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)  
            docuemnt= {
                            "responseCode":0,
                            "documentId": item.get('document_id'),
                            "documentType": item.get('saleType'),                          
                            "status":   item.get('status'),
                            "openDate": item.get('openDate'),
                            "openTime": item.get('openTime'),
                            "closeDate": item.get('closeDate'),                           
                            "closeTime": item.get('closeTime'),
                            "amountDue":item.get('amountDue'),
                            "warrantyDue": item.get('warrantyDue'),
                            "comments": item.get('comments'),
                            "vehicle":  item.get('vehicle'),
                            "customer":  item.get('customer'),
                            "employee": item.get('employee'),
                            "hattagnumber": item.get('hattagnumber'),
                            "contactemail": item.get('contactemail'),
                            "contactphone": item.get('contactphone') ,                             
                            "cellphone": item.get('cellphone') ,  
                            "homephone": item.get('homephone') , 
                            "busphone": item.get('busphone') ,                                    
                            "laborTotal":item.get('laborTotal'),
                            "partsTotal":item.get('partsTotal'),
                            "miscTotal":item.get('miscTotal'),
                            "golTotal":item.get('golTotal'), 
                            "salesTaxTotal":item.get('salesTaxTotal'),
                            "deductibleTotal":item.get('deductibleTotal'),
                            "sublTotal":item.get('sublTotal'),
                            "schgTotal":item.get('schgTotal'),
                            "poNumber":item.get('poNumber'),
                            "rate":item.get('rate'),
                            "payment":item.get('payment'),
                            "promisedDate":item.get('promisedDate'),
                            "promisedTime":item.get('promisedTime'),
                            "waiter":item.get('waiter'),
                            "mileageIn":  item.get('mileageIn') ,
                            "mileageOut":  item.get('mileageOut'),
                            "prodDate":item.get('prodDate'),
                            "warExpDate":item.get('warExpDate'),
                            "license":item.get('license'), 
                            "options":item.get('options'),
                            "spcInstruction": item.get('spc_ins') ,
                            "serviceDetails": item.get('serviceDetails') , 
                            "roOperations":item.get('roOperations') ,  
                            "roParts":item.get('roParts')    ,   
                            "totalEstimate":item.get('totalEstimate'),                       
                            "auth_token":auth_json
                    }            

            return { "status":True,"invoice": docuemnt }
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 
    @classmethod
    def prepareInvoiceFile(self,event,item,fileType,auth_json,optNo='CUSTOMER'):
        _moduleNM="Invoice"
        _functionNM="prepareInvoiceFile"
        try:               
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)  
            if fileType==None or fileType=='' or fileType=='JSON':
                inv= {
                           "responseCode":0,
                           "documentId": item.get('document_id'),
                            "documentType": item.get('saleType'),                          
                            "status":   item.get('status'),
                            "openDate":item.get('openDate'),
                            "openTime":item.get('openTime'),
                            "closeDate": item.get('closeDate'),                           
                            "closeTime": item.get('closeTime'),
                            "amountDue":item.get('amountDue'),
                            "warrantyDue": item.get('warrantyDue'),
                            "comments": item.get('comments'),
                            "vehicle":  item.get('vehicle'),
                            "customer":  item.get('customer'),
                            "employee": item.get('employee'),
                            "hattagnumber": item.get('hattagnumber'),
                            "contactemail": item.get('contactemail'),
                            "contactphone": item.get('contactphone') ,       
                            "cellphone": item.get('cellphone') ,  
                            "homephone": item.get('homephone') , 
                            "busphone": item.get('busphone') ,                                            
                            "laborTotal":item.get('laborTotal'),
                            "partsTotal":item.get('partsTotal'),
                            "miscTotal":item.get('miscTotal'),
                            "golTotal":item.get('golTotal'), 
                            "salesTaxTotal":item.get('salesTaxTotal'),
                            "deductibleTotal":item.get('deductibleTotal'),
                            "sublTotal":item.get('sublTotal'),
                            "schgTotal":item.get('schgTotal'),
                            "poNumber":item.get('poNumber'),
                            "rate":item.get('rate'),
                            "payment":item.get('payment'),
                            "promisedDate":item.get('promisedDate'),
                            "promisedTime":item.get('promisedTime'),
                            "waiter":item.get('waiter'),
                            "mileageIn":  item.get('mileageIn') ,
                            "mileageOut":  item.get('mileageOut'),
                            "prodDate":item.get('prodDate') ,
                            "warExpDate":item.get('warExpDate') ,
                            "license":item.get('license'), 
                            "options":item.get('options'),
                            "spcInstruction": item.get('spc_ins'),  
                            "serviceDetails": item.get('serviceDetails'), 
                            "roOperations":item.get('roOperations') ,  
                            "roParts":item.get('roParts'),
                            "totalEstimate":item.get('totalEstimate'),
                            "auth_token":auth_json
                    } 
                       
            if fileType.upper()=='PDF':
                inv= self.getInvoicePDFFile(event,item,optNo)
            if fileType.upper()=='HTML':
                inv=  self.getInvoiceHTMLFile(event,item,optNo,True)
            return { "status":True,"invoice": inv }  
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 

    @classmethod
    def Validate_InvoiceList_inputs(self,document_type,auth_json):
        res_handler=ResponseHandler()
        if (document_type is None ) or document_type == "" or len(document_type.strip())==0:
           return res_handler.GetErrorResponseJSON(code=322,auth_json=auth_json)
        if document_type != "RO":
            list=[ str(document_type)]
            return res_handler.GetFormattedErrorResponseJSON(code=321,auth_json=auth_json,args=list)
        return {"responseCode":0}

    @classmethod
    def Validate_InvoiceDetail_inputs(self,document_id,document_type,auth_json):
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
    def is_number(self,n):
        is_number = True
        try:
            num = Decimal(n)
            # check for "nan" floats
            is_number = num == num   # or use `math.isnan(num)`
        except Exception:
            is_number = False
        return is_number
    @classmethod
    def Validate_InvoiceDetailPDF_inputs(self,document_id,document_type,fileType,auth_json,copyType):
        res_handler=ResponseHandler()
        if (document_id is None ) or document_id == "" or len(document_id.strip())==0:
            return res_handler.GetErrorResponseJSON(code=310,auth_json=auth_json)     
        if (document_type is None ) or document_type == "" or len(document_type.strip())==0:
            return res_handler.GetErrorResponseJSON(code=322,auth_json=auth_json)  
        if document_type != "RO":
            list=[ str(document_type)]
            return res_handler.GetFormattedErrorResponseJSON(code=321,auth_json=auth_json,args=list)
        if (fileType is None ) or fileType == "" or len(fileType.strip())==0:
            return res_handler.GetErrorResponseJSON(code=358,auth_json=auth_json)  
        if fileType.upper() != "HTML" and fileType.upper() != "PDF":
            list=[ str(fileType)]
            return res_handler.GetFormattedErrorResponseJSON(code=357,auth_json=auth_json,args=list)
        if (copyType is None ) or copyType == "" or len(copyType.strip())==0:
            return res_handler.GetErrorResponseJSON(code=364,auth_json=auth_json)  
        if copyType.upper() != "CUSTOMER" and copyType.upper() != "ACCOUNTING" and copyType.upper() != "WARRANTY":
            list=[ str(copyType)]
            return res_handler.GetFormattedErrorResponseJSON(code=363,auth_json=auth_json,args=list)

        return {"responseCode":0}  
    
    @classmethod 
    def phone_format(self,n): 
        if n!= None and len(n)>0:                                                                                                                                 
           return format(int(n[:-1]), ",").replace(",", "-") + n[-1]  
        return n
    @classmethod
    def PrepareHeader(self,item,template):
            htmlTempData=template
            htmlTempData= htmlTempData.replace('--CUST_NO--',item['customer']['id'])
            htmlTempData= htmlTempData.replace('--CUST_FNAME--',item['customer']['firstName'])
            htmlTempData= htmlTempData.replace('--CUST_LNAME--',item['customer']['lastName'])
            htmlTempData= htmlTempData.replace('--CUST_ADDRESS--',item['customer']['addresses'][0]['addressLine'][0])
            htmlTempData= htmlTempData.replace('--CUST_CITY--',item['customer']['addresses'][0]['city']) 
            htmlTempData= htmlTempData.replace('--CUST_STATE--',item['customer']['addresses'][0]['state']) 
            htmlTempData= htmlTempData.replace('--CUST_ZIP--',item['customer']['addresses'][0]['zip']) 
            htmlTempData= htmlTempData.replace('--CUST_EMAIL--',item['customer']['email'])   
       

            htmlTempData= htmlTempData.replace('--HOME_PHONE--',self.phone_format(item['homephone']))
            htmlTempData= htmlTempData.replace('--BUS_PHONE--',self.phone_format(item['busphone']))
            htmlTempData= htmlTempData.replace('--CONT_PHONE--',self.phone_format(item['contactphone']))
            htmlTempData= htmlTempData.replace('--CELL_PHONE--',self.phone_format(item['cellphone']))

            htmlTempData= htmlTempData.replace('--EMP_NO--',item['employee']['id'])
            htmlTempData= htmlTempData.replace('--EMP_FNAME--',item['employee']['firstName'])
            htmlTempData= htmlTempData.replace('--EMP_LNAME--',item['employee']['lastName'])
            
            htmlTempData= htmlTempData.replace('--RO_NUMBER--',item['document_id'])
           
            htmlTempData= htmlTempData.replace('--COLOR--',str(item['vehicle']['color']).upper())
            htmlTempData= htmlTempData.replace('--YEAR--',item['vehicle']['year'])
            htmlTempData= htmlTempData.replace('--MAKE--',item['vehicle']['make'])
            htmlTempData= htmlTempData.replace('--MODAL--',item['vehicle']['model'])
            htmlTempData= htmlTempData.replace('--VIN--',item['vehicle']['vin']) 
            htmlTempData= htmlTempData.replace('--LICENSE--',item['license'])  
            htmlTempData= htmlTempData.replace('--MILEAGE_IN--',item['mileageIn'])
            htmlTempData= htmlTempData.replace('--MILEAGE_OUT--',item['mileageOut'])
            htmlTempData= htmlTempData.replace('--TAG_NO--',item['hattagnumber'])  

            htmlTempData= htmlTempData.replace('--DEL_DATE--',self.FormatDate(item['vehicle']['delDate']))
            htmlTempData= htmlTempData.replace('--PROD_DATE--',self.FormatDate(item['prodDate']))
            htmlTempData= htmlTempData.replace('--WAR_EXP_DATE--',self.FormatDate(item['warExpDate']))

            if item['waiter']==True :
                htmlTempData= htmlTempData.replace('--PROMISED_DATE--',"WAIT "+self.FormatDate(item['promisedDate'])) 
            else:
                htmlTempData= htmlTempData.replace('--PROMISED_DATE--',self.FormatDate(item['promisedDate'])) 

            htmlTempData= htmlTempData.replace('--PROMISED_TIME--',self.FormatTime(item['promisedTime']))  
            htmlTempData= htmlTempData.replace('--PO_NO--',item['poNumber'])                   
            htmlTempData= htmlTempData.replace('--RATE--',item['rate'])
            htmlTempData= htmlTempData.replace('--PAYMENT--',item['payment'])
            htmlTempData= htmlTempData.replace('--INV_DATE--',self.FormatDate(item['closeDate']))
            htmlTempData= htmlTempData.replace('--OPEN_DATE--',self.FormatDate(item['openDate'])) 
            htmlTempData= htmlTempData.replace('--OPEN_TIME--',self.FormatTime(item['openTime']))            
            htmlTempData= htmlTempData.replace('--READY_DATE--',self.FormatDate(item['closeDate']))   
            htmlTempData= htmlTempData.replace('--READY_TIME--',self.FormatTime(item['closeTime'])) 
            htmlTempData= htmlTempData.replace('--OPTIONS--',item['options'])            
            htmlTempData= htmlTempData.replace('--SALE_TYPE--',item['document_type'])
            htmlTempData= htmlTempData.replace('--STATUS--',item['status'])             
            htmlTempData= htmlTempData.replace('--CLOSE_DATE--',self.FormatDate(item['closeDate']))   
            htmlTempData= htmlTempData.replace('--CLOSE_TIME--',self.FormatTime(item['closeTime']) ) 
            htmlTempData= htmlTempData.replace('--COMMENTS--',item['comments'])
            htmlTempData= htmlTempData.replace('--SPC_INS--',item['spc_ins'])
            return htmlTempData
    @classmethod
    def GetTemplate(self,bodyTempData,tempType):
        bodyTemplates=bodyTempData.split('!!')    
        if tempType =='STAR_LINE':
            return bodyTemplates[0]
        elif tempType =='BLANK_LINE':
            return bodyTemplates[1]
        elif tempType =='LABOR_LINE':
            return bodyTemplates[2]
        elif tempType =='OPCODE':
            return bodyTemplates[3]
        elif tempType =='OPCODE_1':
            return bodyTemplates[4]
        elif tempType =='TOTAL_LINECODE':
            return bodyTemplates[5]
        elif tempType =='PARTS':
            return bodyTemplates[6]
        elif tempType =='CAUSES':
            return bodyTemplates[7]
        elif tempType =='PARTS_NOTES':
            return bodyTemplates[8]
        elif tempType =='TECH_STORY':
            return bodyTemplates[9]
        elif  tempType =='ROMLS':
            return bodyTemplates[10]
        elif tempType =='LAST_PAGE_MSG':
            return bodyTemplates[11]
        elif tempType =='PAGE_TEMP':
            return bodyTemplates[12]
        elif tempType =='RO_OPERATION':
            return bodyTemplates[13]
        elif tempType =='RO_OPERATION_1':
            return bodyTemplates[14] 
        elif tempType =='ESTIMATE':
            return bodyTemplates[15] 
        elif tempType =='CONTACT':
            return bodyTemplates[16] 
        elif tempType =='LONG_STAR_LINE':
            return bodyTemplates[17] 
        elif tempType =='ACCT_TBL':
            return bodyTemplates[18]
        elif tempType =='ACCT_ROW':
            return bodyTemplates[19]
        elif tempType =='ROP_TBL':
            return bodyTemplates[20]
        elif tempType =='ROP_ROW':
            return bodyTemplates[21]  
        elif tempType =='COMP_COST_SALE':
            return bodyTemplates[22] 
       
        return ""
    @classmethod
    def handlePageBreakTextWrap(self,totalLines,currentLine,totalLinePerPage,bodyTempData):
        bodyLineItems=[]    
       
        lineRemains=totalLinePerPage-currentLine
       
        if totalLines>lineRemains:
            self.logger.debug("NO SPACE :: lineRemains="+str(lineRemains))
            i=0
            while i<lineRemains :
                blank_row=self.GetTemplate(bodyTempData,'BLANK_LINE') 
                bodyLineItems.append(blank_row)
                i=i+1
                currentLine=currentLine+1
                if currentLine>=totalLinePerPage:                   
                   currentLine=0 
                              
            self.logger.debug("UPDATED. currentLine="+str(currentLine))        
        return {'bodyLineItems':bodyLineItems,'currentLine':currentLine}   
    @classmethod
    def PrepareLineCodeRow(self,bodyTempData,serviceDetail,totalLinePerPage,currentLine):
        bodyLineItems=[]
        lineCode= serviceDetail['lineCode']
        serviceOrPartsDescription=serviceDetail['lineDesc']

        n =78 # chunk length
        #chunks = [serviceOrPartsDescription[i:i+n] for i in range(0, len(serviceOrPartsDescription), n)]
        chunks=self.splitLongLine(line=serviceOrPartsDescription,width=n)
        index=1
        #handlePageBreakTextWrap
        totalLines=len(chunks)
        self.logger.debug("LINE CODE["+lineCode+"] totalLines="+str(totalLines)+",totalLinePerPage="+str(totalLinePerPage)+",currentLine="+str(currentLine))
        ret_dict=self.handlePageBreakTextWrap(totalLines=totalLines,totalLinePerPage=totalLinePerPage,currentLine=currentLine,bodyTempData=bodyTempData)
        lines=ret_dict['bodyLineItems']
        if len(lines)>0:
           bodyLineItems.extend(lines) 
           currentLine= ret_dict['currentLine']
       
        for line_desc in chunks:
            bodyLineTempData=self.GetTemplate(bodyTempData,'LABOR_LINE')          
            if index==1:
                lcode=lineCode                 
            else:
                lcode='&nbsp;'
            if index==len(chunks):
                style="style='border-bottom:1px solid #000;'"
            else:
                style=""
            bodyLineTempData= bodyLineTempData.replace('--LINE_CODE--',lcode)
            bodyLineTempData= bodyLineTempData.replace('--LINE_DESC--',line_desc.upper())
            bodyLineTempData= bodyLineTempData.replace('--STYLE--',style)
            bodyLineItems.append(bodyLineTempData)
            index=index+1
            currentLine=currentLine+1
            if currentLine>=totalLinePerPage:
               currentLine=0 
        return {'bodyLineItems':bodyLineItems,'currentLine':currentLine}  
    @classmethod
    def PrepareCausesRow(self,bodyTempData,serviceDetail,totalLinePerPage,currentLine):
        bodyLineItems=[]
        causes= serviceDetail['causes']
        if len(causes)>0:
            n =72 # chunk length
            #chunks = [causes[i:i+n] for i in range(0, len(causes), n)]
            chunks=self.splitLongLine(line=causes,width=n)
            index=1
            #handlePageBreakTextWrap
            totalLines=len(chunks)
            self.logger.debug("CAUSES totalLines="+str(totalLines)+",totalLinePerPage="+str(totalLinePerPage)+",currentLine="+str(currentLine))
        
            ret_dict=self.handlePageBreakTextWrap(totalLines=totalLines,totalLinePerPage=totalLinePerPage,currentLine=currentLine,bodyTempData=bodyTempData)
            lines=ret_dict['bodyLineItems']
            if len(lines)>0:
                bodyLineItems.extend(lines) 
                currentLine= ret_dict['currentLine']                
           
            for cause in chunks:
                bodyLineTempData=self.GetTemplate(bodyTempData,'CAUSES')           
                if index==1:
                    ccode="CAUSES:"                 
                else:
                    ccode=''
                bodyLineTempData= bodyLineTempData.replace('--CAUSES_LBL--',ccode)
                bodyLineTempData= bodyLineTempData.replace('--CAUSES--',cause.upper())
                bodyLineItems.append(bodyLineTempData)
                index=index+1
                currentLine=currentLine+1
                if currentLine>=totalLinePerPage:
                    currentLine=0  
        return {'bodyLineItems':bodyLineItems,'currentLine':currentLine} 
    @classmethod
    def PrepareOpcodeRow(self,bodyTempData,serviceDetail,totalLinePerPage,currentLine,copyType = 'CUSTOMER'):
        bodyLineItems=[]
        indexCode=1  
        accountLines=[]  
        sum_labor=Decimal(0.0)
        sum_parts=Decimal(0.0)  
        sum_other=Decimal(0.0)                             
        opCodes=serviceDetail['operations']
        if len(opCodes)>0:
            for opcode in opCodes: 
                line_type=opcode['type']
                if line_type=='LOP-OPS':
                                                 
                        op_code=opcode['code']
                        opCodeDesc=opcode['desc']
                        tech_no=opcode['techNo']
                        lbrType=opcode['lbrType']
                        soldhours=opcode['soldHrs']
                        actualHrs=opcode['actualHrs']
                        cost=opcode['cost']
                        sale=opcode['sale']
                        lbrCostAmt=opcode['lbrCostAmt']
                        lbrSaleAmt=opcode['lbrSaleAmt']
                        lbrSaleCtrlNo=opcode['lbrSaleCtrlNo']  
                        crCo=opcode['crCo']                 
                        lbrSaleAcct=opcode['lbrSaleAcct']
                        account={
                            
                            'TRGT':str(crCo),
                            'ACCOUNT':str(lbrSaleAcct), 
                            'SALE':lbrSaleAmt,
                            'COST':lbrCostAmt,
                            'CONTROL':lbrSaleCtrlNo
                        }
                        accountLines.append(account) 
                        if op_code is not None:
                            op_code=op_code.upper()
                        if opCodeDesc is not None:
                            opCodeDesc=opCodeDesc.upper()

                        n =74-(len(op_code)+3) # chunk length
                        #chunks = [opCodeDesc[i:i+n] for i in range(0, len(opCodeDesc), n)] 
                        chunks=self.splitLongLine(line=opCodeDesc,width=n)                   
                        index=1
                        #handlePageBreakTextWrap
                        totalLines=len(chunks)
                        self.logger.debug("OPCODE totalLines="+str(totalLines)+",totalLinePerPage="+str(totalLinePerPage)+",currentLine="+str(currentLine))
                        ret_dict=self.handlePageBreakTextWrap(totalLines=totalLines,totalLinePerPage=totalLinePerPage,currentLine=currentLine,bodyTempData=bodyTempData)
                        lines=ret_dict['bodyLineItems']
                        if len(lines)>0:
                            bodyLineItems.extend(lines) 
                            currentLine= ret_dict['currentLine']
                        for desc in chunks:
                                
                            if index==1:
                                ccode=op_code                
                            else:
                                i=0
                                ccode=''
                                while i<len(op_code):
                                        ccode=ccode+'&nbsp;'
                                        i=i+1                            
                                    
                                #ccode=''
                        
                            bodyLineTempData=self.GetTemplate(bodyTempData,'OPCODE')
                            bodyLineTempData= bodyLineTempData.replace('--OPCODE--',ccode)
                            bodyLineTempData= bodyLineTempData.replace('--OPCODE_DESC--',desc)
                            bodyLineItems.append(bodyLineTempData)
                            index=index+1
                            currentLine=currentLine+1
                            if currentLine>=totalLinePerPage:
                                currentLine=0   
                        
                        bodyLineTempData=self.GetTemplate(bodyTempData,'OPCODE_1')  

                        if len(str(soldhours))==0:
                            bodyLineTempData= bodyLineTempData.replace('--SOLD_HRS--','&nbsp;')
                        else:
                            bodyLineTempData= bodyLineTempData.replace('--SOLD_HRS--',soldhours)
                        if len(str(actualHrs))==0:
                            bodyLineTempData= bodyLineTempData.replace('--ACTUAL_HRS--','&nbsp;')
                        else:
                            bodyLineTempData= bodyLineTempData.replace('--ACTUAL_HRS--',actualHrs)

                        if len(str(lbrCostAmt))==0:
                            bodyLineTempData= bodyLineTempData.replace('--COST_AMT--','&nbsp;')
                        else:
                            bodyLineTempData= bodyLineTempData.replace('--COST_AMT--',lbrCostAmt) 

                        if len(str(lbrSaleAmt))==0:
                            bodyLineTempData= bodyLineTempData.replace('--SALE_AMT--','&nbsp;')
                        else:
                            bodyLineTempData= bodyLineTempData.replace('--SALE_AMT--',lbrSaleAmt) 
                        if len(str(tech_no))==0:
                            bodyLineTempData= bodyLineTempData.replace('--TECH_NO--','&nbsp;')
                        else:
                            bodyLineTempData= bodyLineTempData.replace('--TECH_NO--',tech_no)
                        if len(str(lbrType))==0:
                            bodyLineTempData= bodyLineTempData.replace('--LBR_TYPE--','&nbsp;')
                        else:
                            bodyLineTempData= bodyLineTempData.replace('--LBR_TYPE--',lbrType)

                        if copyType == 'CUSTOMER':
                            if (str(lbrType).upper().startswith("I") or lbrType.upper().startswith("W")):
                                bodyLineTempData= bodyLineTempData.replace('--NET_AMT--','&nbsp;')
                                bodyLineTempData= bodyLineTempData.replace('--TOTAL_AMT--','(N/C)')
                            else:
                                if len(str(sale))==0:
                                    bodyLineTempData= bodyLineTempData.replace('--NET_AMT--','&nbsp;')
                                    bodyLineTempData= bodyLineTempData.replace('--TOTAL_AMT--','&nbsp;')
                                else:
                                    bodyLineTempData= bodyLineTempData.replace('--NET_AMT--',sale)
                                    bodyLineTempData= bodyLineTempData.replace('--TOTAL_AMT--',sale)
                        else:
                            if len(str(sale))==0:
                                bodyLineTempData= bodyLineTempData.replace('--NET_AMT--','&nbsp;')
                                bodyLineTempData= bodyLineTempData.replace('--TOTAL_AMT--','&nbsp;')
                            else:
                                bodyLineTempData= bodyLineTempData.replace('--NET_AMT--',sale)
                                bodyLineTempData= bodyLineTempData.replace('--TOTAL_AMT--',sale)
                        
                        bodyLineItems.append(bodyLineTempData)
                        currentLine=currentLine+1
                        if currentLine>=totalLinePerPage:
                            currentLine=0
                        
                        if self.is_number(sale):
                            sum_labor=sum_labor+Decimal(sale)
                        parts=opcode['parts']
                        ret_dict=self.PreparePartsRow(bodyTempData=bodyTempData,parts=parts,totalLinePerPage=totalLinePerPage,currentLine=currentLine)
                        lines=ret_dict['bodyLineItems']
                        if len(lines)>0:
                            bodyLineItems.extend(lines) 
                            currentLine= ret_dict['currentLine']
                            sum_parts=sum_parts+ret_dict['sum_parts']
                            acctlines=ret_dict['accountLines']
                            if len(acctlines)>0:  
                                accountLines.extend(acctlines)      
                elif line_type=='RO-MLS':
                    mls=opcode
                    code=mls['code']
                    mls_desc=mls['desc']
                    lbrSaleAcct=mls['lbrSaleAcct']
                    cost=mls['cost']                    
                    saleCo=mls['saleCo']    
                    lbrSaleCtrlNo=mls['lbrSaleCtrlNo'] 
                    lbrType=mls['lbrType']                    
                    feeId=mls['feeId']                    
                    sale=mls['sale']
                    netAmt=sale
                    lbrCostAmt=mls['lbrCostAmt']
                    lbrSaleAmt=mls['lbrSaleAmt']  
                    account={
                        'TRGT':str(saleCo),
                        'ACCOUNT':str(lbrSaleAcct),
                        'SALE':lbrSaleAmt,
                        'COST':lbrCostAmt,
                        'CONTROL':lbrSaleCtrlNo
                    }
                    accountLines.append(account)                         
                    n =74-(len(feeId)+3) # chunk length
                    #chunks = [mls_desc[i:i+n] for i in range(0, len(mls_desc), n)] 
                    chunks=self.splitLongLine(line=mls_desc,width=n)   
                    self.logger.debug("ROMLS DESC length chunk="+str(len(chunks)))                      
                    index=1
                    #handlePageBreakTextWrap
                    totalLines=len(chunks)
                    self.logger.debug("ROMLS totalLines="+str(totalLines)+",totalLinePerPage="+str(totalLinePerPage)+",currentLine="+str(currentLine))
                    ret_dict=self.handlePageBreakTextWrap(totalLines=totalLines,totalLinePerPage=totalLinePerPage,currentLine=currentLine,bodyTempData=bodyTempData)
                    lines=ret_dict['bodyLineItems']
                    if len(lines)>0:
                        bodyLineItems.extend(lines) 
                        currentLine= ret_dict['currentLine'] 
                    for desc in chunks:                            
                        if index==1:
                            ccode=feeId                
                        else:
                            ccode=''
                            i=0
                            while i<len(feeId):
                                ccode=ccode+'&nbsp;'
                                i=i+1
                        bodyLineTempData=self.GetTemplate(bodyTempData,'OPCODE')  
                        bodyLineTempData= bodyLineTempData.replace('--OPCODE--',ccode)
                        bodyLineTempData= bodyLineTempData.replace('--OPCODE_DESC--',desc.upper())
                        bodyLineItems.append(bodyLineTempData)
                        index=index+1
                        currentLine=currentLine+1
                        if currentLine>=totalLinePerPage:
                            currentLine=0

                    bodyLineTempData=self.GetTemplate(bodyTempData,'OPCODE_1')                                   
                    bodyLineTempData= bodyLineTempData.replace('--SOLD_HRS--','&nbsp;')
                    bodyLineTempData= bodyLineTempData.replace('--ACTUAL_HRS--','&nbsp;')
                    bodyLineTempData= bodyLineTempData.replace('--COST_AMT--','&nbsp;')
                    bodyLineTempData= bodyLineTempData.replace('--SALE_AMT--','&nbsp;')
                    bodyLineTempData= bodyLineTempData.replace('--TECH_NO--','&nbsp;')
                    

                    if len(str(lbrType))==0:
                            bodyLineTempData= bodyLineTempData.replace('--LBR_TYPE--','&nbsp;')
                    else:
                            bodyLineTempData= bodyLineTempData.replace('--LBR_TYPE--',lbrType)

                    if copyType == 'CUSTOMER':
                        lbrType_1=str(lbrType)
                        lbrType_1=lbrType_1.upper()
                        if lbrType_1.startswith("I") or lbrType_1.startswith("W"):
                            bodyLineTempData= bodyLineTempData.replace('--NET_AMT--','&nbsp;')
                            bodyLineTempData= bodyLineTempData.replace('--TOTAL_AMT--','(N/C)')
                        else:
                            if len(str(sale))==0:
                                bodyLineTempData= bodyLineTempData.replace('--NET_AMT--','&nbsp;')
                                bodyLineTempData= bodyLineTempData.replace('--TOTAL_AMT--','&nbsp;')
                            else:
                                bodyLineTempData= bodyLineTempData.replace('--NET_AMT--',netAmt)
                                bodyLineTempData= bodyLineTempData.replace('--TOTAL_AMT--',sale)
                    else:
                        if len(str(sale))==0:
                            bodyLineTempData= bodyLineTempData.replace('--NET_AMT--','&nbsp;')
                            bodyLineTempData= bodyLineTempData.replace('--TOTAL_AMT--','&nbsp;')
                        else:
                            bodyLineTempData= bodyLineTempData.replace('--NET_AMT--',netAmt)
                            bodyLineTempData= bodyLineTempData.replace('--TOTAL_AMT--',sale)
                        

                    bodyLineItems.append(bodyLineTempData)
                    currentLine=currentLine+1
                    if currentLine>=totalLinePerPage:
                        currentLine=0

                    self.logger.debug("ROMLS sale="+str(sale))  
                    
                    if copyType == 'CUSTOMER' and  (str(lbrType).upper().startswith("I") or str(lbrType).upper().startswith("W")):
                        ""
                    else:
                        if self.is_number(sale) :
                            sum_other=sum_other+Decimal(sale)
                            self.logger.debug("ROMLS sum_other="+str(sum_other))  
                    parts=mls['parts']
                    ret_dict=self.PreparePartsRow(bodyTempData=bodyTempData,parts=parts,totalLinePerPage=totalLinePerPage,currentLine=currentLine)
                    lines=ret_dict['bodyLineItems']
                    if len(lines)>0:
                        bodyLineItems.extend(lines) 
                        currentLine= ret_dict['currentLine']
                        sum_parts=sum_parts+ret_dict['sum_parts']
                        acctlines=ret_dict['accountLines']
                        if len(acctlines)>0:  
                            accountLines.extend(acctlines)      

                indexCode=indexCode+1  
            #end opcode loop
         
                           
        return {'bodyLineItems':bodyLineItems,'currentLine':currentLine,'accountLines':accountLines,'sum_labor':sum_labor,'sum_parts':sum_parts,"sum_other":sum_other} 
    @classmethod
    def PreparePartsRow(self,bodyTempData,parts,totalLinePerPage,currentLine):
        bodyLineItems=[]
        accountLines=[]
        sum_parts=Decimal(0.0)
       
        for part in parts:
            partNumber=part['partNo']
            partDesc=part['desc']
            quantity=part['qtyOrd']
            sale=part['sale']
            cost=part['cost']
            totalSale=part['tSale']
            list1=part['list']
            totalAmt=part['total'] 
            saleAcct=part['saleAcct']
            saleCo=part['saleCo']
            qtyB=part['qtyB'] 
            lbrCostAmt=part['lbrCostAmt'] 
            lbrSaleAmt=part['lbrSaleAmt']  
            lbrListAmt=part['lbrListAmt']  
            partsNotes=part['partsNotes']
            lbrSaleCtrlNo=''            
            account={
                'TRGT':str(saleCo),
                'ACCOUNT':str(saleAcct),                                   
                'SALE':lbrSaleAmt,
                'COST':lbrCostAmt,
                'CONTROL':lbrSaleCtrlNo
            }
            accountLines.append(account)  
            n =38  # chunk length
            #chunks = [partDesc[i:i+n] for i in range(0, len(partDesc), n)]  
            chunks=self.splitLongLine(line=str(partNumber)+' '+partDesc,width=n)          
            index=1
            #handlePageBreakTextWrap
            totalLines=len(chunks)
            self.logger.debug("PARTS totalLines="+str(totalLines)+",totalLinePerPage="+str(totalLinePerPage)+",currentLine="+str(currentLine))
            ret_dict=self.handlePageBreakTextWrap(totalLines=totalLines,totalLinePerPage=totalLinePerPage,currentLine=currentLine,bodyTempData=bodyTempData)
            lines=ret_dict['bodyLineItems']
            if len(lines)>0:
                bodyLineItems.extend(lines) 
                currentLine= ret_dict['currentLine']  
            if len(chunks)>1:
                last=len(chunks)
                for desc in chunks:
                    qty=''
                    prt=''
                    lsamt=''
                    lcamt=''
                    qb=''
                    lst=''
                    ts=''  
                    saleAmt=''  
                    if index==1:
                        qty=quantity 
                        prt='&nbsp;'  
                        lsamt='&nbsp;'
                        lcamt='&nbsp;'
                        qb='&nbsp;'
                        lst='&nbsp;'
                        ts='&nbsp;'
                        saleAmt='&nbsp;'
                    elif index==last:
                        qty=''
                        i=0
                        while i<len(quantity):
                            qty=qty+'&nbsp;'
                            i=i+1  
                        prt='&nbsp;'
                        lsamt=lbrSaleAmt     
                        lcamt=lbrCostAmt
                        qb=qtyB
                        lst=list1
                        ts=totalSale
                        saleAmt=sale
                    else:
                        qty=''
                        i=0
                        while i<len(quantity):
                            qty=qty+'&nbsp;'
                            i=i+1  
                        prt='&nbsp;'
                        lsamt='&nbsp;'
                        lcamt='&nbsp;'
                        qb='&nbsp;'
                        lst='&nbsp;'
                        ts='&nbsp;'
                        saleAmt='&nbsp;'

                    
                    bodyLineTempData=self.GetTemplate(bodyTempData,'PARTS')  
                    bodyLineTempData= bodyLineTempData.replace('--PART_QTY--',qty)
                    bodyLineTempData= bodyLineTempData.replace('--PART_NO--',prt)
                    bodyLineTempData= bodyLineTempData.replace('--PART_DEC--',desc.upper())                                   
                    
                    bodyLineTempData= bodyLineTempData.replace('--PART_SALE_AMT--',lsamt)
                    bodyLineTempData= bodyLineTempData.replace('--PART_COST_AMT--',lcamt)
                    bodyLineTempData= bodyLineTempData.replace('--PART_QTY_B--',qb)

                    bodyLineTempData= bodyLineTempData.replace('--PART_LIST--',saleAmt)
                    bodyLineTempData= bodyLineTempData.replace('--PART_NET--',saleAmt)
                    bodyLineTempData= bodyLineTempData.replace('--PART_TOTAL--',ts)
                    bodyLineItems.append(bodyLineTempData)
                    index=index+1
                    currentLine=currentLine+1
                    if currentLine>=totalLinePerPage:
                        currentLine=0

            else:
                bodyLineTempData=self.GetTemplate(bodyTempData,'PARTS')  
                bodyLineTempData= bodyLineTempData.replace('--PART_QTY--',quantity)
                bodyLineTempData= bodyLineTempData.replace('--PART_NO--',partNumber)
                bodyLineTempData= bodyLineTempData.replace('--PART_DEC--',str(partNumber)+" "+partDesc.upper())                                   
                
                bodyLineTempData= bodyLineTempData.replace('--PART_SALE_AMT--',lbrSaleAmt)
                bodyLineTempData= bodyLineTempData.replace('--PART_COST_AMT--',lbrCostAmt)
                bodyLineTempData= bodyLineTempData.replace('--PART_QTY_B--',qtyB)

                bodyLineTempData= bodyLineTempData.replace('--PART_LIST--',sale)
                bodyLineTempData= bodyLineTempData.replace('--PART_NET--',sale)
                bodyLineTempData= bodyLineTempData.replace('--PART_TOTAL--',totalSale)
                bodyLineItems.append(bodyLineTempData)
                currentLine=currentLine+1
                if currentLine>=totalLinePerPage:
                    currentLine=0
            
            if  self.is_number(totalSale) :
                sum_parts=sum_parts+Decimal(totalSale)
                    
                #part notes                                     
                if len(partsNotes)>0:
                    ret_dict=self.PreparePartsNotesRow(bodyTempData=bodyTempData,partsNotes=partsNotes,totalLinePerPage=totalLinePerPage,currentLine=currentLine)
                    lines=ret_dict['bodyLineItems']
                    if len(lines)>0:
                        bodyLineItems.extend(lines) 
                        currentLine= ret_dict['currentLine']                     

        # end parts loop
        return {'bodyLineItems':bodyLineItems,'currentLine':currentLine,'accountLines':accountLines,'sum_parts':sum_parts} 
    @classmethod
    def PreparePartsNotesRow(self,bodyTempData,partsNotes,totalLinePerPage,currentLine):
        bodyLineItems=[]
        #part notes                                     
        if len(partsNotes)>0:
            n =72 # chunk length
            #chunks = [partsNotes[i:i+n] for i in range(0, len(partsNotes), n)]
            chunks=self.splitLongLine(line=partsNotes,width=n)
            index=1
            #handlePageBreakTextWrap
            totalLines=len(chunks)
            self.logger.debug("PARTS NOTES totalLines="+str(totalLines)+",totalLinePerPage="+str(totalLinePerPage)+",currentLine="+str(currentLine))
            ret_dict=self.handlePageBreakTextWrap(totalLines=totalLines,totalLinePerPage=totalLinePerPage,currentLine=currentLine,bodyTempData=bodyTempData)
            lines=ret_dict['bodyLineItems']
            if len(lines)>0:
                bodyLineItems.extend(lines) 
                currentLine= ret_dict['currentLine']  
            for note in chunks:
                
                bodyLineTempData=self.GetTemplate(bodyTempData,'PARTS_NOTES')        
                bodyLineTempData= bodyLineTempData.replace('--PART_NOTES--',note.upper())
                bodyLineItems.append(bodyLineTempData)
                index=index+1
                currentLine=currentLine+1
                if currentLine>=totalLinePerPage:
                    currentLine=0  

        return {'bodyLineItems':bodyLineItems,'currentLine':currentLine} 
    @classmethod
    def PrepareROOperationsRow(self,bodyTempData,roMls,totalLinePerPage,currentLine,copyType='CUSTOMER'):
        bodyLineItems=[]
        accountLines=[]
        if len(roMls)>0:
            for mls in roMls:                 
                code=mls['code']
                mls_desc=mls['desc']
                lbrSaleAcct=mls['lbrSaleAcct']
                cost=mls['cost']                    
                saleCo=mls['saleCo']    
                lbrSaleCtrlNo=mls['lbrSaleCtrlNo'] 
                lbrType=mls['lbrType']                    
                feeId=mls['feeId'] 
                discId=mls['discId']                 
                sale=mls['sale']
                netAmt=sale
                lbrCostAmt=mls['lbrCostAmt']
                lbrSaleAmt=mls['lbrSaleAmt']  

                account={
                    'TRGT':str(saleCo),
                    'ACCOUNT':str(lbrSaleAcct),
                    'SALE':lbrSaleAmt,
                    'COST':lbrCostAmt,
                    'CONTROL':lbrSaleCtrlNo
                }
                accountLines.append(account) 
                    
                n =74-(len(discId)+3) # chunk length
                #chunks = [mls_desc[i:i+n] for i in range(0, len(mls_desc), n)] 
                chunks=self.splitLongLine(line=mls_desc,width=n)   
                self.logger.debug("ROMLS DISCOUNT DESC length chunk="+str(len(chunks)))                      
                index=1
                #handlePageBreakTextWrap
                totalLines=len(chunks)
                self.logger.debug("ROMLS DISCOUNT totalLines="+str(totalLines)+",totalLinePerPage="+str(totalLinePerPage)+",currentLine="+str(currentLine))
                ret_dict=self.handlePageBreakTextWrap(totalLines=totalLines,totalLinePerPage=totalLinePerPage,currentLine=currentLine,bodyTempData=bodyTempData)
                lines=ret_dict['bodyLineItems']
                if len(lines)>0:
                    bodyLineItems.extend(lines) 
                    currentLine= ret_dict['currentLine'] 
                for desc in chunks:
                        
                    if index==1:
                        ccode=discId                
                    else:
                        ccode=''
                        i=0
                        while i<len(discId):
                            ccode=ccode+'&nbsp;'
                            i=i+1
                    bodyLineTempData=self.GetTemplate(bodyTempData,'RO_OPERATION')  
                    bodyLineTempData= bodyLineTempData.replace('--OPCODE--',ccode)
                    bodyLineTempData= bodyLineTempData.replace('--OPCODE_DESC--',desc.upper())
                    bodyLineItems.append(bodyLineTempData)
                    index=index+1
                    currentLine=currentLine+1
                    if currentLine>=totalLinePerPage:
                        currentLine=0

                bodyLineTempData=self.GetTemplate(bodyTempData,'RO_OPERATION_1')                                   
                bodyLineTempData= bodyLineTempData.replace('--SOLD_HRS--','&nbsp;')
                bodyLineTempData= bodyLineTempData.replace('--ACTUAL_HRS--','&nbsp;')
                bodyLineTempData= bodyLineTempData.replace('--COST_AMT--','&nbsp;')
                bodyLineTempData= bodyLineTempData.replace('--SALE_AMT--','&nbsp;')
                bodyLineTempData= bodyLineTempData.replace('--TECH_NO--','&nbsp;')
                

                if len(str(lbrType))==0:
                        bodyLineTempData= bodyLineTempData.replace('--LBR_TYPE--','&nbsp;')
                else:
                        bodyLineTempData= bodyLineTempData.replace('--LBR_TYPE--',lbrType)
            
                if len(str(sale))==0:
                    bodyLineTempData= bodyLineTempData.replace('--NET_AMT--','&nbsp;')
                    bodyLineTempData= bodyLineTempData.replace('--TOTAL_AMT--','&nbsp;')
                else:
                    bodyLineTempData= bodyLineTempData.replace('--NET_AMT--',netAmt)
                    bodyLineTempData= bodyLineTempData.replace('--TOTAL_AMT--',sale)
                    

                bodyLineItems.append(bodyLineTempData)
                currentLine=currentLine+1
                if currentLine>=totalLinePerPage:
                    currentLine=0
             

        return {'bodyLineItems':bodyLineItems,'currentLine':currentLine,'accountLines':accountLines} 
    @classmethod
    def PrepareROPRow(self,bodyTempData,serviceDetails,totalLinePerPage,currentLine):
        bodyLineItems=[]
        pt_line_list=[]
        ptList=[]
        for serviceDetail in serviceDetails:
            lineCode=serviceDetail["lineCode"]
            roPts=serviceDetail['roPts']
            if len(roPts)>0:
                for pts in roPts:
                    techNo=pts["techNo"] 
                    date1=self.FormatPuchDate(pts["date"] )
                    duration=pts["duration"] 
                    starttime=self.FormatTime(pts["startTime"] )
                    finishtime=self.FormatTime(pts["finishTime"] )
                    type1=pts["type"]
                    pt={
                        'lineCode':lineCode,
                        'techNo':techNo,
                        'date':date1,
                        'duration':duration,
                        'startTime':starttime,
                        'finishTime':finishtime,
                        'type':type1
                        }
                    ptList.append(pt)

        if len(ptList)>0:
            #handlePageBreakTextWrap
            totalLines=len(ptList)+2
            self.logger.debug("ROP totalLines="+str(totalLines)+",totalLinePerPage="+str(totalLinePerPage)+",currentLine="+str(currentLine))  
            ret_dict=self.handlePageBreakTextWrap(totalLines=totalLines,totalLinePerPage=totalLinePerPage,currentLine=currentLine,bodyTempData=bodyTempData)
            lines=ret_dict['bodyLineItems']
            if len(lines)>0:
                bodyLineItems.extend(lines) 
                currentLine= ret_dict['currentLine'] 
            index=0
            while(index<len(ptList)):
                pts=ptList[index] 
                
                lineCode=pts.get("lineCode")             
                techNo=pts.get("techNo")  
                date1=self.FormatDate(pts.get("date"))
                duration=pts.get("duration") 
                starttime=self.FormatTime(pts.get("startTime")) 
                finishtime=self.FormatTime(pts.get("finishTime")) 
                type1=pts.get("type")
                index=index+1
                bodyLineTempData=self.GetTemplate(bodyTempData,'ROP_ROW')  
                bodyLineTempData= bodyLineTempData.replace('--ROP_DATE--',str(date1))  
                bodyLineTempData= bodyLineTempData.replace('--ROP_START--',str(starttime))                           
                bodyLineTempData= bodyLineTempData.replace('--ROP_FINISH--',str(finishtime))                       
                bodyLineTempData= bodyLineTempData.replace('--ROP_DURATION--',str(duration))
                bodyLineTempData= bodyLineTempData.replace('--ROP_TYPE--',str(type1))
                bodyLineTempData= bodyLineTempData.replace('--ROP_TECH--',str(techNo))
                bodyLineTempData= bodyLineTempData.replace('--ROP_LINE--',str(lineCode))              
                pt_line_list.append(bodyLineTempData)    
                currentLine=currentLine+1
                if currentLine>=totalLinePerPage:
                    currentLine=0                  
            
            acctTableData="".join(pt_line_list)    
            
            if len(pt_line_list )>0:
                blank_row=self.GetTemplate(bodyTempData,'BLANK_LINE') 
                bodyLineItems.append(blank_row)
                currentLine=currentLine+1
                if currentLine>=totalLinePerPage:
                    currentLine=0
                bodyLineTempData=self.GetTemplate(bodyTempData,'ROP_TBL') 
                bodyLineTempData= bodyLineTempData.replace('--ROP_ROW--','')  
                bodyLineItems.append(bodyLineTempData)
                bodyLineItems.extend(pt_line_list)                
                currentLine=currentLine+1
                if currentLine>=totalLinePerPage:
                    currentLine=0  
                   

        return {'bodyLineItems':bodyLineItems,'currentLine':currentLine} 
    @classmethod
    def PrepareTechStoryRow(self,bodyTempData,serviceDetail,totalLinePerPage,currentLine):
        bodyLineItems=[]
        techStory=serviceDetail['techStory']
        if techStory != None:
            techComment=techStory['techComment']
            techEmpNo=techStory['techEmpNo']
            techEmpName=techStory['techEmpName']
            storyDate=self.FormatDate(techStory['storyDate'])
            storyTime=self.FormatTime(techStory['storyTime'] )
            if len(techComment)>0:
                n =78 # chunk length
                #chunks = [techComment[i:i+n] for i in range(0, len(techComment), n)]
                chunks=self.splitLongLine(line=techComment,width=n)
                #handlePageBreakTextWrap
                totalLines=len(chunks)+1
                self.logger.debug("TECH STORY totalLines="+str(totalLines)+",totalLinePerPage="+str(totalLinePerPage)+",currentLine="+str(currentLine))
                ret_dict=self.handlePageBreakTextWrap(totalLines=totalLines,totalLinePerPage=totalLinePerPage,currentLine=currentLine,bodyTempData=bodyTempData)
                lines=ret_dict['bodyLineItems']
                if len(lines)>0:
                    bodyLineItems.extend(lines) 
                    currentLine= ret_dict['currentLine']  
                for tcomment in chunks:
                    bodyLineTempData=self.GetTemplate(bodyTempData,'TECH_STORY')        
                    bodyLineTempData= bodyLineTempData.replace('--TECH_STORY--',tcomment.upper())
                    bodyLineItems.append(bodyLineTempData)
                    currentLine=currentLine+1
                    if currentLine>=totalLinePerPage:
                        currentLine=0    
        star_row =self.GetTemplate(bodyTempData,'STAR_LINE')                                
        bodyLineItems.append(star_row)
        currentLine=currentLine+1
        if currentLine>=totalLinePerPage:
            currentLine=0 

        return {'bodyLineItems':bodyLineItems,'currentLine':currentLine} 
    @classmethod
    def PrepareEstimateRow(self,bodyTempData,item,totalLinePerPage,currentLine):
        bodyLineItems=[]
        totalEstimate=item['totalEstimate']         
        if self.is_number(totalEstimate) and Decimal(totalEstimate)>0:
            totalLines=5
            ret_dict=self.handlePageBreakTextWrap(totalLines=totalLines,totalLinePerPage=totalLinePerPage,currentLine=currentLine,bodyTempData=bodyTempData)
            lines=ret_dict['bodyLineItems']
            if len(lines)>0:
                bodyLineItems.extend(lines) 
                currentLine= ret_dict['currentLine'] 
            blank_row=self.GetTemplate(bodyTempData,'BLANK_LINE') 
            bodyLineItems.append(blank_row)
            currentLine=currentLine+1
            if currentLine>=totalLinePerPage:
                currentLine=0 
            star_row =self.GetTemplate(bodyTempData,'LONG_STAR_LINE')                                
            bodyLineItems.append(star_row)
            currentLine=currentLine+1
            if currentLine>=totalLinePerPage:
                currentLine=0 
            bodyLineTempData=self.GetTemplate(bodyTempData,'ESTIMATE')        
            bodyLineTempData= bodyLineTempData.replace('--TOTAL_EST--',totalEstimate)
            bodyLineTempData= bodyLineTempData.replace('--OPEN_DATE--',self.FormatDate(item['openDate'])) 
            bodyLineTempData= bodyLineTempData.replace('--OPEN_TIME--',self.FormatTime(item['openTime']))
            bodyLineTempData= bodyLineTempData.replace('--EMP_NO--',item['employee']['id']) 
            bodyLineItems.append(bodyLineTempData)
            currentLine=currentLine+1
            if currentLine>=totalLinePerPage:
                currentLine=0  
            bodyLineTempData =self.GetTemplate(bodyTempData,'CONTACT')
            bodyLineTempData= bodyLineTempData.replace('--CONT_PHONE--',self.phone_format(item['contactphone']))                                
            bodyLineItems.append(bodyLineTempData)
            currentLine=currentLine+1
            if currentLine>=totalLinePerPage:
                currentLine=0   
            star_row =self.GetTemplate(bodyTempData,'LONG_STAR_LINE')                                
            bodyLineItems.append(star_row)
            currentLine=currentLine+1
            if currentLine>=totalLinePerPage:
                currentLine=0 
 
        return {'bodyLineItems':bodyLineItems,'currentLine':currentLine} 
    @classmethod
    def PrepareTotalLineRow(self,bodyTempData,serviceDetail,sum_parts,sum_labor,sum_other,currentLine,totalLinePerPage):
        bodyLineItems=[]
        sum_total=sum_parts+sum_labor+sum_other
        lineCode= serviceDetail['lineCode']           
        bodyLineTempData=self.GetTemplate(bodyTempData,'TOTAL_LINECODE')  
        bodyLineTempData= bodyLineTempData.replace('--LINE_CODE--',str(lineCode))
        if str(sum_parts)=='0' or str(sum_parts)=='0.0':
            bodyLineTempData= bodyLineTempData.replace('--TOTAL_LINE_PARTS--',str('0.00'))
        else:
            bodyLineTempData= bodyLineTempData.replace('--TOTAL_LINE_PARTS--',str(sum_parts))
             
        if str(sum_labor)=='0' or str(sum_labor)=='0.0':
            bodyLineTempData= bodyLineTempData.replace('--TOTAL_LINE_LABOR--',str('0.00'))
        else:
            bodyLineTempData= bodyLineTempData.replace('--TOTAL_LINE_LABOR--',str(sum_labor))
        
        if str(sum_other)=='0' or str(sum_other)=='0.0':
            bodyLineTempData= bodyLineTempData.replace('--TOTAL_LINE_OTHERS--',str('0.00'))
        else:
            bodyLineTempData= bodyLineTempData.replace('--TOTAL_LINE_OTHERS--',str(sum_other))
        
        if str(sum_total)=='0' or str(sum_total)=='0.0':
            bodyLineTempData= bodyLineTempData.replace('--TOTAL_LINE_AMOUNT--',str('0.00'))
        else:
            bodyLineTempData= bodyLineTempData.replace('--TOTAL_LINE_AMOUNT--',str(sum_total))
        
        bodyLineItems.append(bodyLineTempData)
        currentLine=currentLine+1
        if currentLine>=totalLinePerPage:
            currentLine=0 
        blank_row=self.GetTemplate(bodyTempData,'BLANK_LINE') 
        bodyLineItems.append(blank_row)
        currentLine=currentLine+1
        if currentLine>=totalLinePerPage:
            currentLine=0 
        return {'bodyLineItems':bodyLineItems,'currentLine':currentLine} 
    @classmethod
    def PrepareTotalAccountingLineRow(self,bodyTempData,totalCost,totalSale,total_comp,currentLine,totalLinePerPage):
        bodyLineItems=[]
        bodyLineTempData=self.GetTemplate(bodyTempData,'COMP_COST_SALE')  
        bodyLineTempData= bodyLineTempData.replace('--COST--',str(totalCost))
        bodyLineTempData= bodyLineTempData.replace('--SALE--',str(totalSale))
        bodyLineTempData= bodyLineTempData.replace('--COMP_TOTALS--',str(total_comp))        
        bodyLineItems.append(bodyLineTempData)
        currentLine=currentLine+1
        if currentLine>=totalLinePerPage:
            currentLine=0 
        blank_row=self.GetTemplate(bodyTempData,'BLANK_LINE') 
        bodyLineItems.append(blank_row)
        currentLine=currentLine+1
        if currentLine>=totalLinePerPage:
           currentLine=0 
        return {'bodyLineItems':bodyLineItems,'currentLine':currentLine} 
    @classmethod
    def PrepareAccountingRow(self,bodyTempData,accountLines,totalLinePerPage,currentLine):
        bodyLineItems=[]
        acctTableData=''
        totalCost=0
        totalSale=0
        self.logger.debug("account table len="+str(len(accountLines)))
        if len(accountLines)>0:
            n=2
            acct_batches = self.chunks(accountLines,n)   
            #handlePageBreakTextWrap
            acct_batches_list=list(acct_batches)
            
            totalLines=len(acct_batches_list)+2
            if totalLines>1:
                self.logger.debug("ACCOUNTING totalLines="+str(totalLines)+",totalLinePerPage="+str(totalLinePerPage)+",currentLine="+str(currentLine))
                ret_dict=self.handlePageBreakTextWrap(totalLines=totalLines,totalLinePerPage=totalLinePerPage,currentLine=currentLine,bodyTempData=bodyTempData)
                lines=ret_dict['bodyLineItems']
                if len(lines)>0:
                    bodyLineItems.extend(lines) 
                    currentLine= ret_dict['currentLine']  

                acc_line_list =[]                
                for acctList in acct_batches_list:
                    self.logger.debug("acctList="+str(acctList))
                    TRGT=''
                    ACCOUNT=''
                    COST=''
                    SALE=''
                    CONTROL=''
                    TRGT1=''
                    ACCOUNT1=''
                    COST1=''
                    SALE1=''
                    CONTROL1=''
                    TRGT_ACCOUNT=''
                    TRGT_ACCOUNT1=''
                    if len(acctList)>0:  
                        acct_line=acctList[0]                      
                        TRGT=acct_line['TRGT']
                        ACCOUNT=acct_line['ACCOUNT']
                        TRGT_ACCOUNT=TRGT+"/"+ACCOUNT
                        COST=acct_line['COST']                        
                        SALE=acct_line['SALE']
                        if  self.is_number(COST) :
                            totalCost=totalCost+Decimal(COST)
                        if  self.is_number(SALE) :
                            totalSale=totalSale+Decimal(SALE)
                        CONTROL=acct_line['CONTROL']
                        if len(acctList)>1:
                            acct_line=acctList[1]
                            TRGT1=acct_line['TRGT']
                            ACCOUNT1=acct_line['ACCOUNT']
                            TRGT_ACCOUNT1=TRGT1+"/"+ACCOUNT1
                            COST1=acct_line['COST']
                            SALE1=acct_line['SALE']
                            if  self.is_number(COST1) :
                                totalCost=totalCost+Decimal(COST1)
                            if  self.is_number(SALE1) :
                                totalSale=totalSale+Decimal(SALE1)
                            CONTROL1=acct_line['CONTROL']                        
                    bodyLineTempData=self.GetTemplate(bodyTempData,'ACCT_ROW')  
                    bodyLineTempData= bodyLineTempData.replace('--TRGT_ACCOUNT--',TRGT_ACCOUNT)  
                    bodyLineTempData= bodyLineTempData.replace('--TRGT--',TRGT)                           
                    bodyLineTempData= bodyLineTempData.replace('--ACCOUNT--',ACCOUNT)                       
                    bodyLineTempData= bodyLineTempData.replace('--COST--',COST)
                    bodyLineTempData= bodyLineTempData.replace('--SALE--',SALE)
                    bodyLineTempData= bodyLineTempData.replace('--CONTROL--',CONTROL)
                    bodyLineTempData= bodyLineTempData.replace('--TRGT_ACCOUNT1--',TRGT_ACCOUNT1)  
                    bodyLineTempData= bodyLineTempData.replace('--TRGT1--',TRGT1)                           
                    bodyLineTempData= bodyLineTempData.replace('--ACCOUNT1--',ACCOUNT1)                       
                    bodyLineTempData= bodyLineTempData.replace('--COST1--',COST1)
                    bodyLineTempData= bodyLineTempData.replace('--SALE1--',SALE1)
                    bodyLineTempData= bodyLineTempData.replace('--CONTROL1--',CONTROL1)
                    acc_line_list.append(bodyLineTempData)
                    currentLine=currentLine+1
                    if currentLine>=totalLinePerPage:
                        currentLine=0
                                     
                
                acctTableData="".join(acc_line_list)    
            
                if len(acc_line_list )>0:
                    blank_row=self.GetTemplate(bodyTempData,'BLANK_LINE') 
                    bodyLineItems.append(blank_row)
                    currentLine=currentLine+1
                    if currentLine>=totalLinePerPage:
                        currentLine=0    
                    bodyLineTempData=self.GetTemplate(bodyTempData,'ACCT_TBL') 
                    bodyLineTempData= bodyLineTempData.replace('--ACCOUNT_LINE--','')  
                    bodyLineItems.append(bodyLineTempData)
                    bodyLineItems.extend(acc_line_list)    
                    currentLine=currentLine+1
                    if currentLine>=totalLinePerPage:
                        currentLine=0 
                    

        return {'bodyLineItems':bodyLineItems,'currentLine':currentLine,'totalCost':totalCost,'totalSale':totalSale} 
    
    @classmethod
    def GetInnerPageFooter(self,pageData,pageNumber,pageFooterTemplate):
        pageTemplate=pageFooterTemplate
        pageTemplate=pageTemplate.replace('--LAST_PAGE_MSG--','')    
        pageTemplate=pageTemplate.replace('--BODY--',pageData)   
        pageTemplate= pageTemplate.replace('--PAGE_NO--',str(pageNumber))              
        pageTemplate= pageTemplate.replace('--TOTAL_LABOR--',' ')
        pageTemplate= pageTemplate.replace('--TOTAL_PARTS--','')
        pageTemplate= pageTemplate.replace('--TOTAL_GOG--','')
        pageTemplate= pageTemplate.replace('--TOTAL_SUBL--','')
        pageTemplate= pageTemplate.replace('--TOTAL_MISC--','')
        pageTemplate= pageTemplate.replace('--TOTAL_SCHG--','')
        pageTemplate= pageTemplate.replace('--TOTAL_ENV_DISP--','')
        pageTemplate= pageTemplate.replace('--TOTAL_CHARGES--','')             
    
        pageTemplate= pageTemplate.replace('--TOTAL_DEDUCT--','')
        pageTemplate= pageTemplate.replace('--TOTAL_SALES_TAX--','')
        pageTemplate= pageTemplate.replace('--TOTAL_AMOUNT--','')
        pageTemplate= pageTemplate.replace('--TOTAL_WAR_AMOUNT--','')
        if pageNumber==1:
            style="" 
            style="style='page-break-after:always;'"                          
        else:
            style="style='page-break-after:always;'"
            #style="" 
        pageTemplate= pageTemplate.replace('--STYLE--',style)  
        return   pageTemplate
    @classmethod
    def GetLastPageFooter(self,pageData,pageNumber,pageFooterTemplate,item,lastPageMessage):
        pageTemplate=pageFooterTemplate
       
        pageTemplate=pageTemplate.replace('--LAST_PAGE_MSG--',lastPageMessage)
                       
        pageTemplate=pageTemplate.replace('--BODY--',pageData)                     
        pageTemplate= pageTemplate.replace('--PAGE_NO--',str(pageNumber))  
        lessInsurance=Decimal(self.ToAmount(item['miscTotal']))+Decimal(self.ToAmount(item['deductibleTotal']))
        totalCharges=Decimal(self.ToAmount(item['laborTotal']))+Decimal(self.ToAmount(item['partsTotal']))+Decimal(self.ToAmount(item['golTotal']))+Decimal(self.ToAmount(item['sublTotal']))+Decimal(self.ToAmount(item['schgTotal']))
                            
        pageTemplate= pageTemplate.replace('--TOTAL_LABOR--',self.ToAmount(item['laborTotal']))
        pageTemplate= pageTemplate.replace('--TOTAL_PARTS--',self.ToAmount(item['partsTotal']))
        pageTemplate= pageTemplate.replace('--TOTAL_GOG--',self.ToAmount(item['golTotal']))
        pageTemplate= pageTemplate.replace('--TOTAL_SUBL--',self.ToAmount(item['sublTotal']))
        pageTemplate= pageTemplate.replace('--TOTAL_ENV_DISP--',self.ToAmount(item['schgTotal']))
        pageTemplate= pageTemplate.replace('--TOTAL_CHARGES--',self.ToAmount(str(totalCharges)))              
        pageTemplate= pageTemplate.replace('--TOTAL_DEDUCT--',self.ToAmount(str(lessInsurance)))
        pageTemplate= pageTemplate.replace('--TOTAL_SALES_TAX--',self.ToAmount(item['salesTaxTotal']))
        pageTemplate= pageTemplate.replace('--TOTAL_AMOUNT--',self.ToAmount(item['amountDue']))
        pageTemplate= pageTemplate.replace('--TOTAL_WAR_AMOUNT--',self.ToAmount(item['warrantyDue']))
        pageTemplate= pageTemplate.replace('--STYLE--','') 
        return   pageTemplate
    @classmethod
    def splitLongLine(self,line,width):
        lines=[]
        lines=  textwrap.TextWrapper(width=width,break_long_words=False).wrap(line)       
        return lines
    @classmethod
    def GenrateHTMLInvoice(self,invoiceHtml,item,bodyTempData,isPageBreak=True,copyType='CUSTOMER'):

        pageFinalData=""
        try:            
                  
            blank_row= self.GetTemplate(bodyTempData,'BLANK_LINE')  
            lastPageMessage=self.GetTemplate(bodyTempData,'LAST_PAGE_MSG')  
            htmlTempData=self.GetTemplate(bodyTempData,'PAGE_TEMP') 
            
            htmlTempData=self.PrepareHeader(item=item,template=htmlTempData)             
            pageTemplate_Orginal=htmlTempData
            bodyLineItems=[]
            serviceDetails=item['serviceDetails']  
            roOperations=item['roOperations'] 
          
            #roParts=item['roParts']
            accountLines=[]
            currentLine=0
            totalLinePerPage=33
            for serviceDetail in serviceDetails:
                sum_parts=Decimal(0.00)
                sum_labor=Decimal(0.00)
                sum_other=Decimal(0.00) 

                #LINE CODE AND LINE DESCRIPTION
                ret_dict=self.PrepareLineCodeRow(bodyTempData=bodyTempData,serviceDetail=serviceDetail,totalLinePerPage=totalLinePerPage,currentLine=currentLine)
                lines=ret_dict['bodyLineItems']
                if len(lines)>0:
                    bodyLineItems.extend(lines) 
                    currentLine= ret_dict['currentLine']
                
                #CAUSES
                ret_dict=self.PrepareCausesRow(bodyTempData=bodyTempData,serviceDetail=serviceDetail,totalLinePerPage=totalLinePerPage,currentLine=currentLine)
                lines=ret_dict['bodyLineItems']
                if len(lines)>0:
                    bodyLineItems.extend(lines) 
                    currentLine= ret_dict['currentLine']
                 
                #OPCODE 
                ret_dict=self.PrepareOpcodeRow(bodyTempData=bodyTempData,serviceDetail=serviceDetail,totalLinePerPage=totalLinePerPage,currentLine=currentLine,copyType=copyType)
                lines=ret_dict['bodyLineItems']
                if len(lines)>0:
                    bodyLineItems.extend(lines) 
                    currentLine= ret_dict['currentLine']
                    sum_parts=ret_dict['sum_parts']  
                    sum_labor=ret_dict['sum_labor'] 
                    sum_other=ret_dict['sum_other']  
                    acctlines=ret_dict['accountLines']
                    if len(acctlines)>0:  
                        accountLines.extend(acctlines)    

                 

                #LINE END TOTAL -LINE-FOOTER    
                ret_dict=self.PrepareTotalLineRow(bodyTempData=bodyTempData,serviceDetail=serviceDetail,sum_labor=sum_labor,sum_other=sum_other,sum_parts=sum_parts,currentLine=currentLine,totalLinePerPage=totalLinePerPage)
                lines=ret_dict['bodyLineItems']
                if len(lines)>0:
                    bodyLineItems.extend(lines) 
                    currentLine= ret_dict['currentLine']

                #TECH STORY
                ret_dict=self.PrepareTechStoryRow(bodyTempData=bodyTempData,serviceDetail=serviceDetail,totalLinePerPage=totalLinePerPage,currentLine=currentLine)
                lines=ret_dict['bodyLineItems']
                if len(lines)>0:
                    bodyLineItems.extend(lines) 
                    currentLine= ret_dict['currentLine']
            #END LINE SERVICE LOOP

            #discount line
            ret_dict=self.PrepareROOperationsRow(bodyTempData=bodyTempData,roMls=roOperations,totalLinePerPage=totalLinePerPage,currentLine=currentLine,copyType=copyType)
            lines=ret_dict['bodyLineItems']
            if len(lines)>0:
                bodyLineItems.extend(lines) 
                currentLine= ret_dict['currentLine']                    
                acctlines=ret_dict['accountLines']
                if len(acctlines)>0:  
                    accountLines.extend(acctlines)    
            #estimate
            ret_dict=self.PrepareEstimateRow(bodyTempData,item,totalLinePerPage,currentLine) 
            lines=ret_dict['bodyLineItems']
            if len(lines)>0:
                bodyLineItems.extend(lines) 
                currentLine= ret_dict['currentLine']    

            if copyType == 'ACCOUNTING':

                ret_dict=self.PrepareROPRow(bodyTempData=bodyTempData,serviceDetails=serviceDetails,totalLinePerPage=totalLinePerPage,currentLine=currentLine)
                lines=ret_dict['bodyLineItems']
                if len(lines)>0:
                    bodyLineItems.extend(lines) 
                    currentLine= ret_dict['currentLine']

                totalSale=0
                totalCost=0    
                ret_dict=self.PrepareAccountingRow(bodyTempData=bodyTempData,accountLines=accountLines,totalLinePerPage=totalLinePerPage,currentLine=currentLine)
                lines=ret_dict['bodyLineItems']
                if len(lines)>0:
                    bodyLineItems.extend(lines) 
                    currentLine= ret_dict['currentLine']
                    totalSale=ret_dict['totalSale']
                    totalCost=ret_dict['totalCost']    
                    ret_dict=self.PrepareTotalAccountingLineRow(bodyTempData=bodyTempData,total_comp=0,totalCost=totalCost,totalSale=totalSale,totalLinePerPage=totalLinePerPage,currentLine=currentLine)
                    lines=ret_dict['bodyLineItems']
                    if len(lines)>0:
                        bodyLineItems.extend(lines) 
                        currentLine= ret_dict['currentLine']
               
                

            self.logger.debug("count bodyLineItems="+str(len(bodyLineItems)))
            pagesGen = self.chunks(bodyLineItems,totalLinePerPage) 
            pages=list(pagesGen) 
            self.logger.debug("count pages="+str(len(pages)))
            lastPage=len(pages)-1
            pageList=[]
           
            if isPageBreak:

                if(len(pages)>1):
                    
                    pageList=[]
                    pageNumber=0
                    for index in range(len(pages)-1):
                        page=pages[index]
                        pageNumber=pageNumber+1
                        self.logger.debug("before count lines pages="+str(len(page)))
                        if len(page)<totalLinePerPage:
                          while len(page)<totalLinePerPage:
                               page.append(blank_row)                                
                        self.logger.debug("After count lines pages="+str(len(page)))    
                        pageTemplate=pageTemplate_Orginal
                        pageData="".join(page)
                        pageTemplate=self.GetInnerPageFooter(pageNumber=pageNumber,pageData=pageData,pageFooterTemplate=pageTemplate_Orginal)
                        pageList.append(pageTemplate)
                        
                    pageTemplate=pageTemplate_Orginal 
                    page=pages[lastPage]
                    pageNumber=lastPage+1
                    self.logger.debug("last page before count lines pages="+str(len(page)))
                    totalLineInLastPage=len(page)
                    lineRequiredForLastMessage=4
                    totalLinePerPageLastPage=30
                    lineRemaining=totalLinePerPageLastPage-len(page)

                    isLastPage=False
                    if lineRemaining>=lineRequiredForLastMessage:
                       totalLineInLastPage= totalLineInLastPage+4
                       isLastPage=True

                    if isLastPage:
                        if totalLineInLastPage<totalLinePerPageLastPage:
                            while totalLineInLastPage<totalLinePerPage:
                                page.append(blank_row)
                                totalLineInLastPage=totalLineInLastPage+1 
                    else:
                        if totalLineInLastPage<totalLinePerPage:
                            while totalLineInLastPage<totalLinePerPage:
                                page.append(blank_row)
                                totalLineInLastPage=totalLineInLastPage+1 

                    self.logger.debug("isLastPage="+str(isLastPage))  
                    self.logger.debug("totalLineInLastPage="+str(totalLineInLastPage))
                    self.logger.debug("last page After count lines pages="+str(len(page)))
                   

                    if isLastPage:
                        pageData="".join(page)
                        pageTemplate=self.GetLastPageFooter(pageNumber=pageNumber,pageData=pageData,pageFooterTemplate=pageTemplate_Orginal,item=item,lastPageMessage=lastPageMessage)
                        pageList.append(pageTemplate)
                    else:
                        pageData="".join(page)
                        pageTemplate=self.GetInnerPageFooter(pageNumber=pageNumber,pageData=pageData,pageFooterTemplate=pageTemplate_Orginal)
                        pageList.append(pageTemplate)

                        pageTemplate=pageTemplate_Orginal
                        pageNumber=pageNumber+1
                        page=[]
                        totalLineInLastPage=0
                        while totalLineInLastPage<totalLinePerPage-4:
                            page.append(blank_row)
                            totalLineInLastPage=totalLineInLastPage+1 
                        self.logger.debug("Last Page Msg>> totalLineInLastPage="+str(totalLineInLastPage))
                        self.logger.debug("Last Page Msg>> last page After count lines pages="+str(len(page)))
                        #page.append(lastPageMessage)
                        pageData="".join(page)
                        pageTemplate=self.GetLastPageFooter(pageNumber=pageNumber,pageData=pageData,pageFooterTemplate=pageTemplate_Orginal,item=item,lastPageMessage=lastPageMessage)
                        pageList.append(pageTemplate)

                else:
                    pageList=[]
                    pageTemplate=pageTemplate_Orginal
                    page=pages[0]
                    pageNumber=1
                    self.logger.debug("page len="+str(len(page)))  
                    totalLineInLastPage=len(page)
                    lineRequiredForLastMessage=4
                    totalLinePerPageLastPage=30
                    lineRemaining=totalLinePerPage-len(page)
                    self.logger.debug("lineRemaining="+str(lineRemaining)) 
                    isLastPage=False
                    if lineRemaining>=lineRequiredForLastMessage:
                       totalLineInLastPage= totalLineInLastPage+4
                       isLastPage=True
                    
                    if isLastPage:
                        if totalLineInLastPage<totalLinePerPageLastPage:
                            while totalLineInLastPage<totalLinePerPage:
                                page.append(blank_row)
                                totalLineInLastPage=totalLineInLastPage+1 
                    else:
                        if totalLineInLastPage<totalLinePerPage:
                            while totalLineInLastPage<totalLinePerPage:
                                page.append(blank_row)
                                totalLineInLastPage=totalLineInLastPage+1 

                    self.logger.debug("isLastPage="+str(isLastPage))  
                    self.logger.debug("totalLineInLastPage="+str(totalLineInLastPage))
                    self.logger.debug("last page After count lines pages="+str(len(page)))
                    
                    if isLastPage:
                        #page.append(lastPageMessage)
                        pageData="".join(page)
                        pageTemplate=self.GetLastPageFooter(pageNumber=pageNumber,pageData=pageData,pageFooterTemplate=pageTemplate_Orginal,item=item,lastPageMessage=lastPageMessage)
                        pageList.append(pageTemplate)
 
                    else:
                        pageData="".join(page)
                        pageTemplate=self.GetInnerPageFooter(pageNumber=pageNumber,pageData=pageData,pageFooterTemplate=pageTemplate_Orginal)
                        pageList.append(pageTemplate)                        

                        pageNumber=pageNumber+1
                        pageTemplate=pageTemplate_Orginal
                        page=[]
                        totalLineInLastPage=0
                        while totalLineInLastPage<totalLinePerPage-4:
                            page.append(blank_row)
                            totalLineInLastPage=totalLineInLastPage+1 
                        self.logger.debug("Last Page Msg>> totalLineInLastPage="+str(totalLineInLastPage))
                        self.logger.debug("Last Page Msg>> last page After count lines pages="+str(len(page)))
                        
                        pageData="".join(page)
                        pageTemplate=self.GetLastPageFooter(pageNumber=pageNumber,pageData=pageData,pageFooterTemplate=pageTemplate_Orginal,item=item,lastPageMessage=lastPageMessage)
                        pageList.append(pageTemplate)
 

            else:
                pageTemplate=pageTemplate_Orginal
                page=bodyLineItems
                pageNumber=1
                if len(page)<totalLinePerPage:
                        while len(page)<totalLinePerPage:
                            page.append(blank_row)   

                pageData="".join(page)
                pageTemplate=self.GetLastPageFooter(pageNumber=pageNumber,pageData=pageData,pageFooterTemplate=pageTemplate_Orginal,item=item,lastPageMessage=lastPageMessage)
                pageList.append(pageTemplate)            
                       
            page_str="".join(pageList) 
            pageFinalData=invoiceHtml.replace('--PAGE--',page_str)           

        except Exception as e2:
                self.logger.error("error in GenrateHTMLInvoice",e2)
        return pageFinalData
    @classmethod
    def ToAmount(self,strAmt):
        if strAmt=='':
            return "0.00" 
        return strAmt
    @classmethod
    def getInvoiceHTMLFile(self,event,item,optNo='CUSTOMER',isPageBreak=True):
            self.logger.debug("getInvoiceHTMLFile>> invoiceItemJSON="+str(item))  
            s3=S3Manager(Invoice.region) 
            bucket=event['BUCKET']
            invoiceHtmlTemplate=event['INVOICE_TEMPLATE_HTML']
            invoiceHtmlBodyTemplate=event['INVOICE_TEMPLATE_HTML_BODY']

            acct_invoiceHtmlTemplate=event['INVOICE_TEMPLATE_HTML_ACCT']
            acct_invoiceHtmlBodyTemplate=event['INVOICE_TEMPLATE_HTML_BODY_ACCT']

            war_invoiceHtmlTemplate=event['INVOICE_TEMPLATE_HTML_WAR']
            war_invoiceHtmlBodyTemplate=event['INVOICE_TEMPLATE_HTML_BODY_WAR']

            self.logger.debug("getInvoiceHTMLFile>> invoiceHtmlTemplate="+str(invoiceHtmlTemplate))  
            htmlList=[]
            if optNo == 'CUSTOMER' or optNo == 'ALL' :
               htmlTempData=s3.read_s3_file(bucket,invoiceHtmlTemplate)  
               bodyTempData=s3.read_s3_file(bucket,invoiceHtmlBodyTemplate) 
               htmlTempData= self.GenrateHTMLInvoice(htmlTempData,item,bodyTempData,isPageBreak,optNo) 
               htmlList.append(htmlTempData)    
            if optNo == 'ACCOUNTING' or optNo == 'ALL' :
               htmlTempData=s3.read_s3_file(bucket,acct_invoiceHtmlTemplate)  
               bodyTempData=s3.read_s3_file(bucket,acct_invoiceHtmlBodyTemplate) 
               htmlTempData= self.GenrateHTMLInvoice(htmlTempData,item,bodyTempData,isPageBreak,optNo) 
               htmlList.append(htmlTempData)    
            if optNo == 'WARRANTY' or optNo == 'ALL' :
               htmlTempData=s3.read_s3_file(bucket,war_invoiceHtmlTemplate)  
               bodyTempData=s3.read_s3_file(bucket,war_invoiceHtmlBodyTemplate) 
               htmlTempData= self.GenrateHTMLInvoice(htmlTempData,item,bodyTempData,isPageBreak,optNo) 
               htmlList.append(htmlTempData)   

            htmlTempData="".join(htmlList)                    
            return htmlTempData
    def FormatDate(dt_str):
        try:                    
            if dt_str is not None and  len(str(dt_str).strip())>0:
                dt_str=str(dt_str).strip()               
                dt_date = datetime.strptime(dt_str, "%m/%d/%Y") 
                dt_date_str=dt_date.strftime("%d%b%y")
                return dt_date_str               
            else:
                return ""
        except Exception as e:
            return ""
    def FormatPuchDate(dt_str):
        try:                    
            if dt_str is not None and  len(str(dt_str).strip())>0:
                dt_str=str(dt_str).strip()               
                dt_date = datetime.strptime(dt_str, "%m/%d/%Y") 
                dt_date_str=dt_date.strftime("%m-%d-%y")
                return dt_date_str               
            else:
                return ""
        except Exception as e:
            return ""
    def FormatTime(print_time_seconds_str):
        try:                    
            if print_time_seconds_str is not None and  len(str(print_time_seconds_str).strip())>0:
                    print_time_seconds_str=str(print_time_seconds_str).strip() 
                    f_dt = datetime.strptime(print_time_seconds_str,"%I:%M %p" )
                    f_dt=f_dt.strftime("%H:%M")
                    return f_dt
            else:
                return ""
        except Exception as e:
            return ""
    @classmethod
    def getInvoicePDFFile(self,event,item,optNo='CUSTOMER'):
            self.logger.debug("getInvoicePDFFile>> invoiceItemJSON="+str(item)+",optNo="+str(optNo))              
            invoiceNumber=item.get('document_id')          
            htmlTempData= self.getInvoiceHTMLFile(event,item,optNo,True)
            timestamp = str(datetime.now()).replace('.', '').replace(' ', '_')
            local_filename = f'/tmp/INVOICE_{invoiceNumber}_{timestamp}.html'
            # Delete any existing files with that name
            try:
                os.unlink(local_filename)
            except FileNotFoundError:
                pass
            with open(local_filename, 'w') as f:
                f.write(htmlTempData)
            # Now we can check for the option wkhtmltopdf_options and map them to values
            # Again, part of our assumptions are that these are valid
            wkhtmltopdf_options = {}
            #wkhtmltopdf_options['margin-top'] = '10.16mm'
            #wkhtmltopdf_options['margin-right'] = '9.906mm'
            #wkhtmltopdf_options['margin-bottom'] = '9.906mm'
            #wkhtmltopdf_options['margin-left'] = '10.16mm'
            #wkhtmltopdf_options['orientation'] = 'portrait'
            if 'wkhtmltopdf_options' in event:
                # Margin is <top> <right> <bottom> <left>
                if 'margin' in event['wkhtmltopdf_options']:
                    margins = event['wkhtmltopdf_options']['margin'].split(' ')
                    if len(margins) == 4:
                        wkhtmltopdf_options['margin-top'] = margins[0]
                        wkhtmltopdf_options['margin-right'] = margins[1]
                        wkhtmltopdf_options['margin-bottom'] = margins[2]
                        wkhtmltopdf_options['margin-left'] = margins[3]

                if 'orientation' in event['wkhtmltopdf_options']:
                    wkhtmltopdf_options['orientation'] = 'portrait' \
                        if event['wkhtmltopdf_options']['orientation'].lower() not in ['portrait', 'landscape'] \
                        else event['wkhtmltopdf_options']['orientation'].lower()

                if 'title' in event['wkhtmltopdf_options']:
                    wkhtmltopdf_options['title'] = event['wkhtmltopdf_options']['title']
            # Now we can create our command string to execute and upload the result to s3
            command = 'wkhtmltopdf  --load-error-handling ignore'  # ignore unecessary errors
            for key, value in wkhtmltopdf_options.items():
                if key == 'title':
                    value = f'"{value}"'
                command += ' --{0} {1}'.format(key, value)
            command += ' {0} {1}'.format(local_filename, local_filename.replace('.html', '.pdf'))

            # Important! Remember, we said that we are assuming we're accepting valid HTML
            # this should always be checked to avoid allowing any string to be executed
            # from this command. The reason we use shell=True here is because our title
            # can be multiple words.
            subprocess.run(command, shell=True)
            self.logger.info('Successfully generated the PDF.')
            pdfFile=local_filename.replace('.html', '.pdf')
            #file_key = pdfFile.replace('/tmp/', '')
            #s31 = boto3.resource('s3',region_name=S3Manager.region)
            #s31 = boto3.client('s3')
            #s31.upload_file(Filename=pdfFile, Bucket=bucket, Key=file_key)
            #file_key = s3.uploadFile(bucket, pdfFile,)
            myArr=None
          
            with open(pdfFile, "rb") as binaryfile :
                 myArr = binaryfile.read()
             # Delete any existing files with that name
            self.logger.info('after laoding generated the PDF file.')
            try:
                os.unlink(pdfFile)
            except FileNotFoundError:
                pass
            try:
                os.unlink(local_filename)
            except FileNotFoundError:
                pass
            return myArr
    @classmethod
    def parallel_scan_table(self,dynamo_client, *, TableName, **kwargs):
        """
        Generates all the items in a DynamoDB table.

        :param dynamo_client: A boto3 client for DynamoDB.
        :param TableName: The name of the table to scan.

        Other keyword arguments will be passed directly to the Scan operation.
        See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.scan

        This does a Parallel Scan operation over the table.

        """
        # How many segments to divide the table into?  As long as this is >= to the
        # number of threads used by the ThreadPoolExecutor, the exact number doesn't
        # seem to matter.
        total_segments = 100

        # How many scans to run in parallel?  If you set this really high you could
        # overwhelm the table read capacity, but otherwise I don't change this much.
        max_scans_in_parallel = 5

        # Schedule an initial scan for each segment of the table.  We read each
        # segment in a separate thread, then look to see if there are more rows to
        # read -- and if so, we schedule another scan.
        tasks_to_do = [
            {
                **kwargs,
                "TableName": TableName,
                "Segment": segment,
                "TotalSegments": total_segments,
            }
            for segment in range(total_segments)
        ]

        # Make the list an iterator, so the same tasks don't get run repeatedly.
        scans_to_run = iter(tasks_to_do)

        with concurrent.futures.ThreadPoolExecutor() as executor:

            # Schedule the initial batch of futures.  Here we assume that
            # max_scans_in_parallel < total_segments, so there's no risk that
            # the queue will throw an Empty exception.
            futures = {
                executor.submit(dynamo_client.scan, **scan_params): scan_params
                for scan_params in itertools.islice(scans_to_run, max_scans_in_parallel)
            }

            while futures:
                # Wait for the first future to complete.
                done, _ = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )

                for fut in done:
                    yield from fut.result()["Items"]

                    scan_params = futures.pop(fut)

                    # A Scan reads up to N items, and tells you where it got to in
                    # the LastEvaluatedKey.  You pass this key to the next Scan operation,
                    # and it continues where it left off.
                    try:
                        scan_params["ExclusiveStartKey"] = fut.result()["LastEvaluatedKey"]
                    except KeyError:
                        break
                    tasks_to_do.append(scan_params)

                # Schedule the next batch of futures.  At some point we might run out
                # of entries in the queue if we've finished scanning the table, so
                # we need to spot that and not throw.
                for scan_params in itertools.islice(scans_to_run, len(done)):
                    futures[executor.submit(dynamo_client.scan, **scan_params)] = scan_params
    
     