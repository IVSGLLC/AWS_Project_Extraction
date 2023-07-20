import itertools
import json
from operator import itemgetter
import os
import subprocess
import textwrap
 
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
class PartsInvoice(object):
    logger=LogManger()   
    err_handler=ErrorHandler()
    region='us-east-1'
    table_Per_Store_Parts=1
    def __init__(self):        
        PartsInvoice.region="us-east-1"
        PartsInvoice.table_Per_Store_Parts=1
        
    def __init__(self,region,table_Per_Store_Parts=1):  
        PartsInvoice.region=region
        PartsInvoice.table_Per_Store_Invoice=table_Per_Store_Parts
    
    @classmethod
    def chunks(self,lst, n):       
        for i in range(0, len(lst), n):
            yield lst[i:i + n]  

    @classmethod   
    def getTableName(self,store_code):
        TableName=store_code+"_PARTS_FILE"          
        return TableName

    @classmethod
    def GetPartsInvoiceDetail(self,store_code,document_id,document_type):
        _moduleNM="PartsInvoice"
        _functionNM="GetPartsInvoiceDetail"
        try:
            self.logger.debug("GetPartsInvoiceDetail>>store_code="+str(store_code)+",document_id="+str(document_id)+",document_type="+str(document_type))
            dynamodb = boto3.resource('dynamodb', region_name=PartsInvoice.region)
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
    def GetPartsInvoiceList(self,store_code,document_type,last_key=None,page_size=-1):
        _moduleNM="PartsInvoice"
        _functionNM="PartsInvoiceList"
        try:           
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)       
            self.logger.debug("PartsInvoice>> store_code:"+str(store_code)+",document_type"+str(document_type))
           
            dynamodb = boto3.resource('dynamodb', region_name=PartsInvoice.region)
            TableName=self.getTableName(store_code)
            table = dynamodb.Table(TableName)       
            fetchAll=False
            LastEvaluatedKey=None
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
                     ConsistentRead=False,Limit=page_size)
                   if 'LastEvaluatedKey' in response:
                       LastEvaluatedKey=response['LastEvaluatedKey']  
                else:
                    response = table.query(
                     KeyConditionExpression= Key('document_type').eq(document_type),
                     ConsistentRead=False)
                    if 'LastEvaluatedKey' in response:
                       LastEvaluatedKey=response['LastEvaluatedKey']   
                    fetchAll=True
            #try:
             #   self.logger.debug("Invoice>> start parallel_scan_table")
              #  dynamo_client = boto3.resource("dynamodb").meta.client
               # items=[]
                #for item in self.parallel_scan_table(dynamo_client, TableName=TableName):
                 #       items.append(item)
                        #self.logger.debug(item)  
                #self.logger.debug("Invoice>> end parallel_scan_table")   
                #return { "status":True,"items": items }
            #except :
             #    pass   
             
                   
            
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
                return { "status":True,"items": items, "LastEvaluatedKey": LastEvaluatedKey}
            except:
                return { "status":True,"items": [],"LastEvaluatedKey": LastEvaluatedKey }                
          
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 

    @classmethod
    def preparePartsInvoiceList(self,items):
        _moduleNM="Invoice"
        _functionNM="preparePartsInvoiceList"
        try:           
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)  
            documentList=[]
            for item in items :                 
                               
                document={
                            "documentId": item.get('document_id'),
                            "documentType": item.get('saleType'),                          
                            "orderNo": item.get('orderNo') ,
                            "openDate": item.get('openDate') , 
                            "shippedDate": item.get('shippedDate') , 
                            "shipVia":item.get('shipVia'), 
                            "slsm":item.get('slsm'),
                            "blNumber":item.get('blNumber'),
                            "term":item.get('term'),				
                            "fobPoint":item.get('fobPoint'),
                            "soldTo":item.get('soldTo'),
                            "shipTo": item.get('shipTo') ,                          
                            "contactPhone":item.get('contactPhone'),
                            "partsList":item.get('partsList'),
                            "partsTotal":item.get('partsTotal'),
                            "subletTotal":item.get('subletTotal'),
                            "freightTotal":item.get('freightTotal'),
                            "saleTaxTotal":item.get('saleTaxTotal'),
                            "totalAmount":item.get('totalAmount'),
                            "advisor":item.get('slsm'),
                            "status":item.get('status'),
                            "closeDate":item.get('closeDate'),
                            "ppFlag":item.get('ppFlag')
                        }
                        
                documentList.append(document)
            documentList=sorted(documentList, key=itemgetter('documentId'))   
            return { "status":True,"partsInvoiceList": documentList }
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)  
    
    @classmethod
    def preparePartsInvoice(self,item,auth_json):
        _moduleNM="PartsInvoice"
        _functionNM="preparePartsInvoice"
        try:           
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)  
            docuemnt= {
                            "responseCode":0,
                            "documentId": item.get('document_id'),
                            "documentType": item.get('saleType'),                          
                            "orderNo": item.get('orderNo') ,
                            "openDate": item.get('openDate') , 
                            "shippedDate": item.get('shippedDate') , 
                            "shipVia":item.get('shipVia'), 
                            "slsm":item.get('slsm'),
                            "blNumber":item.get('blNumber'),
                            "term":item.get('term'),				
                            "fobPoint":item.get('fobPoint'),
                            "slsm":item.get('slsm'),
                            "soldTo":item.get('soldTo'),
                            "shipTo": item.get('shipTo') ,                          
                            "contactPhone":item.get('contactPhone'),
                            "partsList":item.get('partsList'),
                            "partsTotal":item.get('partsTotal'),
                            "subletTotal":item.get('subletTotal'),
                            "freightTotal":item.get('freightTotal'),
                            "saleTaxTotal":item.get('saleTaxTotal'),
                            "totalAmount":item.get('totalAmount'), 
                            "advisor":item.get('slsm'),
                            "status":item.get('status'),
                            "closeDate":item.get('closeDate'),     
                            "ppFlag":item.get('ppFlag'),                  
                            "auth_token":auth_json
                    }            

            return { "status":True,"partsInvoice": docuemnt }
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 
    @classmethod
    def preparePartsInvoiceFile(self,event,item,fileType,auth_json,copyType):
        _moduleNM="PartsInvoice"
        _functionNM="preparePartsInvoiceFile"
        try:               
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)  
            if fileType==None or fileType=='' or fileType=='JSON':
                inv= {
                            "responseCode":0,
                            "documentId": item.get('document_id'),
                            "documentType": item.get('saleType'),                          
                            "orderNo": item.get('orderNo') ,
                            "openDate":item.get('openDate'), 
                            "shippedDate":item.get('shippedDate'),  
                            "shipVia":item.get('shipVia'), 
                            "slsm":item.get('slsm'),
                            "blNumber":item.get('blNumber'),
                            "term":item.get('term'),				
                            "fobPoint":item.get('fobPoint'),
                            "slsm":item.get('slsm'),
                            "soldTo":item.get('soldTo'),
                            "shipTo": item.get('shipTo') ,                          
                            "contactPhone":item.get('contactPhone'),
                            "partsList":item.get('partsList'),
                            "partsTotal":item.get('partsTotal'),
                            "subletTotal":item.get('subletTotal'),
                            "freightTotal":item.get('freightTotal'),
                            "saleTaxTotal":item.get('saleTaxTotal'),
                            "totalAmount":item.get('totalAmount'),  
                            "advisor":item.get('slsm'),
                            "status":item.get('status'),
                            "closeDate":item.get('closeDate'), 
                            "ppFlag":item.get('ppFlag'),                     
                            "auth_token":auth_json
                    } 
                       
            if fileType.upper()=='PDF':
                inv= self.getPartsInvoicePDFFile(event,item,copyType)
            if fileType.upper()=='HTML':
                inv=  self.getPartsInvoiceHTMLFile(event,item,copyType)
            return { "status":True,"partsInvoice": inv }  
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 
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
    def Validate_PartsInvoiceList_inputs(self,document_type,auth_json):
        res_handler=ResponseHandler()
        if (document_type is None ) or document_type == "" or len(document_type.strip())==0:
           return res_handler.GetErrorResponseJSON(code=322,auth_json=auth_json)
        if document_type != "PARTS":
            list=[ str(document_type)]
            return res_handler.GetFormattedErrorResponseJSON(code=321,auth_json=auth_json,args=list)
        return {"responseCode":0}

    @classmethod
    def Validate_PartsInvoiceDetail_inputs(self,document_id,document_type,auth_json):
        res_handler=ResponseHandler()
        if (document_id is None ) or document_id == "" or len(document_id.strip())==0:
            return res_handler.GetErrorResponseJSON(code=310,auth_json=auth_json)     
        if (document_type is None ) or document_type == "" or len(document_type.strip())==0:
            return res_handler.GetErrorResponseJSON(code=322,auth_json=auth_json)  
        if document_type != "PARTS":
            list=[ str(document_type)]
            return res_handler.GetFormattedErrorResponseJSON(code=321,auth_json=auth_json,args=list)
        return {"responseCode":0} 
    @classmethod
    def Validate_PartsInvoiceDetailPDF_inputs(self,document_id,document_type,fileType,auth_json,copyType):
        res_handler=ResponseHandler()
        if (document_id is None ) or document_id == "" or len(document_id.strip())==0:
            return res_handler.GetErrorResponseJSON(code=310,auth_json=auth_json)     
        if (document_type is None ) or document_type == "" or len(document_type.strip())==0:
            return res_handler.GetErrorResponseJSON(code=322,auth_json=auth_json)  
        if document_type != "PARTS":
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
    def splitLongLine(self,line,width):
        lines=[]
        lines=  textwrap.TextWrapper(width=width,break_long_words=False).wrap(line)       
        return lines
    @classmethod
    def getInnerFooter(self,pageTemplate,pageData,pageNumber,totalPage):
        pageTemplate=pageTemplate.replace('--PAGE_NO--',str(pageNumber)) 
        pageTemplate=pageTemplate.replace('--TOTAL_PAGE_NO--',str(totalPage))       
        pageTemplate=pageTemplate.replace('--BODY--',pageData)                 
        pageTemplate= pageTemplate.replace('--TOTAL_PARTS--','')             
        pageTemplate= pageTemplate.replace('--TOTAL_SUBLET--','')        
        pageTemplate= pageTemplate.replace('--TOTAL_FREIGHT--','') 
        pageTemplate= pageTemplate.replace('--TOTAL_SALES_TAX--','') 
        pageTemplate= pageTemplate.replace('--TOTAL_AMOUNT--','') 
        style="style='page-break-after:always;'"            
        pageTemplate= pageTemplate.replace('--STYLE--',style)  
        return pageTemplate
    @classmethod
    def getLastFooter(self,pageTemplate,pageData,pageNumber,totalPage,item):
        pageTemplate=pageTemplate.replace('--BODY--',pageData)
        pageTemplate=pageTemplate.replace('--PAGE_NO--',str(pageNumber)) 
        pageTemplate=pageTemplate.replace('--TOTAL_PAGE_NO--',str(totalPage))  
        pageTemplate= pageTemplate.replace('--TOTAL_PARTS--',item['partsTotal'])             
        pageTemplate= pageTemplate.replace('--TOTAL_SUBLET--',item['subletTotal'])           
        pageTemplate= pageTemplate.replace('--TOTAL_FREIGHT--',item['freightTotal'])
        pageTemplate= pageTemplate.replace('--TOTAL_SALES_TAX--',item['saleTaxTotal'])
        amtStr=''
        if len(str(item['totalAmount']))>0:
            totalAmt=0
            try:
                totalAmt=float(item['totalAmount']) 
            except:
                totalAmt=0
                pass  
            if totalAmt>0:
                amtStr='$'+item['totalAmount']
            else:
                amtStr=item['totalAmount']
        
        pageTemplate= pageTemplate.replace('--TOTAL_AMOUNT--',amtStr)
        style=""            
        pageTemplate= pageTemplate.replace('--STYLE--',style)   
        return pageTemplate
    @classmethod
    def getHeaderHtml(self,htmlTempData,item):
        htmlTempData= htmlTempData.replace('--INVOICE_NUMBER--',item['document_id'])
        htmlTempData= htmlTempData.replace('--DATE_ENTERED--',self.FormatDate(item['openDate']))
        htmlTempData= htmlTempData.replace('--YOUR_ORDER_NO--',item['orderNo'])
        htmlTempData= htmlTempData.replace('--INVOICE_DATE--',self.FormatDate(item['openDate']))
        htmlTempData= htmlTempData.replace('--SHIPPED_DATE--',self.FormatDate(item['shippedDate']))

        htmlTempData= htmlTempData.replace('--SOLD_TO_ACC_NO--',item['soldTo']['id'])
        htmlTempData= htmlTempData.replace('--SOLD_TO_FNAME--',item['soldTo']['firstName'])
        htmlTempData= htmlTempData.replace('--SOLD_TO_LNAME--',item['soldTo']['lastName'])
        addLines=item['soldTo']['addressLine']
        if len(addLines)>=1:
            htmlTempData= htmlTempData.replace('--SOLD_TO__ADDRESSLINE_1--',item['soldTo']['addressLine'][0])
            if len(addLines)>=2:
                htmlTempData= htmlTempData.replace('--SOLD_TO__ADDRESSLINE_2--',item['soldTo']['addressLine'][1])
            else:
                htmlTempData= htmlTempData.replace('--SOLD_TO__ADDRESSLINE_2--','')
            
            if len(addLines)>=3:
                htmlTempData= htmlTempData.replace('--SOLD_TO__ADDRESSLINE_3--',item['soldTo']['addressLine'][2])
            else:
                htmlTempData= htmlTempData.replace('--SOLD_TO__ADDRESSLINE_3--','')
            
        else :
            htmlTempData= htmlTempData.replace('--SOLD_TO__ADDRESSLINE_1--','')
            htmlTempData= htmlTempData.replace('--SOLD_TO__ADDRESSLINE_2--','')
            htmlTempData= htmlTempData.replace('--SOLD_TO__ADDRESSLINE_3--','')
        htmlTempData= htmlTempData.replace('--SOLD_TO__CITY--',item['soldTo']['city'])
        htmlTempData= htmlTempData.replace('--SOLD_TO__STATE--',item['soldTo']['state'])
        htmlTempData= htmlTempData.replace('--SOLD_TO__ZIP--',item['soldTo']['zip'])  

        htmlTempData= htmlTempData.replace('--SHIP_TO_ACC_NO--',item['shipTo']['id'])
        htmlTempData= htmlTempData.replace('--SHIP_TO_FNAME--',item['shipTo']['firstName'])
        htmlTempData= htmlTempData.replace('--SHIP_TO_LNAME--',item['shipTo']['lastName'])
        addLines=item['shipTo']['addressLine']
        if len(addLines)>=1:
            htmlTempData= htmlTempData.replace('--SHIP_TO__ADDRESSLINE_1--',item['shipTo']['addressLine'][0])
            if len(addLines)>=2:
                htmlTempData= htmlTempData.replace('--SHIP_TO__ADDRESSLINE_2--',item['shipTo']['addressLine'][1])
            else:
                htmlTempData= htmlTempData.replace('--SHIP_TO__ADDRESSLINE_2--','')
            
            if len(addLines)>=3:
                htmlTempData= htmlTempData.replace('--SHIP_TO__ADDRESSLINE_3--',item['shipTo']['addressLine'][2])
            else:
                htmlTempData= htmlTempData.replace('--SHIP_TO__ADDRESSLINE_3--','')
            
        else :
            htmlTempData= htmlTempData.replace('--SHIP_TO__ADDRESSLINE_1--','')
            htmlTempData= htmlTempData.replace('--SHIP_TO__ADDRESSLINE_2--','')
            htmlTempData= htmlTempData.replace('--SHIP_TO__ADDRESSLINE_3--','') 


        htmlTempData= htmlTempData.replace('--SHIP_TO__CITY--',item['shipTo']['city'])
        htmlTempData= htmlTempData.replace('--SHIP_TO__STATE--',item['shipTo']['state'])
        htmlTempData= htmlTempData.replace('--SHIP_TO__ZIP--',item['shipTo']['zip'])  

        htmlTempData= htmlTempData.replace('--SHIP_VIA--',item['shipVia'])           
        htmlTempData= htmlTempData.replace('--SLSM--',item['slsm'])
        htmlTempData= htmlTempData.replace('--BL_NUMBER--',item['blNumber'])
        htmlTempData= htmlTempData.replace('--TERM--',item['term'])
        htmlTempData= htmlTempData.replace('--FOB_POINT--',item['fobPoint'])
        htmlTempData= htmlTempData.replace('--CONTACT_PHONE--',item['contactPhone'])
        return htmlTempData
    @classmethod
    def GenrateHTMLInvoice(self,invoiceHtml,item,bodyTemplate,isPageBreak=True):
        pageFinalData=""
        
        try:
            bodyTemplates=bodyTemplate.split('!!') 
            bodyTemplate=bodyTemplates[0]
            blank_row=bodyTemplates[1] 
            pageTemplate=bodyTemplates[2]           
            pageTemplate_Orginal=self.getHeaderHtml(pageTemplate,item)            
            bodyLineItems=[]
            partsList=item['partsList']
            for parts in partsList:
                bodyLineTempData=bodyTemplate
                bodyLineTempData= bodyLineTempData.replace('--QTY_ORD--',parts['ordQty'])
                bodyLineTempData= bodyLineTempData.replace('--QTY_SHIP--',parts['shipQty'])
                bodyLineTempData= bodyLineTempData.replace('--QTY_BO--',parts['boQty'])
                bodyLineTempData= bodyLineTempData.replace('--LIST--',parts['listPrice'])
                bodyLineTempData= bodyLineTempData.replace('--NET--',parts['netPrice'])
                bodyLineTempData= bodyLineTempData.replace('--AMOUNT--',parts['amount'])
                bodyLineTempData= bodyLineTempData.replace('--BIN#--',parts['binNum'])
                
                bodyLineTempData= bodyLineTempData.replace('--ISLE#--',parts['isleNum'])
                bodyLineTempData= bodyLineTempData.replace('--PART_NO--',parts['partsNum'])
                
                bodyLineTempData= bodyLineTempData.replace('--PART_DESC--',parts['partsDesc'])
               
               
                binNum=parts['binNum']
                partNum=parts['partsNum']

               
                partNum_countLine=1
                if len(partNum)>15:
                   partNum_chunks=self.splitLongLine(partNum,15)  
                   partNum_countLine=len(partNum_chunks)
                self.logger.debug("partNum_countLine="+str(partNum_countLine))
                
                desc=parts['partsDesc']                
                partDesc_countLine=1
                if len(desc)>30: 
                   desc_chunks=self.splitLongLine(desc,30)                   
                   partDesc_countLine=len(desc_chunks)                
                self.logger.debug("partDesc_countLine="+str(partDesc_countLine))

                blNum_countLine=1
                if len(binNum)>6:
                   binNum_chunks=self.splitLongLine(binNum,6)                   
                   blNum_countLine=len(binNum_chunks)    
                self.logger.debug("binNum_countLine="+str(blNum_countLine))            
                countLine=1
                if partNum_countLine>=1:
                    countLine=partNum_countLine
                if partDesc_countLine>countLine:
                    countLine=partDesc_countLine
                if blNum_countLine>countLine:
                    countLine=blNum_countLine

                self.logger.debug("countLine="+str(countLine))
                bodyLineItems.append({'data':bodyLineTempData, 'linecount':countLine})          
                
                if countLine>1:
                    for i in range(countLine-1):
                        bodyLineItems.append({'data':'', 'linecount':1})  

            #END FOR LOOP
            self.logger.debug("count bodyLineItems="+str(len(bodyLineItems)))
            totalLinePerPage=21
            pagesGen = self.chunks(bodyLineItems,totalLinePerPage) 
            pages=list(pagesGen) 
            self.logger.debug("count pages="+str(len(pages)))
            lastPage=len(pages)-1
            pageList=[]
            pageNumber=1
            totalPage=len(pages)
            if isPageBreak:

                if(len(pages)>1):
                    pageList=[]
                   
                    for index in range(len(pages)-1):
                        page_items=pages[index]                   
                        page = [pItem for pItem in page_items if pItem['data']!=""]                    
                        pageTemplate=pageTemplate_Orginal
                        pageData=""
                        lineCountPerPage=0
                        for lineItem in page:
                            pageData=pageData+lineItem['data']
                            lineCountPerPage=lineCountPerPage+lineItem['linecount']
                        if lineCountPerPage<totalLinePerPage:
                            cIndex=lineCountPerPage
                            while cIndex<=totalLinePerPage:
                               pageData=pageData+blank_row
                               cIndex=cIndex+1
                          
                        self.logger.debug("count cIndex="+str(cIndex))  
                        pageTemplate=self.getInnerFooter(pageTemplate,pageData,pageNumber,totalPage) 
                        pageList.append(pageTemplate)
                        pageNumber=pageNumber+1
                    
                    pageTemplate=pageTemplate_Orginal    
                    page_items=pages[lastPage]
                    pageNumber=lastPage+1
                    page = [pItem for pItem in page_items if pItem['data']!=""]                    
                    pageTemplate=pageTemplate_Orginal
                    pageData=""
                    lineCountPerPage=0
                    for lineItem in page:
                        pageData=pageData+lineItem['data']
                        lineCountPerPage=lineCountPerPage+lineItem['linecount']
                    if lineCountPerPage<totalLinePerPage:
                        cIndex=lineCountPerPage
                        while cIndex<=totalLinePerPage:
                            pageData=pageData+blank_row
                            cIndex=cIndex+1                   
                    pageTemplate=self.getLastFooter(pageTemplate,pageData,pageNumber,totalPage,item)  
                    pageList.append(pageTemplate)
                else:
                    pageList=[]
                    pageTemplate=pageTemplate_Orginal
                    page_items=bodyLineItems                   
                    page = [pItem for pItem in page_items if pItem['data']!=""]                    
                    pageTemplate=pageTemplate_Orginal
                    pageData=""
                    lineCountPerPage=0
                    for lineItem in page:
                        pageData=pageData+lineItem['data']
                        lineCountPerPage=lineCountPerPage+lineItem['linecount']
                    if lineCountPerPage<totalLinePerPage:
                        cIndex=lineCountPerPage
                        while cIndex<=totalLinePerPage:
                            pageData=pageData+blank_row
                            cIndex=cIndex+1
                     
                    pageTemplate=self.getLastFooter(pageTemplate,pageData,pageNumber,totalPage,item) 
                    pageList.append(pageTemplate)
            else:
                  
                    pageList=[]
                    pageTemplate=pageTemplate_Orginal
                    page_items=pages[0]                   
                    page = [pItem for pItem in page_items if pItem['data']!=""]                    
                    pageTemplate=pageTemplate_Orginal
                    pageData=""
                    lineCountPerPage=0
                    for lineItem in page:
                        pageData=pageData+lineItem['data']
                        lineCountPerPage=lineCountPerPage+lineItem['linecount']
                    if lineCountPerPage<totalLinePerPage:
                        cIndex=lineCountPerPage
                        while cIndex<=totalLinePerPage:
                            pageData=pageData+blank_row
                            cIndex=cIndex+1                    
                    pageTemplate=self.getLastFooter(pageTemplate,pageData,1,1,item) 
                    pageList.append(pageTemplate)

            #pageFinalData="".join(pageList)
            page_str="".join(pageList) 
            pageFinalData=invoiceHtml.replace('--PAGE--',page_str)  
             
        except Exception as e2:
                self.logger.error("error in GenrateHTMLInvoice",e2)
        return pageFinalData
    @classmethod
    def getPartsInvoiceHTMLFile(self,event,item,optNo):
            self.logger.debug("getPartsInvoiceHTMLFile>> PartsInvoiceItemJSON="+str(item))  
            s3=S3Manager(PartsInvoice.region) 
            bucket=event['BUCKET']
           
            if optNo == 'CUSTOMER' or optNo == 'ALL' :
                invoiceHtmlTemplate=event['PARTS_INVOICE_TEMPLATE_HTML']
                invoiceHtmlBodyTemplate=event['PARTS_INVOICE_TEMPLATE_HTML_BODY']
                self.logger.debug("getInvoiceHTMLFile>> CUSTOMER invoiceHtmlTemplate="+str(invoiceHtmlTemplate))  
                htmlTempData=s3.read_s3_file(bucket,invoiceHtmlTemplate)
                bodyTemplate=s3.read_s3_file(bucket,invoiceHtmlBodyTemplate) 
                htmlTempData= self.GenrateHTMLInvoice(htmlTempData,item,bodyTemplate)   
            if optNo == 'ACCOUNTING' or optNo == 'ALL' :
               invoiceHtmlTemplate=event['PARTS_INVOICE_TEMPLATE_HTML_ACCT']
               invoiceHtmlBodyTemplate=event['PARTS_INVOICE_TEMPLATE_HTML_BODY_ACCT']
               self.logger.debug("getInvoiceHTMLFile>>ACCOUNTING invoiceHtmlTemplate="+str(invoiceHtmlTemplate))  
               htmlTempData=s3.read_s3_file(bucket,invoiceHtmlTemplate)
               bodyTemplate=s3.read_s3_file(bucket,invoiceHtmlBodyTemplate) 
               htmlTempData= self.GenrateHTMLInvoice(htmlTempData,item,bodyTemplate)   
          
            return htmlTempData
    @classmethod
    def getPartsInvoicePDFFile(self,event,item,optNo):
            self.logger.debug("getPartsInvoicePDFFile>> PaersInvoiceItemJSON="+str(item))  
            s3=S3Manager(PartsInvoice.region) 
            invoiceNumber=item.get('document_id')
            bucket=event['BUCKET']
            if optNo == 'ACCOUNTING' or optNo == 'ALL' :
                invoiceHtmlTemplate=event['PARTS_INVOICE_TEMPLATE_HTML_ACCT']
                invoiceHtmlBodyTemplate=event['PARTS_INVOICE_TEMPLATE_HTML_BODY_ACCT']
            else:
                invoiceHtmlTemplate=event['PARTS_INVOICE_TEMPLATE_HTML']
                invoiceHtmlBodyTemplate=event['PARTS_INVOICE_TEMPLATE_HTML_BODY']

            self.logger.debug("getPartsInvoicePDFFile>> PartsInvoiceHtmlTemplate="+str(invoiceHtmlTemplate))  
            htmlTempData=s3.read_s3_file(bucket,invoiceHtmlTemplate) 
            bodyTemplate=s3.read_s3_file(bucket,invoiceHtmlBodyTemplate) 
            htmlTempData= self.GenrateHTMLInvoice(htmlTempData,item,bodyTemplate)
            timestamp = str(datetime.now()).replace('.', '').replace(' ', '_')
            local_filename = f'/tmp/PARTS_INVOICE_{invoiceNumber}_{timestamp}.html'
            
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
            self.logger.info('Successfully generated the PARTS INVOICE PDF.')
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
            self.logger.info('after laoding generated the PARTS INVOICE PDF file.')
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
    
    