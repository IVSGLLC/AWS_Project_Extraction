 
import json
import sys
import logging
import os
from datetime import datetime
from DBHelper import DBHelper
from EPDE_Client import AppClient
loglevel = int(os.environ['LOG_LEVEL'])
logger = logging.getLogger('SalesCustDBHelper')
logger.setLevel(loglevel)
dbhelper=DBHelper()
class SalesCustDBHelper():
    @classmethod
    def SaveSalesCust(self,store_code,df,client_id):
        logger.info("Inside SaveSalesCust Method....store_code:"+store_code+",client_id="+client_id)
        try:
            # Get the service resource.
            region=os.environ['REGION']            
            TableName=store_code+"_SALESCUST_FILE"      
            items=[]
            batch_items=[]
            LAMBDA_BATCH_SIZE=100
            local=True
            ct = datetime.now()
            create_date = ct.strftime("%Y-%m-%d %I:%M:%S %p")
            create_date_only = ct.strftime("%Y-%m-%d")
            recount=0
            df_final=df['df_final']
            account_lines=df['account_line']
            accCodeList=['FI']
            if not dbhelper.ValidateAccounts(store_code=store_code,account_lines=account_lines,region=region,accCodeList=accCodeList):
                lines=str(','.join(account_lines))
                logger.error("Error Occure in Save data to table ["+TableName+"] in DB. INVALID LOGON IDENTIFIED ,Logon in Data:"+lines,exc_info=True)
                return { "operation_status":"FAILED","error_code":-1 ,"error_message":"INVALID LOGON IDENTIFIED, SKIP SAVE IN DB :"+lines,"total_item_count":"0","total_item_parsed":"0","items":[]} 

            for index, row in df_final.iterrows():
                recount=recount+1
                SOLD_DATE=self.FormatDateNew(open_dt_str=dbhelper.CovertToString( row["DATE-SOLD"]).strip(),print_time_seconds_str="")
                item={
                            "client_id": str(client_id),
                            "store_code": str(store_code),
                            "deal_number": str(row["DEAL-NUMBER"]).strip(),
                            "buyer_number": dbhelper.RemoveNewLineCarrigeReturn(row["BUYER-NUMBER"]).strip(),
                            "buyer_name": dbhelper.RemoveNewLineCarrigeReturn(row["BUYER-NAME"]).strip(),
                            "buyer_email": dbhelper.RemoveNewLineCarrigeReturn(row["BUYER-EMAIL"]).strip(),
                            "buyer_email_desc": dbhelper.RemoveNewLineCarrigeReturn(row["BUYER-EMAIL-DESC"]).strip(),
                            "date_sold": SOLD_DATE,
                            "sold_age": dbhelper.CovertToString(row["SOLD-AGE"]).strip(),
                            "vin": str(row["VIN"]).strip(),
                            "make": dbhelper.RemoveNewLineCarrigeReturn(row["MAKE"]),
                            "model": dbhelper.RemoveNewLineCarrigeReturn(row["MODEL"]),
                            "year": str(row["YEAR"]).strip(),
                            "status": str(row["STATUS"]).strip(),  
                            "cashDown": str(row["CASH-DOWN"]).strip(),                           
                            "create_date": dbhelper.CovertToString(create_date_only),
                            "create_date_time":create_date,
                            "exp_time":dbhelper.get_time_to_live()
                            }
                   
                items.append(item)
                if not local:
                    json_string = json.dumps(items)
                    byte_ = json_string.encode("utf-8")
                    size_in_bytes = len(byte_)
                    if size_in_bytes>=240000 or len(items)>= LAMBDA_BATCH_SIZE:
                        batch_items.append(items)
                        items=[]
                
            if not local and len(items)>0:
                batch_items.append(items)
                
            et = datetime.now()
            delta=et-ct
            logger.debug("Build JSON total_seconds="+str(delta.total_seconds()))
            region=os.environ['REGION']            
            TableName=store_code+"_SALESCUST_FILE"      
            MAX_PROC=4
            BATCH_SIZE=25            
            overwrite_by_pkeys=[ 'buyer_number','deal_number'] 
            if items is not None and  len(items)>0:
                attributes_to_compare=[
                                  
                                "deal_number" ,
                                "buyer_number" ,
                                "buyer_name" ,
                                "buyer_email" ,
                                "date_sold" ,
                                "sold_age" ,
                                "vin" ,
                                "make" ,
                                "model" ,
                                "year" ,
                                "status" ,  
                                "cashDown"                 
                                        
                                        ]
                beForFilterCount=len(items)
                logger.debug("CDK DEALS- SalesCust Total Record Count Before Filter=."+str(len(items)))   
                items=dbhelper.GetChangedItems(region=region,tableName=TableName,items=items,attributes_to_compare=attributes_to_compare)
                if items is not None and  len(items)>0:
                        
                    dbhelper.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
                    logger.info("CDK DEALS- SalesCust  Data has been inserted successfuly in table ["+TableName+"] Total Filtered Record Count=."+str(len(items))+", Total Parsed Record Count "+str(beForFilterCount))                 
                else:
                    logger.info("CDK DEALS- SalesCust  Data has not been inserted in table ["+TableName+"] Filtered Total Record Count=0")
            else:
                logger.info("CDK DEALS- SalesCust  Data has not been inserted in table ["+TableName+"] Total Record Count=0")
    
            """ if not local:
                dbhelper.submitSaveData(TableName=TableName,items=batch_items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
            else:
                dbhelper.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
                          """
            et = datetime.now()
            delta=et-ct
            logger.debug("after calling lambda total_seconds="+str(delta.total_seconds()))                   
            logger.info("CDK SalesCust Data has been inserted successfuly in table ["+TableName+"] Record Count=."+str(len(items)))
            return { "operation_status":"SUCCESS","total_item_count":str(recount),"total_item_parsed":str(len(items)),"items":items} 
        except Exception as e:
            # Return failed record's sequence number
            logger.error("Error Occure in Save salescust data to table ["+TableName+"] in DB..." , exc_info=True)
            ex_type, ex_value, ex_traceback = sys.exc_info()
            return { "operation_status":"FAILED","error_code":-1 ,"error_message":ex_value} 
    
    @classmethod
    def GetDealStatusMapping(self,store_code):
        filters=[]
        app_client=AppClient()
        region=os.environ['REGION']  
        store_resp=app_client.GetStoreDetail(store_code,region=region)
        if store_resp['status']:
            item=store_resp['item']
            if 'deal_status_mapping' in item:
                filters=item['deal_status_mapping']
        return filters
    def FormatDateNew(open_dt_str,print_time_seconds_str):
        try:
                    
            if open_dt_str is not None and  len(str(open_dt_str).strip())>0:
                open_dt_str=str(open_dt_str).strip()               
                open_dt_date = datetime.strptime(open_dt_str, "%d%b%y")                
                if print_time_seconds_str is not None and  len(str(print_time_seconds_str).strip())>0:
                    print_time_seconds_str=str(print_time_seconds_str) 
                    f_dt = datetime.strptime(open_dt_str+' '+print_time_seconds_str, "%d%b%y %H:%M")
                    f_dt=f_dt.strftime("%m/%d/%Y %I:%M %p")
                    return f_dt
                else:
                    f_dt=open_dt_date
                    f_dt=f_dt.strftime("%m/%d/%Y")
                    return f_dt               
            else:
                return ""
        except Exception as e:
            return ""
    @classmethod
    def GetDealTypeMapping(self,store_code):
        filters=[]
        app_client=AppClient()
        region=os.environ['REGION']  
        store_resp=app_client.GetStoreDetail(store_code,region=region)
        if store_resp['status']:
            item=store_resp['item']
            if 'deal_type_mapping' in item:
                filters=item['deal_type_mapping']
        return filters
    @classmethod
    def Save_MRSalesCust(self,store_code,json_deals,client_id):
        logger.info("Inside Save_MRSalesCust Method....store_code:"+store_code+",client_id="+client_id)
        try:
            region=os.environ['REGION']
            # Get the service resource.
            items=[]
            batch_items=[]
            LAMBDA_BATCH_SIZE=100
            local=True
            ct = datetime.now()
            create_date = ct.strftime("%Y-%m-%d %I:%M:%S %p")
            create_date_only = ct.strftime("%Y-%m-%d")
            recount=0
            statusMap=self.GetDealStatusMapping(store_code)
            #typeMap=self.GetDealTypeMapping(store_code,region)
            for row in json_deals:
                recount=recount+1
                dealNumber=""
                cashDown=""
                if 'soldFinance' in row :
                    soldFinance=row['soldFinance']
                    if 'salesOrderNumber' in soldFinance:
                        dealNumber=str(soldFinance['salesOrderNumber']) 
                    if 'finalFinancingAmounts' in soldFinance:
                            for finalFinancingAmount in soldFinance['finalFinancingAmounts']:
                                if 'amountType' in finalFinancingAmount and 'amount' in finalFinancingAmount and finalFinancingAmount['amountType']=='DownPaymentAmount':
                                    cashDown=finalFinancingAmount['amount']
                sts=''
                if "dealStatus" in row:
                    sts=dbhelper.CovertToString(row["dealStatus"])
                    try:
                        if sts.upper() in statusMap:                      
                            sts=statusMap[sts.upper()]
                        else:
                            sts=''  
                    except:
                        ""
                vin=''
                make=''
                model=''
                year=''
                if "retailDeliveryReportingVehicleLineItem" in row and row["retailDeliveryReportingVehicleLineItem"] is not None :                    
                    vehicleLineItem = row["retailDeliveryReportingVehicleLineItem"]
                    if "vehicle" in vehicleLineItem:
                        vehicle= vehicleLineItem["vehicle"]
                        if "vehicleId" in vehicle:
                            vin=vehicle["vehicleId"]
                        if "make" in vehicle:
                            make=vehicle["make"]
                        if "model" in vehicle:
                            model=vehicle["model"]
                        if "modelYear" in vehicle:
                            year=vehicle["modelYear"]
                         
                
                dateSold=''
                soldAge=''
                if 'salesDate' in row and row["salesDate"] is not None and len(row["salesDate"])>0:
                    try:
                        dateSold=row["salesDate"]
                        dateSold_dt=datetime.strptime(dateSold, "%Y-%m-%d")  
                        dateSold=dateSold_dt.strftime("%m/%d/%Y")
                        if dateSold_dt is not None:
                                    try:
                                        delta= datetime.now()-dateSold_dt
                                        if delta.days>=0:
                                            soldAge=str(delta.days)
                                    except:
                                           ""
                    except:
                            ""               
               
               

                buyer_number="null"  
                buyer_name="" 
                buyer_f_name="" 
                buyer_n_name="" 
                buyer_l_name="" 
                buyer_email=""
                customerParty=None
                if 'dealParties' in row and len(row["dealParties"])>0:                
                    for dealParty in row['dealParties']:
                        if 'partyType' in dealParty and  dealParty['partyType'] == 'Buyer':   
                            customerParty=self.ExtractPartyDetail(dealParty)  

                if customerParty is not None:
                    if 'partyId' in customerParty and customerParty['partyId'] is not None:
                        buyer_number=customerParty['partyId']

                    if 'partyDetail' in customerParty and customerParty['partyDetail'] is not None:
                        partyDetail=customerParty['partyDetail']
                        if partyDetail is not None:
                            buyer_f_name= partyDetail['firstName']
                            buyer_n_name=partyDetail['middleName']
                            buyer_l_name= partyDetail['lastName']
                            companyName=partyDetail['companyName']
                            personName=partyDetail['personName']
                            if (buyer_f_name is None or buyer_f_name=="") and (buyer_l_name is None or buyer_l_name==""):
                                if companyName is not None and companyName!="":
                                   buyer_name= companyName
                                if personName is not None and personName!="":
                                  buyer_name=personName
                                 
                                  
                            """ homePhone=partyDetail['homePhone']
                            mobilePhone=partyDetail['mobilePhone']
                            workPhone=partyDetail['workPhone']
                            otherPhone=partyDetail['otherPhone']
                            faxPhone=partyDetail['faxPhone']"""
                            buyer_email=partyDetail['emailAddress'] 

                     



                logger.debug("buyer_f_name="+str(buyer_f_name))  
                logger.debug("buyer_n_name="+str(buyer_n_name))  
                logger.debug("buyer_l_name="+str(buyer_l_name))            
                if buyer_name=="" or len(buyer_name)==0:
                    if  len(buyer_f_name)>0: 
                        buyer_name=buyer_f_name
                    if  len(buyer_n_name)>0: 
                        buyer_name=buyer_name+ " "+buyer_n_name
                    if  len(buyer_l_name)>0: 
                        buyer_name=buyer_name+ " "+buyer_l_name

               
                if buyer_number!="" and dealNumber!="":
                    item={
                                "client_id": str(client_id),
                                "store_code": str(store_code),
                                "deal_number":str(dealNumber),
                                "buyer_number": str(buyer_number),
                                "buyer_name":buyer_name,
                                "buyer_email":buyer_email,
                                "date_sold": str(dateSold),
                                "sold_age": str(soldAge),
                                "vin": dbhelper.CovertToString(vin),
                                "make": dbhelper.CovertToString(make),
                                "model": dbhelper.CovertToString(model),
                                "year": dbhelper.CovertToString(year),
                                "status": sts,  
                                "cashDown": dbhelper.CovertToString(cashDown),                           
                                "create_date": dbhelper.CovertToString(create_date_only),
                                "create_date_time":create_date,
                                "exp_time":dbhelper.get_time_to_live()
                                }
                    
                    items.append(item)
                else:
                    logger.debug("byer number is BLANK="+str(row))
                if not local:
                    json_string = json.dumps(items)
                    byte_ = json_string.encode("utf-8")
                    size_in_bytes = len(byte_)
                    if size_in_bytes>=240000 or len(items)>= LAMBDA_BATCH_SIZE:
                        batch_items.append(items)
                        items=[]
                
            if not local and len(items)>0:
                batch_items.append(items)
                
            et = datetime.now()
            delta=et-ct
            logger.debug("Build JSON total_seconds="+str(delta.total_seconds()))
                       
            TableName=store_code+"_SALESCUST_FILE"      
            MAX_PROC=4
            BATCH_SIZE=25            
            overwrite_by_pkeys=[ 'buyer_number','deal_number'] 
            if items is not None and  len(items)>0:
                attributes_to_compare=[
                                  
                                "deal_number" ,
                                "buyer_number" ,
                                "buyer_name" ,
                                "buyer_email" ,
                                "date_sold" ,
                                "sold_age" ,
                                "vin" ,
                                "make" ,
                                "model" ,
                                "year" ,
                                "status" ,  
                                "cashDown"                 
                                        
                                        ]
                beForFilterCount=len(items)
                logger.debug("MR DEALS- SalesCust Total Record Count Before Filter=."+str(len(items)))   
                items=dbhelper.GetChangedItems(region=region,tableName=TableName,items=items,attributes_to_compare=attributes_to_compare)
                if items is not None and  len(items)>0:
                        
                    dbhelper.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
                    logger.info("MR DEALS- SalesCust  Data has been inserted successfuly in table ["+TableName+"] Total Filtered Record Count=."+str(len(items))+", Total Parsed Record Count "+str(beForFilterCount))                 
                else:
                    logger.info("MR DEALS- SalesCust  Data has not been inserted in table ["+TableName+"] Filtered Total Record Count=0")
            else:
                logger.info("MR DEALS- SalesCust  Data has not been inserted in table ["+TableName+"] Total Record Count=0")
    
            """ if not local:
                dbhelper.submitSaveData(TableName=TableName,items=batch_items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
            else:
                dbhelper.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
            """                         
            et = datetime.now()
            delta=et-ct
            logger.debug("after calling lambda total_seconds="+str(delta.total_seconds()))                   
            logger.info("MR DEALS- SalesCust Data has been inserted successfuly in table ["+TableName+"] Record Count=."+str(len(items)))
            return { "operation_status":"SUCCESS","total_item_count":str(recount),"total_item_parsed":str(len(items)),"items":items} 
        except Exception as e:
            # Return failed record's sequence number
            logger.error("Error Occure in Save MR DEALS- salescust data to table ["+TableName+"] in DB..." , exc_info=True)
            ex_type, ex_value, ex_traceback = sys.exc_info()
            return { "operation_status":"FAILED","error_code":-1 ,"error_message":ex_value}
             
    
    @classmethod
    def ExtractCustomerDetail(self,partsInvoiceParty):
        companyName=""
        personName=""
        givenName=""
        middleName=""
        familyName=""
        mobilePhone=""
        homePhone=""
        workPhone=""
        otherPhone=""
        faxPhone=""
        email=""
        #Company
        
        if 'companyName' in partsInvoiceParty:
            companyName= partsInvoiceParty["companyName"] 
            familyName=companyName
                
        #Person
                  
        if 'personName' in partsInvoiceParty:                        
            personName= partsInvoiceParty["personName"]
            
        if 'givenName' in partsInvoiceParty:                        
            givenName= partsInvoiceParty["givenName"] 
            
        if 'middleName' in partsInvoiceParty:                        
            middleName= partsInvoiceParty["middleName"] 
            
        if 'familyName' in partsInvoiceParty:
            familyName= partsInvoiceParty["familyName"] 
        if "communication" in partsInvoiceParty :
            for communication in partsInvoiceParty['communication']:
                if 'channelType' in communication:                                               
                    channelType=communication['channelType']
                    #primaryIndicator=communication['primaryIndicator']
                    if 'channelCode' in communication:
                        channelCode=communication['channelCode']
                        if channelType=="Phone":
                            if channelCode=="Cell" and 'completeNumber' in communication:
                                mobilePhone=communication['completeNumber']
                            if channelCode=="Home"and 'completeNumber' in communication:
                                homePhone=communication['completeNumber']
                            if channelCode=="Work" and 'completeNumber' in communication:
                                workPhone=communication['completeNumber']
                            if channelCode=="Other" and 'completeNumber' in communication:
                                otherPhone=communication['completeNumber']
                            if channelCode=="Fax" and 'completeNumber' in communication:
                                faxPhone=communication['completeNumber']    
                    if channelType=="Email" and 'emailAddress' in communication:
                        email=communication['emailAddress']
        return  {                 
                    "personName": (personName),
                    "companyName": (companyName),                                                                       
                    "firstName": (givenName),
                    "middleName": (middleName),                                                                      
                    "lastName": (familyName),
                    "homePhone":(homePhone),
                    "mobilePhone":(mobilePhone),
                    "otherPhone":(otherPhone),
                    "workPhone":(workPhone),
                    "faxPhone":(faxPhone),
                    "emailAddress":(email) ,                               
                }

    @classmethod
    def ExtractPartyDetail(self,partsInvoiceParty):
        primaryContact=None
        customerId=None
        partyDetail=self.ExtractCustomerDetail(partsInvoiceParty)
        #primaryContact
        if 'primaryContact' in partsInvoiceParty and  partsInvoiceParty['primaryContact'] is not None:
            primaryContact= self.ExtractCustomerDetail(partsInvoiceParty['primaryContact']  )
        #Id
        if 'idList' in partsInvoiceParty and len(partsInvoiceParty["idList"])>0:  
            idList=partsInvoiceParty['idList']                                  
            for id in idList:
                if 'typeId' in id and 'id' in id and id['typeId'] == 'DMSId': 
                    customerId= id["id"]                                    
                    break
              
        return  {                 
                "partyId":str(customerId), 
                "partyDetail": partyDetail,                 
                "primaryContact":primaryContact ,
               
            }
                
                
            
    @classmethod
    def Save_TKSalesCust(self,store_code,json_deals,client_id):
        logger.info("Inside Save_TKSalesCust Method....store_code:"+store_code+",client_id="+client_id)
        try:
            region=os.environ['REGION']
            # Get the service resource.
            items=[]
            batch_items=[]
            LAMBDA_BATCH_SIZE=100
            local=True
            ct = datetime.now()
            create_date = ct.strftime("%Y-%m-%d %I:%M:%S %p")
            create_date_only = ct.strftime("%Y-%m-%d")
            recount=0
            statusMap=self.GetDealStatusMapping(store_code)
            #typeMap=self.GetDealTypeMapping(store_code,region)
            for row in json_deals['data']:
                recount=recount+1
                dealNumber=row['dealNumber']  
                sts=dbhelper.CovertToString(row["status"])
                try:
                    if sts.upper() in statusMap:
                      
                        sts=statusMap[sts.upper()]
                    else:
                        sts=''  
                except:
                    ""
                vin=''
                make=''
                model=''
                year=''
                dateSold=''
                soldAge=''
                if 'vehicles' in row:
                    for vehicle in row['vehicles']:  
                        logger.debug("vehicle="+str(vehicle))
                        if 'isPrimary' in vehicle and vehicle['isPrimary']==True:
                            if 'vin' in vehicle:
                                vin=vehicle['vin']
                            if 'make' in vehicle:
                                make=vehicle['make']
                            if 'model' in vehicle:
                                model=vehicle['model']
                            if 'year' in vehicle:
                                year=vehicle['year']
                            if 'soldTime' in vehicle:
                                soldTime=vehicle['soldTime']
                            if soldTime is not None: 
                                dateSold_dt=None
                                try:
                                    dateSold_dt = datetime.fromtimestamp(soldTime / 1000)
                                    #dateSold=dateSold_dt.strftime("%d%b%y")
                                    dateSold=dateSold_dt.strftime("%m/%d/%Y")
                                    
                                except:
                                    ""
                                if dateSold_dt is not None:
                                   try:
                                    delta= datetime.now()-dateSold_dt
                                    if delta.days>=0:
                                        soldAge=str(delta.days)
                                   except:
                                    ""
                                   
                            break

                buyer_number="null"  
                buyer_name="" 
                buyer_email=""  
                if 'customers' in row: 
                    for customer in row['customers']:
                        if customer['type']=='BUYER':
                            buyer_number=customer['arcId']  
                            buyer_name=''
                            if 'firstName' in customer and customer['firstName'] is not None and   len(customer['firstName'] )>0:
                                buyer_name= customer['firstName']
                            
                            if 'middleName' in customer and customer['middleName'] is not None and  len(customer['middleName'] )>0:
                                if len(buyer_name )>0:
                                    buyer_name=buyer_name+" "+ customer['middleName']
                                else:
                                    buyer_name=customer['middleName']
                            if 'lastName' in customer and customer['lastName'] is not None and len(customer['lastName'] )>0:
                                if len(buyer_name )>0:
                                    buyer_name=buyer_name+" "+ customer['lastName']
                                else:
                                    buyer_name=customer['lastName']
                            if 'emails' in customer:
                                for email in customer['emails']:
                                    buyer_email= email['emailId'] 
                                    if email['isPrimary'] :
                                        buyer_email= email['emailId'] 
                                        break
                            break
                
                cashDown=""
                if buyer_number!="" and dealNumber!="":
                    item={
                                "client_id": str(client_id),
                                "store_code": str(store_code),
                                "deal_number":str(dealNumber),
                                "buyer_number": str(buyer_number),
                                "buyer_name":buyer_name,
                                "buyer_email":buyer_email,
                                "date_sold": dateSold,
                                "sold_age": soldAge,
                                "vin": dbhelper.CovertToString(vin),
                                "make": dbhelper.CovertToString(make),
                                "model": dbhelper.CovertToString(model),
                                "year": dbhelper.CovertToString(year),
                                "status": sts,  
                                "cashDown": dbhelper.CovertToString(cashDown),                           
                                "create_date": dbhelper.CovertToString(create_date_only),
                                "create_date_time":create_date,
                                "exp_time":dbhelper.get_time_to_live()
                                }
                    
                    items.append(item)
                else:
                    logger.debug("byer number is BLANK="+str(row))
                if not local:
                    json_string = json.dumps(items)
                    byte_ = json_string.encode("utf-8")
                    size_in_bytes = len(byte_)
                    if size_in_bytes>=240000 or len(items)>= LAMBDA_BATCH_SIZE:
                        batch_items.append(items)
                        items=[]
                
            if not local and len(items)>0:
                batch_items.append(items)
                
            et = datetime.now()
            delta=et-ct
            logger.debug("Build JSON total_seconds="+str(delta.total_seconds()))
                       
            TableName=store_code+"_SALESCUST_FILE"      
            MAX_PROC=4
            BATCH_SIZE=25            
            overwrite_by_pkeys=[ 'buyer_number','deal_number'] 

            if items is not None and  len(items)>0:
                attributes_to_compare=[
                                  
                                "deal_number" ,
                                "buyer_number" ,
                                "buyer_name" ,
                                "buyer_email" ,
                                "date_sold" ,
                                "sold_age" ,
                                "vin" ,
                                "make" ,
                                "model" ,
                                "year" ,
                                "status" ,  
                                "cashDown"                 
                                        
                                        ]
                beForFilterCount=len(items)
                logger.debug("TKION DEALS- SalesCust Total Record Count Before Filter=."+str(len(items)))   
                items=dbhelper.GetChangedItems(region=region,tableName=TableName,items=items,attributes_to_compare=attributes_to_compare)
                if items is not None and  len(items)>0:
                        
                    dbhelper.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
                    logger.info("TKION DEALS- SalesCust  Data has been inserted successfuly in table ["+TableName+"] Total Filtered Record Count=."+str(len(items))+", Total Parsed Record Count "+str(beForFilterCount))                 
                else:
                    logger.info("TKION DEALS- SalesCust  Data has not been inserted in table ["+TableName+"] Filtered Total Record Count=0")
            else:
                logger.info("TKION DEALS- SalesCust  Data has not been inserted in table ["+TableName+"] Total Record Count=0")
            
            #if not local:
                #dbhelper.submitSaveData(TableName=TableName,items=batch_items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
            #else:
                #dbhelper.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
                         
            et = datetime.now()
            delta=et-ct
            logger.debug("after calling lambda total_seconds="+str(delta.total_seconds()))                   
            logger.info("TKION DEALS- SalesCust Data has been inserted successfuly in table ["+TableName+"] Record Count=."+str(len(items)))
            return { "operation_status":"SUCCESS","total_item_count":str(recount),"total_item_parsed":str(len(items)),"items":items} 
        except Exception as e:
            # Return failed record's sequence number
            logger.error("Error Occure in Save TKEION DEALS- salescust data to table ["+TableName+"] in DB..." , exc_info=True)
            ex_type, ex_value, ex_traceback = sys.exc_info()
            return { "operation_status":"FAILED","error_code":-1 ,"error_message":ex_value} 