 
import json
import sys
import logging
import os
from datetime import datetime, timedelta
from DBHelper import DBHelper
from EPDE_Client import AppClient
loglevel = int(os.environ['LOG_LEVEL'])
logger = logging.getLogger('RODBHelper')
logger.setLevel(loglevel)
dbhelper=DBHelper()
class RODBHelper():
    @classmethod
    def Save(self,store_code,df,client_id):
        logger.info("Inside Save RODBHelper Method....store_code:"+store_code+",client_id="+client_id)
        try:
            # Get the service resource.
            items=[]
            LAMBDA_BATCH_SIZE=100
            local=True
            batch_items=[]
            ct = datetime.now()
            create_date = ct.strftime("%Y-%m-%d %I:%M:%S %p")
            create_date_only = ct.strftime("%Y-%m-%d")
            recount=0
            region=os.environ['REGION']            
            TableName=store_code+"_WIP_FILE"
            df_final=df['df_final']
            account_lines=df['account_line']
            accCodeList=['S']
            if not dbhelper.ValidateAccounts(store_code=store_code,account_lines=account_lines,region=region,accCodeList=accCodeList):
                lines=str(','.join(account_lines))
                logger.error("Error Occure in Save data to table ["+TableName+"] in DB. INVALID LOGON IDENTIFIED ,Logon in Data:"+lines,exc_info=True)
                return { "operation_status":"FAILED","error_code":-1 ,"error_message":"INVALID LOGON IDENTIFIED, SKIP SAVE IN DB :"+lines,"total_item_count":"0","total_item_parsed":"0","items":[]} 

            for index, row in df_final.iterrows():
                recount=recount+1
                #row["CUSTOMER LINE1"]
                cust_FLNM=str(row["CUSTOMER LINE1"])
                custFNM=""
                custLNM=""
                if len(cust_FLNM)>0 and cust_FLNM.__contains__(','):

                    cust_FLNM_arr=cust_FLNM.split(',')
                    if len(cust_FLNM_arr)>1:
                            custLNM=cust_FLNM_arr[0]
                            custFNM=cust_FLNM_arr[1]
                    else:
                            custLNM=cust_FLNM                                                  
                else:
                    custLNM=cust_FLNM

                #row["SR-NAME"]   
                emp_FLNM=str(row["SR-NAME"]) 
                empFNM=""
                empLNM=""
                if len(emp_FLNM)>0 and emp_FLNM.__contains__(','):
                    emp_FLNM_arr=emp_FLNM.split(',')
                    if len(emp_FLNM_arr)>1:
                            empLNM=emp_FLNM_arr[0]
                            empFNM=emp_FLNM_arr[1]
                    else:
                            empLNM=emp_FLNM
                                            
                else:
                    empLNM=emp_FLNM
               
                open_date_time=self.FormatDateNew(open_dt_str=row["OPEN-DATE"],print_time_seconds_str=row["PRINT-TIME"])
                
                close_date_time=self.FormatDateNew(open_dt_str=row["CLOSED"],print_time_seconds_str="")
                sts=dbhelper.CovertToString(row["STATUS"])
                sts=dbhelper.GetLatestStatus(sts)
                #if len(sts)==0:
                #    logger.info("BLANK_STATUS_RO:"+str(row["REFER"] )+",store_code:"+str(store_code)+",client_id="+str(client_id))
                # if len(row["TOTAL$"])==0:
                #    logger.info("BLANK_AMOUNT_RO:"+str(row["REFER"] )+",store_code:"+str(store_code)+",client_id="+str(client_id))
                item={
                            "client_id": str(client_id),
                            "store_code": str(store_code),
                            "document_id": str(row["REFER"]),
                            "document_type": "RO",
                            "status": sts,
                            "open_date_time": open_date_time,
                            "close_date_time":close_date_time,
                            "amount_due": str(row["TOTAL$"]),
                            "total_estimate":str(row["EPDE.TOTAL.ESTIMATE"]),
                            "comments": dbhelper.RemoveNewLineCarrigeReturn(row["COMMENTS"]),
                            "vehicle_vin":dbhelper.CovertToString( row["SERIAL NO"]),
                            "vehicle_make": dbhelper.CovertToString(row["MAKE"]),
                            "vehicle_model": dbhelper.CovertToString(row["MODEL"]),
                            "vehicle_year": dbhelper.CovertToString(row["YR"]),
                            "customer_id": dbhelper.CovertToString(row["CUST"]),
                            "customer_first_name":dbhelper.RemoveNewLineCarrigeReturn( custFNM),
                            "customer_last_name": dbhelper.RemoveNewLineCarrigeReturn(custLNM),
                            "customer_email": dbhelper.RemoveNewLineCarrigeReturn(row["EMAIL"]),
                            "customer_addresses_addressline": dbhelper.RemoveNewLineCarrigeReturn(row["CUSTOMER LINE3"]),
                            "customer_addresses_addressline1": "",
                            "customer_addresses_city": dbhelper.RemoveNewLineCarrigeReturn(row["CITY"]),
                            "customer_addresses_state": dbhelper.RemoveNewLineCarrigeReturn(row["STATE"]),
                            "customer_addresses_zip": dbhelper.RemoveNewLineCarrigeReturn(row["ZIP"]),
                            "customer_addresses_contact_number":dbhelper.CovertToString(row["PHONE"]) ,
                            "employee_id": dbhelper.CovertToString(row["SR-R"]),
                            "employee_first_name":dbhelper.RemoveNewLineCarrigeReturn( empFNM),
                            "employee_last_name": dbhelper.RemoveNewLineCarrigeReturn(empLNM),
                            "hat_tag_number":dbhelper.CovertToString( row["TAG-NO"]),
                            "contact_email": dbhelper.RemoveNewLineCarrigeReturn(row["EMAIL"]),
                            "contact_phone": dbhelper.CovertToString(row["PHONE"]),
                            "spc_ins": dbhelper.RemoveNewLineCarrigeReturn(row["EPDE.SPC.INS"]),
                            "warranty_due": dbhelper.CovertToDecimalNumericString(row["WP"]),
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
            TableName=store_code+"_WIP_FILE"
            overwrite_by_pkeys=['document_type', 'document_id'] 
            MAX_PROC=4
            BATCH_SIZE=25           
            overwrite_by_pkeys=['document_type', 'document_id']
           
            if items is not None and  len(items)>0:
                attributes_to_compare=[
                                       "document_id",
                                       "status",
                                       "open_date_time",
                                       "close_date_time",
                                       "amount_due",
                                       "total_estimate",
                                       "comments",
                                       "vehicle_vin",
                                       "vehicle_make",
                                        "vehicle_model",
                                        "vehicle_model",
                                        "vehicle_year",
                                        "customer_id",
                                        "customer_first_name",
                                        "customer_last_name", 
                                        "customer_email",
                                        "customer_addresses_addressline",
                                        "customer_addresses_addressline1",
                                        "customer_addresses_city",
                                        "customer_addresses_state", 
                                        "customer_addresses_zip", 
                                        "customer_addresses_contact_number",
                                        "employee_id",
                                        "employee_first_name",
                                        "employee_last_name", 
                                        "hat_tag_number", 
                                        "contact_email",
                                        "contact_phone",
                                        "spc_ins",
                                        "warranty_due"
                                        ]
                beForFilterCount=len(items)
                logger.debug("CDK WIP Total Record Count Before Filter=."+str(len(items)))   
                items=dbhelper.GetChangedItems(region=region,tableName=TableName,items=items,attributes_to_compare=attributes_to_compare)
                if items is not None and  len(items)>0:
                        
                    dbhelper.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
                    logger.info("WIP Data has been inserted successfuly in table ["+TableName+"] Total Filtered Record Count=."+str(len(items))+", Total Parsed Record Count "+str(beForFilterCount))                 
                else:
                    logger.info("WIP Data has not been inserted in table ["+TableName+"] Filtered Total Record Count=0")
            else:
                logger.info("WIP Data has not been inserted in table ["+TableName+"] Total Record Count=0")
            
            """  if not local:
                dbhelper.submitSaveData(TableName=TableName,items=batch_items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
            else:
                 dbhelper.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
            """
            et = datetime.now()
            delta=et-ct
            logger.debug("after calling lambda total_seconds="+str(delta.total_seconds()))             
            logger.info("WIP Data has been inserted successfuly in table ["+TableName+"] Record Count=."+str(len(items)))
           
            return { "operation_status":"SUCCESS","total_item_count":str(recount),"total_item_parsed":str(len(items)),"items":items} 
        except Exception as e:
            # Return failed record's sequence number
            logger.error("Error Occure in Save wip data to table ["+TableName+"] in DB..." , exc_info=True)
            ex_type, ex_value, ex_traceback = sys.exc_info()
            return { "operation_status":"FAILED","error_code":-1 ,"error_message":ex_value} 
     
    def FormatDateNew(open_dt_str,print_time_seconds_str):
        try:
                    
            if open_dt_str is not None and  len(str(open_dt_str).strip())>0:
                open_dt_str=str(open_dt_str).strip()               
                open_dt_date = datetime.strptime(open_dt_str, "%d%b%y")                
                if print_time_seconds_str is not None and  len(str(print_time_seconds_str).strip())>0:
                    print_time_seconds_str=str(print_time_seconds_str) 
                    f_dt = datetime.strptime(open_dt_str+' '+print_time_seconds_str, "%d%b%y %H:%M")
                    f_dt=f_dt.strftime("%m/%d/%Y %I:%M %p")
                    #logger.debug("f_dt:"+f_dt)
                    return f_dt
                else:
                    f_dt=open_dt_date
                    f_dt=f_dt.strftime("%m/%d/%Y")
                    #logger.debug("1. f_dt:"+f_dt)
                    return f_dt               
            else:
                return ""
        except Exception as e:
            return ""
    def FormatDate(open_dt_str,print_time_seconds_str):
        try:
            open_dt_str=str(open_dt_str)
           # if loglevel==logging.DEBUG:
              #  logger.debug("open_dt_str:"+open_dt_str)
         
            print_time_seconds_str=str(print_time_seconds_str)
           # if loglevel==logging.DEBUG:
               # logger.debug("print_time_seconds_str:"+print_time_seconds_str)
            if len(open_dt_str)>0:
                open_dt_date = datetime.strptime(open_dt_str, "%d%b%y")
                print_time_seconds_str=print_time_seconds_str.strip()
                if len(print_time_seconds_str)>0:
                    pt_arr=print_time_seconds_str.split(" ")
                    if len(pt_arr)>=2:
                        print_time_seconds_str=pt_arr[1]
                        p_added_seconds = timedelta(seconds=int(print_time_seconds_str))
                        f_dt=open_dt_date+p_added_seconds
                        f_dt=f_dt.strftime("%Y-%m-%d %I:%M:%S %p").lower()
                        return f_dt
                    else:
                        f_dt=open_dt_date
                        f_dt=f_dt.strftime("%Y-%m-%d").lower()
                        return f_dt

                else:
                    f_dt=open_dt_date
                    f_dt=f_dt.strftime("%Y-%m-%d").lower()
                    return f_dt
            else:
                return ""
        except Exception as e:
            #logger.error("Error Occure in FormatDate ..." , exc_info=True)
            return ""
    @classmethod
    def GetROStatusMapping(self,store_code):
        filters=[]
        app_client=AppClient()
        region=os.environ['REGION']  
        store_resp=app_client.GetStoreDetail(store_code,region=region)
        if store_resp['status']:
            item=store_resp['item']
            if 'ro_status_mapping' in item:
                filters=item['ro_status_mapping']
        return filters
    @classmethod
    def Save_TKRO(self,store_code,json_ro,client_id):
        logger.info("Inside Save_TKRO RODBHelper Method....store_code:"+store_code+",client_id="+client_id)
        try:
            # Get the service resource.
            items=[]
            region=os.environ['REGION']    
            LAMBDA_BATCH_SIZE=100
            local=True
            batch_items=[]
            ct = datetime.now()
            create_date = ct.strftime("%Y-%m-%d %I:%M:%S %p")
            create_date_only = ct.strftime("%Y-%m-%d")
            recount=0
            statusMap=self.GetROStatusMapping(store_code)
            logger.debug("TEKION json_ro="+str(json_ro))
            for row in json_ro['data']:
                recount=recount+1
                createdTime=None
                externalnotes=""
                if "externalnotes" in row and row["externalnotes"] is not None: 
                    externalnotes=str(row["externalnotes"])
                internalnotes=""
                if "internalnotes" in row and row["internalnotes"] is not None: 
                    internalnotes=str(row["internalnotes"])

                if 'createdTime' in row:
                    createdTime=row['createdTime']  

                open_date_time=""
                if createdTime is not None: 
                    open_date_time_dt = datetime.fromtimestamp(createdTime / 1000)
                    #open_date_time=open_date_time_dt.strftime("%Y-%m-%d %I:%M:%S %p").lower()
                    open_date_time= open_date_time_dt.strftime("%m/%d/%Y %I:%M %p") 
                closedTime=None
                if 'closedTime' in row:
                    closedTime=row['closedTime']   
                close_date_time=""
                if closedTime is not None: 
                    close_date_time_dt = datetime.fromtimestamp(closedTime / 1000)
                    #close_date_time=close_date_time_dt.strftime("%Y-%m-%d %I:%M:%S %p").lower()
                    close_date_time=close_date_time_dt.strftime("%m/%d/%Y %I:%M %p") 

                sts=dbhelper.CovertToString(row["status"])
                try:
                    if sts.upper() in statusMap:
                        logger.debug("TEKION sts="+sts)
                        sts=statusMap[sts.upper()]
                    else:
                        sts=''  
                except:
                    ""                             
                homePhone=""
                mobilePhone=""
                contactPhone=""
                if 'customer' in row and 'phones' in row['customer']:
                    for phone in row['customer']['phones']:
                        if phone['phoneType'] == 'HOME':                            
                            homePhone=str(phone['number'])
                        if phone['phoneType'] == 'MOBILE':                            
                            mobilePhone=str(phone['number'])
                        if phone['isPrimary']==True :
                            contactPhone=str(phone['number'])
                advisorId=''
                if 'advisorId' in row:
                    advisorId=row['advisorId']
                employee_id=""
                employee_first_name=""
                employee_last_name=""
                for primaryAdvisor in row['primaryAdvisor']:
                    if advisorId!='' and  advisorId==primaryAdvisor['id']:
                        if "employeeDisplayNumber" in primaryAdvisor:
                            employee_id= str(primaryAdvisor["employeeDisplayNumber"])
                        if "firstName" in primaryAdvisor:
                            employee_first_name=str(primaryAdvisor["firstName"])
                        if "lastName" in primaryAdvisor:
                            employee_last_name=str(primaryAdvisor["lastName"])
                        break
                #Added on 30 Nov 2022 /Requested by Andrew 
                amount_due=str(row["totalWithTax"])
                tk_ro_status=row["status"]
                if tk_ro_status=='INVOICED' and  'invoice' in row and row['invoice'] is not None:
                    invoice=row['invoice']
                    if 'invoiceAmount' in invoice and invoice["invoiceAmount"] is not None:
                        amount_due=str(invoice["invoiceAmount"])
                total_estimate=""
                item={
                            "client_id": str(client_id),
                            "store_code": str(store_code),
                            "document_id": str(row["repairOrderNumber"]),
                            "document_type": "RO",
                            "status": sts,
                            "open_date_time": open_date_time,
                            "close_date_time":close_date_time,
                            "amount_due": str(amount_due),
                            "total_estimate":str(total_estimate),
                            "comments": externalnotes,
                            "vehicle_vin":str(row["vehicle"]["vin"]),
                            "vehicle_make": str(row["vehicle"]["make"]),
                            "vehicle_model": str(row["vehicle"]["model"]),
                            "vehicle_year": str(row["vehicle"]["year"]),
                            "customer_id": str(row["customer"]["arcId"]),
                            "customer_first_name": str(row["customer"]["firstName"]),
                            "customer_last_name": str(row["customer"]["lastName"]),
                            "customer_email": str(row["customer"]["email"]),
                            "customer_addresses_addressline": str(row["customer"]["address"]["line1"]),
                            "customer_addresses_addressline1": str(row["customer"]["address"]["line2"]),
                            "customer_addresses_city":str(row["customer"]["address"]["city"]),
                            "customer_addresses_state": str(row["customer"]["address"]["state"]),
                            "customer_addresses_zip": str(row["customer"]["address"]["zip"]),
                            "customer_addresses_country": str(row["customer"]["address"]["country"]),
                            "customer_addresses_contact_number":contactPhone,
                            "employee_id": employee_id,
                            "employee_first_name":employee_first_name,
                            "employee_last_name": employee_last_name,
                            "hat_tag_number":str(row["tagNumber"]),
                            "contact_email": str(row["customer"]["email"]),
                            "contact_phone": contactPhone,
                            "cell_phone": mobilePhone,
                            "spc_ins":internalnotes ,
                            "warranty_due": '',
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
                  
            TableName=store_code+"_WIP_FILE"
            overwrite_by_pkeys=['document_type', 'document_id'] 
            MAX_PROC=4
            BATCH_SIZE=25           
            overwrite_by_pkeys=['document_type', 'document_id']

            if items is not None and  len(items)>0:                
                attributes_to_compare=[
                                       "document_id",
                                       "status",
                                       "open_date_time",
                                       "close_date_time",
                                       "amount_due",
                                       "total_estimate",
                                       "comments",
                                       "vehicle_vin",
                                       "vehicle_make",
                                        "vehicle_model",
                                        "vehicle_model",
                                        "vehicle_year",
                                        "customer_id",
                                        "customer_first_name",
                                        "customer_last_name", 
                                        "customer_email",
                                        "customer_addresses_addressline",
                                        "customer_addresses_addressline1",
                                        "customer_addresses_city",
                                        "customer_addresses_state", 
                                        "customer_addresses_zip", 
                                        "customer_addresses_contact_number",
                                        "employee_id",
                                        "employee_first_name",
                                        "employee_last_name", 
                                        "hat_tag_number", 
                                        "contact_email",
                                        "contact_phone",
                                        "spc_ins",
                                        "warranty_due"
                                        ]
                beForFilterCount=len(items)
                logger.debug(" TEKION Total Record Count Before Filter=."+str(len(items)))    
                items=dbhelper.GetChangedItems(region=region,tableName=TableName,items=items,attributes_to_compare=attributes_to_compare)
                if items is not None and  len(items)>0:                      
                    dbhelper.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
                    logger.info("TEKION WIP Data has been inserted successfuly in table ["+TableName+"] Total Filtered Record Count=."+str(len(items))+", Total Parsed Record Count "+str(beForFilterCount))                 
                else:
                    logger.info("TEKION WIP Data has not been inserted in table ["+TableName+"] Filtered Total Record Count=0")
            else:
                logger.info("TEKION WIP Data has not been inserted in table ["+TableName+"] Total Record Count=0")
            
            """ if not local:
                dbhelper.submitSaveData(TableName=TableName,items=batch_items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
            else:
                 dbhelper.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
            """                       
            et = datetime.now()
            delta=et-ct
            logger.debug("after calling lambda total_seconds="+str(delta.total_seconds()))             
            logger.info("TEKION RO WIP Data has been inserted successfuly in table ["+TableName+"] Record Count=."+str(len(items)))
            return { "operation_status":"SUCCESS","total_item_count":str(recount),"total_item_parsed":str(len(items)),"items":items} 
        except Exception as e:
            # Return failed record's sequence number
            logger.error("Error Occure in Save TEKION RO WIP data to table ["+TableName+"] in DB..." , exc_info=True)
            ex_type, ex_value, ex_traceback = sys.exc_info()
            return { "operation_status":"FAILED","error_code":-1 ,"error_message":ex_value} 
        
    @classmethod
    def Save_MRRO(self,store_code,json_ro,client_id):
        logger.info("Inside Save_MRRO RODBHelper Method....store_code:"+store_code)
        try:
            # Get the service resource.
            items=[]
            region=os.environ['REGION']    
            LAMBDA_BATCH_SIZE=100
            local=True
            batch_items=[]
            ct = datetime.now()
            create_date = ct.strftime("%Y-%m-%d %I:%M:%S %p")
            create_date_only = ct.strftime("%Y-%m-%d")
            recount=0
            statusMap=self.GetROStatusMapping(store_code)
            logger.debug("MR json_ro="+str(json_ro))
            for row in json_ro:
                recount=recount+1
                createdTime=None               
                orderNotes=""
                orderInternalNotes=""

                if "orderNotes" in row and row["orderNotes"] is not None: 
                    orderNotes="\n".join(row["orderNotes"])
                if "orderInternalNotes" in row and row["orderInternalNotes"] is not None: 
                    orderInternalNotes="\n".join(row["orderInternalNotes"])

                if 'repairOrderOpenedDateTime' in row:
                    createdTime=row['repairOrderOpenedDateTime']  

                open_date_time=""
                if createdTime is not None: 
                    open_date_time_dt=datetime.strptime(createdTime, "%Y-%m-%dT%H:%M:%SZ")                    
                    open_date_time=open_date_time_dt.strftime("%m/%d/%Y %I:%M %p")  
                
                closedTime=None
                if 'repairOrderCompletedDateTime' in row:
                    closedTime=row['repairOrderCompletedDateTime']   
                close_date_time=""
                if closedTime is not None and len(closedTime)>0:                 
                    close_date_time_dt=datetime.strptime(closedTime, "%Y-%m-%dT%H:%M:%SZ")                    
                    close_date_time=close_date_time_dt.strftime("%m/%d/%Y %I:%M %p") 
                sts=""
                if "repairOrderStatus" in row:
                    sts=dbhelper.CovertToString(row["repairOrderStatus"])
                    try:
                        if sts.upper() in statusMap:
                            logger.debug("MR sts="+sts)
                            sts=statusMap[sts.upper()]
                        else:
                            sts=''  
                    except:
                        ""                             
                vehicleHatNumber=""
                if "vehicleHatNumber" in row:
                    vehicleHatNumber=row["vehicleHatNumber"]

                make=""
                model=""
                year=""
                vin=""
                if "repairOrderVehicleLineItem" in row:
                    repairOrderVehicleLineItem=row["repairOrderVehicleLineItem"]
                    if "vehicle" in repairOrderVehicleLineItem:
                        vehicle= repairOrderVehicleLineItem["vehicle"]
                        if "vehicleId" in vehicle:
                            vin=vehicle["vehicleId"]
                        if "make" in vehicle:
                            make=vehicle["make"]
                        if "model" in vehicle:
                            model=vehicle["model"]
                        if "modelYear" in vehicle:
                            year=vehicle["modelYear"]
                
               
                personName=""
                customer_id=''
                customer_name=""
                customer_first_name=''
                customer_middle_name=""
                customer_last_name=''              
                customer_email=""
                mobilePhone=""
                contactPhone=""
                homePhone=""  
                workPhone=""
                otherPhone=""
                faxPhone=""          
                lineOne=""
                lineTwo=""
                stateOrProvision=""
                cityName=""
                postCode=""
                country=""

                employee_id=""
                employee_first_name=""
                employee_last_name=""
                employee_name=""
                employee_middle_name=""
                
                companyName=""
                customerParty=None
                if 'repairOrderParties' in row and len(row["repairOrderParties"])>0:
                    for repairOrderParty in row['repairOrderParties']:                        
                        if 'partyType' in repairOrderParty and  repairOrderParty['partyType'] == 'OwnerParty': 
                            customerParty=self.ExtractPartyDetail(repairOrderParty)   
            
                        if 'partyType' in repairOrderParty and repairOrderParty['partyType'] == 'ServiceAdvisorParty':
                            if 'personName' in repairOrderParty:                        
                                employee_name= repairOrderParty["personName"]
                            if 'middleName' in repairOrderParty:                        
                                employee_middle_name= repairOrderParty["middleName"] 
                            if 'givenName' in repairOrderParty:
                                employee_first_name= repairOrderParty["givenName"] 
                            if 'familyName' in repairOrderParty:
                                employee_last_name= repairOrderParty["familyName"] 
                            if 'idList' in repairOrderParty and len(repairOrderParty["idList"])>0:
                                idList= repairOrderParty["idList"] 
                                for id in idList:
                                    if 'typeId' in id and 'id' in id and id['typeId'] == 'DMSId':  
                                       employee_id= id["id"]
                
                #End RepairOrderParties extract
                resdenceAddress=None
                billAddress=None
                mailAddress=None
                previousAddress=None
                shipAddress=None
                if customerParty is not None:
                    if 'partyId' in customerParty and customerParty['partyId'] is not None:
                        customer_id=customerParty['partyId']

                    if 'partyDetail' in customerParty and customerParty['partyDetail'] is not None:
                        partyDetail=customerParty['partyDetail']
                        if partyDetail is not None:
                            customer_first_name= partyDetail['firstName']
                            customer_middle_name=partyDetail['middleName']
                            customer_last_name= partyDetail['lastName']
                            companyName=partyDetail['companyName']
                            personName=partyDetail['personName']
                            customer_name=personName
                            if (customer_first_name is None or customer_first_name=="") and (customer_last_name is None or customer_last_name==""):
                                if companyName is not None and companyName!="":
                                    customer_last_name= companyName
                                if personName is not None and personName!="":
                                  nameArray= personName.split() 
                                  if nameArray is not None and len(nameArray)==1:                                 
                                    customer_last_name=nameArray[0]
                                  if nameArray is not None and len(nameArray)==2:                                   
                                    customer_first_name=nameArray[0]
                                    customer_last_name=nameArray[1]
                                  if nameArray is not None and len(nameArray)==2:                                   
                                    customer_first_name=nameArray[0]                                    
                                    customer_middle_name=nameArray[1]  
                                    customer_last_name=nameArray[2]
                            homePhone=partyDetail['homePhone']
                            mobilePhone=partyDetail['mobilePhone']
                            workPhone=partyDetail['workPhone']
                            otherPhone=partyDetail['otherPhone']
                            faxPhone=partyDetail['faxPhone']
                            customer_email=partyDetail['emailAddress']
                            addressGroups=partyDetail['addressGroups'] 
                            if addressGroups is not None and 'Residence' in addressGroups :
                                resdenceAddress=addressGroups['Residence']
                            if addressGroups is not None and 'Billing' in addressGroups :
                                billAddress=addressGroups['Billing']
                            if addressGroups is not None and 'Mail' in addressGroups :
                                mailAddress=addressGroups['Mail']
                            if addressGroups is not None and 'Shipping' in addressGroups :
                                shipAddress=addressGroups['Shipping']
                            if addressGroups is not None and 'Previous' in addressGroups :
                                previousAddress=addressGroups['Previous']
                            
                        if 'primaryContact' in customerParty and customerParty['primaryContact'] is not None:
                            primaryContact=customerParty['primaryContact']
                            homePhone=primaryContact['homePhone']
                            mobilePhone=primaryContact['mobilePhone']
                            workPhone=primaryContact['workPhone']
                            otherPhone=primaryContact['otherPhone']
                            faxPhone=primaryContact['faxPhone']
                            customer_email=primaryContact['emailAddress']
                            addressGroups=primaryContact['addressGroups'] 
                            if addressGroups is not None and 'Residence' in addressGroups :
                                resdenceAddress=addressGroups['Residence']
                            if addressGroups is not None and 'Billing' in addressGroups :
                                billAddress=addressGroups['Billing']
                            if addressGroups is not None and 'Mail' in addressGroups :
                                mailAddress=addressGroups['Mail']
                            if addressGroups is not None and 'Shipping' in addressGroups :
                                shipAddress=addressGroups['Shipping']
                            if addressGroups is not None and 'Previous' in addressGroups :
                                previousAddress=addressGroups['Previous']
                        if 'addressGroups' in customerParty and customerParty['addressGroups'] is not None:
                            addressGroups=customerParty['addressGroups'] 
                            if addressGroups is not None and 'Residence' in addressGroups :
                                resdenceAddress=addressGroups['Residence']
                            if addressGroups is not None and 'Billing' in addressGroups :
                                billAddress=addressGroups['Billing']
                            if addressGroups is not None and 'Mail' in addressGroups :
                                mailAddress=addressGroups['Mail']
                            if addressGroups is not None and 'Shipping' in addressGroups :
                                shipAddress=addressGroups['Shipping']  
                            if addressGroups is not None and 'Previous' in addressGroups :
                                previousAddress=addressGroups['Previous']      
                addressFound=False
                if resdenceAddress is not None:
                    addressFound=True
                    custAddress=self.prePareAddress(resdenceAddress)
                    lineOne=custAddress["customer_addresses_addressline"] 
                    lineTwo=custAddress["customer_addresses_addressline1"] 
                    cityName=custAddress["customer_addresses_city"] 
                    stateOrProvision=custAddress["customer_addresses_state"] 
                    postCode=custAddress["customer_addresses_zip"] 
                    country=custAddress["customer_addresses_country"] 
                if addressFound == False and mailAddress is not None:
                    custAddress=self.prePareAddress(mailAddress)
                    lineOne=custAddress["customer_addresses_addressline"] 
                    lineTwo=custAddress["customer_addresses_addressline1"] 
                    cityName=custAddress["customer_addresses_city"] 
                    stateOrProvision=custAddress["customer_addresses_state"] 
                    postCode=custAddress["customer_addresses_zip"] 
                    country=custAddress["customer_addresses_country"]      
                if addressFound == False and billAddress is not None:
                    custAddress=self.prePareAddress(billAddress)
                    lineOne=custAddress["customer_addresses_addressline"] 
                    lineTwo=custAddress["customer_addresses_addressline1"] 
                    cityName=custAddress["customer_addresses_city"] 
                    stateOrProvision=custAddress["customer_addresses_state"] 
                    postCode=custAddress["customer_addresses_zip"] 
                    country=custAddress["customer_addresses_country"]      
                if addressFound == False and shipAddress is not None:
                    custAddress=self.prePareAddress(shipAddress)
                    lineOne=custAddress["customer_addresses_addressline"] 
                    lineTwo=custAddress["customer_addresses_addressline1"] 
                    cityName=custAddress["customer_addresses_city"] 
                    stateOrProvision=custAddress["customer_addresses_state"] 
                    postCode=custAddress["customer_addresses_zip"] 
                    country=custAddress["customer_addresses_country"]   
                if addressFound == False and previousAddress is not None:
                    custAddress=self.prePareAddress(previousAddress)
                    lineOne=custAddress["customer_addresses_addressline"] 
                    lineTwo=custAddress["customer_addresses_addressline1"] 
                    cityName=custAddress["customer_addresses_city"] 
                    stateOrProvision=custAddress["customer_addresses_state"] 
                    postCode=custAddress["customer_addresses_zip"] 
                    country=custAddress["customer_addresses_country"]      
   

                contactPhone=homePhone
                if contactPhone == None or contactPhone =="":
                   contactPhone=workPhone
                if contactPhone == None or contactPhone =="":
                   contactPhone=mobilePhone   
                if contactPhone == None or contactPhone =="":
                   contactPhone=otherPhone   
                if contactPhone == None or contactPhone =="":
                   contactPhone=faxPhone   

                warranty_due=""
                amount_due=""
                if 'price' in row:
                    for price in row['price']:
                        if "priceCode" in price:
                            priceCode=price["priceCode"]
                            if priceCode=="CustomerPayTotalSale" and "chargeAmount" in price :                            
                                    amount_due=str(price["chargeAmount"])
                            if priceCode=="WarrantyTotalSale" and "chargeAmount" in price :                            
                                    warranty_due=str(price["chargeAmount"])
                total_estimate=""

                item={
                            "client_id": str(client_id),
                            "store_code": str(store_code),
                            "document_id": str(row["repairOrderNumber"]),
                            "document_type": "RO",
                            "status": str(sts),
                            "open_date_time": str(open_date_time),
                            "close_date_time":str(close_date_time),
                            "amount_due": str(amount_due),
                            "total_estimate":str( total_estimate),
                            "comments": str(orderNotes),
                            "vehicle_vin":str(vin),
                            "vehicle_make": str(make),
                            "vehicle_model": str(model),
                            "vehicle_year": str(year),
                            "company":str(companyName),
                            "customer_id": str(customer_id),
                            "customer_name": str(customer_name),
                            "customer_first_name": str(customer_first_name),
                            "customer_middle_name": str(customer_middle_name),
                            "customer_last_name": str(customer_last_name),
                            "customer_email": str(customer_email),
                            "customer_addresses_addressline": str(lineOne),
                            "customer_addresses_addressline1": str(lineTwo),
                            "customer_addresses_city":str(cityName),
                            "customer_addresses_state": str(stateOrProvision),
                            "customer_addresses_zip": str(postCode),
                            "customer_addresses_country": str(country),
                            "customer_addresses_contact_number":str(contactPhone),
                            "employee_id": str(employee_id),
                            "employee_name":str(employee_first_name),
                            "employee_first_name":str(employee_first_name),
                            "employee_middle_name":str(employee_middle_name),
                            "employee_last_name": str(employee_last_name),
                            "hat_tag_number":str(vehicleHatNumber),
                            "contact_email": str(customer_email),
                            "contact_phone": str(contactPhone),
                            "cell_phone": str(mobilePhone),
                            "spc_ins":orderInternalNotes ,
                            "warranty_due": str(warranty_due),
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
                  
            TableName=store_code+"_WIP_FILE"
            overwrite_by_pkeys=['document_type', 'document_id'] 
            MAX_PROC=4
            BATCH_SIZE=25           
            overwrite_by_pkeys=['document_type', 'document_id']
            if items is not None and  len(items)>0:
                attributes_to_compare=[
                                       "document_id",
                                       "status",
                                       "open_date_time",
                                       "close_date_time",
                                       "amount_due",
                                       "total_estimate",
                                       "comments",
                                       "vehicle_vin",
                                       "vehicle_make",
                                        "vehicle_model",
                                        "vehicle_model",
                                        "vehicle_year",
                                        "customer_id",
                                        "customer_first_name",
                                        "customer_last_name", 
                                        "customer_email",
                                        "customer_addresses_addressline",
                                        "customer_addresses_addressline1",
                                        "customer_addresses_city",
                                        "customer_addresses_state", 
                                        "customer_addresses_zip", 
                                        "customer_addresses_contact_number",
                                        "employee_id",
                                        "employee_first_name",
                                        "employee_last_name", 
                                        "hat_tag_number", 
                                        "contact_email",
                                        "contact_phone",
                                        "spc_ins",
                                        "warranty_due",
                                        "company"
                                        ]
                beForFilterCount=len(items)
                logger.debug("MR  Total Record Count Before Filter=."+str(len(items)))       
                items=dbhelper.GetChangedItems(region=region,tableName=TableName,items=items,attributes_to_compare=attributes_to_compare)
                if items is not None and  len(items)>0:
                    
                    dbhelper.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
                    logger.info("MR WIP Data has been inserted successfuly in table ["+TableName+"] Total Filtered Record Count=."+str(len(items))+", Total Parsed Record Count "+str(beForFilterCount))                 
                else:
                    logger.info("MR WIP Data has not been inserted in table ["+TableName+"] Filtered Total Record Count=0")
            else:
                logger.info("MR WIP Data has not been inserted in table ["+TableName+"] Total Record Count=0")
            """ if not local:
                dbhelper.submitSaveData(TableName=TableName,items=batch_items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
            else:
                dbhelper.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
                      """  
            et = datetime.now()
            delta=et-ct
            logger.debug("after calling lambda total_seconds="+str(delta.total_seconds()))             
            logger.info("MR RO WIP Data has been inserted successfuly in table ["+TableName+"] Record Count=."+str(len(items)))
            return { "operation_status":"SUCCESS","total_item_count":str(recount),"total_item_parsed":str(len(items)),"items":items} 
        except Exception as e:
            # Return failed record's sequence number
            logger.error("Error Occure in Save MR RO WIP data to table ["+TableName+"] in DB..." , exc_info=True)
            ex_type, ex_value, ex_traceback = sys.exc_info()
            return { "operation_status":"FAILED","error_code":-1 ,"error_message":ex_value} 
    @classmethod
    def prePareAddress(self,address):
        addressLine=[]        
        lineOne=""
        lineTwo=""
        cityName=""
        stateOrProvision=""
        postCode=""
        country=""
        if address is not None:
            if "addressLine" in address:
                addressLine=address["addressLine"] 
                if addressLine is not None and len(addressLine)>=1:
                    lineOne=addressLine[0]
                if addressLine is not None and len(addressLine)>=2:
                    lineTwo=addressLine[1]
                if addressLine is not None and len(addressLine)>=3:
                    lineTwo=lineTwo+"\n"+addressLine[2]
                if addressLine is not None and len(addressLine)>=4:
                    lineTwo=lineTwo+"\n"+addressLine[3]
            if "city" in address:
                cityName=address["city"] 
            if "state" in address:
                stateOrProvision=address["state"] 
            if "zip" in address:
                postCode=address["zip"] 
            if "country" in address:
                country=address["country"] 
        return  {
                    "customer_addresses_addressline": str(lineOne),
                    "customer_addresses_addressline1": str(lineTwo),
                    "customer_addresses_city":str(cityName),
                    "customer_addresses_state": str(stateOrProvision),
                    "customer_addresses_zip": str(postCode),
                    "customer_addresses_country": str(country),
                }              
    @classmethod
    def ExtractAddressGroups(self,roParty):
        address_groups={}
        if "address" in roParty:
            for address in roParty["address"]:
                if "addressType" in address:
                    addressType=address["addressType"]
                    if addressType not in address_groups:
                        addressLine=[]  
                        city=""
                        state=""
                        zip=""  
                        country=""                                                                                   
                        if "lineOne" in address and len(address['lineOne'])>0 :                                        
                            addressLine.append(str(address['lineOne']))
                        if "lineTwo" in address and len(address['lineTwo'])>0 :                                        
                            addressLine.append(str(address['lineTwo']))
                        if "lineThree" in address and len(address['lineThree'])>0 :                                        
                            addressLine.append(str(address['lineThree']))
                        if "lineFour" in address and len(address['lineFour'])>0 :                                        
                            addressLine.append(str(address['lineFour']))   
                        if "cityName" in address:
                            city=address["cityName"]
                        if "postCode" in address:
                            zip=address["postCode"]
                        if "stateOrProvinceCountrySubDivisionId" in address:
                            state=address["stateOrProvinceCountrySubDivisionId"]
                        if "countryId" in address: 
                            country=address["countryId"]
                        address_groups[addressType] = {
                                                   "addressLine": addressLine,
                                                    "city":str(city),
                                                    "state": str(state),
                                                    "zip": str(zip),
                                                    "country": str(country),
                                                    } 
        return address_groups
    
    @classmethod
    def ExtractCustomerDetail(self,roParty):
        companyName=""
        personName=""
        givenName=""
        middleName=""
        familyName=""
        address_groups = {} 
        mobilePhone=""
        homePhone=""
        workPhone=""
        otherPhone=""
        faxPhone=""
        email=""
        #Company
        
        if 'companyName' in roParty:
            companyName= roParty["companyName"] 
            familyName=companyName
                
        #Person
                  
        if 'personName' in roParty:                        
            personName= roParty["personName"]
            
        if 'givenName' in roParty:                        
            givenName= roParty["givenName"] 
            
        if 'middleName' in roParty:                        
            middleName= roParty["middleName"] 
            
        if 'familyName' in roParty:
            familyName= roParty["familyName"] 
        if "communication" in roParty :
            for communication in roParty['communication']:
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
        address_groups=self.ExtractAddressGroups(roParty)  
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
                    "addressGroups":address_groups                
                }

    @classmethod
    def ExtractPartyDetail(self,roParty):
        primaryContact=None
        customerId=None
        partyDetail=self.ExtractCustomerDetail(roParty)
        #primaryContact
        if 'primaryContact' in roParty and  roParty['primaryContact'] is not None:
            primaryContact= self.ExtractCustomerDetail(roParty['primaryContact']  )
        #Id
        if 'idList' in roParty and len(roParty["idList"])>0:  
            idList=roParty['idList']                                  
            for id in idList:
                if 'typeId' in id and 'id' in id and id['typeId'] == 'DMSId': 
                    customerId= id["id"]                                    
                    break
        address_groups=self.ExtractAddressGroups(roParty)               
        return  {                 
                "partyId":str(customerId), 
                "partyDetail": partyDetail,                 
                "primaryContact":primaryContact ,
                "addressGroups":address_groups   
            }
                
                
        
        