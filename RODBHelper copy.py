 
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
            df_final=df["df_final"]
            df_opcodes=df["df_opcodes"]
            RO_DATA=None
            OPCODES=None
            if  df_opcodes is not None:
                RO_DATA= df_opcodes.groupby(["REFER"]) 

            for index, row in df_final.iterrows():
                recount=recount+1
                #row["CUSTOMER LINE1"]
                OPCODES=None
                if RO_DATA is not None:
                    RO_OPCODES_DATA=RO_DATA.get_group(str(row["REFER"]))
                    if RO_OPCODES_DATA is not None and len(RO_OPCODES_DATA)>0:
                        OPCODES=[]
                        for OP_CODE in RO_OPCODES_DATA:
                            if str(OP_CODE).strip() not  in OPCODES:
                                OPCODES.append(str(OP_CODE).strip())
                        

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
               
                open_date_time=self.FormatDate(open_dt_str=row["OPEN-DATE"],print_time_seconds_str=row["PRINT-TIME"])
                
                close_date_time=self.FormatDate(open_dt_str=row["CLOSED"],print_time_seconds_str="")
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
                            "comments": dbhelper.RemoveNewLineCarrigeReturn(row["COMMENTS"]).strip(),
                            "vehicle_vin":dbhelper.CovertToString( row["SERIAL NO"]),
                            "vehicle_make": dbhelper.CovertToString(row["MAKE"]),
                            "vehicle_model": dbhelper.CovertToString(row["MODEL"]),
                            "vehicle_year": dbhelper.CovertToString(row["YR"]),
                            "customer_id": dbhelper.CovertToString(row["CUST"]),
                            "customer_first_name":dbhelper.CovertToString( custFNM),
                            "customer_last_name": dbhelper.CovertToString(custLNM),
                            "customer_email": dbhelper.CovertToString(row["EMAIL"]),
                            "customer_addresses_addressline": dbhelper.CovertToString(row["CUSTOMER LINE3"]),
                            "customer_addresses_addressline1": "",
                            "customer_addresses_city": dbhelper.CovertToString(row["CITY"]),
                            "customer_addresses_state": dbhelper.CovertToString(row["STATE"]),
                            "customer_addresses_zip": dbhelper.CovertToString(row["ZIP"]),
                            "customer_addresses_contact_number":dbhelper.CovertToString(row["PHONE"]) ,
                            "employee_id": dbhelper.CovertToString(row["SR-R"]),
                            "employee_first_name":dbhelper.CovertToString( empFNM),
                            "employee_last_name": dbhelper.CovertToString(empLNM),
                            "hat_tag_number":dbhelper.CovertToString( row["TAG-NO"]),
                            "contact_email": dbhelper.CovertToString(row["EMAIL"]),
                            "contact_phone": dbhelper.CovertToString(row["PHONE"]),
                            "spc_ins": dbhelper.RemoveNewLineCarrigeReturn(row["EPDE.SPC.INS"]).strip(),
                            "warranty_due": dbhelper.CovertToDecimalNumericString(row["WP"]),
                            "create_date": dbhelper.CovertToString(create_date_only),
                            "create_date_time":create_date
                            }
                if OPCODES is not None and len(OPCODES)>0:
                    item['OPCODES']:OPCODES

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
            if not local:
                dbhelper.submitSaveData(TableName=TableName,items=batch_items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
            else:
                 dbhelper.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
                       
            et = datetime.now()
            delta=et-ct
            logger.debug("after calling lambda total_seconds="+str(delta.total_seconds()))             
            logger.info("WIP Data has been inserted successfuly in table ["+TableName+"] Record Count=."+str(recount))
            return { "operation_status":"SUCCESS","total_item_count":str(recount),"total_item_parsed":str(len(items)),"items":items} 
        except Exception as e:
            # Return failed record's sequence number
            logger.error("Error Occure in Save wip data to table ["+TableName+"] in DB..." , exc_info=True)
            ex_type, ex_value, ex_traceback = sys.exc_info()
            return { "operation_status":"FAILED","error_code":-1 ,"error_message":ex_value} 
    
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
                    open_date_time=open_date_time_dt.strftime("%Y-%m-%d %I:%M:%S %p").lower()
               
                closedTime=None
                if 'closedTime' in row:
                    closedTime=row['closedTime']   
                close_date_time=""
                if closedTime is not None: 
                    close_date_time_dt = datetime.fromtimestamp(closedTime / 1000)
                    close_date_time=close_date_time_dt.strftime("%Y-%m-%d %I:%M:%S %p").lower()

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
                OPCODES=None
                if  'jobs' in row and row['jobs'] is not None and len( row['jobs'])>0:
                    for job in  row['jobs']:
                        if  'operations' in job and job['operations'] is not None and len( job['operations'])>0: 
                            OPCODES=[]  
                            for operation in job['operations'] :
                                if 'opcode' in operation and operation['opcode'] is not None:
                                    if str(operation['opcode']) not  in OPCODES:
                                        OPCODES.append(str(operation['opcode']))
                
                amount_due=str(row["totalWithTax"])
                tk_ro_status=row["status"]
                if tk_ro_status=='INVOICED' and  'invoice' in row and row['invoice'] is not None:
                    invoice=row['invoice']
                    if 'invoiceAmount' in invoice and invoice["invoiceAmount"] is not None:
                        amount_due=str(invoice["invoiceAmount"])

                    
                item={
                            "client_id": str(client_id),
                            "store_code": str(store_code),
                            "document_id": str(row["repairOrderNumber"]),
                            "document_type": "RO",
                            "status": sts,
                            "open_date_time": open_date_time,
                            "close_date_time":close_date_time,
                            "amount_due": str(amount_due),
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
                            "create_date_time":create_date
                            }
                if OPCODES is not None and len(OPCODES)>0:
                    item['OPCODES']:OPCODES  

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
            if not local:
                dbhelper.submitSaveData(TableName=TableName,items=batch_items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
            else:
                 dbhelper.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
                       
            et = datetime.now()
            delta=et-ct
            logger.debug("after calling lambda total_seconds="+str(delta.total_seconds()))             
            logger.info("TEKION RO WIP Data has been inserted successfuly in table ["+TableName+"] Record Count=."+str(recount))
            return { "operation_status":"SUCCESS","total_item_count":str(recount),"total_item_parsed":str(len(items)),"items":items} 
        except Exception as e:
            # Return failed record's sequence number
            logger.error("Error Occure in Save TEKION RO WIP data to table ["+TableName+"] in DB..." , exc_info=True)
            ex_type, ex_value, ex_traceback = sys.exc_info()
            return { "operation_status":"FAILED","error_code":-1 ,"error_message":ex_value} 