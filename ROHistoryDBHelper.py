import boto3
import concurrent.futures 
import json
import sys
import logging
import os
from datetime import datetime
from DBHelper import DBHelper
import Batching
from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.conditions import Key 
loglevel = int(os.environ['LOG_LEVEL'])
logger = logging.getLogger('ROHistoryDBHelper')
logger.setLevel(loglevel)
dbHelper=DBHelper()
class ROHistoryDBHelper():
    @classmethod
    def SaveROHistoryNew(self,store_code,df_final,client_id):
        logger.info("Inside SaveROHistoryNew Method....store_code:"+store_code+",client_id="+client_id)
        TableName=client_id+"_ROHISTORY_FILE"
        try:
            region=os.environ['REGION']
            items=[]
            LAMBDA_BATCH_SIZE=100
            local=True
            batch_items=[]
            ct = datetime.now()
            create_date = ct.strftime("%Y-%m-%d %I:%M:%S %p")
            create_date_only = ct.strftime("%Y-%m-%d")
            recount=0
            df=df_final["ro-hist"]
            df_lines_opcodes=df_final["hist-opcodes"]   
            account_lines=df_final['account_line']
            accCodeList=['S']
            if not dbHelper.ValidateAccounts(store_code=store_code,account_lines=account_lines,region=region,accCodeList=accCodeList):
                lines=str(','.join(account_lines))
                logger.error("Error Occure in Save data to table ["+TableName+"] in DB. INVALID LOGON IDENTIFIED ,Logon in Data:"+lines,exc_info=True)
                return { "operation_status":"FAILED","error_code":-1 ,"error_message":"INVALID LOGON IDENTIFIED, SKIP SAVE IN DB :"+lines,"total_item_count":"0","total_item_parsed":"0","items":[]} 
       
            RO_DATA_1= df_lines_opcodes.groupby(["RO"])
            HIST_GROUPED_BY_VIN = df.to_dict('records')
            for vin_records in df.to_dict('records'):
                recount=recount+1            
                 
                VIN=str(vin_records["SERIAL"]).strip() 
                vechicle={
                    "vin": VIN,
                    "make": dbHelper.RemoveNewLineCarrigeReturn(vin_records["MAKE"]),  
                    "model":dbHelper.RemoveNewLineCarrigeReturn(vin_records["MODEL"]),         
                    "year":str(vin_records["YEAR"]).strip(),
                    "color":dbHelper.RemoveNewLineCarrigeReturn(vin_records["COLOR"]),
                    "delDate": str(vin_records["DELIVERY.DATE"]).strip(),
                    "dlrCd": str(vin_records["DEALER-CD"]).strip(),
                    "vehId": str(vin_records["VEH-ID"]).strip(),
                    "vehLastSvcDate":str(vin_records["VEH-LAST-SVC-DATE"]).strip(),
                    "warrExpDate":str(vin_records["WARR-EXP-DATE"]).strip(),           
                    "stockType":str(vin_records["STOCK.TYPE"]).strip(),        
                    "stockNo": str(vin_records['STOCK-NO']).strip(),
                    "soldDate":str(vin_records['SOLD.DATE']).strip()
                }
                cust_FLNM=vin_records['CUST-NAME']                    
                custFNM=""
                custLNM=""
                CUST=str(vin_records['CUST-NBR']).strip()
                if len(cust_FLNM)>0 and cust_FLNM.__contains__(','):

                    cust_FLNM_arr=cust_FLNM.split(',')
                    if len(cust_FLNM_arr)>1:
                            custLNM=cust_FLNM_arr[0]
                            custFNM=cust_FLNM_arr[1]
                    else:
                            custLNM=cust_FLNM                                                  
                else:
                    custLNM=cust_FLNM            
                customer= {
                    "id": str(CUST).strip(),
                    "firstName":custFNM,
                    "lastName": custLNM,
                    "addressLine1": dbHelper.RemoveNewLineCarrigeReturn(vin_records['ADDRESS']),
                    "addressLine2": "",
                    "city": str(vin_records['CITY']).strip(),
                    "state":str(vin_records['STATE']).strip(), 
                    "zip": str(vin_records['ZIP']).strip(),
                    "email": str(vin_records['EMAIL']).strip(),   
                    "businessPhone": str(vin_records['B-PHONE']).strip(),    
                    "homePhone": str(vin_records['H-PHONE']).strip(),
                    "cellPhone": str(vin_records['CELL-PHONE']).strip(),  
                    "contactNumber": "",                    
                    "custLastSvcDate": str(vin_records['CUST-LAST-SVC-DATE']).strip()
                } 
                ro_records=vin_records
                RO=str(ro_records["RO"]).strip()
                ro_item={
                    "ro":RO,                           
                    "openDate": str(ro_records["OPENED"]).strip(),                            
                    "closeDate":  str(ro_records["CLOSE-DATE"]).strip(),                            
                    "amountDue": str(ro_records["RO-TOTAL"]).strip(),                         
                    "mileage": str(ro_records["MILEAGE"]).strip(),
                    "saNo": str(ro_records["RESNO"]).strip(),
                
                    "comeback": str(ro_records["COMEBACK"]).strip(),
                    "deniedBy": str(ro_records["DENIED-BY"]).strip(),
                    "est": str(ro_records["EST"]).strip(),
                    "resNo":str(ro_records["RESNO"]).strip(),
                                            
                    "ipSoldHrs": str(ro_records["IP-SOLD-HRS"]).strip(),
                    "ipActualHrs": str(ro_records["IP-ACTUAL-HRS"]).strip(),                            
                    "ipSublSale": str(ro_records["IP-SUBL-SALE$"]).strip(),
                    "ipSublCost": str(ro_records["IP-SUBL-COST$"]).strip(),
                    "ipLaborCost": str(ro_records["IP-LABOR-COST$"]).strip(),
                    "ipLaborSale": str(ro_records["IP-LABOR-SALE$"]).strip(),
                    "ipPartCost": str(ro_records["IP-PART-COST$"]).strip(),
                    "ipPartSale": str(ro_records["IP-PART-SALE$"]).strip(),                            
                    "ipTotalSale": str(ro_records["IP-TOTAL-SALE$"]).strip(),
                    "ipTotalCost": str(ro_records["IP-TOTAL-COST$"]).strip(),

                    "cpSoldHrs": str(ro_records["CP-SOLD-HOURS"]).strip(),
                    "cpActualHrs": str(ro_records["CP-ACTUAL-HRS"]).strip(),
                    "cpSublSale": str(ro_records["CP-SUBL-SALE$"]).strip(),
                    "cpSublCost": str(ro_records["CP-SUBL-COST$"]).strip(),
                    "cpLaborCost": str(ro_records["CP-LABOR-COST$"]).strip(),
                    "cpLaborSale": str(ro_records["CP-LABOR-SALE$"]).strip(),
                    "cpPartCost": str(ro_records["CP-PART-COST$"]).strip(),
                    "cpPartSale": str(ro_records["CP-PART-SALE$"]).strip(),
                    "cpTotalSale": str(ro_records["CP-TOTAL-SALE$"]).strip(),
                    "cpTotalCost": str(ro_records["CP-TOTAL-COST$"]).strip(),

                    "wpSoldHrs": str(ro_records["WP-SOLD-HOURS"]).strip(),
                    "wpActulHrs": str(ro_records["WP-ACTUAL-HRS"]).strip(),
                    "wpSublSale": str(ro_records["WP-SUBL-SALE$"]).strip(),
                    "wpSublCost": str(ro_records["WP-SUBL-COST$"]).strip(),
                    "wpLaborCost": str(ro_records["WP-LABOR-COST$"]).strip(),
                    "wpLaborSale": str(ro_records["WP-LABOR-SALE$"]).strip(),
                    "wpPartCost": str(ro_records["WP-PART-COST$"]).strip(),
                    "wpPartSale": str(ro_records["WP-PART-SALE$"]).strip(),
                    "wpTotalCost": str(ro_records["WP-TOTAL-COST$"]).strip(),
                    "wpTotalSale": str(ro_records["WP-TOTAL-SALE$"]).strip()                           
                                            
                } 
                RO_DATA1=RO_DATA_1.get_group(ro_records["RO"])
                RO_LINES = RO_DATA1.groupby(["LINE-CODE"])
                serviceDetails=[]
                for LINE_CODE, LINES_DATA in RO_LINES:
                    lines_records=LINES_DATA.to_dict('records') 
                    serviceLine_item={
                        "lineCode": LINE_CODE,
                        "cause"   :dbHelper.RemoveNewLineCarrigeReturn(lines_records[0]["CAUSE"])                             
                    }                    
                    LINE_OPERATIONS = LINES_DATA.groupby(["OP-CODES"])
                    operations=[]  
                    comment=""                        
                    for OP_CODE, OPCODE_DATA in LINE_OPERATIONS:
                        opcode_records=OPCODE_DATA.to_dict('records') 
                        #logger.debug("opcode_records="+str(opcode_records))     
                        opcode_item={                                     
                            "opCode":dbHelper.RemoveNewLineCarrigeReturn(OP_CODE),
                            "opCodeDesc":dbHelper.RemoveNewLineCarrigeReturn(opcode_records[0]['OP-DESC']),
                            "lbrCost": str(opcode_records[0]["LBR-COST$"]).strip(),
                            "ptsCost": str(opcode_records[0]["PTS-COST$"]).strip(),
                            "lbrSaleAmts":str(opcode_records[0]["LBR-SALE-AMTS"]),
                            "lbrTypes": str(opcode_records[0]['LBR-TYPES']).strip(),
                            "ptsSaleAmts":str(opcode_records[0]['PTS-SALE-AMTS']).strip(),
                            "miscSaleAmts":str(opcode_records[0]['MISC-SALE-AMTS']).strip()                                  
                                                                
                        }
                        if 'PART-NUM' in OPCODE_DATA:                            
                            OPCODE_PARTS=OPCODE_DATA.groupby(["PART-NUM"])
                            parts=[]
                            for PART_NUM, PARTS_DATA in OPCODE_PARTS:
                                part_records=PARTS_DATA.to_dict('records')  
                                #logger.debug("part_records="+str(part_records)) 
                                if len(dbHelper.RemoveNewLineCarrigeReturn(PART_NUM))>0:
                                    part_item={  
                                        "partNum":dbHelper.RemoveNewLineCarrigeReturn(PART_NUM),
                                        "partDesc":dbHelper.RemoveNewLineCarrigeReturn(part_records[0]["PART-DESC"]),
                                        "qtyOrd":str(part_records[0]["PART-QTYORD"]).strip(),
                                        "partCost":str(part_records[0]["PART-COST"]).strip(),
                                        "partSale":str(part_records[0]["PART-SALE"]).strip()
                                    }
                                    parts.append(part_item)
                            opcode_item["parts"]=parts
                        else:
                            opcode_item["parts"]=[]
                        comment=comment+opcode_records[0]['COMMENT']
                        operations.append(opcode_item)
                    #end for operations
                    serviceLine_item["operations"]=operations
                    serviceLine_item["comments"]=dbHelper.RemoveNewLineCarrigeReturn(comment)
                    serviceDetails.append(serviceLine_item)
                #end for service lines
                ro_item["serviceDetails"]=serviceDetails                
                closeDate=str(ro_records["CLOSE-DATE"]).strip()
                try:
                    closeDateDt=datetime.strptime(closeDate, '%d%b%y')
                    searchDate = closeDateDt.strftime("%Y-%m-%d")  
                    

                except ValueError:
                    searchDate= ""
              
                item={
                        "document_id": VIN+"_"+RO,
                        "document_type": "ROHIST",
                        "vin":VIN,
                        "customerNo":CUST,
                        "roNo":RO,
                        "vechicle":vechicle,
                        "customer":customer,
                        "ro":ro_item,
                        "createDateTime":str(create_date)  ,
                        "createDate":str(create_date_only) ,
                        "searchCloseDate":searchDate,                                         
                        "roCount" :1                         
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
            TableName=store_code+"_ROHISTORY_FILE"   
            MAX_PROC=4
            BATCH_SIZE=25              
            overwrite_by_pkeys=['document_type', 'document_id']
            if not local:
                dbHelper.submitSaveData(TableName=TableName,items=batch_items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
            else:
                self.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
            et = datetime.now()
            delta=et-ct
            logger.debug("after calling lambda total_seconds="+str(delta.total_seconds()))
            logger.info("ROHistory Data has been inserted successfuly in table ["+TableName+"] Record Count=."+str(recount))
            return { "operation_status":"SUCCESS","total_item_count":str(recount),"total_item_parsed":str(len(items)),"items":items} 
        except Exception as e:
            # Return failed record's sequence number
            logger.error("Error Occure in Save ROHistory data to table ["+TableName+"] in DB..." , exc_info=True)
            ex_type, ex_value, ex_traceback = sys.exc_info()
            return { "operation_status":"FAILED","error_code":-1 ,"error_message":ex_value}          

    @classmethod
    def SaveROHistory(self,store_code,df_final,client_id):
        logger.info("Inside SaveROHistory Method....store_code:"+store_code+",client_id="+client_id)
        TableName=client_id+"_ROHISTORY_FILE"
        try:
            region=os.environ['REGION']
            items=[]
            LAMBDA_BATCH_SIZE=100
            local=True
            batch_items=[]
            ct = datetime.now()
            create_date = ct.strftime("%Y-%m-%d %I:%M:%S %p")
            create_date_only = ct.strftime("%Y-%m-%d")
            recount=0
            df=df_final["ro-hist"]
            df_lines_opcodes=df_final["hist-opcodes"]   
            account_lines=df_final['account_line']
            accCodeList=['S']
            if not dbHelper.ValidateAccounts(store_code=store_code,account_lines=account_lines,region=region,accCodeList=accCodeList):
                lines=str(','.join(account_lines))
                logger.error("Error Occure in Save data to table ["+TableName+"] in DB. INVALID LOGON IDENTIFIED ,Logon in Data:"+lines,exc_info=True)
                return { "operation_status":"FAILED","error_code":-1 ,"error_message":"INVALID LOGON IDENTIFIED, SKIP SAVE IN DB :"+lines,"total_item_count":"0","total_item_parsed":"0","items":[]} 
       
            RO_DATA_1= df_lines_opcodes.groupby(["RO"])

            HIST_GROUPED_BY_VIN = df.groupby(["SERIAL"])
            for VIN, VIN_DATA in HIST_GROUPED_BY_VIN:
                recount=recount+1             
                vin_records=VIN_DATA.to_dict('records')
                vechicle={
                    "vin": VIN,
                    "make": dbHelper.RemoveNewLineCarrigeReturn(vin_records[0]["MAKE"]),  
                    "model":dbHelper.RemoveNewLineCarrigeReturn(vin_records[0]["MODEL"]),         
                    "year":str(vin_records[0]["YEAR"]).strip(),
                    "color":dbHelper.RemoveNewLineCarrigeReturn(vin_records[0]["COLOR"]),
                    "delDate": str(vin_records[0]["DELIVERY.DATE"]).strip(),
                    "dlrCd": str(vin_records[0]["DEALER-CD"]).strip(),
                    "vehId": str(vin_records[0]["VEH-ID"]).strip(),
                    "vehLastSvcDate":str(vin_records[0]["VEH-LAST-SVC-DATE"]).strip(),
                    "warrExpDate":str(vin_records[0]["WARR-EXP-DATE"]).strip(),           
                    "stockType":str(vin_records[0]["STOCK.TYPE"]).strip(),        
                    "stockNo": str(vin_records[0]['STOCK-NO']).strip(),
                    "soldDate":str(vin_records[0]['SOLD.DATE']).strip()
                }
                VIN_GROUPED_BY_CUST = VIN_DATA.groupby(["CUST-NBR"])
                for CUST, CUST_DATA in VIN_GROUPED_BY_CUST:
                    cust_records=CUST_DATA.to_dict('records') 
                    cust_FLNM=cust_records[0]['CUST-NAME']                    
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
                    customer= {
                        "id": str(CUST).strip(),
                        "firstName":custFNM,
                        "lastName": custLNM,
                        "addressLine1": dbHelper.RemoveNewLineCarrigeReturn(cust_records[0]['ADDRESS']),
                        "addressLine2": "",
                        "city": str(cust_records[0]['CITY']).strip(),
                        "state":str(cust_records[0]['STATE']).strip(), 
                        "zip": str(cust_records[0]['ZIP']).strip(),
                        "email": str(cust_records[0]['EMAIL']).strip(),   
                        "businessPhone": str(cust_records[0]['B-PHONE']).strip(),    
                        "homePhone": str(cust_records[0]['H-PHONE']).strip(),
                        "cellPhone": str(cust_records[0]['CELL-PHONE']).strip(),  
                        "contactNumber": "",                    
                        "custLastSvcDate": str(cust_records[0]['CUST-LAST-SVC-DATE']).strip()
                    }           
                    ro_list=[]          
                    CUST_RO = CUST_DATA.groupby(["RO"])
                    roMinCloseDate=None
                    roMaxCloseDate=None
                    for RO, RO_DATA in CUST_RO:
                        ro_records=RO_DATA.to_dict('records')  
                        res_dt= self.calculateCloseDate( str(ro_records[0]["CLOSE-DATE"]).strip(),roMaxCloseDate,roMinCloseDate)
                        roMinCloseDate=res_dt['roMinCloseDate']
                        roMaxCloseDate=res_dt['roMaxCloseDate']                      
                        ro_item={
                            "ro":str(RO).strip(),                           
                            "openDate": str(ro_records[0]["OPENED"]).strip(),                            
                            "closeDate":  str(ro_records[0]["CLOSE-DATE"]).strip(),                            
                            "amountDue": str(ro_records[0]["RO-TOTAL"]).strip(),                         
                            "mileage": str(ro_records[0]["MILEAGE"]).strip(),
                            "saNo": str(ro_records[0]["RESNO"]).strip(),
                           
                            "comeback": str(ro_records[0]["COMEBACK"]).strip(),
                            "deniedBy": str(ro_records[0]["DENIED-BY"]).strip(),
                            "est": str(ro_records[0]["EST"]).strip(),
                            "resNo":str(ro_records[0]["RESNO"]).strip(),
                                                       
                            "ipSoldHrs": str(ro_records[0]["IP-SOLD-HRS"]).strip(),
                            "ipActualHrs": str(ro_records[0]["IP-ACTUAL-HRS"]).strip(),                            
                            "ipSublSale": str(ro_records[0]["IP-SUBL-SALE$"]).strip(),
                            "ipSublCost": str(ro_records[0]["IP-SUBL-COST$"]).strip(),
                            "ipLaborCost": str(ro_records[0]["IP-LABOR-COST$"]).strip(),
                            "ipLaborSale": str(ro_records[0]["IP-LABOR-SALE$"]).strip(),
                            "ipPartCost": str(ro_records[0]["IP-PART-COST$"]).strip(),
                            "ipPartSale": str(ro_records[0]["IP-PART-SALE$"]).strip(),                            
                            "ipTotalSale": str(ro_records[0]["IP-TOTAL-SALE$"]).strip(),
                            "ipTotalCost": str(ro_records[0]["IP-TOTAL-COST$"]).strip(),

                            "cpSoldHrs": str(ro_records[0]["CP-SOLD-HOURS"]).strip(),
                            "cpActualHrs": str(ro_records[0]["CP-ACTUAL-HRS"]).strip(),
                            "cpSublSale": str(ro_records[0]["CP-SUBL-SALE$"]).strip(),
                            "cpSublCost": str(ro_records[0]["CP-SUBL-COST$"]).strip(),
                            "cpLaborCost": str(ro_records[0]["CP-LABOR-COST$"]).strip(),
                            "cpLaborSale": str(ro_records[0]["CP-LABOR-SALE$"]).strip(),
                            "cpPartCost": str(ro_records[0]["CP-PART-COST$"]).strip(),
                            "cpPartSale": str(ro_records[0]["CP-PART-SALE$"]).strip(),
                            "cpTotalSale": str(ro_records[0]["CP-TOTAL-SALE$"]).strip(),
                            "cpTotalCost": str(ro_records[0]["CP-TOTAL-COST$"]).strip(),

                            "wpSoldHrs": str(ro_records[0]["WP-SOLD-HOURS"]).strip(),
                            "wpActulHrs": str(ro_records[0]["WP-ACTUAL-HRS"]).strip(),
                            "wpSublSale": str(ro_records[0]["WP-SUBL-SALE$"]).strip(),
                            "wpSublCost": str(ro_records[0]["WP-SUBL-COST$"]).strip(),
                            "wpLaborCost": str(ro_records[0]["WP-LABOR-COST$"]).strip(),
                            "wpLaborSale": str(ro_records[0]["WP-LABOR-SALE$"]).strip(),
                            "wpPartCost": str(ro_records[0]["WP-PART-COST$"]).strip(),
                            "wpPartSale": str(ro_records[0]["WP-PART-SALE$"]).strip(),
                            "wpTotalCost": str(ro_records[0]["WP-TOTAL-COST$"]).strip(),
                            "wpTotalSale": str(ro_records[0]["WP-TOTAL-SALE$"]).strip()                            
                                            
                        }
                        RO_DATA1=RO_DATA_1.get_group(RO)
                        RO_LINES = RO_DATA1.groupby(["LINE-CODE"])
                        serviceDetails=[]
                        for LINE_CODE, LINES_DATA in RO_LINES:
                            lines_records=LINES_DATA.to_dict('records') 
                            serviceLine_item={
                                "lineCode": LINE_CODE,
                                "cause"   :dbHelper.RemoveNewLineCarrigeReturn(lines_records[0]["CAUSE"])                             
                            }                    
                            LINE_OPERATIONS = LINES_DATA.groupby(["OP-CODES"])
                            operations=[]  
                            comment=""                        
                            for OP_CODE, OPCODE_DATA in LINE_OPERATIONS:
                                opcode_records=OPCODE_DATA.to_dict('records') 
                                #logger.debug("opcode_records="+str(opcode_records))     
                                opcode_item={                                     
                                    "opCode":dbHelper.RemoveNewLineCarrigeReturn(OP_CODE),
                                    "opCodeDesc":dbHelper.RemoveNewLineCarrigeReturn(opcode_records[0]['OP-DESC']),
                                    "lbrCost": str(opcode_records[0]["LBR-COST$"]).strip(),
                                    "ptsCost": str(opcode_records[0]["PTS-COST$"]).strip(),
                                    "lbrSaleAmts":str(opcode_records[0]["LBR-SALE-AMTS"]),
                                    "lbrTypes": str(opcode_records[0]['LBR-TYPES']).strip(),
                                    "ptsSaleAmts":str(opcode_records[0]['PTS-SALE-AMTS']).strip(),
                                    "miscSaleAmts":str(opcode_records[0]['MISC-SALE-AMTS']).strip()                                  
                                                                       
                                }
                                if 'PART-NUM' in OPCODE_DATA:                                    
                                    OPCODE_PARTS=OPCODE_DATA.groupby(["PART-NUM"])
                                    parts=[]
                                    for PART_NUM, PARTS_DATA in OPCODE_PARTS:
                                        part_records=PARTS_DATA.to_dict('records')  
                                        #logger.debug("part_records="+str(part_records)) 
                                        if len(dbHelper.RemoveNewLineCarrigeReturn(PART_NUM))>0:
                                            part_item={  
                                                "partNum":dbHelper.RemoveNewLineCarrigeReturn(PART_NUM),
                                                "partDesc":dbHelper.RemoveNewLineCarrigeReturn(part_records[0]["PART-DESC"]),
                                                "qtyOrd":str(part_records[0]["PART-QTYORD"]).strip(),
                                                "partCost":str(part_records[0]["PART-COST"]).strip(),
                                                "partSale":str(part_records[0]["PART-SALE"]).strip()
                                            }
                                            parts.append(part_item)
                                    opcode_item["parts"]=parts
                                else:
                                   opcode_item["parts"]=[] 
                                comment=comment+opcode_records[0]['COMMENT']
                                operations.append(opcode_item)
                            #end for operations
                            serviceLine_item["operations"]=operations
                            serviceLine_item["comments"]=dbHelper.RemoveNewLineCarrigeReturn(comment)
                            serviceDetails.append(serviceLine_item)
                        #end for service lines
                        ro_item["serviceDetails"]=serviceDetails
                        ro_list.append(ro_item)
                    #end for ro-item
                   
                    item={
                        "document_id": VIN+"_"+CUST,
                        "document_type": "ROHIST",
                        "vin":VIN,
                        "customerNo":CUST,
                        "vechicle":vechicle,
                        "customer":customer,
                        "roList":ro_list,
                        "createDateTime":str(create_date)  ,
                        "createDate":str(create_date_only) ,
                        "roMinCloseDate":roMinCloseDate,
                        "roMaxCloseDate":roMaxCloseDate,
                        "roCount" :len(ro_list)                         
                        } 
                    if item["roMinCloseDate"] == item["roMaxCloseDate"] :
                       item["searchCloseDate"]=item["roMinCloseDate"]
                    else:
                       item["searchCloseDate"]=""
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
            TableName=store_code+"_ROHISTORY_FILE"   
            MAX_PROC=4
            BATCH_SIZE=25              
            overwrite_by_pkeys=['document_type', 'document_id']
            if not local:
                dbHelper.submitSaveData(TableName=TableName,items=batch_items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=True,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
            else:
                self.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=True,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
                       
            et = datetime.now()
            delta=et-ct
            logger.debug("after calling lambda total_seconds="+str(delta.total_seconds()))
            logger.info("ROHistory Data has been inserted successfuly in table ["+TableName+"] Record Count=."+str(recount))
            return { "operation_status":"SUCCESS","total_item_count":str(recount),"total_item_parsed":str(len(items)),"items":items} 
        except Exception as e:
            # Return failed record's sequence number
            logger.error("Error Occure in Save ROHistory data to table ["+TableName+"] in DB..." , exc_info=True)
            ex_type, ex_value, ex_traceback = sys.exc_info()
            return { "operation_status":"FAILED","error_code":-1 ,"error_message":ex_value}          

    @classmethod
    def calculateCloseDate(self,closeDate,roMaxCloseDate,roMinCloseDate):
        searchDate=""   
        roMinCloseDateDt=None
        roMaxCloseDateDt=None  
        if closeDate is not None and len(closeDate)>0:
            if roMinCloseDate is not None and len(roMinCloseDate)>0:
                roMinCloseDateDt=datetime.strptime(roMinCloseDate, "%Y-%m-%d")
            if roMaxCloseDate is not None and len(roMaxCloseDate)>0:
                roMaxCloseDateDt=datetime.strptime(roMaxCloseDate, "%Y-%m-%d")
                
            try:
                closeDateDt=datetime.strptime(closeDate, '%d%b%y')
                searchDate = closeDateDt.strftime("%Y-%m-%d")  
                if roMaxCloseDateDt is None:
                    roMaxCloseDateDt=closeDateDt
                else:
                    if roMaxCloseDateDt<closeDateDt:
                        roMaxCloseDateDt=closeDateDt
                if roMinCloseDateDt is None:
                    roMinCloseDateDt=closeDateDt
                else:
                    if roMinCloseDateDt>closeDateDt:
                        roMinCloseDateDt=closeDateDt 

            except ValueError:
                searchDate= ""
        if roMaxCloseDateDt is not None:
            roMaxCloseDate=str(roMaxCloseDateDt.strftime("%Y-%m-%d"))
        if roMinCloseDateDt is not None:
            roMinCloseDate=str(roMinCloseDateDt.strftime("%Y-%m-%d"))
        return {"searchDate":searchDate,"roMinCloseDate":roMinCloseDate,"roMaxCloseDate":roMaxCloseDate}
        
    @classmethod
    def handleROHistoryTableDataUpdate(self,dynamodb,batch_list,table_name):
        logger.debug("handleROHistoryTableDataUpdate  batch_list size="+str(len(batch_list))+",table_name="+table_name)  
        tab_existingitems=Batching.get_batch_data(dynamodb=dynamodb,tableName=table_name,item_list=batch_list)
        existing_items=tab_existingitems[table_name]
        logger.debug("get_batch_data  existing_items size="+str(len(existing_items)))  
        new_batch_list=[]
        if len(existing_items)==0:
            new_batch_list=batch_list
        else:
            for rohist_item in batch_list:
                vin_cust_item_list = [existing_item for existing_item in existing_items if (existing_item['document_type'] == rohist_item['document_type']) and (existing_item['document_id'] == rohist_item['document_id'])]
                if len(vin_cust_item_list)>0:
                    old_rohist_item=vin_cust_item_list[0]                   
                    if len(old_rohist_item["roList"])>0:
                        updatedROList=rohist_item["roList"]
                        roMaxCloseDate=rohist_item['roMaxCloseDate']
                        roMinCloseDate=rohist_item['roMinCloseDate']
                        for old_ro in old_rohist_item["roList"]:
                            ro_item_list = [new_ro for new_ro in rohist_item["roList"] if (new_ro['ro'] == old_ro['ro']) ]
                            if len(ro_item_list)==0:
                                updatedROList.append(old_ro)
                                res_dt= self.calculateCloseDate(old_ro['closeDate'],roMaxCloseDate,roMinCloseDate)
                                roMinCloseDate=res_dt['roMinCloseDate']
                                roMaxCloseDate=res_dt['roMaxCloseDate']   
                                                  
                        rohist_item["roList"]=updatedROList
                        rohist_item["roMinCloseDate"] =roMinCloseDate
                        rohist_item["roMaxCloseDate"] =roMaxCloseDate
                        if rohist_item["roMinCloseDate"] == rohist_item["roMaxCloseDate"] :
                            rohist_item["searchCloseDate"]=rohist_item["roMinCloseDate"]
                        else:
                            rohist_item["searchCloseDate"]=""
                        rohist_item["roCount"]=len(updatedROList)  
                      
                    new_batch_list.append(rohist_item)
                else:
                    new_batch_list.append(rohist_item)
        return new_batch_list
    @classmethod
    def deduplicationRO1(self,dynamodb,batch_list,table_name):        
        updated_batch_list=[]
        delete_batch_list=[]
        if batch_list is not None and len(batch_list)>0:
            try:
                logger.debug("deduplicationRO  batch_list size="+str(len(batch_list))+",table_name="+table_name)
                existing_items=[] 
                key_data=[]
                for  item_data in batch_list: 
                     key_data.append(item_data['vin'])
                table = dynamodb.Table(table_name)
                keyConditionExpression= Key('document_type').eq('ROHIST')   
                filterExpression=Attr("vin").is_in(key_data) 
                response = table.query(
                                        KeyConditionExpression= keyConditionExpression, 
                                        FilterExpression= filterExpression,
                                        ConsistentRead=False)
                LastEvaluatedKey=None
                if 'LastEvaluatedKey' in response:
                    LastEvaluatedKey=response['LastEvaluatedKey'] 
                if 'Items' in response:
                    existing_items = response['Items']  
                    while 'LastEvaluatedKey' in response:
                        response = table.query(
                            KeyConditionExpression= keyConditionExpression,
                            FilterExpression= filterExpression,
                            ConsistentRead=False,               
                            ExclusiveStartKey=response['LastEvaluatedKey']
                            )                     
                        try:
                            items1=response['Items']
                            #self.logger.debug("loop inner Total items="+str(len(items1)))
                            existing_items.extend(items1)
                            #self.logger.debug("loop after adding final Total items"+str(len(items)))
                            LastEvaluatedKey=None
                            if 'LastEvaluatedKey' in response:
                                LastEvaluatedKey=response['LastEvaluatedKey'] 
                                 
                            if LastEvaluatedKey is None:
                                self.logger.debug("break Loop LastEvaluatedKey="+str(LastEvaluatedKey))
                                break                               
                        except:
                            ""
                   
                logger.debug("deduplicationRO - get all VIN Data  existing_items size="+str(len(existing_items)))                
                if len(existing_items)>0: 
                    changed_list=[]              
                    for rohist_item in batch_list:
                        new_batch_list=[]
                        vin_cust_item_list = [existing_item for existing_item in existing_items if (existing_item['document_type'] == rohist_item['document_type']) and (existing_item['vin'] == rohist_item['vin'])]
                        if len(vin_cust_item_list)>0:                            
                            for old_rohist_item in vin_cust_item_list:
                                if rohist_item["document_id"] != old_rohist_item["document_id"] and  len(old_rohist_item["roList"])>0:
                                    roMinCloseDate=None
                                    roMaxCloseDate=None
                                    updatedROList=[] 
                                    isChanged=False                       
                                    for old_ro in old_rohist_item["roList"]:
                                        ro_item_list = [new_ro for new_ro in rohist_item["roList"] if (new_ro['ro'] == old_ro['ro']) ]
                                        if len(ro_item_list)==0:
                                            updatedROList.append(old_ro)  
                                            res_dt= self.calculateCloseDate(old_ro['closeDate'],roMaxCloseDate,roMinCloseDate)
                                            roMinCloseDate=res_dt['roMinCloseDate']
                                            roMaxCloseDate=res_dt['roMaxCloseDate']
                                        else:
                                            isChanged=True
                                    
                                    if isChanged:
                                        changed_list.append( old_rohist_item["document_id"])                                    
                                        old_rohist_item["roList"]=updatedROList
                                        old_rohist_item["roCount"]=len(updatedROList) 
                                        old_rohist_item["roMinCloseDate"] =roMinCloseDate
                                        old_rohist_item["roMaxCloseDate"] =roMaxCloseDate
                                        if old_rohist_item["roMinCloseDate"] == old_rohist_item["roMaxCloseDate"] :
                                            old_rohist_item["searchCloseDate"]=old_rohist_item["roMinCloseDate"]
                                        else:
                                            old_rohist_item["searchCloseDate"]="" 
                                        
                                        new_batch_list.append(old_rohist_item) 
                               
                        if len(new_batch_list)>0:
                            counter=0                            
                            for existing_item in existing_items:
                                new_vin_cust_item_list = [new_batch_list_item for new_batch_list_item in new_batch_list if (new_batch_list_item['document_type'] == existing_item['document_type']) and (new_batch_list_item['document_id'] == existing_item['document_id'])]
                                if len(new_vin_cust_item_list)>0:
                                  existing_items[counter]= new_vin_cust_item_list[0]
                    
                    changed_list=set(changed_list)
                    for existing_item in existing_items:
                        if existing_item['document_id'] in changed_list:
                            if len(existing_item["roList"])>0:
                                updated_batch_list.append(existing_item) 
                            else:
                                delete_batch_list.append(existing_item)
                
                logger.debug("deduplicationRO -operation completed, updated_batch_list size="+str(len(updated_batch_list)))  
                logger.debug("deduplicationRO -operation completed, delete_batch_list size="+str(len(delete_batch_list))) 
            except:
                  logger.error("Error Occure in deduplicationRO" , exc_info=True)
        else:
            logger.debug("deduplicationRO - No operation empty batch_list size="+str(len(updated_batch_list)))
        
        return { 'updated_batch_list':updated_batch_list,'delete_batch_list':delete_batch_list}
    @classmethod
    def deduplicationRO(self,dynamodb,batch_list,table_name):        
        new_batch_list=[]
        delete_batch_list=[]
        if batch_list is not None and len(batch_list)>0:
            try:
                logger.debug("deduplicationRO  batch_list size="+str(len(batch_list))+",table_name="+table_name)
                key_data=[]
                for  item_data in batch_list: 
                    if item_data['customerNo']  is not None and  len(item_data['customerNo'])>0:        
                        KeyDataItem={'document_type':item_data['document_type'],'document_id':item_data['vin']+"_"}
                        key_data.append(KeyDataItem)
                tab_existingitems=Batching.get_batch_data(dynamodb=dynamodb,tableName=table_name,item_list=key_data)
                existing_items=tab_existingitems[table_name]
                logger.debug("deduplicationRO - get_batch_data  existing_items without customerNo size="+str(len(existing_items)))  
               
                if len(existing_items)>0:                   
                    for rohist_item in batch_list:
                        vin_cust_item_list = [existing_item for existing_item in existing_items if (existing_item['document_type'] == rohist_item['document_type']) and (existing_item['vin'] == rohist_item['vin'])]
                        if len(vin_cust_item_list)>0:
                            old_rohist_item=vin_cust_item_list[0]                   
                            if len(old_rohist_item["roList"])>0:
                                roMinCloseDate=None
                                roMaxCloseDate=None
                                updatedROList=[]                        
                                for old_ro in old_rohist_item["roList"]:
                                    ro_item_list = [new_ro for new_ro in rohist_item["roList"] if (new_ro['ro'] == old_ro['ro']) ]
                                    if len(ro_item_list)==0:
                                        updatedROList.append(old_ro)  
                                        res_dt= self.calculateCloseDate(old_ro['closeDate'],roMaxCloseDate,roMinCloseDate)
                                        roMinCloseDate=res_dt['roMinCloseDate']
                                        roMaxCloseDate=res_dt['roMaxCloseDate']                              
                                
                                if len(updatedROList)==0:
                                    delete_batch_list.append(old_rohist_item)
                                else:
                                    old_rohist_item["roList"]=updatedROList
                                    old_rohist_item["roCount"]=len(updatedROList) 
                                    old_rohist_item["roMinCloseDate"] =roMinCloseDate
                                    old_rohist_item["roMaxCloseDate"] =roMaxCloseDate
                                    if old_rohist_item["roMinCloseDate"] == old_rohist_item["roMaxCloseDate"] :
                                        old_rohist_item["searchCloseDate"]=old_rohist_item["roMinCloseDate"]
                                    else:
                                        old_rohist_item["searchCloseDate"]="" 
                                    new_batch_list.append(old_rohist_item) 
                            else:
                                delete_batch_list.append(old_rohist_item)
                logger.debug("deduplicationRO -operation completed, new_batch_list size="+str(len(new_batch_list)))  
                logger.debug("deduplicationRO -operation completed, delete_batch_list size="+str(len(delete_batch_list))) 
            except:
                  logger.error("Error Occure in deduplicationRO" , exc_info=True)
        else:
            logger.debug("deduplicationRO - No operation empty batch_list size="+str(len(new_batch_list)))
        
        return { 'updated_batch_list':new_batch_list,'delete_batch_list':delete_batch_list}
    @classmethod
    def submitSaveDataLocal(self,TableName,items,region='us-east-1',overwrite_by_pkeys=['document_type', 'document_id'],checkForUpdateExisting=False,MAX_PROC=4,BATCH_SIZE=25):
            # Define the client to interact with AWS Lambda
           # logger.debug("Started.submitSaveDataLocal TableName="+str(TableName))
            results=[]
            futures=[]
            dynamodb = boto3.resource('dynamodb', region_name=region)
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_PROC) as executor:
                batches = self.chunks(items,BATCH_SIZE)                
                for batch in batches:
                    #logger.debug("batch added size="+str(len(batch)))
                    #futures.append(executor.submit(self.load_batch,dynamodb, TableName,overwrite_by_pkeys, batch))
                    futures.append(executor.submit(self.load_batch,dynamodb, TableName,overwrite_by_pkeys, batch,checkForUpdateExisting))
                for future in concurrent.futures.as_completed(futures):                     
                    results.append(future.result())  
    
    @classmethod
    def load_batch(self,dynamodb,table_name,pkeys, batch_list,checkForUpdateExisting):
        logger.debug("load_batch -checkForUpdateExisting="+str(checkForUpdateExisting)+" batch_list size="+str(len(batch_list)))  
        if str(checkForUpdateExisting).lower()=='true':
            updated_batch_list=self.handleROHistoryTableDataUpdate(dynamodb=dynamodb,batch_list=batch_list,table_name=table_name)
            """ if 'ROHIST_DEDUPLICATE' in os.environ:
                deduplicationRO= os.environ['ROHIST_DEDUPLICATE']
                if deduplicationRO=="True":
                    res_dup=self.deduplicationRO(dynamodb=dynamodb,batch_list=updated_batch_list,table_name=table_name)
                    if len(res_dup['updated_batch_list'])>0:
                        updated_batch_list.extend(res_dup['updated_batch_list'])

                    if len(res_dup['delete_batch_list'])>0:
                        deleted_batch_list=res_dup['delete_batch_list']   
                        try:
                            table = dynamodb.Table(table_name)
                            with table.batch_writer() as batch:
                                for item in deleted_batch_list:
                                    batch.delete_item(Key={
                                    "document_type": item["document_type"],
                                    "document_id": item["document_id"]
                                    })
                            logger.debug("after batch delete")  
                        except Exception:
                            logger.exception("load_batch Couldn't delete data into table %s.", table_name) """
        else:
            updated_batch_list=batch_list
        try:
            table = dynamodb.Table(table_name)
            with table.batch_writer(overwrite_by_pkeys=pkeys) as batch:
                for item in updated_batch_list:
                    batch.put_item(
                        Item=item
                    )
            #logger.debug("after batchwrite ")  
        except Exception:
            logger.exception("load_batch Couldn't load data into table %s.", table_name)
            raise
     
    @classmethod
    def chunks(self,lst, n):       
        for i in range(0, len(lst), n):
            yield lst[i:i + n]     
    