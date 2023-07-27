
from decimal import Decimal
import json
import sys
import logging
import os
from datetime import datetime
from DBHelper import DBHelper
from EPDE_Client import AppClient
loglevel = int(os.environ['LOG_LEVEL'])
logger = logging.getLogger('PartsDBHelper')
logger.setLevel(loglevel)
dbhelper=DBHelper()
class PartsDBHelper():
    @classmethod
    def SaveParts(self,store_code,df,client_id):
        logger.info("Inside SaveParts Method....store_code:"+store_code+",client_id="+client_id)
        try:
            # Get the service resource.
            batch_items=[]
            LAMBDA_BATCH_SIZE=100
            local=True
            items=[]
            ct = datetime.now()
            create_date = ct.strftime("%Y-%m-%d %I:%M:%S %p")
            create_date_only = ct.strftime("%Y-%m-%d")
            recount=0
            df_ro=df["ro"]
            df_parts=df["parts"]   
            account_lines=df['account_line']
            region=os.environ['REGION']            
            TableName=store_code+"_PARTS_FILE"
            accCodeList=['I']
            if not dbhelper.ValidateAccounts(store_code=store_code,account_lines=account_lines,region=region,accCodeList=accCodeList):
                lines=str(','.join(account_lines))
                logger.error("Error Occure in Save data to table ["+TableName+"] in DB. INVALID LOGON IDENTIFIED ,Logon in Data:"+lines,exc_info=True)
                return { "operation_status":"FAILED","error_code":-1 ,"error_message":"INVALID LOGON IDENTIFIED, SKIP SAVE IN DB :"+lines,"total_item_count":"0","total_item_parsed":"0","items":[]} 


            df_ro=df_ro.to_dict('records')            
            for  row in df_ro:
                referNumber=row['REFER#']  
                email=dbhelper.RemoveNewLineCarrigeReturn(str(row['EMAIL'] ))                   
                recount=recount+1                
                sold_FLNM=dbhelper.RemoveNewLineCarrigeReturn(str(row["CUST-N1"]))
                soldFNM=""
                soldLNM=""
                if len(sold_FLNM)>0 and sold_FLNM.__contains__(','):

                    sold_FLNM_arr=sold_FLNM.split(',')
                    if len(sold_FLNM_arr)>1:
                            soldLNM=sold_FLNM_arr[0]
                            soldFNM=sold_FLNM_arr[1]
                    else:
                            soldLNM=sold_FLNM                                                  
                else:
                    soldLNM=sold_FLNM

                ship_FLNM=dbhelper.RemoveNewLineCarrigeReturn(str(row["SHIP-N1"]))
                shipFNM=""
                shipLNM=""
                if len(ship_FLNM)>0 and ship_FLNM.__contains__(','):

                    ship_FLNM_arr=ship_FLNM.split(',')
                    if len(ship_FLNM_arr)>1:
                            shipLNM=ship_FLNM_arr[0]
                            shipFNM=ship_FLNM_arr[1]
                    else:
                            shipLNM=ship_FLNM                                                  
                else:
                    shipLNM=ship_FLNM

                #SOLD CITY STATE ZIP
                cust_N4=dbhelper.RemoveNewLineCarrigeReturn(str(row["CUST-N4"]))
                sold_city=""
                sold_state=""
                sold_zip=""
                if len(cust_N4.strip())>0 :
                    csz=cust_N4.strip()
                   # if loglevel==logging.DEBUG:
                    #    logger.debug(".cust_N4.csz:"+str(csz))
                    arr_csz=csz.split(',')
                    if len(arr_csz)>=2:
                        sold_city=arr_csz[0]
                        sz=arr_csz[1]
                        arr_sz=sz.strip().split(' ')
                        if len(arr_sz)>=2:
                            sold_state=arr_sz[0]
                            sold_zip=arr_sz[1]
                        else:
                            sold_state=sz
                            sold_zip="" 
                    else:
                        arr_csz=csz.strip().split(' ')
                        if len(arr_csz)>=3:
                            sold_city=arr_csz[0]
                            sold_state=arr_csz[1]
                            sold_zip=arr_csz[2]
                        else:  
                            if len(arr_csz)>=2:
                                sold_city=arr_csz[0]
                                sold_state=arr_csz[1] 
                                sold_zip=""
                            else: 
                                sold_city=csz
                                sold_state=""
                                sold_zip="" 
                # SOLD CITY STATE ZIP
                #SHIP CITY STATE ZIP
                ship_N3=dbhelper.RemoveNewLineCarrigeReturn(str(row["SHIP-N3"]))
                ship_city=""
                ship_state=""
                ship_zip=""
                csz=""
                if len(ship_N3.strip())>0 :
                    csz=ship_N3.strip()
                    #if loglevel==logging.DEBUG:
                     #   logger.debug(".ship_N3.csz:"+str(csz))
                    arr_csz=csz.split(',')
                    if len(arr_csz)>=2:
                        ship_city=arr_csz[0]
                        sz=arr_csz[1]
                        arr_sz=sz.strip().split(' ')
                        if len(arr_sz)>=2:
                            ship_state=arr_sz[0]
                            ship_zip=arr_sz[1]
                        else:
                            ship_state=sz
                            ship_zip="" 
                    else:
                        arr_csz=csz.strip().split(' ')
                        if len(arr_csz)>=3:
                            ship_city=arr_csz[0]
                            ship_state=arr_csz[1]
                            ship_zip=arr_csz[2]
                        else:  
                            if len(arr_csz)>=2:
                                ship_city=arr_csz[0]
                                ship_state=arr_csz[1] 
                                ship_zip=""
                            else: 
                                ship_city=csz
                                ship_state=""
                                ship_zip="" 
                # SHIP CITY STATE ZIP 
                # 
                soldAddressLine=[]                
                if len(row["CUST-N2"].strip())>0 :
                    soldAddressLine.append(dbhelper.RemoveNewLineCarrigeReturn(str(row["CUST-N2"])))
                if len(row["CUST-N3"].strip())>0 :
                    soldAddressLine.append(dbhelper.RemoveNewLineCarrigeReturn(str(row["CUST-N3"]))) 
                if len(row["CUST-N4"].strip())>0 :
                    soldAddressLine.append(dbhelper.RemoveNewLineCarrigeReturn(str(row["CUST-N4"])))                  
                soldAddress={
                            "id":str(row["CUST"]),
                            "firstName": str(soldFNM),
                            "lastName": str(soldLNM),
                            "addressLine": soldAddressLine,
                            "city":str(sold_city),
                            "state": str(sold_state),
                            "zip": str(sold_zip),
                            "email":str(email)
                            }          
                #logger.info("soldAddress="+str(soldAddress))

                shipAddressLine=[]
                if len(row["SHIP-N1"].strip())>0 :
                    shipAddressLine.append(dbhelper.RemoveNewLineCarrigeReturn(str(row["SHIP-N1"])))
                if len(row["SHIP-N2"].strip())>0 :
                    shipAddressLine.append(dbhelper.RemoveNewLineCarrigeReturn(str(row["SHIP-N2"])))
                if len(row["SHIP-N3"].strip())>0 :
                    shipAddressLine.append(dbhelper.RemoveNewLineCarrigeReturn(str(row["SHIP-N3"])))    
                shipAddress={
                               "id":"",
                               "firstName":str(shipFNM),
                               "lastName": str(shipLNM),
                               "addressLine": shipAddressLine,
                               "city":str( ship_city),
                               "state":str( ship_state),
                               "zip": str(ship_zip)
                            }  
               
                dict_parts= self.GetFilteredDF(df_parts,referNumber,None)
                parts=[]
                emp=""
                shipVia=""
                fright=0
                totalParts=0
                totalFright=0            

                if len(dict_parts)>0:
                    dict_parts=dict_parts.to_dict('records')
                    
                    for rowParts in dict_parts :                                        
                        desc=dbhelper.RemoveNewLineCarrigeReturn(rowParts["DESC"] )                        
                        net=0 
                        shipVia=dbhelper.RemoveNewLineCarrigeReturn(rowParts["SVIA"]) 
                        emp=rowParts["EMP#"] 
                        try:
                            fright=Decimal(rowParts["FREIGHT"])                           
                            totalFright=Decimal(totalFright+fright)
                        except:
                            pass 
                        try:
                            tSaleAmt=Decimal(rowParts["T-SALE"])                           
                            totalParts=Decimal(totalParts+tSaleAmt)
                        except:
                            pass 
                        try:
                            lst=Decimal(rowParts["LIST"])
                            discount=Decimal(rowParts["DISCOUNT"])
                            net=Decimal(lst-discount)
                        except:
                            pass                
                        part={				
                            "partsNum":rowParts["PART-NO"] ,			
                            "partsDesc":desc.strip(),			
                            "ordQty":str(rowParts["Q.O."] ).strip(),		
                            "shipQty":str(rowParts["Q.S."] ).strip(),		
                            "boQty":str(rowParts["Q.B."]  ).strip(),			
                            "listPrice":str(rowParts["LIST"]).strip() ,			
                            "netPrice":str(net) ,
                            "discount":	str(rowParts["DISCOUNT"]).strip() ,
                            "fright":	str(rowParts["FREIGHT"] ).strip(),		
                            "amount":str(rowParts["T-SALE"] ).strip(),		
                            "binNum":dbhelper.RemoveNewLineCarrigeReturn(rowParts["BIN"]) ,			
                            "isleNum":"",
                            "unitSalePrice":str(rowParts['SALE'] ).strip()                         	
                            }  
                        parts.append(part)
                    #END FOR PARTS
                #END IF PARTS
                totalSaleTax=0
                try:
                            total=Decimal(rowParts["TOTAL$"])                           
                            totalSaleTax=Decimal(total-totalParts)
                except:
                            pass 
                orderNo=str(row["PO#"])                 
                saleType=str(row["SALE-TYPE"])
                if orderNo is not None:
                  orderNo=orderNo.replace(saleType,'').strip()
                CLOSE_DATE=self.FormatDateNew(open_dt_str=row["CLOSE-DATE"],print_time_seconds_str="")
                OPEN_DATE=self.FormatDateNew(open_dt_str=row["OPEN-DATE"],print_time_seconds_str="")
                SHIPPED_DATE=self.FormatDateNew(open_dt_str=row["SHIPPED"],print_time_seconds_str="")
                item={
                            "client_id": str(client_id),
                            "store_code": str(store_code),
                            "document_id": str(row["REFER#"]).strip(),
                            "document_type": "PARTS",
                            "saleType":str(row["SALE-TYPE"]).strip(),                        
                            "soldTo":soldAddress,
                            "shipTo":shipAddress,
                            "contactPhone": str(row["HPHONE"]).strip(),
                            "orderNo":orderNo,
                            "openDate":OPEN_DATE,
                            "shippedDate":SHIPPED_DATE,
                            "shipVia":str(shipVia),			
                            "slsm":str(emp),				
                            "blNumber":"",				
                            "term":"",				
                            "fobPoint":"",
                            "partsList":parts,
                            "partsTotal":str(totalParts),	
                            "cpPartSale":str(row["CP-PART-SALE$"]).strip(),			
                            "subletTotal":str(row["CP-SUBL-SALE$"]).strip(),			
                            "freightTotal":str(totalFright),				
                            "saleTaxTotal":str(totalSaleTax),
                            "cpTaxSale":str(row["CP-TAX-SALE$"]).strip(),			
                            "totalAmount":str(row["TOTAL$"]).strip(),
                            "closeDate":CLOSE_DATE,
                            "status":str(row["STATUS"]).strip(),
                            "ppFlag":str(row["PPFLAG"]).strip(),
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
            TableName=store_code+"_PARTS_FILE"
            overwrite_by_pkeys=['document_type', 'document_id'] 
            MAX_PROC=4
            BATCH_SIZE=25            
            overwrite_by_pkeys=['document_type', 'document_id']
            if items is not None and  len(items)>0:
                attributes_to_compare=[
                                  
                            "document_id" ,
                            
                            "saleType" ,                        
                            {
                                "soldTo":  [
                                    "id" ,
                                    "firstName" ,
                                    "lastName" ,
                                    "addressLine" ,
                                    "city" ,
                                    "state" ,
                                    "zip" 
                                ]
                             },
                            {
                                "shipTo": [
                                    "id" ,
                                    "firstName" ,
                                    "lastName" ,
                                    "addressLine" ,
                                    "city" ,
                                    "state" ,
                                    "zip" 
                                ]
                            },
                            "contactPhone",
                            "orderNo",
                            "openDate",
                            "shippedDate",
                            "shipVia",			
                            "slsm",				
                            "blNumber",				
                            "term",				
                            "fobPoint",
                            {
                            "partsList":[
                                        "partsNum" ,			
                                        "partsDesc",			
                                        "ordQty",		
                                        "shipQty",		
                                        "boQty",			
                                        "listPrice",			
                                        "netPrice" ,
                                        "discount" ,
                                        "fright",		
                                        "amount",		
                                        "binNum" ,			
                                        "isleNum",
                                        "unitSalePrice"
                                        ]
                             },
                            "partsTotal",	
                            "cpPartSale",			
                            "subletTotal",			
                            "freightTotal",				
                            "saleTaxTotal",
                            "cpTaxSale",			
                            "totalAmount",
                            "closeDate",
                            "status",
                            "ppFlag"
                        ]
                beForFilterCount=len(items)
                logger.debug("CDK PARTS Total Record Count Before Filter=."+str(len(items)))   
                items=dbhelper.GetChangedItems(region=region,tableName=TableName,items=items,attributes_to_compare=attributes_to_compare)
                if items is not None and  len(items)>0:
                        
                    dbhelper.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
                    logger.info("CDK PARTS Data has been inserted successfuly in table ["+TableName+"] Total Filtered Record Count=."+str(len(items))+", Total Parsed Record Count "+str(beForFilterCount))                 
                else:
                    logger.info("CDK PARTS Data has not been inserted in table ["+TableName+"] Filtered Total Record Count=0")
            else:
                logger.info("CDK PARTS  Data has not been inserted in table ["+TableName+"] Total Record Count=0")
    
            """  if not local:
                dbhelper.submitSaveData(TableName=TableName,items=batch_items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
            else:
                 dbhelper.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
            """                       
            et = datetime.now()
            delta=et-ct
            logger.debug("after calling lambda total_seconds="+str(delta.total_seconds())) 
            logger.info("Parts Data has been inserted successfuly in table ["+TableName+"] Record Count=."+str(len(items)))
            return { "operation_status":"SUCCESS","total_item_count":str(recount),"total_item_parsed":str(len(items)),"items":items} 
        except Exception as e:
            # Return failed record's sequence number
            logger.error("Error Occure in Save parts data to table ["+TableName+"] in DB..." , exc_info=True)
            ex_type, ex_value, ex_traceback = sys.exc_info()
            return { "operation_status":"FAILED","error_code":-1 ,"error_message":ex_value}  
    @classmethod
    def GetFilteredDF(self,df,roNumber,lineCode):

        if lineCode is not None :
            dict_filtered=df.loc[(df['REFER#'] == roNumber)&(df['LINE-CDS'] == lineCode)] 
        else:
             dict_filtered=df.loc[(df['REFER#'] == roNumber) ]        
        return dict_filtered
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
    def GetPartsStatusMapping(self,store_code):
        filters=[]
        app_client=AppClient()
        region=os.environ['REGION']  
        store_resp=app_client.GetStoreDetail(store_code,region=region)
        if store_resp['status']:
            item=store_resp['item']
            if 'parts_status_mapping' in item:
                filters=item['parts_status_mapping']        
        return filters
    @classmethod
    def GetPartsStatusMappingMR(self,store_code):
        filters=[]
        source="AM"
        app_client=AppClient()
        region=os.environ['REGION']  
        store_resp=app_client.GetStoreDetail(store_code,region=region)
        if store_resp['status']:
            item=store_resp['item']
            if 'source' in item:
                source=item['source']    
            if 'parts_status_mapping' in item:
                filters=item['parts_status_mapping']   

        return {'parts_status_mapping':filters,'source':source}
    
    @classmethod
    def Save_TKParts(self,store_code,json_parts,client_id):
        logger.info("Inside Save_TKParts Method....store_code:"+store_code+",client_id="+client_id)
        try:
            region=os.environ['REGION']    
            # Get the service resource.
            batch_items=[]
            LAMBDA_BATCH_SIZE=100
            local=True
            items=[]
            ct = datetime.now()
            create_date = ct.strftime("%Y-%m-%d %I:%M:%S %p")
            create_date_only = ct.strftime("%Y-%m-%d")
            recount=0
            statusMap=self.GetPartsStatusMapping(store_code)
            for row in json_parts['data']:
                recount=recount+1
                referNumber=row['id']    
                
                sts=dbhelper.CovertToString(row["status"])
                try:
                    if sts.upper() in statusMap:
                        logger.debug("TEKION sts="+sts)
                        sts=statusMap[sts.upper()]
                    else:
                        sts=''  
                except:
                    ""        
                open_date_time=""
                close_date_time=""

                checkinTime=row['createdTime']          
                if checkinTime is not None: 
                    try:
                        open_date_time_dt = datetime.fromtimestamp(checkinTime / 1000)
                        #open_date_time=open_date_time_dt.strftime("%Y-%m-%d %I:%M:%S %p").lower()
                        open_date_time=open_date_time_dt.strftime("%m/%d/%Y %I:%M %p")                           
                    except:
                        ""

                """ closedTime=row['closedTime']               
                if closedTime is not None: 
                    close_date_time_dt = datetime.fromtimestamp(closedTime / 1000)
                    close_date_time=close_date_time_dt.strftime("%Y-%m-%d %I:%M:%S %p").lower()    """            
                
                homePhone="" 
                soldFNM=""
                soldLNM=""
                sold_city=""
                sold_state=""
                sold_zip=""
                soldAddressLine=[] 
                soldCustId="" 
                if "customer" in row and row["customer"]  is not None and len(row["customer"])>0:
                    customer=row["customer"][0]
                    if customer is not None:
                        if "id" in customer and customer["displayId"] is not None:
                            soldCustId=str(customer["displayId"])
                        if  "firstName" in customer and customer["firstName"] is not None:
                            soldFNM=str(customer["firstName"])
                        if  "lastName" in customer and customer["lastName"] is not None:
                            soldLNM= str(customer["lastName"])



                if "billingAddress" in row and row["billingAddress"] is not None  and "city" in row["billingAddress"]and row["billingAddress"]["city"] is not None:
                    sold_city=str(row["billingAddress"]["city"])
                if "billingAddress" in row and row["billingAddress"] is not None   and "state" in row["billingAddress"]and row["billingAddress"]["state"] is not None:
                    sold_state=str(row["billingAddress"]["state"])
                if "billingAddress" in row and row["billingAddress"] is not None   and  "postalCode" in row["billingAddress"] and row["billingAddress"]["postalCode"] is not None:    
                    sold_zip=str(row["billingAddress"]["postalCode"])                                  
                if  "billingAddress" in row and row["billingAddress"] is not None  and "line1" in row["billingAddress"] and  row["billingAddress"]["line1"] is not None and len(row["billingAddress"]["line1"])>0 :
                    soldAddressLine.append(str(row["billingAddress"]["line1"]))
                if  "billingAddress" in row and row["billingAddress"] is not None  and "line2" in row["billingAddress"] and  row["billingAddress"]["line2"] is not None and len(row["billingAddress"]["line2"])>0 :
                    soldAddressLine.append(str(row["billingAddress"]["line2"]))               
                
                soldAddress={
                            "id":str(soldCustId),
                            "firstName": str(soldFNM),
                            "lastName": str(soldLNM),
                            "addressLine": soldAddressLine,
                            "city":str(sold_city),
                            "state": str(sold_state),
                            "zip": str(sold_zip)
                            }          
                
                shipFNM=""
                shipLNM= ""
                ship_city=""
                ship_state=""
                ship_zip="" 
                shipCustId=""            
                shipAddressLine=[]                
                if "customer" in row and row["customer"]  is not None and len(row["customer"])>0:
                    customer=row["customer"][0]
                    if customer is not None:
                        if "id" in customer and customer["displayId"] is not None:
                            shipCustId=str(customer["displayId"])
                        if  "firstName" in customer and customer["firstName"] is not None:
                            shipFNM=str(customer["firstName"])
                        if  "lastName" in customer and customer["lastName"] is not None:
                            shipLNM= str(customer["lastName"])

                if "shippingAddress" in row and row["shippingAddress"] is not None  and "city" in row["shippingAddress"] and row["shippingAddress"]["city"] is not None:
                    ship_city=str(row["shippingAddress"]["city"])
                if "shippingAddress" in row and row["shippingAddress"] is not None   and "state" in row["shippingAddress"]and row["shippingAddress"]["state"] is not None:
                    ship_state=str(row["shippingAddress"]["state"])
                if "shippingAddress" in row and row["shippingAddress"] is not None   and "postalCode" in row["shippingAddress"]and row["shippingAddress"]["postalCode"] is not None:    
                    ship_zip=str(row["shippingAddress"]["postalCode"])                                  
                if  "shippingAddress" in row and row["shippingAddress"] is not None  and "line1" in row["shippingAddress"] and  row["shippingAddress"]["line1"] is not None and len(row["shippingAddress"]["line1"])>0 :
                    shipAddressLine.append(str(row["shippingAddress"]["line1"]))
                if  "shippingAddress" in row and row["shippingAddress"] is not None  and "line2" in row["shippingAddress"] and  row["shippingAddress"]["line2"] is not None and len(row["shippingAddress"]["line2"])>0 :
                    shipAddressLine.append(str(row["shippingAddress"]["line2"]))     
            
                       
                shipAddress={
                               "id":str(shipCustId),
                               "firstName":str(shipFNM),
                               "lastName": str(shipLNM),
                               "addressLine": shipAddressLine,
                               "city":str( ship_city),
                               "state":str( ship_state),
                               "zip": str(ship_zip)
                            }                
               
                parts=[]
                emp=""
                shipVia=""
                fright=0
                totalParts=0
                totalFright='' 
                if 'partSaleDetails' in row and row["partSaleDetails"] is not None  and  len(row['partSaleDetails'] )>0:
                    for rowParts in row['partSaleDetails'] :                                        
                        part={				
                            "partsNum":rowParts["partId"] ,			
                            "partsDesc":'',			
                            "ordQty":str(rowParts["requiredQty"] ).strip(),		
                            "shipQty": str(rowParts["requiredQty"]-rowParts["shortageQty"]),		
                            "boQty":str(rowParts["returnQty"] ).strip(),			
                            "listPrice":'' ,			
                            "netPrice":'' ,
                            "discount":	'' ,
                            "fright":	'',		
                            "amount":'',		
                            "binNum":'' ,			
                            "isleNum":"",
                            "unitSalePrice":''                         	
                            }  
                        parts.append(part)
                    #END FOR PARTS
                    totalParts=len(parts)
                #END IF PARTS
                totalSaleTax=''
                try:
                                         
                    totalSaleTax=row['tax']['saleTaxAmount']["amount"]
                    if totalSaleTax is None:
                        totalSaleTax=''                   

                except:
                       totalSaleTax=''
                orderNo=""
                saleType=""
                if "orderNo" in row and row["orderNo"] is not None:
                    orderNo=str(row["orderNo"])   
                if "saleType" in row and row["saleType"] is not None:              
                    saleType=str(row["saleType"])
                poNumber=""
                if 'poNumber' in row and row['poNumber'] is not None:
                    poNumber=row['poNumber'] 
                shipVia=""
                if 'deliveryMethod' in row and row['deliveryMethod'] is not None:
                    shipVia=row['deliveryMethod'] 
                saleAmount=""
                if 'saleAmount' in row and row['saleAmount'] is not None:
                    if  'amount' in row['saleAmount'] and row['saleAmount']['amount'] is not None:
                        saleAmount= row['saleAmount']['amount']
                item={
                            "client_id": str(client_id),
                            "store_code": str(store_code),
                            "document_id": str(referNumber),
                            "document_type": "PARTS",
                            "saleType":str(saleType),                        
                            "soldTo":soldAddress,
                            "shipTo":shipAddress,
                            "contactPhone": str(homePhone),
                            "orderNo":str(orderNo),
                            "openDate":str(open_date_time),
                            "shippedDate":str(open_date_time),
                            "shipVia":str(shipVia),			
                            "slsm":str(emp),				
                            "blNumber":str(poNumber),				
                            "term":"",				
                            "fobPoint":"",
                            "partsList":parts,
                            "partsTotal":str(totalParts),	
                            "cpPartSale":'',			
                            "subletTotal":'',			
                            "freightTotal":'',				
                            "saleTaxTotal":str(totalSaleTax),
                            "cpTaxSale":'',			
                            "totalAmount":str(saleAmount),
                            "closeDate":str(close_date_time),
                            "status":str(sts),
                            "ppFlag":'',
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
            TableName=store_code+"_PARTS_FILE"
            overwrite_by_pkeys=['document_type', 'document_id'] 
            MAX_PROC=4
            BATCH_SIZE=25            
            overwrite_by_pkeys=['document_type', 'document_id']
            if items is not None and  len(items)>0:
                attributes_to_compare=[
                                  
                            "document_id" ,
                            
                            "saleType" ,                        
                            {
                                "soldTo":  [
                                    "id" ,
                                    "firstName" ,
                                    "lastName" ,
                                    "addressLine" ,
                                    "city" ,
                                    "state" ,
                                    "zip" 
                                ]
                             },
                            {
                                "shipTo": [
                                    "id" ,
                                    "firstName" ,
                                    "lastName" ,
                                    "addressLine" ,
                                    "city" ,
                                    "state" ,
                                    "zip" 
                                ]
                            },
                            "contactPhone",
                            "orderNo",
                            "openDate",
                            "shippedDate",
                            "shipVia",			
                            "slsm",				
                            "blNumber",				
                            "term",				
                            "fobPoint",
                            {
                            "partsList":[
                                        "partsNum" ,			
                                        "partsDesc",			
                                        "ordQty",		
                                        "shipQty",		
                                        "boQty",			
                                        "listPrice",			
                                        "netPrice" ,
                                        "discount" ,
                                        "fright",		
                                        "amount",		
                                        "binNum" ,			
                                        "isleNum",
                                        "unitSalePrice"
                                        ]
                             },
                            "partsTotal",	
                            "cpPartSale",			
                            "subletTotal",			
                            "freightTotal",				
                            "saleTaxTotal",
                            "cpTaxSale",			
                            "totalAmount",
                            "closeDate",
                            "status",
                            "ppFlag"
                        ]
                beForFilterCount=len(items)
                logger.debug("TKN PARTS Total Record Count Before Filter=."+str(len(items)))   
                items=dbhelper.GetChangedItems(region=region,tableName=TableName,items=items,attributes_to_compare=attributes_to_compare)
                if items is not None and  len(items)>0:
                        
                    dbhelper.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
                    logger.info("TKN PARTS Data has been inserted successfuly in table ["+TableName+"] Total Filtered Record Count=."+str(len(items))+", Total Parsed Record Count "+str(beForFilterCount))                 
                else:
                    logger.info("TKN PARTS Data has not been inserted in table ["+TableName+"] Filtered Total Record Count=0")
            else:
                logger.info("TKN PARTS  Data has not been inserted in table ["+TableName+"] Total Record Count=0")
    
            """ if not local:
                dbhelper.submitSaveData(TableName=TableName,items=batch_items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
            else:
                 dbhelper.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
            """                       
            et = datetime.now()
            delta=et-ct
            logger.debug("after calling lambda total_seconds="+str(delta.total_seconds())) 
            logger.info("TEKION Parts Data has been inserted successfuly in table ["+TableName+"] Record Count=."+str(len(items)))
            return { "operation_status":"SUCCESS","total_item_count":str(recount),"total_item_parsed":str(len(items)),"items":items} 
        except Exception as e:
            # Return failed record's sequence number
            logger.error("Error Occure in Save TEKION parts data to table ["+TableName+"] in DB..." , exc_info=True)
            ex_type, ex_value, ex_traceback = sys.exc_info()
            return { "operation_status":"FAILED","error_code":-1 ,"error_message":ex_value}  
        
    @classmethod
    def Save_MRParts(self,store_code,json_parts,client_id):
        logger.info("Inside Save_MRParts Method....store_code:"+store_code+",client_id="+client_id)
        try:
            region=os.environ['REGION']    
            # Get the service resource.
            batch_items=[]
            LAMBDA_BATCH_SIZE=100
            local=True
            items=[]
            ct = datetime.now()
            create_date = ct.strftime("%Y-%m-%d %I:%M:%S %p")
            create_date_only = ct.strftime("%Y-%m-%d")
            recount=0
            ret_dict=self.GetPartsStatusMappingMR(store_code)
            statusMap=ret_dict['parts_status_mapping']
            source=ret_dict['source']
            for row in json_parts:
                recount=recount+1
                referNumber=row['invoiceNumber']    
                sts=''  
                if "invoiceState" in row:
                    sts=dbhelper.CovertToString(row["invoiceState"])
                    try:
                        if sts.upper() in statusMap:
                            logger.debug("MR sts="+sts)
                            sts=statusMap[sts.upper()]
                        else:
                            sts=''  
                    except:
                        ""        
                open_date_time=""
                if 'invoiceDate' in row:
                    openDate=row['invoiceDate']          
                    if openDate is not None  and len(openDate)>0:  
                        try:
                            open_date_time_dt=datetime.strptime(openDate, "%Y-%m-%d")                    
                            open_date_time=open_date_time_dt.strftime("%m/%d/%Y")                             
                        except:
                            ""
                close_date_time=""
                if 'quoteCloseDate' in row:
                    closeDate=row['quoteCloseDate']          
                    if closeDate is not None and len(closeDate)>0: 
                        try:
                            close_date_time=datetime.strptime(closeDate, "%Y-%m-%d")                    
                            close_date_time=close_date_time.strftime("%m/%d/%Y")                             
                        except:
                            ""
                        
                orderNo=""
                if "dealerOrderNumber" in row and row["dealerOrderNumber"] is not None:
                    orderNo=str(row["dealerOrderNumber"])   

                shipDate=""
                if "shipDate" in row and row["shipDate"] is not None:
                    try:
                        shipDate=str(row["shipDate"])    
                        shipDate_dt=datetime.strptime(shipDate, "%Y-%m-%d")  
                        shipDate=shipDate_dt.strftime("%m/%d/%Y")
                    except:
                            ""
                
                saleType=""
                if "paymentMethod" in row and row["paymentMethod"] is not None:              
                    saleType=str(row["paymentMethod"])             
                
                shipVia=""
                if 'shipmentMethod' in row and row['shipmentMethod'] is not None:
                    shipVia=row['shipmentMethod']                     
               
               
                employee_id="" 
                employee_first_name=""
                employee_last_name=""  
                
                customerParty=None
                shipToParty=None
                addressGroups=None
                customerId=""
                firstName=""
                lastName= ""
                homePhone=""
                mobilePhone=""
                workPhone=""
                otherPhone=""
                emailAddress=""
                resdenceAddress=None
                billAddress=None
                shipAddress=None
                mailAddress=None
                shipToAddress={
                                "zip": "",
                                "firstName": "",
                                "lastName": "",
                                "city": "",
                                "id": "",
                                "state": "",
                                "addressLine": []}
                soldToAddress={
                                "zip": "",
                                "firstName": "",
                                "lastName": "",
                                "city": "",
                                "id": "",
                                "state": "",
                                "addressLine": []}
                if 'partsInvoiceParties' in row and len(row["partsInvoiceParties"])>0:                    
                    for partsInvoiceParty in  row['partsInvoiceParties']:
                        #Extract Sales Person Info
                        if 'partyType' in  partsInvoiceParty and partsInvoiceParty['partyType'] == 'SalesPerson':
                            if 'givenName' in partsInvoiceParty:
                                employee_first_name= partsInvoiceParty["givenName"] 
                            if 'familyName' in partsInvoiceParty:
                                employee_last_name= partsInvoiceParty["familyName"] 
                            if 'idList' in partsInvoiceParty and len(partsInvoiceParty["idList"])>0:
                                idList= partsInvoiceParty["idList"] 
                                for id in idList:
                                    if 'typeId' in id and 'id' in id and id['typeId'] == 'DMSId': 
                                       employee_id= id["id"]
                        #Extract Customer Info - CustomerParty
                        if 'partyType' in partsInvoiceParty and  partsInvoiceParty['partyType'] == 'CustomerParty': 
                            customerParty=self.ExtractPartyDetail(partsInvoiceParty)                          
                        if 'partyType' in  partsInvoiceParty and partsInvoiceParty['partyType'] == 'ShipToParty': 
                            shipToParty=self.ExtractPartyDetail(partsInvoiceParty)  
                        if 'address' in  partsInvoiceParty and len(partsInvoiceParty['address'] )>0:     
                            addressGroups=self.ExtractAddressGroups(partsInvoiceParty=partsInvoiceParty)
                
                
                #if source=='automate':
                partyDetail=None

                if customerParty is not None:
                    if 'partyId' in customerParty and customerParty['partyId'] is not None:
                        customerId=customerParty['partyId']

                    if 'partyDetail' in customerParty and customerParty['partyDetail'] is not None:
                        partyDetail=customerParty['partyDetail']
                        if partyDetail is not None:
                            firstName= partyDetail['firstName']
                            lastName= partyDetail['lastName']
                            companyName=partyDetail['companyName']
                            personName=partyDetail['personName']
                            if (firstName is None or firstName=="") and (lastName is None or lastName==""):
                                if companyName is not None and companyName!="":
                                    lastName= companyName
                                if personName is not None and personName!="":
                                  nameArray= personName.split() 
                                  if nameArray is not None and len(nameArray)==1:                                 
                                    lastName=nameArray[0]
                                  if nameArray is not None and len(nameArray)==2:                                   
                                    firstName=nameArray[0]
                                    lastName=nameArray[1]
                                  if nameArray is not None and len(nameArray)==2:                                   
                                    firstName=nameArray[0]
                                    #lastName=nameArray[1]
                                    lastName=nameArray[2]
                            homePhone=partyDetail['homePhone']
                            mobilePhone=partyDetail['mobilePhone']
                            workPhone=partyDetail['workPhone']
                            otherPhone=partyDetail['otherPhone']
                            emailAddress=partyDetail['emailAddress']
                            addressGroups=partyDetail['addressGroups'] 
                            if addressGroups is not None and 'Residence' in addressGroups :
                                resdenceAddress=addressGroups['Residence']
                            if addressGroups is not None and 'Billing' in addressGroups :
                                billAddress=addressGroups['Billing']
                            if addressGroups is not None and 'Mail' in addressGroups :
                                mailAddress=addressGroups['Mail']
                            if addressGroups is not None and 'Shipping' in addressGroups :
                                shipAddress=addressGroups['Shipping']
                            
                        if 'primaryContact' in customerParty and customerParty['primaryContact'] is not None:
                            primaryContact=customerParty['primaryContact']
                            homePhone=primaryContact['homePhone']
                            mobilePhone=primaryContact['mobilePhone']
                            workPhone=primaryContact['workPhone']
                            otherPhone=primaryContact['otherPhone']
                            emailAddress=primaryContact['emailAddress']
                            addressGroups=primaryContact['addressGroups'] 
                            if addressGroups is not None and 'Residence' in addressGroups :
                                resdenceAddress=addressGroups['Residence']
                            if addressGroups is not None and 'Billing' in addressGroups :
                                billAddress=addressGroups['Billing']
                            if addressGroups is not None and 'Mail' in addressGroups :
                                mailAddress=addressGroups['Mail']
                            if addressGroups is not None and 'Shipping' in addressGroups :
                                shipAddress=addressGroups['Shipping']
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

                if shipToParty is not None:
                    if 'partyId' in customerParty and customerParty['partyId'] is not None:
                        customerId=customerParty['partyId']

                    if 'partyDetail' in customerParty and customerParty['partyDetail'] is not None:
                        partyDetail=shipToParty['partyDetail']
                        if partyDetail is not None:
                            firstName= partyDetail['firstName']
                            lastName= partyDetail['lastName']
                            companyName=partyDetail['companyName']
                            personName=partyDetail['personName']
                            if (firstName is None or firstName=="") and (lastName is None or lastName==""):
                                if companyName is not None and companyName!="":
                                    lastName= companyName
                                if personName is not None and personName!="":
                                  nameArray= personName.split() 
                                  if nameArray is not None and len(nameArray)==1:                                 
                                    lastName=nameArray[0]
                                  if nameArray is not None and len(nameArray)==2:                                   
                                    firstName=nameArray[0]
                                    lastName=nameArray[1]
                                  if nameArray is not None and len(nameArray)==2:                                   
                                    firstName=nameArray[0]
                                    #lastName=nameArray[1]
                                    lastName=nameArray[2]
                            homePhone=partyDetail['homePhone']
                            mobilePhone=partyDetail['mobilePhone']
                            workPhone=partyDetail['workPhone']
                            emailAddress=partyDetail['emailAddress']
                            addressGroups=partyDetail['addressGroups'] 
                            if addressGroups is not None and 'Residence' in addressGroups :
                                resdenceAddress=addressGroups['Residence']
                            if addressGroups is not None and 'Billing' in addressGroups :
                                billAddress=addressGroups['Billing']
                            if addressGroups is not None and 'Mail' in addressGroups :
                                mailAddress=addressGroups['Mail']
                            if addressGroups is not None and 'Shipping' in addressGroups :
                                shipAddress=addressGroups['Shipping']
                            
                        if 'primaryContact' in shipToParty and shipToParty['primaryContact'] is not None:
                            primaryContact=shipToParty['primaryContact']
                            homePhone=primaryContact['homePhone']
                            mobilePhone=primaryContact['mobilePhone']
                            workPhone=primaryContact['workPhone']
                            emailAddress=primaryContact['emailAddress']
                            addressGroups=primaryContact['addressGroups'] 
                            if addressGroups is not None and 'Residence' in addressGroups :
                                resdenceAddress=addressGroups['Residence']
                            if addressGroups is not None and 'Billing' in addressGroups :
                                billAddress=addressGroups['Billing']
                            if addressGroups is not None and 'Mail' in addressGroups :
                                mailAddress=addressGroups['Mail']
                            if addressGroups is not None and 'Shipping' in addressGroups :
                                shipAddress=addressGroups['Shipping']           
                        if 'addressGroups' in shipToParty and shipToParty['addressGroups'] is not None:
                            addressGroups=shipToParty['addressGroups'] 
                            if addressGroups is not None and 'Residence' in addressGroups :
                                resdenceAddress=addressGroups['Residence']
                            if addressGroups is not None and 'Billing' in addressGroups :
                                billAddress=addressGroups['Billing']
                            if addressGroups is not None and 'Mail' in addressGroups :
                                mailAddress=addressGroups['Mail']
                            if addressGroups is not None and 'Shipping' in addressGroups :
                                shipAddress=addressGroups['Shipping']

                 
                if resdenceAddress is not None:
                    soldToAddress=self.prePareAddress(firstName=firstName,customerId=customerId,lastName=lastName,address=resdenceAddress)
                    shipToAddress=soldToAddress
                if mailAddress is not None:
                    soldToAddress=self.prePareAddress(firstName=firstName,customerId=customerId,lastName=lastName,address=mailAddress)
                    shipToAddress=soldToAddress
                if billAddress is not None:
                   soldToAddress=self.prePareAddress(firstName=firstName,customerId=customerId,lastName=lastName,address=billAddress)
                   if shipToAddress is None:
                      shipToAddress=soldToAddress 
                if shipAddress is not None:
                    shipAddress=self.prePareAddress(firstName=firstName,customerId=customerId,lastName=lastName,address=shipAddress) 
                    if soldToAddress is None:
                       soldToAddress=shipAddress

                parts=[] 
                totalParts=""
                totalPartLineFright=None
                if 'partsInvoiceLine' in row and row["partsInvoiceLine"] is not None  and  len(row['partsInvoiceLine'] )>0:
                    for rowParts in row['partsInvoiceLine'] :   
                        if "partsProductItem" in  rowParts:                     
                            partsProductItem=rowParts["partsProductItem"] 
                            partsNum=""
                            partsDesc=""
                            if "itemIdentificationGroup" in partsProductItem:
                                itemIdentificationGroup=partsProductItem["itemIdentificationGroup"]  
                                if "itemId" in itemIdentificationGroup:
                                    partsNum=itemIdentificationGroup["itemId"]
                            if "partItemDescription" in partsProductItem:
                                partsDesc=partsProductItem["partItemDescription"]
                            if "orderQuantity" in partsProductItem:
                                ordQty=partsProductItem["orderQuantity"] 
                            listPrice=""
                            Freight=""
                            DiscountAmount=""
                            UnitPrice=""
                            amount=""
                            netPrice=""
                            if 'pricing' in partsProductItem and partsProductItem["pricing"] is not None  and  len(partsProductItem['pricing'] )>0: 
                                for pricing in partsProductItem['pricing'] : 
                                    if "priceCode" in pricing and "chargeAmount" in pricing:
                                        if pricing["priceCode"]=="List":
                                            listPrice=str(pricing["chargeAmount"])
                                        if pricing["priceCode"]=="UnitPrice":
                                            UnitPrice=str(pricing["chargeAmount"])
                                        if pricing["priceCode"]=="PartCost":
                                            amount=str(pricing["chargeAmount"])                                             
                                        if pricing["priceCode"]=="DealerDiscountAmount":
                                            DiscountAmount=str(pricing["chargeAmount"]) 
                                        if pricing["priceCode"]=="TotalPartsAmount":
                                            netPrice=str(pricing["chargeAmount"]) 
                                        if pricing["priceCode"]=="Freight":
                                            if pricing["chargeAmount"] is not None and len(str(pricing["chargeAmount"])) >0:
                                                totalPartLineFright=totalPartLineFright+pricing["chargeAmount"]
                                                Freight=str(pricing["chargeAmount"])
                            part={				
                                "partsNum":str(partsNum) ,			
                                "partsDesc":partsDesc,			
                                "ordQty":str(ordQty),		
                                "shipQty":"",		
                                "boQty":"",			
                                "listPrice":listPrice ,			
                                "netPrice":netPrice ,
                                "discount":	DiscountAmount ,
                                "fright":	Freight,		
                                "amount":amount,		
                                "binNum":'' ,			
                                "isleNum":"",
                                "unitSalePrice":UnitPrice                         	
                                }  
                            parts.append(part)
                    #END FOR PARTS
                    
                #END IF PARTS
                dealerDiscountAmount=""
                poNumber=""
                totalSaleTax=''
                if "tax" in row and row["tax"] is not None and len(row["tax"])>0:
                    for tax in row["tax"]:
                        if 'taxAmount' in tax:
                            totalSaleTax=str(tax['taxAmount'] )

                freightTotal=""                 
                totalFright=None
                totalPrice=""
                totalCost=""
                if "pricing" in row and row["pricing"] is not None and len(row["pricing"])>0:
                    for pricing in row["pricing"]:
                        if "priceCode" in pricing and "chargeAmount" in pricing:
                            if pricing["priceCode"]=="TotalCost":
                                totalCost=str(pricing['chargeAmount'] ) 
                            if pricing["priceCode"]=="TotalPrice":
                                totalPrice=str(pricing['chargeAmount'] )   
                            if pricing["priceCode"]=="Fright":
                                freightTotal=str(pricing['chargeAmount'] ) 
                                totalFright= freightTotal
                            if pricing["priceCode"]=="DealerDiscountAmount":
                                dealerDiscountAmount=str(pricing['chargeAmount'] )   

                if totalFright is None :
                    if totalPartLineFright is not None:
                        freightTotal=str(totalPartLineFright)
                
                contactPhone=homePhone
                if contactPhone == None or contactPhone =="":
                   contactPhone=workPhone
                if contactPhone == None or contactPhone =="":
                   contactPhone=mobilePhone   
                if contactPhone == None or contactPhone =="":
                   contactPhone=otherPhone   
                item={
                            "client_id": str(client_id),
                            "store_code": str(store_code),
                            "document_id": str(referNumber),
                            "document_type": "PARTS",
                            "saleType":str(saleType),                        
                            "soldTo":soldToAddress,
                            "shipTo":shipToAddress,
                            "contactPhone": str(contactPhone),
                            "orderNo":str(orderNo),
                            "openDate":str(open_date_time),
                            "shippedDate":str(shipDate),
                            "shipVia":str(shipVia),			
                            "slsm":str(employee_id),				
                            "blNumber":str(poNumber),				
                            "term":"",				
                            "fobPoint":"",
                            "partsList":parts,
                            "partsTotal":str(totalCost),	
                            "cpPartSale":'',			
                            "subletTotal":'',			
                            "freightTotal":freightTotal,				
                            "saleTaxTotal":str(totalSaleTax),
                            "cpTaxSale":'',			
                            "totalAmount":str(totalPrice),
                            "closeDate":str(close_date_time),
                            "status":str(sts),
                            "ppFlag":'',
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
            TableName=store_code+"_PARTS_FILE"
            overwrite_by_pkeys=['document_type', 'document_id'] 
            MAX_PROC=4
            BATCH_SIZE=25            
            overwrite_by_pkeys=['document_type', 'document_id']
            if items is not None and  len(items)>0:
                attributes_to_compare=[
                                  
                            "document_id" ,
                            
                            "saleType" ,                        
                            {
                                "soldTo":  [
                                    "id" ,
                                    "firstName" ,
                                    "lastName" ,
                                    "addressLine" ,
                                    "city" ,
                                    "state" ,
                                    "zip" 
                                ]
                             },
                            {
                                "shipTo": [
                                    "id" ,
                                    "firstName" ,
                                    "lastName" ,
                                    "addressLine" ,
                                    "city" ,
                                    "state" ,
                                    "zip" 
                                ]
                            },
                            "contactPhone",
                            "orderNo",
                            "openDate",
                            "shippedDate",
                            "shipVia",			
                            "slsm",				
                            "blNumber",				
                            "term",				
                            "fobPoint",
                            {
                            "partsList":[
                                        "partsNum" ,			
                                        "partsDesc",			
                                        "ordQty",		
                                        "shipQty",		
                                        "boQty",			
                                        "listPrice",			
                                        "netPrice" ,
                                        "discount" ,
                                        "fright",		
                                        "amount",		
                                        "binNum" ,			
                                        "isleNum",
                                        "unitSalePrice"
                                        ]
                             },
                            "partsTotal",	
                            "cpPartSale",			
                            "subletTotal",			
                            "freightTotal",				
                            "saleTaxTotal",
                            "cpTaxSale",			
                            "totalAmount",
                            "closeDate",
                            "status",
                            "ppFlag"
                        ]
                beForFilterCount=len(items)
                logger.debug("MR PARTS Total Record Count Before Filter=."+str(len(items)))   
                items=dbhelper.GetChangedItems(region=region,tableName=TableName,items=items,attributes_to_compare=attributes_to_compare)
                if items is not None and  len(items)>0:
                        
                    dbhelper.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
                    logger.info("MR PARTS Data has been inserted successfuly in table ["+TableName+"] Total Filtered Record Count=."+str(len(items))+", Total Parsed Record Count "+str(beForFilterCount))                 
                else:
                    logger.info("MR PARTS Data has not been inserted in table ["+TableName+"] Filtered Total Record Count=0")
            else:
                logger.info("MR PARTS  Data has not been inserted in table ["+TableName+"] Total Record Count=0")
    
            """  if not local:
                dbhelper.submitSaveData(TableName=TableName,items=batch_items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
            else:
                 dbhelper.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
                        """
            et = datetime.now()
            delta=et-ct
            logger.debug("after calling lambda total_seconds="+str(delta.total_seconds())) 
            logger.info("MR Parts Data has been inserted successfuly in table ["+TableName+"] Record Count=."+str(len(items)))
            return { "operation_status":"SUCCESS","total_item_count":str(recount),"total_item_parsed":str(len(items)),"items":items} 
        except Exception as e:
            # Return failed record's sequence number
            logger.error("Error Occure in Save MR parts data to table ["+TableName+"] in DB..." , exc_info=True)
            ex_type, ex_value, ex_traceback = sys.exc_info()
            return { "operation_status":"FAILED","error_code":-1 ,"error_message":ex_value}  
    @classmethod
    def prePareAddress(self, customerId,firstName,lastName,address):
        addressLine=[]
        city=""
        state=""
        zip=""
        if address is not None:
            if "addressLine" in address:
                addressLine=address["addressLine"] 
            if "city" in address:
                city=address["city"] 
            if "state" in address:
                state=address["state"] 
            if "zip" in address:
                zip=address["zip"] 
        return  {
                "id":str(customerId),
                "firstName": str(firstName),
                "lastName": str(lastName),
                "addressLine": addressLine,
                "city":str(city),
                "state": str(state),
                "zip": str(zip)
                }              
    @classmethod
    def ExtractAddressGroups(self,partsInvoiceParty):
        address_groups={}
        if "address" in partsInvoiceParty:
            for address in partsInvoiceParty["address"]:
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
    def ExtractCustomerDetail(self,partsInvoiceParty):
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
        address_groups=self.ExtractAddressGroups(partsInvoiceParty)  
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
        address_groups=self.ExtractAddressGroups(partsInvoiceParty)               
        return  {                 
                "partyId":str(customerId), 
                "partyDetail": partyDetail,                 
                "primaryContact":primaryContact ,
                "addressGroups":address_groups   
            }
                
                
        