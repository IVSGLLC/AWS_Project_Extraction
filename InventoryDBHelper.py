import boto3
import concurrent.futures  
from decimal import Decimal
import logging
import os
import sys
import Batching
from datetime import datetime
from EPDE_Client import AppClient
from DBHelper import DBHelper
loglevel = int(os.environ['LOG_LEVEL'])
logger = logging.getLogger('InventoryDBHelper')
logger.setLevel(loglevel)
dbHelper=DBHelper()
class InventoryDBHelper():    
    @classmethod
    def SaveInventory(self,store_code,df,client_id):
        logger.info("Inside SaveInventory Method....store_code:"+store_code+",client_id="+client_id)
        try:
            items=[]
            IN_STCK_BAL=5000
            region=os.environ['REGION']
            TableName=store_code+"_INVENTORY_FILE"   
            recount=0
            ct = datetime.now()
            create_date = ct.strftime("%Y-%m-%d %I:%M:%S %p")
            create_date_only = ct.strftime("%Y-%m-%d")
            recount=0            
            df_car_inv=df["car_inv"]
            df_fi_wip=df["fi_wip"]
            df_wip= df["wip_appt"]
            df_labor_1=df["labor_appt"]
            df_wip_2= df["wip_labor"]
            df_labor_2=df["labor_2"]
            df_po_orders=df["po_orders"]
            account_lines=df['account_line']
            accCodeList=['S','FI','A']
            if not dbHelper.ValidateAccounts(store_code=store_code,account_lines=account_lines,region=region,accCodeList=accCodeList):
                lines=str(','.join(account_lines))
                logger.error("Error Occure in Save data to table ["+TableName+"] in DB. INVALID LOGON IDENTIFIED ,Logon in Data:"+lines,exc_info=True)
                return { "operation_status":"FAILED","error_code":-1 ,"error_message":"INVALID LOGON IDENTIFIED, SKIP SAVE IN DB :"+lines,"total_item_count":"0","total_item_parsed":"0","items":[]} 

            result_df=self.GetDescriptionFilters(store_code=store_code,region=region)
            inventorytype_filters=result_df['inventorytype_filters']
            logger.debug("inventorytype_filters="+str(inventorytype_filters))  
            filters=result_df['plan_code_filters']
            gl_acct_filters=result_df['gl_acct_filters']    
            logger.debug("gl_acct_filters="+str(gl_acct_filters))       
            df_car_inv=df_car_inv.to_dict('records')  
            logger.debug("TOTAL CAR-INV ="+str(len(df_car_inv)))        
            for  row in df_car_inv:
                vin=row['VIN'] 
                if self.isValidVIN(vin) == False:
                   continue

                stock_no=row['STOCK'] 
                lienHolderAmount=None
                lienHolderName=None
                acct=None
                openROList=[]
                poOrderList=[]
                stockStatus= str(row['STATUS'])
                stockType=str(row['STOCK-TYPE'])
                isOpenPO=  False  
                isOpenROWithSubLet=  False  
                gps=False
                apptRO=False
                # Stock GetDeals
                dict_stock_deal_list=self.getStockDealList(stock_no=stock_no,vin=vin,filters=filters,df_fi_wip=df_fi_wip)
                fi_wip_List=dict_stock_deal_list["dealList"]                
                inventoryStatus=dict_stock_deal_list["inventoryStatus"]
                #dealSoldDateList=dict_stock_deal_list["dealSoldDateList"]
                servicePlanList=dict_stock_deal_list["servicePlanList"]
                dealMaxSoldDate=dict_stock_deal_list["dealMaxSoldDate"]
                dealMinSoldDate=dict_stock_deal_list["dealMinSoldDate"] 

                stkType=dbHelper.RemoveNewLineCarrigeReturn(row['STOCK-TYPE']).strip()
                inventoryType='MISC'
                if inventorytype_filters is not None and len(inventorytype_filters)>0:
                    inventoryType=inventorytype_filters['EPDE.DEFAULT']
                    if list(filter(stkType.__eq__, inventorytype_filters)) != []:
                        inventoryType= str(inventorytype_filters[stkType])  
                                
                balance=0
                stockFlag='A'               
                try:
                    balance=Decimal(str(row['BALANCE']).strip().replace(',', ''))
                    if balance>IN_STCK_BAL:
                        stockFlag='I'
                    else:
                        stockFlag='O'
                except:
                    stockFlag='O'

                if 'ACCT' in row and row['ACCT'] is not None and len(str(row['ACCT']).strip())>0:
                    acct=row['ACCT']
                    postAmount=row['POST.AMT.EXT']
                    logger.debug("STOCK_ACCT VIN="+str(vin)+",stock_no="+str(stock_no)+",acct="+str(acct)+",postAmount="+str(postAmount))
                    try:                    
                        lienHolderAmount_decimal = Decimal(postAmount.replace(',',''))
                        if lienHolderAmount_decimal<0:
                           lienHolderAmount_decimal=lienHolderAmount_decimal*-1
                        if lienHolderAmount_decimal!=0:
                            lienHolderName=self.GetLender(acct,gl_acct_filters) 
                            lienHolderAmount=str(lienHolderAmount_decimal)
                            logger.debug("STOCK_ACCT lienHolderName="+str(lienHolderName))                               
                        else:
                            lienHolderAmount=None
                            lienHolderName=None
                    except:
                        logger.error("EXCEPTION STOCK_ACCT....")
                        lienHolderAmount=None
                        lienHolderName=None
                    logger.debug("STOCK_ACCT VIN="+str(vin)+",stock_no="+str(stock_no)+",acct="+str(acct)+",lienHolderAmount="+str(lienHolderAmount)+",lienHolderName="+str(lienHolderName))
                acctOther=None
                if 'ACCT-OTHER' in row and row['ACCT-OTHER'] is not None and len(str(row['ACCT-OTHER']).strip())>0:
                    acctOther=row['ACCT-OTHER']
                    postAmountOther=row['POST.AMT.EXT-OTHER']
                    logger.debug("LOANER_DEMO_ACCT VIN="+str(vin)+",stock_no="+str(stock_no)+",acctOther="+str(acctOther)+",postAmountOther="+str(postAmountOther)+",stockFlag="+str(stockFlag)+",inventoryType="+str(inventoryType))
                    try:                    
                        lienHolderAmount_decimal = Decimal(postAmountOther.replace(',',''))
                        if lienHolderAmount_decimal<0:
                           lienHolderAmount_decimal=lienHolderAmount_decimal*-1
                        if lienHolderAmount_decimal!=0:
                            lender_resp=self.GetLenderInventoryMapping(acctOther,gl_acct_filters)
                            if lender_resp is not None:
                                lienHolderName=lender_resp['desc']
                                if inventoryType=="NEW" and 'inventory_type_map' in lender_resp and lender_resp['inventory_type_map'] is not None:
                                   inventoryType=lender_resp['inventory_type_map']
                                   
                            lienHolderAmount=str(lienHolderAmount_decimal)
                            logger.debug("LOANER_DEMO_ACCT lienHolderName="+str(lienHolderName)+",inventoryType="+str(inventoryType)) 
                            stockFlag ='I'                             
                        else:
                            lienHolderAmount=None
                            lienHolderName=None
                            stockFlag ='O'
                    except:
                        logger.error("EXCEPTION LOANER_DEMO_ACCT....")
                        lienHolderAmount=None
                        lienHolderName=None
                        stockFlag ='O'
                    logger.debug("LOANER_DEMO_ACCT VIN="+str(vin)+",stock_no="+str(stock_no)+",acctOther="+str(acctOther)+",lienHolderAmount="+str(lienHolderAmount)+",lienHolderName="+str(lienHolderName)+",stockFlag="+str(stockFlag)+",inventoryType="+str(inventoryType))
                
                logger.debug("FINAL VIN="+str(vin)+",stock_no="+str(stock_no)+",stockFlag="+str(stockFlag)+",lienHolderAmount="+str(lienHolderAmount)+",lienHolderName="+str(lienHolderName))
                
                stockMinEntryDate= None
                stockMaxEntryDate= None
                dealCount=0
                if  fi_wip_List is not None and len(fi_wip_List)>0:
                    dealCount=len(fi_wip_List)
                
                #BR ADDED ON 15 JUL 2023
                if row['STATUS'] is None or len(str(row['STATUS']).strip())==0 :
                   row['STATUS']='S'

                #BR ADDED ON 11 FEB 2023
                if row['STATUS'] is not None and len(str(row['STATUS']).strip())>0 and str(row['STATUS']).strip()=='G' and  dealCount==0 and stockFlag=='I' :
                   row['STATUS']='S'
               
               
                stock_item={
                        "stockNo":str(row['STOCK']).strip(),
                        "stockType":stkType,
                        "stockStatus":str(row['STATUS']).strip(),                        
                        "soldDate":str(row['DATE-SOLD']).strip().replace(" ",""),
                        "entryDate":str(row['ENTRY']).strip().replace(" ",""),
                        "balance":str(row['BALANCE']).strip(),
                        "iCompany":str(row['I-COMPANY']).strip(),
                        "iAcct":str(row['I-ACCT']).strip(),
                        "deals":fi_wip_List,
                        "dealCount":dealCount,
                        "lienHolderAmount":lienHolderAmount,
                        "lienHolderName":lienHolderName,
                        "inStockFlag":  stockFlag ,
                        "inventoryType":inventoryType                    
                }
                stockEntryDateStr=str(row['ENTRY']).strip().replace(" ","")
                if stockEntryDateStr is not None:
                    try:
                        stockEntryDateDt=datetime.strptime(stockEntryDateStr, '%d%b%y')
                        stockMaxEntryDate= stockEntryDateDt
                        stockMinEntryDate= stockEntryDateDt
                    except Exception:
                            ""
              
                
                indexVin=-1
                vin_item_list=[element for element in items if element['document_id'] == vin]
                if len(vin_item_list)>0:
                    inv_item=vin_item_list[0]
                    indexVin=items.index(inv_item)                   
                    openROList=items[indexVin]["repairOrders"]
                    open_ro_with_sublet_list=[ro for ro in openROList if ro['internal'] == True and ro['status'] == 'C97' ]
                    if len(open_ro_with_sublet_list)>0:
                       isOpenROWithSubLet=True 
                    poOrderList=items[indexVin]["poOrders"]
                    open_po_list=[po for po in poOrderList if po['poType'].upper() == 'SUBLET' and po['status'] == 'OPEN' ]
                    if len(open_po_list)>0:
                       isOpenPO=True
                    servicePlans=items[indexVin]["servicePlans"] 
                    for plan in servicePlanList:
                        if not plan in servicePlans:
                           items[indexVin]["servicePlans"].append(plan)
                    #dealSoldDates=items[indexVin]["dealSoldDates"] 
                    #for soldDateDeal in dealSoldDateList:
                    #    if not soldDateDeal in dealSoldDates:
                    #       items[indexVin]["dealSoldDates"].append(soldDateDeal)
                    if stockStatus.upper()=='DT':
                     inventoryStatus="dealertraded"
                    if isOpenPO or isOpenROWithSubLet:
                        inventoryStatus="sublet"
                    if stockType.upper()=='NEW' or  stockType.upper()=='USED' or  stockType.upper()=='MISC' or  stockType.upper()=='DEMO' or stockType.upper()=='FLEET':
                        inventoryStatus="new-used designation"   
                    if stockStatus.upper()=='T':
                        inventoryStatus="transferred" 
                    stock_item["inventoryStatus"]=inventoryStatus
                    items[indexVin]["stocks"].append(stock_item)
                    items[indexVin]["stockCount"]=len(items[indexVin]["stocks"])
                    if len(items[indexVin]["stocks"])>0:
                        stockFlag=self.getUpdatedStockFlag(items[indexVin])
                        items[indexVin]["stockFlag"]=stockFlag                       
                        
                    if apptRO==True:
                        items[indexVin]["apptRO"]=apptRO 
                    if gps==True:
                        items[indexVin]["gps"]=gps  
                    dealMaxSoldDateStr=items[indexVin]["dealMaxSoldDate"]
                    if dealMaxSoldDateStr!="" :
                        try:
                            dealMaxSoldDt=datetime.strptime(dealMaxSoldDateStr, "%Y-%m-%d")                           
                            if dealMaxSoldDate is not None:
                               if  dealMaxSoldDate<  dealMaxSoldDt:
                                        dealMaxSoldDate= dealMaxSoldDt                 
                        except ValueError:                    
                                    ""
                    dealMinSoldDateStr=items[indexVin]["dealMinSoldDate"]
                    if dealMinSoldDateStr!="" :
                        try:
                            dealMinSoldDt=datetime.strptime(dealMinSoldDateStr, "%Y-%m-%d")                           
                            if dealMinSoldDate is not None:
                               if  dealMinSoldDate>  dealMinSoldDt:
                                        dealMinSoldDate= dealMinSoldDt                 
                        except ValueError:
                            ""
                    if dealMaxSoldDate is not None:  
                        try:                    
                            items[indexVin]["dealMaxSoldDate"]= str(dealMaxSoldDate.strftime("%Y-%m-%d"))
                        except:
                            items[indexVin]["dealMaxSoldDate"]=""
                    else:
                        items[indexVin]["dealMaxSoldDate"]=""
                    if dealMinSoldDate is not None:  
                        try:                    
                            items[indexVin]["dealMinSoldDate"]= str(dealMinSoldDate.strftime("%Y-%m-%d"))
                        except:
                            items[indexVin]["dealMinSoldDate"]=""
                    else:
                       items[indexVin]["dealMinSoldDate"]=""
                   
                    if items[indexVin]["dealMinSoldDate"] == items[indexVin]["dealMaxSoldDate"] :
                       items[indexVin]["searchDate"]=items[indexVin]["dealMinSoldDate"]
                    else:
                       items[indexVin]["searchDate"]=""
                    ############
                    stockMaxEntryDateStr=items[indexVin]["stockMaxEntryDate"]
                    if stockMaxEntryDateStr!="" :
                        try:
                            stockMaxEntryDt=datetime.strptime(stockMaxEntryDateStr, "%Y-%m-%d")                           
                            if stockMaxEntryDate is not None:
                               if  stockMaxEntryDate<  stockMaxEntryDt:
                                        stockMaxEntryDate= stockMaxEntryDt                 
                        except Exception:                    
                                    ""
                    stockMinEntryDateStr=items[indexVin]["stockMinEntryDate"]
                    if stockMinEntryDateStr!="" :
                        try:
                            stockMinEntryDt=datetime.strptime(stockMinEntryDateStr, "%Y-%m-%d")                           
                            if stockMinEntryDate is not None:
                               if  stockMinEntryDate>  stockMinEntryDt:
                                    stockMinEntryDate= stockMinEntryDt                 
                        except Exception:
                            ""
                    if stockMinEntryDate is not None:  
                        try:                    
                            items[indexVin]["stockMinEntryDate"]= str(stockMinEntryDate.strftime("%Y-%m-%d"))
                        except:
                            items[indexVin]["stockMinEntryDate"]=""
                    else:
                        items[indexVin]["stockMinEntryDate"]=""
                    
                    if stockMaxEntryDate is not None:  
                        try:                    
                            items[indexVin]["stockMaxEntryDate"]= str(stockMaxEntryDate.strftime("%Y-%m-%d"))
                        except:
                            items[indexVin]["stockMaxEntryDate"]=""
                    else:
                       items[indexVin]["stockMaxEntryDate"]=""
                   
                    if items[indexVin]["stockMaxEntryDate"] == items[indexVin]["stockMinEntryDate"] :
                       items[indexVin]["searchEntryDate"]=items[indexVin]["stockMinEntryDate"]
                    else:
                       items[indexVin]["searchEntryDate"]=""

                    ############
                    #updated_resp=self.getRO_OpenDate(openROList=openROList)
                    #self.populateROOpenDate(updated_resp,inv_item)
                else:   
                    #OpenROList/OpenPOList WIP RO with Appt                
                    dict_appt_ro_list=self.getApptROData(vin=vin,df_wip=df_wip,df_labor_1=df_labor_1,df_po_orders=df_po_orders,openROList=openROList,poOrderList=poOrderList, apptRO=apptRO,isOpenPO=isOpenPO) 
                    openROList=dict_appt_ro_list["openROList"]
                    poOrderList=dict_appt_ro_list["poOrderList"]
                    isOpenPO=dict_appt_ro_list["isOpenPO"]
                    apptRO=dict_appt_ro_list["apptRO"]
                    dict_labor_ro_list=self.getLaborRoData(vin=vin,df_wip_2=df_wip_2,df_labor_2=df_labor_2,df_po_orders=df_po_orders,openROList=openROList,poOrderList=poOrderList,isOpenPO=isOpenPO,isOpenROWithSubLet=isOpenROWithSubLet)
                    openROList=dict_labor_ro_list["openROList"]
                    poOrderList=dict_labor_ro_list["poOrderList"]
                    isOpenPO=dict_labor_ro_list["isOpenPO"]
                    isOpenROWithSubLet=dict_labor_ro_list["isOpenROWithSubLet"]
                    if stockStatus.upper()=='DT':
                     inventoryStatus="dealertraded"
                    if isOpenPO or isOpenROWithSubLet:
                        inventoryStatus="sublet"
                    if stockType.upper()=='NEW' or  stockType.upper()=='USED' or  stockType.upper()=='MISC' or  stockType.upper()=='DEMO' or stockType.upper()=='FLEET':
                        inventoryStatus="new-used designation"   
                    if stockStatus.upper()=='T':
                        inventoryStatus="transferred" 
                    stock_item["inventoryStatus"]=inventoryStatus
                    poCount=0
                    roCount=0
                    if openROList is not  None:
                        roCount=len(openROList)
                    if poOrderList is not  None:
                        poCount=len(poOrderList)    

                    inv_item={
                            "document_type":'INV',
                            "document_id":str(vin).strip(),   
                            "vin":str(vin).strip(),                
                            "make":dbHelper.RemoveNewLineCarrigeReturn(row['MAKE']).strip(),
                            "model":dbHelper.RemoveNewLineCarrigeReturn(row['MODEL']).strip(),
                            "year":str(row['YEAR']).strip(),                       
                            "color":dbHelper.RemoveNewLineCarrigeReturn(row['COLOR']).strip(), 
                            "stocks":[stock_item],
                            "repairOrders":openROList,
                            "poOrders":poOrderList,  
                            "servicePlans":servicePlanList ,   
                            #"dealSoldDates":dealSoldDateList ,                   
                            "create_date": dbHelper.CovertToString(create_date_only),
                            "create_date_time":create_date ,                            
                            "gps":gps,
                            "appt":apptRO ,
                            "stockFlag" :stockFlag,
                            "stockCount":1,
                            "roCount":roCount,
                            "poCount":poCount                       
                            }
                    if dealMaxSoldDate is not None:  
                        try:                    
                            inv_item["dealMaxSoldDate"]= str(dealMaxSoldDate.strftime("%Y-%m-%d"))
                        except:
                            inv_item["dealMaxSoldDate"]=""
                    else:
                        inv_item["dealMaxSoldDate"]=""
                    if dealMinSoldDate is not None:  
                        try:                    
                            inv_item["dealMinSoldDate"]= str(dealMinSoldDate.strftime("%Y-%m-%d"))
                        except:
                            inv_item["dealMinSoldDate"]=""
                    else:
                        inv_item["dealMinSoldDate"]=""

                    if inv_item["dealMinSoldDate"] == inv_item["dealMaxSoldDate"] :
                       inv_item["searchDate"]=inv_item["dealMinSoldDate"]
                    else:
                       inv_item["searchDate"]=""
                    ##########
                    if stockMaxEntryDate is not None:  
                        try:                    
                            inv_item["stockMaxEntryDate"]= str(stockMaxEntryDate.strftime("%Y-%m-%d"))
                        except:
                            inv_item["stockMaxEntryDate"]=""
                    else:
                        inv_item["stockMaxEntryDate"]=""
                    if stockMinEntryDate is not None:  
                        try:                    
                            inv_item["stockMinEntryDate"]= str(stockMinEntryDate.strftime("%Y-%m-%d"))
                        except:
                            inv_item["stockMinEntryDate"]=""
                    else:
                        inv_item["stockMinEntryDate"]=""

                    if inv_item["stockMinEntryDate"] == inv_item["stockMaxEntryDate"] :
                       inv_item["searchEntryDate"]=inv_item["stockMinEntryDate"]
                    else:
                       inv_item["searchEntryDate"]=""
                    ##########
                    updated_resp=self.getRO_OpenDate(openROList=openROList)
                    self.populateROOpenDate(updated_resp,inv_item)
                    items.append(inv_item)
                    recount=recount+1
            #END FOR LOOP CAR-INV 
            local=True       
            if not local:
                batch_items=dbHelper.getBatchList(items)

            et = datetime.now()
            delta=et-ct
            logger.debug("Build JSON total_seconds="+str(delta.total_seconds()))
            logger.debug("TOTAL final CAR-INV recount ="+str(recount))      
            TableName=store_code+"_INVENTORY_FILE"   
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
            
            """ dynamodb = boto3.resource('dynamodb', region_name=region)
            TableName=client_id+"_INVENTORY_FILE"
            overwrite_by_pkeys=['document_type', 'document_id']
            MAX_PROC=3
            results=[]
            futures=[]
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_PROC) as executor:
                batches = self.chunks(items,50)                
                for batch in batches:
                    futures.append(executor.submit(self.load_batch,dynamodb, TableName,overwrite_by_pkeys, batch))
                for future in concurrent.futures.as_completed(futures):
                    results.append(future.result()) """

            logger.info("Inventory Data has been inserted successfuly in table ["+TableName+"] Record Count=."+str(recount))
            return { "operation_status":"SUCCESS","total_item_count":str(recount),"total_item_parsed":str(len(items)),"items":items} 
        except Exception as e:
            # Return failed record's sequence number
            logger.error("Error Occure in Save Inventory data to table ["+TableName+"] in DB..." , exc_info=True)
            ex_type, ex_value, ex_traceback = sys.exc_info()
            return { "operation_status":"FAILED","error_code":-1 ,"error_message":ex_value} 
    @classmethod
    def getLaborRoData(self,vin,df_wip_2,df_labor_2,df_po_orders,openROList,poOrderList,isOpenPO,isOpenROWithSubLet):
        #start WIP Labor     
        dict_wip_2=df_wip_2.loc[(df_wip_2['VIN'] == vin) ]   
        if len(dict_wip_2)>0:
            dict_wip_2=dict_wip_2.to_dict('records')
            for rowWIP2 in dict_wip_2: 
                roNumber= rowWIP2['REFER#']                              
                df_labor_21=df_labor_2.loc[(df_labor_2['VIN'] == vin) ]  
                df_labor_211=df_labor_21.loc[(df_labor_21['REFER#'] == roNumber) ]            
                dict_labor_2=df_labor_211.to_dict('records')
                serviceLines=[]
                mileage=""
                ro_status=str(rowWIP2['STATUS']).strip()
                index=-1
                roListAppt = [d for  d in openROList if d['roNumber'] == roNumber]
                if roListAppt is not None and  len(roListAppt)>0:
                    index=openROList.index(roListAppt[0])
                    openROList[index]["internal"]=True

                for rowLineItem2 in dict_labor_2:
                    lineCode=str(rowLineItem2['LINE-CDS']).strip()
                    lineDesc=dbHelper.RemoveNewLineCarrigeReturn(rowLineItem2['SVC-DESCS']).strip()
                    stock_no=   str(rowLineItem2['STOCK-NO']).strip()
                    mileage = str(rowLineItem2['MILEAGE']).strip()  
                    defLbrType=dbHelper.RemoveNewLineCarrigeReturn(rowLineItem2['DEF-LBR-TYP']).strip()
                    if  ro_status.upper()=='C97' and defLbrType.upper().startswith("I"):
                        isOpenROWithSubLet=True    
                    line_item={
                            "lineCode":lineCode,
                            "desc":lineDesc,
                            "stockNo":stock_no,
                            "defLbrType":defLbrType                                       

                    } 
                    if index>-1:                                    
                        servcieLinesROListAppt = [d for  d in openROList[index]["serviceLines"] if d['lineCode'] == lineCode]
                        if servcieLinesROListAppt is not None and  len(servcieLinesROListAppt)>0:
                            indexLine=openROList[index]["serviceLines"].index(servcieLinesROListAppt[0])
                            openROList[index]["serviceLines"][indexLine]=line_item
                        else:
                            openROList[index]["serviceLines"].append(line_item)
                    else:
                        serviceLines.append(line_item) 
                if index==-1:         
                    wip_item2={
                            "roNumber":str(roNumber).strip(),
                            "status":ro_status,
                            "openDate":str(rowWIP2['OPEN-DATE']).strip(),
                            "closeDate":str(rowWIP2['CLOSE-DATE']).strip(),
                            "cpTotalSale":str(rowWIP2['CP-TOTAL-SALE'] ).strip(),
                            "cpTotalCost":str(rowWIP2['CP-TOTAL-COST']).strip() ,
                            "totalCost":str(rowWIP2['TOTAL-COST']).strip() ,
                            "total":str(rowWIP2['TOTAL$']).strip() ,
                            "apptIds":"" ,
                            "apptDate":"" ,
                            "customerName":dbHelper.RemoveNewLineCarrigeReturn(rowWIP2['CUSTOMER-NAME']).strip(),
                            "vehId":str(rowWIP2['VEHID']).strip(),
                            "mileage"  :mileage,
                            "internal":True  , 
                            "appt":False,
                            "serviceLines":serviceLines                                         
                            }
                    if not openROList.__contains__(wip_item2):
                        openROList.append(wip_item2)
                #start PO order for RO
                dict_po_orders=df_po_orders.loc[(df_po_orders['REFER#'] == roNumber) ] 
                if len(dict_po_orders)>0:
                    dict_po_orders=dict_po_orders.to_dict('records')
                    for rowPO in dict_po_orders: 
                        poNumber= rowPO['PO-NO']  
                        poType=str(rowPO['PO-TYPE']).strip()
                        poStatus=str(rowPO['STATUS']).strip()
                        if  poStatus.upper()=='OPEN' and poType.upper()=='SUBLET' :
                                isOpenPO=True                  
                        po_item={
                                    "poNumber":str(poNumber).strip(),
                                    "poType":poType,
                                    "desc":dbHelper.RemoveNewLineCarrigeReturn(rowPO['DESC']).strip(),
                                    "createDate":str(rowPO['CREATE-DATE']) ,
                                    "closeDate":str(rowPO['CLOSE-DATE'] ),
                                    "amount":str(rowPO['AMOUNT']).strip(),
                                    "status":poStatus,
                                    "venderName":dbHelper.RemoveNewLineCarrigeReturn(rowPO['VEND-NAME']).strip(),
                                    "roNumber":roNumber 
                                }
                        if not poOrderList.__contains__(po_item):
                            poOrderList.append(po_item) 
                #end PO order for RO
            #end for dict_wip_2
        #End WIP Labor 
        return {"vin":vin,"openROList":openROList,"poOrderList":poOrderList,"isOpenPO":isOpenPO,"isOpenROWithSubLet":isOpenROWithSubLet}
    @classmethod
    def getApptROData(self,vin,df_wip,df_labor_1,df_po_orders,openROList,poOrderList,apptRO,isOpenPO):
        dict_wip=df_wip.loc[(df_wip['VIN'] == vin) ]          
        if len(dict_wip)>0:
            apptRO=True
            dict_wip=dict_wip.to_dict('records')                                
            for rowWIP in dict_wip: 
                roNumber= rowWIP['REFER#']  
                df_labor_11=df_labor_1.loc[(df_labor_1['VIN'] == vin) ]  
                df_labor_111=df_labor_11.loc[(df_labor_11['REFER#'] == roNumber) ]            
                dict_labor_1=df_labor_111.to_dict('records')                           
                serviceLines=[]
                mileage=""
                ro_status=str(rowWIP['STATUS']).strip()                             
                for rowLineItem in dict_labor_1:
                    lineCode=str(rowLineItem['LINE-CDS']).strip()
                    lineDesc=dbHelper.RemoveNewLineCarrigeReturn(rowLineItem['SVC-DESCS']).strip()
                    stock_no=   str(rowLineItem['STOCK-NO']).strip()
                    mileage = str(rowLineItem['MILEAGE']).strip()  
                    line_item={
                            "lineCode":lineCode,
                            "desc":lineDesc,
                            "stockNo":stock_no,
                            "defLbrType":""                                
                    } 
                serviceLines.append(line_item)          
                wip_item={
                            "roNumber":str(roNumber).strip(),
                            "status":ro_status,
                            "openDate":str(rowWIP['OPEN-DATE']).strip(),
                            "closeDate":str(rowWIP['CLOSE-DATE']).strip(),
                            "cpTotalSale":str(rowWIP['CP-TOTAL-SALE'] ).strip(),
                            "cpTotalCost":str(rowWIP['CP-TOTAL-COST']).strip() ,
                            "totalCost":str(rowWIP['TOTAL-COST']).strip() ,
                            "total":str(rowWIP['TOTAL$']).strip() ,
                            "apptIds":str(rowWIP['APPT-IDS']).strip() ,
                            "apptDate":str(rowWIP['APPT-DATE']).strip() ,
                            "customerName":dbHelper.RemoveNewLineCarrigeReturn(rowWIP['CUSTOMER-NAME']).strip(),
                            "vehId":str(rowWIP['VEHID']).strip() ,
                            "mileage"  :mileage,
                            "internal":False  , 
                            "appt":True,
                            "serviceLines":serviceLines     
                            }
                if not openROList.__contains__(wip_item):
                        openROList.append(wip_item)
                    
                #start PO order for RO                    
                dict_po_orders=df_po_orders.loc[(df_po_orders['REFER#'] == roNumber) ] 
                if len(dict_po_orders)>0:
                    dict_po_orders=dict_po_orders.to_dict('records')
                    for rowPO in dict_po_orders: 
                        poNumber= rowPO['PO-NO']                                        
                        poType=str(rowPO['PO-TYPE']).strip()
                        poStatus=str(rowPO['STATUS']).strip()
                        if  poStatus.upper()=='OPEN' and poType.upper()=='SUBLET' :
                                isOpenPO=True    
                        
                        po_item={
                                    "poNumber":str(poNumber).strip(),
                                    "poType":poType,
                                    "desc":dbHelper.RemoveNewLineCarrigeReturn(rowPO['DESC']).strip(),
                                    "createDate":str(rowPO['CREATE-DATE']) ,
                                    "closeDate":str(rowPO['CLOSE-DATE'] ),
                                    "amount":str(rowPO['AMOUNT']).strip(),
                                    "status":poStatus,
                                    "venderName":dbHelper.RemoveNewLineCarrigeReturn(rowPO['VEND-NAME']).strip(),
                                    "roNumber":roNumber 
                                }
                        if not poOrderList.__contains__(po_item):
                            poOrderList.append(po_item)  
                    #end for PO
                #end PO order for RO
            #end for dict_wip
        #End WIP Appt 
        return {"vin":vin,"openROList":openROList,"poOrderList":poOrderList,"isOpenPO":isOpenPO,"apptRO":apptRO}
                         
    @classmethod
    def getRO_OpenDate(self,openROList):
        roMaxOpenDate=None
        roMinOpenDate=None  
        searchOpenDate=""       
        if len(openROList)>0:                                   
            for rowWIP in openROList: 
                openDate= rowWIP['openDate']  
                searchOpenDate=""     
                if openDate is not None and len(openDate)>0:
                    try: 
                        openDateDt=datetime.strptime(openDate, '%d%b%y')
                        searchOpenDate = openDateDt.strftime("%Y-%m-%d")  
                        if roMaxOpenDate is None:
                            roMaxOpenDate=openDateDt
                        else:
                            if roMaxOpenDate<openDateDt:
                                roMaxOpenDate=openDateDt
                        if roMinOpenDate is None:
                            roMinOpenDate=openDateDt
                        else:
                            if roMinOpenDate>openDateDt:
                                roMinOpenDate=openDateDt
                                                            
                    except ValueError:
                        searchOpenDate= ""     
            #end for dict_wip
        #End WIP Appt 
        return { "searchOpenDate":searchOpenDate,"roMaxOpenDate":roMaxOpenDate,"roMinOpenDate":roMinOpenDate}
           
    @classmethod
    def getStockDealList(self,stock_no,vin,filters,df_fi_wip):
        #start fi-WIP
        dict_fi_wip=df_fi_wip.loc[(df_fi_wip['STOCK-NO'] == stock_no) ]               
        servicePlanList=[] 
        #dealSoldDateList=[]  
        dealMaxSoldDate=None
        dealMinSoldDate=None
        inventoryStatus="" 
        deals=[]  
        acct=None      
        if len(dict_fi_wip)>0:
                dict_fi_wip=dict_fi_wip.to_dict('records')
                for rowFIWIP in dict_fi_wip: 
                    saleType= rowFIWIP['SALETYPE'] 
                    if saleType=='RETAIL' and vin==rowFIWIP['VIN']:
                        inventoryStatus="sold"
                    bank=str(rowFIWIP['FINANCE-CO-NAME'])
                    if  saleType=='WHOLESALE' or bank.upper()== 'WHLSE' or bank.upper()== 'W':
                        inventoryStatus="wholesaled"

                    dealNumber= rowFIWIP['FI-WIP-NO'] 
                    desc1=str(rowFIWIP['DESCRIPTION'] )  
                    sale1=str(rowFIWIP['WE-OWE-SALE'])                            
                    cost1=str(rowFIWIP['WE-OWE-COST'])                          
                    descArr=desc1.split('||')
                    saleArr=sale1.split('||')
                    costArr=cost1.split('||')                           
                    index = 0
                    desc=""
                    cost=""
                    sale=""
                    for dsc in descArr:                                                              
                        if list(filter(dsc.upper().__eq__, filters)) != []:
                            desc=dsc
                            sale=saleArr[index]
                            cost=costArr[index]
                            break
                        index += 1
                    
                    status= rowFIWIP['STATUS']
                    soldDate= rowFIWIP['DATE-SOLD']
                    entryDate=rowFIWIP['ENTRY-DT']                           
                    salesPersonNo=rowFIWIP['SALESPERSON-NO-1']  
                    salesPersonName=rowFIWIP['SLSPERSON-1-NAME']                                                  
                    buyerName=rowFIWIP['BUYER-NAME']
                    contractDate=rowFIWIP['CONTRACT-DT']                    
                    if not desc.upper() in servicePlanList:
                        servicePlanList.append(desc.upper())
                    proceed=True     
                    searchDate=""     
                    if soldDate is not None and len(soldDate)>0:
                        try:
                            proceed=self.isValidDeal(status,soldDate)                             
                            if proceed:
                                soldDateDt=datetime.strptime(soldDate, '%d%b%y')
                                searchDate = soldDateDt.strftime("%Y-%m-%d")  
                                if dealMaxSoldDate is None:
                                    dealMaxSoldDate=soldDateDt
                                else:
                                    if dealMaxSoldDate<soldDateDt:
                                        dealMaxSoldDate=soldDateDt
                                if dealMinSoldDate is None:
                                    dealMinSoldDate=soldDateDt
                                else:
                                    if dealMinSoldDate>soldDateDt:
                                        dealMinSoldDate=soldDateDt

                                                              
                        except ValueError:
                            searchDate= ""
                    #if not searchDate in dealSoldDateList:
                    #    dealSoldDateList.append(searchDate)
                    
                    if proceed:
                        fiwip_item={
                                    "dealNo":str(dealNumber).strip(),
                                    "dealStatus":str(status).strip(),
                                    "desc":dbHelper.RemoveNewLineCarrigeReturn(desc).strip(),
                                    "soldDate": str(soldDate).strip() ,   
                                    "entryDate":str(entryDate).strip().replace(" ",""), 
                                    "contractDate":str(contractDate).strip().replace(" ",""), 
                                    "salesPersonNo":str(salesPersonNo).strip(),   
                                    "salesPersonName":dbHelper.RemoveNewLineCarrigeReturn(salesPersonName).strip(),
                                    "buyerName":dbHelper.RemoveNewLineCarrigeReturn(buyerName).strip(),
                                    "sale":str(sale).strip(), 
                                    "cost":str(cost).strip()                                              
                                    }
                        deals.append(fiwip_item)
        #End  fi-WIP   
        #return {"dealList":deals,"inventoryStatus":inventoryStatus,"dealSoldDateList":dealSoldDateList,"servicePlanList":servicePlanList,"dealMaxSoldDate":dealMaxSoldDate,"dealMinSoldDate":dealMinSoldDate}
        return {"dealList":deals,"inventoryStatus":inventoryStatus,"servicePlanList":servicePlanList,"dealMaxSoldDate":dealMaxSoldDate,"dealMinSoldDate":dealMinSoldDate}
    @classmethod
    def GetLender(self,acct,gl_accts):
        #logger.debug("acct="+str(acct)+",gl_accts="+str(gl_accts))
        desc=None
        if acct is not None and gl_accts is not None and len(gl_accts)>0:
            acct_list = [gl_acct for gl_acct in gl_accts if (gl_acct['acct_no'] == acct)]
            if acct_list is not None and  len(acct_list)>0:
                desc= acct_list[0]['acct_desc_map']                           
        return desc
    @classmethod
    def GetLenderInventoryMapping(self,acct,gl_accts):
        #logger.debug("acct="+str(acct)+",gl_accts="+str(gl_accts))
        inventorytype=None
        desc=None
        if acct is not None and gl_accts is not None and len(gl_accts)>0:
            acct_list = [gl_acct for gl_acct in gl_accts if (gl_acct['acct_no'] == acct)]
            if acct_list is not None and  len(acct_list)>0:
                desc= acct_list[0]['acct_desc_map']   
                if 'inventorytype_map' in acct_list[0]:
                    inventorytype= acct_list[0]['inventorytype_map']                                         
        return {'inventory_type_map':inventorytype,'desc':desc}
    @classmethod
    def GetDescriptionFilters(self,store_code,region):
        plan_code_filters=[]
        inventorytype_filters=[]
        gl_accts=[]
        app_client=AppClient()
        store_resp=app_client.GetStoreDetail(store_code,region=region)
        if store_resp['status']:
            item=store_resp['item']
            if 'plan_mapping' in item:
                plan_code_filters=item['plan_mapping']
            if 'gl_accts' in item:
                gl_accts= item['gl_accts']
            if 'inventorytype_mapping' in item:
                inventorytype_filters=item['inventorytype_mapping']
                
        return {"plan_code_filters":plan_code_filters,'gl_acct_filters':gl_accts,'inventorytype_filters':inventorytype_filters }
    @classmethod
    def isValidDeal(self,status,soldDate):
        if soldDate is not None and len(soldDate.strip())>0  and status is not None and len(status.strip())>0 and status=='P':
            try:
                soldDateDt=datetime.strptime(soldDate, '%d%b%y')
                right_now_10_days_ago = datetime.datetime.today() - datetime.timedelta(days=10)
                if soldDateDt > right_now_10_days_ago:
                    return False 
            except:
                return False             
        return True
    @classmethod
    def getUpdatedStockFlag(self,inv_item) :
        stockFlag=None
        isMixedStockFlag=False
        for stock_obj in inv_item["stocks"]:                                   
            if stockFlag is None:
                stockFlag=stock_obj['inStockFlag']
            else:
                if stockFlag != stock_obj['inStockFlag']:
                   isMixedStockFlag=True

        if  isMixedStockFlag==True:
            stockFlag='A'           
        return stockFlag; 
    @classmethod
    def populateStockEntryDate(self,old_inv_item,inv_item):
        stockMinEntryDate=None
        stockMaxEntryDate=None                    
        stockMaxEntryDateStr=inv_item['stockMaxEntryDate']
        stockMinEntryDateStr=inv_item['stockMinEntryDate']        
        if stockMaxEntryDateStr!="" :
            try:
                stockMaxEntryDate=datetime.strptime(stockMaxEntryDateStr, "%Y-%m-%d")                       
            except ValueError:
                    ""
        if stockMinEntryDateStr!="" :
            try:
                stockMinEntryDate=datetime.strptime(stockMinEntryDateStr, "%Y-%m-%d")                      
            except ValueError:
                ""
        ############
        stockMaxEntryDateStr=old_inv_item["stockMaxEntryDate"]
        if stockMaxEntryDateStr!="" :
            try:
                stockMaxEntryDt=datetime.strptime(stockMaxEntryDateStr, "%Y-%m-%d")                           
                if stockMaxEntryDate is not None:
                    if  stockMaxEntryDate<  stockMaxEntryDt:
                            stockMaxEntryDate= stockMaxEntryDt                 
            except Exception:                    
                        ""
        stockMinEntryDateStr=old_inv_item["stockMinEntryDate"]
        if stockMinEntryDateStr!="" :
            try:
                stockMinEntryDt=datetime.strptime(stockMinEntryDateStr, "%Y-%m-%d")                           
                if stockMinEntryDate is not None:
                    if  stockMinEntryDate>  stockMinEntryDt:
                        stockMinEntryDate= stockMinEntryDt                 
            except Exception:
                ""
        if stockMinEntryDate is not None:  
            try:                    
                inv_item["stockMinEntryDate"]= str(stockMinEntryDate.strftime("%Y-%m-%d"))
            except:
                inv_item["stockMinEntryDate"]=""
        else:
            inv_item["stockMinEntryDate"]=""
        
        if stockMaxEntryDate is not None:  
            try:                    
                inv_item["stockMaxEntryDate"]= str(stockMaxEntryDate.strftime("%Y-%m-%d"))
            except:
                inv_item["stockMaxEntryDate"]=""
        else:
            inv_item["stockMaxEntryDate"]=""
        
        if inv_item["stockMaxEntryDate"] == inv_item["stockMinEntryDate"] :
            inv_item["searchEntryDate"]=inv_item["stockMinEntryDate"]
        else:
            inv_item["searchEntryDate"]=""

        return inv_item
    @classmethod
    def populateDealSoldDate(self,updatedStocks_resp,inv_item):
        #inv_item["dealSoldDates"]=updatedStocks_resp['dealSoldDates'] 
        #update Deal Max and Min Date  
        dealMaxSoldDate=updatedStocks_resp['dealMaxSoldDate']
        dealMinSoldDate=updatedStocks_resp['dealMinSoldDate']
        if dealMaxSoldDate is not None:  
            try:                    
                inv_item["dealMaxSoldDate"]= str(dealMaxSoldDate.strftime("%Y-%m-%d"))
            except:
                inv_item["dealMaxSoldDate"]=""
        else:
            inv_item["dealMaxSoldDate"]=""
        if dealMinSoldDate is not None:  
            try:                    
                inv_item["dealMinSoldDate"]= str(dealMinSoldDate.strftime("%Y-%m-%d"))
            except:
                inv_item["dealMinSoldDate"]=""
        else:
            inv_item["dealMinSoldDate"]=""

        if inv_item["dealMinSoldDate"] == inv_item["dealMaxSoldDate"] :
            inv_item["searchDate"]=inv_item["dealMinSoldDate"]
        else:
            inv_item["searchDate"]=""
        return inv_item
    @classmethod
    def populateROOpenDate(self,updatedStocks_resp,inv_item):
        #update RO  Max and Min Openm Date  
        roMaxOpenDate=updatedStocks_resp['roMaxOpenDate']
        roMinOpenDate=updatedStocks_resp['roMinOpenDate']
        if roMaxOpenDate is not None:  
            try:                    
                inv_item["roMaxOpenDate"]= str(roMaxOpenDate.strftime("%Y-%m-%d"))
            except:
                inv_item["roMaxOpenDate"]=""
        else:
            inv_item["roMaxOpenDate"]=""
        if roMinOpenDate is not None:  
            try:                    
                inv_item["roMinOpenDate"]= str(roMinOpenDate.strftime("%Y-%m-%d"))
            except:
                inv_item["roMinOpenDate"]=""
        else:
            inv_item["roMinOpenDate"]=""

        if inv_item["roMinOpenDate"] == inv_item["roMaxOpenDate"] :
            inv_item["searchOpenDate"]=inv_item["roMinOpenDate"]
        else:
            inv_item["searchOpenDate"]=""
        return inv_item
    @classmethod
    def handleInventoryTableDataUpdate(self,dynamodb,batch_list,table_name):
        logger.debug("handleInventoryTableDataUpdate  batch_list size="+str(len(batch_list))+",table_name="+table_name)  
        tab_existingitems=Batching.get_batch_data(dynamodb=dynamodb,tableName=table_name,item_list=batch_list)
        existing_items=tab_existingitems[table_name]
        logger.debug("get_batch_data  existing_items size="+str(len(existing_items)))  
        new_batch_list=[]
        if len(existing_items)==0:
            new_batch_list=batch_list
        else:
            for inv_item in batch_list:
                vin_item_list = [existing_item for existing_item in existing_items if (existing_item['document_type'] == inv_item['document_type']) and (existing_item['document_id'] == inv_item['document_id'])]
                if len(vin_item_list)>0:
                    old_inv_item=vin_item_list[0]
                    #updatePO    
                    updated_poOrders= self.getUpdatedPOOrders(old_inv_item,inv_item)      
                    inv_item["poOrders"]=updated_poOrders
                    inv_item["poCount"]=len(updated_poOrders)
                    open_po_list=[po for po in updated_poOrders if po['poType'].upper() == 'SUBLET' and po['status'] == 'OPEN' ]
                    isOpenPO=False
                    isOpenROWithSubLet=False
                    if len(open_po_list)>0:
                        isOpenPO =True  
                    #updateRO 
                    # updated_repairOrders= self.getUpdatedRepairOrders(old_inv_item,inv_item)      
                    #inv_item["repairOrders"]=updated_repairOrders
                    #inv_item["roCount"]=len(updated_repairOrders)

                    openROList=inv_item["repairOrders"]
                    open_ro_with_sublet_list=[ro for ro in openROList if ro['internal'] == True and ro['status'] == 'C97' ]
                    if len(open_ro_with_sublet_list)>0:
                        isOpenROWithSubLet=True                       
                    #getupdated stocks
                    updatedStocks_resp=self.getUpdatedStocks(old_inv_item,inv_item,isOpenPO,isOpenROWithSubLet)
                    inv_item["stocks"]=updatedStocks_resp['updatedStocks']                    
                    inv_item["stockCount"]=len(inv_item["stocks"])
                    if len(inv_item["stocks"])>0:
                        stockFlag=self.getUpdatedStockFlag(inv_item)
                        inv_item["stockFlag"]=stockFlag
                    inv_item=self.populateStockEntryDate(old_inv_item,inv_item)                   
                     
                    #update Deal Max and Min Date  
                    inv_item=self.populateDealSoldDate(updatedStocks_resp,inv_item)
                     
                    #update service plan
                    existing_servicePlans=old_inv_item["servicePlans"] 
                    for plan in existing_servicePlans:
                        if not plan in inv_item["servicePlans"]:
                            inv_item["servicePlans"].append(plan)

                    new_batch_list.append(inv_item) 
                else:  
                    #new VIN 
                    new_batch_list.append(inv_item)
        logger.debug("new_batch_list size="+str(len(new_batch_list)))  
        return new_batch_list
    @classmethod
    def calculateSoldMinMaxDate(self,soldDate,dealMaxSoldDate,dealMinSoldDate):     
        searchDate=""     
        if soldDate is not None and len(soldDate)>0:
            try:
                soldDateDt=datetime.strptime(soldDate, '%d%b%y')
                searchDate = soldDateDt.strftime("%Y-%m-%d")  
                if dealMaxSoldDate is None:
                    dealMaxSoldDate=soldDateDt
                else:
                    if dealMaxSoldDate<soldDateDt:
                        dealMaxSoldDate=soldDateDt
                if dealMinSoldDate is None:
                    dealMinSoldDate=soldDateDt
                else:
                    if dealMinSoldDate>soldDateDt:
                        dealMinSoldDate=soldDateDt
                                                    
            except ValueError:
                searchDate= ""
        #if not searchDate in dealSoldDateList:
        #    dealSoldDateList.append(searchDate)
        #return {'dealSoldDateList':dealSoldDateList,'dealMinSoldDate':dealMinSoldDate,'dealMaxSoldDate':dealMaxSoldDate}
        return {'dealMinSoldDate':dealMinSoldDate,'dealMaxSoldDate':dealMaxSoldDate}
    @classmethod
    def populateDeals(self,old_stock_item,new_stock_item_list,isOpenPO,isOpenROWithSubLet,dealMaxSoldDate,dealMinSoldDate):
        new_stock_item=new_stock_item_list[0]
        old_deals=old_stock_item["deals"]
        new_deals=new_stock_item["deals"]
        updated_deals=[]
        if len(old_deals)==0:
            updated_deals=[]
            for deal in new_deals:
                status= deal["dealStatus"]
                soldDate= deal["soldDate"]   
                if self.isValidDeal(status=status,soldDate=soldDate):
                   updated_deals.append(deal)
                   soldDate=deal['soldDate'] 
                   result=self.calculateSoldMinMaxDate(soldDate,dealMaxSoldDate,dealMinSoldDate)
                   dealMaxSoldDate=result['dealMaxSoldDate']
                   dealMinSoldDate=result['dealMinSoldDate']

        else:
            if len(new_deals)==0:
                updated_deals=[]
                for deal in old_deals:
                   status= deal["dealStatus"]
                   soldDate= deal["soldDate"]   
                   if self.isValidDeal(status=status,soldDate=soldDate):
                      updated_deals.append(deal)
                      soldDate=deal['soldDate']                     
                      result=self.calculateSoldMinMaxDate(soldDate,dealMaxSoldDate,dealMinSoldDate)
                      dealMaxSoldDate=result['dealMaxSoldDate']
                      dealMinSoldDate=result['dealMinSoldDate']

            else:
                #updated_deals=new_deals
                updated_deals=[]
                new_updated_deals=[]
                soldDate=None
                #add existing deals
                for deal in old_deals:                     
                    deal_item_list = [new_deal for new_deal in new_deals if (new_deal['dealNo'] == deal['dealNo']) ]
                    if len(deal_item_list)==0:
                        new_updated_deals.append(deal) 

                new_updated_deals.extend(new_deals)

                for deal in new_updated_deals:
                    status= deal["dealStatus"]
                    soldDate= deal["soldDate"]   
                    if self.isValidDeal(status=status,soldDate=soldDate):
                      updated_deals.append(deal)
                      soldDate=deal['soldDate']                     
                      result=self.calculateSoldMinMaxDate(soldDate,dealMaxSoldDate,dealMinSoldDate)
                      dealMaxSoldDate=result['dealMaxSoldDate']
                      dealMinSoldDate=result['dealMinSoldDate']
                    
            
        new_stock_item["deals"]=updated_deals
        dealCount=0
        if  updated_deals is not None and len(updated_deals)>0:
            dealCount=len(updated_deals)
        new_stock_item["dealCount"]=dealCount
        stockStatus=new_stock_item['stockStatus']             
        stockType=new_stock_item['stockType']
        inventoryStatus=""
        if stockStatus.upper()=='DT':
            inventoryStatus="dealertraded"
        if isOpenPO or isOpenROWithSubLet:
            inventoryStatus="sublet"
        if stockType.upper()=='NEW' or  stockType.upper()=='USED' or  stockType.upper()=='MISC' or  stockType.upper()=='DEMO' or stockType.upper()=='FLEET':
            inventoryStatus="new-used designation"   
        if stockStatus.upper()=='T':
            inventoryStatus="transferred" 
            
        new_stock_item["inventoryStatus"]=inventoryStatus 
        return {"new_stock_item":new_stock_item,"dealMaxSoldDate":dealMaxSoldDate,"dealMinSoldDate":dealMinSoldDate}
    @classmethod
    def getUpdatedStocks(self,old_inv_item,inv_item,isOpenPO,isOpenROWithSubLet):
        old_stocks=old_inv_item["stocks"] 
        new_stocks= inv_item["stocks"]
        updatedStocks=[]
        #dealSoldDateList=inv_item['dealSoldDates']  
        dealMinSoldDate=None
        dealMaxSoldDate=None  
        dealMaxSoldDateStr=inv_item['dealMaxSoldDate']
        dealMinSoldDateStr=inv_item['dealMinSoldDate']       
        if dealMaxSoldDateStr!="" :
            try:
                dealMaxSoldDate=datetime.strptime(dealMaxSoldDateStr, "%Y-%m-%d")                       
            except ValueError:
                    ""
        if dealMinSoldDateStr!="" :
            try:
                dealMinSoldDate=datetime.strptime(dealMinSoldDateStr, "%Y-%m-%d")                      
            except ValueError:
                ""       
        if len(old_stocks)==0:       
            updatedStocks=new_stocks
        else:
            if len(new_stocks)==0:
                updatedStocks=old_stocks
                dealMaxSoldDateStr=old_inv_item['dealMaxSoldDate']
                dealMinSoldDateStr=old_inv_item['dealMinSoldDate']            
                if dealMaxSoldDateStr!="" :
                    try:
                        dealMaxSoldDate=datetime.strptime(dealMaxSoldDateStr, "%Y-%m-%d")                       
                    except ValueError:
                            ""
                if dealMinSoldDateStr!="" :
                    try:
                        dealMinSoldDate=datetime.strptime(dealMinSoldDateStr, "%Y-%m-%d")                      
                    except ValueError:
                            ""
            else:
                #update all old stock with new
                for old_stock_item in old_stocks:
                    new_stock_item_list = [new_stock_item for new_stock_item in new_stocks if (new_stock_item['stockNo'] == old_stock_item['stockNo']) ]
                    if len(new_stock_item_list)>0:
                        resp=self.populateDeals(old_stock_item,new_stock_item_list,isOpenPO,isOpenROWithSubLet,dealMaxSoldDate,dealMinSoldDate)
                        new_stock_item=resp["new_stock_item"]
                        dealMaxSoldDate=resp['dealMaxSoldDate']
                        dealMinSoldDate=resp['dealMinSoldDate']
                        updatedStocks.append(new_stock_item)
                    else:
                        for deal in old_stock_item["deals"]:
                            soldDate=deal['soldDate']  
                            #result=self.calculateSoldMinMaxDate(soldDate,dealMaxSoldDate,dealMinSoldDate,dealSoldDateList)
                            result=self.calculateSoldMinMaxDate(soldDate,dealMaxSoldDate,dealMinSoldDate)
                            dealMaxSoldDate=result['dealMaxSoldDate']
                            dealMinSoldDate=result['dealMinSoldDate']
                            #dealSoldDateList=result["dealSoldDateList"]
                        updatedStocks.append(old_stock_item)
                #add remaining new stocks
                for new_stock_item in new_stocks:
                    updated_stock_item_list = [updated_stock_item for updated_stock_item in updatedStocks if (new_stock_item['stockNo'] == updated_stock_item['stockNo']) ]
                    if len(updated_stock_item_list)==0:
                        for deal in new_stock_item["deals"]:
                            soldDate=deal['soldDate']
                            #result=self.calculateSoldMinMaxDate(soldDate,dealMaxSoldDate,dealMinSoldDate,dealSoldDateList)
                            result=self.calculateSoldMinMaxDate(soldDate,dealMaxSoldDate,dealMinSoldDate)
                            dealMaxSoldDate=result['dealMaxSoldDate']
                            dealMinSoldDate=result['dealMinSoldDate']
                        updatedStocks.append(new_stock_item)

        #return  {'dealSoldDates':dealSoldDateList,'updatedStocks':updatedStocks,'dealMaxSoldDate':dealMaxSoldDate,'dealMinSoldDate':dealMinSoldDate }
        return  {'updatedStocks':updatedStocks,'dealMaxSoldDate':dealMaxSoldDate,'dealMinSoldDate':dealMinSoldDate }
    @classmethod
    def getUpdatedPOOrders(self,old_inv_item,inv_item):
        existing_poOrders=old_inv_item["poOrders"] 
        new_poOrders=inv_item["poOrders"] 
        updated_poOrders=[]
        if len(existing_poOrders)==0:
            updated_poOrders=inv_item["poOrders"]
        else:
            if len(new_poOrders)==0:
                updated_poOrders=old_inv_item["poOrders"]
            else:
                updated_poOrders=[]
                for po in existing_poOrders:
                    po_item_list = [new_po for new_po in inv_item["poOrders"] if (new_po['poNumber'] == po['poNumber']) ]
                    if len(po_item_list)==0:
                        updated_poOrders.append(po)
                updated_poOrders.extend(new_poOrders)  
        return updated_poOrders    
    @classmethod
    def getUpdatedRepairOrders(self,old_inv_item,inv_item):
        existing_repairOrders=old_inv_item["repairOrders"] 
        new_repairOrders=inv_item["repairOrders"] 
        updated_repairOrders=[]
        if len(existing_repairOrders)==0:
            updated_repairOrders=inv_item["repairOrders"]
        else:
            if len(new_repairOrders)==0:
                updated_repairOrders=old_inv_item["repairOrders"]
            else:
                updated_repairOrders=[]
                for ro in existing_repairOrders:
                    ro_item_list = [new_ro for new_ro in inv_item["repairOrders"] if (new_ro['roNumber'] == ro['roNumber']) ]
                    if len(ro_item_list)==0:
                        updated_repairOrders.append(ro)
                updated_repairOrders.extend(new_repairOrders)  
        return updated_repairOrders          
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
                    futures.append(executor.submit(self.load_batch,dynamodb,TableName,overwrite_by_pkeys, batch,checkForUpdateExisting))
                for future in concurrent.futures.as_completed(futures):                     
                    results.append(future.result())  
    
    @classmethod
    def load_batch(self,dynamodb,table_name,pkeys, batch_list,checkForUpdateExisting):
        logger.debug("load_batch -checkForUpdateExisting="+str(checkForUpdateExisting)+" batch_list size="+str(len(batch_list)))  
        if str(checkForUpdateExisting).lower()=='true':
           updated_batch_list=self.handleInventoryTableDataUpdate(dynamodb=dynamodb,batch_list=batch_list,table_name=table_name)
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
    
    @classmethod  
    def isValidVIN(self,s): 
        if s is not None and len(s.strip())==17:
            s=s[-6:] 
            s1 = []    
            # Insert characters in
            # the set
            for i in range(len(s)):
                s1.append(s[i])
        
            # If all characters are same
            # Size of set will always be 1
            s1 = list(set(s1))
            if(len(s1) == 1):
                return False
            else:
                return True
        else:
                return False
