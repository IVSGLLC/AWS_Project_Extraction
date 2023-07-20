 
from decimal import Decimal
import json
from pickle import NONE
import sys
import logging
import os
from datetime import datetime
from EPDE_Client import AppClient
from DBHelper import DBHelper
from EPDE_Client import AppClient
loglevel = int(os.environ['LOG_LEVEL'])
logger = logging.getLogger('DealDBHelper')
logger.setLevel(loglevel)
dbhelper=DBHelper()
class DealDBHelper():
    @classmethod
    def SaveDeal(self,store_code,df,client_id):
        logger.info("Inside SaveDeal Method....store_code:"+store_code+",client_id="+client_id)
        try:
           
            TableName=store_code+"_DEAL_FILE"  
            items=[]
            batch_items=[]
            LAMBDA_BATCH_SIZE=200
            local=True
            region=os.environ['REGION']
            recount=0
            ct = datetime.now()
            create_date = ct.strftime("%Y-%m-%d %I:%M:%S %p")
            create_date_only = ct.strftime("%Y-%m-%d")
            recount=0  
            if "fi_wip" not in df :  
                logger.info("Deal Data has not been processed no data found from Parser")
                return { "operation_status":"FAILED","total_item_count":str(recount),"total_item_parsed":str(len(items)),"items":items} 
            df_car_inv=df["car_inv"]
            df_fi_wip=df["fi_wip"]
            df_labor=df["labor"]
            account_lines=df['account_line']
            accCodeList=['S','FI']
            if not dbhelper.ValidateAccounts(store_code=store_code,account_lines=account_lines,region=region,accCodeList=accCodeList):
                lines=str(','.join(account_lines))
                logger.error("Error Occure in Save data to table ["+TableName+"] in DB. INVALID LOGON IDENTIFIED ,Logon in Data:"+lines,exc_info=True)
                return { "operation_status":"FAILED","error_code":-1 ,"error_message":"INVALID LOGON IDENTIFIED, SKIP SAVE IN DB :"+lines,"total_item_count":"0","total_item_parsed":"0","items":[]} 

            filters=self.GetDescriptionFilters(store_code=store_code,region=region)
            #wholeSaleFilters=self.GetWholesaleMapping(store_code=store_code,region=region)
            #logger.info("wholeSaleFilters="+str(wholeSaleFilters))
            df_fi_wip=df_fi_wip.to_dict('records') 
            logger.info("df_fi_wip=Count"+str(len(df_fi_wip)))           
            for  row in df_fi_wip:
                soldDateStr=row['SOLD']
                searchDate="NONE"
                if soldDateStr is not None and len(soldDateStr)>0:
                    try:
                        soldDate=datetime.strptime(soldDateStr, '%d%b%y')
                        searchDate = soldDate.strftime("%Y-%m-%d")                       
                    except ValueError:
                          searchDate= "NONE"
                stock_no=row['STOCK-NO']
                vin=row['SERIAL-NEW']
                country=""
                engine=""
                vehStyle=""
                stockSoldDate=""
                stockStatus=""
                #stockNo=""
                ro_no=""
                prodDate=""
                roCLosedDate=""
                isCoBuyer=False
                mile=""
                if len(row['COBUY-NO'])>0:
                     isCoBuyer=True
                dict_car_wip=df_car_inv.loc[(df_car_inv['STOCK-NO'] == stock_no) ]                 
                if len(dict_car_wip)>0:
                        dict_car_wip=dict_car_wip.to_dict('records')
                        if len(dict_car_wip)>0:
                            rowCarInv=dict_car_wip[0]
                            country=dbhelper.RemoveNewLineCarrigeReturn(rowCarInv['COUNTRY']).strip()
                            engine=dbhelper.RemoveNewLineCarrigeReturn(rowCarInv['ENGINE']).strip()
                            vehStyle=dbhelper.RemoveNewLineCarrigeReturn(rowCarInv['VEH_STYLE']).strip()
                            mile=rowCarInv['MILE']
                            #stockSoldDate=rowCarInv['SOLD-DATE']
                            #stockStatus=rowCarInv['STATUS']
                            #stockNo=rowCarInv["STOCK-NO"]
 
                dict_labor= df_labor.loc[(df_labor['SERIAL'] == vin) ]  
                if len(dict_labor)>0:
                    dict_labor=dict_labor.to_dict('records')
                    if len(dict_labor)>0:
                            rowLabor=dict_labor[0]
                            #ro_no=rowLabor['RO']
                            prodDate=rowLabor['PROD-DATE']
                            roCLosedDate=rowLabor['CLOSE-DATE']
                
                desc1=str(row['DESCRIPTION'] )  
                sale1=str(row['WE-OWE-SALE'])                            
                cost1=str(row['WE-OWE-COST'])                          
                descArr=desc1.split('||')
                saleArr=sale1.split('||')
                costArr=cost1.split('||')                           
                index = 0
                desc=""
                cost=""
                sale=""
                for dsc in descArr:                       
                    if dsc.upper() in filters:
                        desc=dsc
                        sale=saleArr[index]
                        cost=costArr[index]
                        break
                    index += 1
                
                GSI_DATE=searchDate
                if GSI_DATE is NONE or len(GSI_DATE.strip())==0:
                    GSI_DATE='NONE'
                
                GSI_PLAN=desc
                if GSI_PLAN is NONE or len(GSI_PLAN.strip())==0:
                    GSI_PLAN='NONE'
                
                #BANK=dbhelper.RemoveNewLineCarrigeReturn(row['FINANCE-CO-NAME']).strip()
                #logger.debug("BANK="+BANK)
                saleTax=''
                r_or_w='WHOLESALE' 
                try:
                    saleTax=row['TAX-1-AMOUNT']
                    saleTax = saleTax.replace(',', '')
                    if saleTax is None or len(saleTax.strip()) ==0 or saleTax.strip()=="0" or saleTax.strip()=="0.0":
                       r_or_w='WHOLESALE'
                    else:
                        saTax= Decimal(saleTax)
                        if saTax>0 :
                            r_or_w='RETAIL'

                except Exception as e:
                        r_or_w='WHOLESALE'
                GSI_TYPE=r_or_w                 
                
                GSI_STATUS=row['STATUS']
                if GSI_STATUS is NONE or len(GSI_STATUS.strip())==0:
                    GSI_STATUS='NONE'

                GSI_TYPE_PLAN=GSI_TYPE+"#"+GSI_PLAN
                GSI_TYPE_STATUS=GSI_TYPE+"#"+GSI_STATUS
                GSI_PLAN_STATUS=GSI_PLAN+"#"+GSI_STATUS
                GSI_TYPE_PLAN_STATUS=GSI_TYPE+"#"+GSI_PLAN+"#"+GSI_STATUS

                dateInService=str(row['IN-SERVICE-DATE']).strip().replace(" ","")

                deal_item={
                    "document_type":"DEAL",
                    "document_id":row['FI-WIP'],
                    "type":row['NEW-USED'],
                    "status":row['STATUS'],
                    "saleTax":row['TAX-1-AMOUNT'],
                    "saleType":r_or_w,
                    "soldDate":soldDateStr,
                    "description":desc, 
                    "sale":sale, 
                    "cost":cost,  
                    "r_or_w":r_or_w, 
                    "pOrL":row['FIN-LSE'], 
                    "cashDown":row['CASH-D'], 
                    "cashPrice":row['CASH-P'], 
                    "contractTerm":row['TERM'], 
                    "fiManager":row['FI-MGR'],
                    "financeCompanyName":dbhelper.RemoveNewLineCarrigeReturn(row['FINANCE-CO-NAME']).strip(), 
                    "financeCharge":row['FINANCE-CHG'], 
                    "financeTotalAmount":row['FINANCE-TOTAL'], 
                    "leaseCashDown":row['CASH-CAP-REDUCT'], 
                    "leaseContractTerm":row['LEASE-TERM'], 
                    "leaseEndValue":row['LEASE-END-VALUE'],
                    "leaseFinanceAmount":row['TOTAL-ADJ-CAP-COST'],
                    "leaseFinanceCharge":row['TOTAL-FIN-CHARGE'],
                    "leaseMoneyFactor":row['APR-RATE-FULL'],
                    "leaseMSRPO":row['MSRPPL'],
                    "leasePayment":row['MONTHLY-PMT'],
                    "leaseTotal":row['TOTAL-MONTHLY-PMTS'],
                    "leftInTerm":row['TERM'],
                    "trade1SerialNo":row['TRADE2'],
                    "trade1StockNo":row['TRADE1'],
                    "payment":row['MONTHLY-PMT'],
                    "prodDate":prodDate, 
                    "trimLevel":row['TRIMLVL'],
                    "shortStyle":row['TRIMLVL'], 
                    "tradeInNet":row['TRADE1-NET-VALUE'],                  
                    "buyerNumber":row['BUYER-NO'],                    
                    "buyerFullName":dbhelper.RemoveNewLineCarrigeReturn(row['BUYER-NAME']).strip(),  
                    "firstName":dbhelper.RemoveNewLineCarrigeReturn(row['BUYER-FIRST']).strip(), 
                    "middleName":dbhelper.RemoveNewLineCarrigeReturn(row['MIDDLE']).strip(),
                    "lastName":dbhelper.RemoveNewLineCarrigeReturn(row['BUYER-LAST']).strip(), 
                    "buyerBirth":row['BUYER-BIRTH'], 
                    "gender":None, 
                    "buyerStreet":dbhelper.RemoveNewLineCarrigeReturn(row['BUYER-STREET']).strip(),
                    "buyerCity":dbhelper.RemoveNewLineCarrigeReturn(row['BUYER-CITY']).strip(), 
                    "buyerState":dbhelper.RemoveNewLineCarrigeReturn(row['BUYER-STATE']).strip(),                     
                    "buyerZip":dbhelper.RemoveNewLineCarrigeReturn(row['BUYER-ZIP']).strip(), 
                    "buyerPhone1":row['BUYER-PHONE1'], 
                    "buyerPhone2":row['BUYER-PHONE2'], 
                    "email1":row['EMAIL1'], 
                    "email2":row['EMAIL2'],
                    "dhGender":None, 
                    "children":None, 
                    "isCoBuyer":isCoBuyer, 
                    "coBuyerNumber":row['COBUY-NO'], 
                    "coBuyerName":dbhelper.RemoveNewLineCarrigeReturn(row['COBUY-NAME']).strip(), 
                    "coBuyerPhone1":row['COBUY-PHONE1'], 
                    "coBuyerPhone2":row['COBUY-PHONE2'], 
                    "vehicleStockNo":row['STOCK-NO'], 
                    "vin":vin, 
                    "engine":engine, 
                    "vehStyle":vehStyle, 
                    "madeIn":country, 
                    "make":dbhelper.RemoveNewLineCarrigeReturn(row['VEH-MAKE']).strip(), 
                    "model":dbhelper.RemoveNewLineCarrigeReturn(row['VEH-MODEL']).strip(), 
                    "color":dbhelper.RemoveNewLineCarrigeReturn(row['COLOR-NEW']).strip(), 
                    "year":row['YEAR-NEW'],
                    "mileage":mile, #row['ODOMETER-NEW'],
                    "roClosedDate":roCLosedDate,
                    "serialNoVehicle":row['SERIAL-NEW'],
                    "dateInService":dateInService,
                    "customerNo":row['BUYER-NO'], 
                    "apr":row['APR'],
                    "leaseTotalPayments":row['TOT-MNTH-PMT-W-TAX'],                     
                    "createDate":create_date_only,
                    "createDateTime":create_date,
                    "searchDate":searchDate,
                    "GSI_TYPE_PLAN":GSI_TYPE_PLAN,
                    "GSI_TYPE_STATUS":GSI_TYPE_STATUS,
                    "GSI_PLAN_STATUS":GSI_PLAN_STATUS,
                    "GSI_TYPE_PLAN_STATUS":GSI_TYPE_PLAN_STATUS,
                    "GSI_STATUS":GSI_STATUS,
                    "GSI_PLAN":GSI_PLAN,
                    "GSI_DATE":GSI_DATE                                 
                }             
                items.append(deal_item)
                
                recount=recount+1
                if not local:
                    json_string = json.dumps(items)
                    byte_ = json_string.encode("utf-8")
                    size_in_bytes = len(byte_)
                    if size_in_bytes>=240000 or len(items)>= LAMBDA_BATCH_SIZE:
                        batch_items.append(items)
                        items=[]
                
            if not local and len(items)>0:
                batch_items.append(items)

            #END FOR LOOP CAR-INV  
            et = datetime.now()
            delta=et-ct
            logger.debug("Build JSON total_seconds="+str(delta.total_seconds()))
            TableName=store_code+"_DEAL_FILE"   
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
            """  dynamodb = boto3.resource('dynamodb', region_name=region)
            TableName=store_code+"_DEAL_FILE"
            overwrite_by_pkeys=['document_type', 'document_id']
            MAX_PROC=5
            results=[]
            futures=[]
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_PROC) as executor:
                batches = self.chunks(items,25)                
                for batch in batches:
                    futures.append(executor.submit(dbhelper.load_batch,dynamodb, TableName,overwrite_by_pkeys, batch))
                for future in concurrent.futures.as_completed(futures):
                    results.append(future.result()) """
            logger.info("Deal Data has been inserted successfuly in table ["+TableName+"] Record Count=."+str(recount))
            return { "operation_status":"SUCCESS","total_item_count":str(recount),"total_item_parsed":str(len(items)),"items":items} 
        except Exception as e:
            # Return failed record's sequence number
            logger.error("Error Occure in Save Deal data to table ["+TableName+"] in DB..." , exc_info=True)
            ex_type, ex_value, ex_traceback = sys.exc_info()
            return { "operation_status":"FAILED","error_code":-1 ,"error_message":ex_value}     
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
    @classmethod
    def GetWholesaleMapping(self,store_code,region):
        filters=[]
        app_client=AppClient()
        store_resp=app_client.GetStoreDetail(store_code,region=region)
        if store_resp['status']:
            item=store_resp['item']
            if 'wholesale_mapping' in item:
                filters=item['wholesale_mapping']
        return filters