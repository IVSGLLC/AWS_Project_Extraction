 
import json
import sys
import logging
from decimal import Decimal
import os
from datetime import datetime
from DBHelper import DBHelper
loglevel = int(os.environ['LOG_LEVEL'])
logger = logging.getLogger('InvoiceDBHelper')
logger.setLevel(loglevel)
dbhelper=DBHelper()
class InvoiceDBHelper():
    @classmethod
    def SaveInvoice(self,store_code,df,client_id):
        logger.info("Inside SaveInvoice Method....store_code:"+store_code+",client_id="+client_id)
        LAMBDA_BATCH_SIZE=100
        local=True
        region=os.environ['REGION']          
        TableName=store_code+"_INVOICE_FILE"
        try:
            # Get the service resource.
            batch_items=[]
            items=[]
            ct = datetime.now()
            create_date = ct.strftime("%Y-%m-%d %I:%M:%S %p")
            create_date_only = ct.strftime("%Y-%m-%d")
            recount=0
            df_ro=df["ro"]
            df_labor=df["labor"]
            df_rop=df["ro_rop"]
            df_labor_ops=df["labor_ops"]
            df_parts=df["parts"]     
            df_ro_mls=df["ro_mls"] 
            df_story=df["story"]   
            account_lines=df['account_line']
            accCodeList=['S']
            if not dbhelper.ValidateAccounts(store_code=store_code,account_lines=account_lines,region=region,accCodeList=accCodeList):
                lines=str(','.join(account_lines))
                logger.error("Error Occure in Save  data to table ["+TableName+"] in DB. INVALID LOGON IDENTIFIED ,Logon in Data:"+lines,exc_info=True)
                return { "operation_status":"FAILED","error_code":-1 ,"error_message":"INVALID LOGON IDENTIFIED, SKIP SAVE IN DB :"+lines,"total_item_count":"0","total_item_parsed":"0","items":[]} 
     
            df_ro=df_ro.to_dict('records')            
            for  row in df_ro:
                referNumber=row['REFER#']                      
                recount=recount+1                 
                cust_FLNM=str(row["CUST-N1"])
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
                #row["SA-NAME"]   
                emp_FLNM=str(row["SA-NAME"]) 
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
              
                sts=row["RO-STATUS"]
                sts=dbhelper.GetLatestStatus(sts)

                dict_labor= self.GetFilteredDF(df_labor,referNumber,None)                
                mileage_in=''
                mileage_out=''
                prod_date=''
                war_exp_date=''
                license=''
                payment=''      
                color=''                
                servicedetails=[]
                eng_no=row["EPDE.ENG.NO"] 
                dealer_cd=row["EPDE.DLR"] 
                waiter=False
                totalEstimate=Decimal(0.0)
                if len(dict_labor)>0:                    
                    dict_labor=dict_labor.to_dict('records')                    
                    for  rowLabor in dict_labor:

                        lineCode=rowLabor["LINE-CDS"] 
                        servicePartsDescription=dbhelper.RemoveNewLineCarrigeReturn(rowLabor["SVC-DESCS"] )
                        causes=dbhelper.RemoveNewLineCarrigeReturn(rowLabor["CAUSES"] )
                        estimate=dbhelper.RemoveNewLineCarrigeReturn(rowLabor['TOTAL'])
                        estimate=estimate.strip()
                        #logger.info("estimate="+str(estimate))
                        if dbhelper.is_number(estimate):
                            totalEstimate=totalEstimate+Decimal(estimate)
                        mileage_in=rowLabor["MILEAGE-IN"] 
                        mileage_out=rowLabor["MILEAGE-OUT"] 
                        war_exp_date=self.FormatDateNew(open_dt_str= rowLabor["WARR-EXP-DATE"],print_time_seconds_str="")
                       
                        license=rowLabor["LICENSE"] 
                        color=rowLabor["COLOR"] 
                        payment=dbhelper.RemoveNewLineCarrigeReturn(rowLabor["PAY-TYPE"]) 
                        prod_date=self.FormatDateNew(open_dt_str= rowLabor["PROD-DATE"],print_time_seconds_str="")
                        waiter=False
                        if len(rowLabor['WAITER'] ) >0 and rowLabor['WAITER']=='1':
                            waiter=True
                        
                        dict_rop= self.GetFilteredDF(df_rop,referNumber,lineCode) 
                        rops=[]
                        if len(dict_rop)>0:
                            dict_rop=dict_rop.to_dict('records')
                            for rowRo_rop in dict_rop:   
                                techNo=rowRo_rop["TECH-NO"]                             
                                t_type=rowRo_rop["TYPE"]  
                                t_date=self.FormatPuchDate(rowRo_rop["DATE"])
                                startTime=self.FormatTime(rowRo_rop["START-TIME"])
                                finishTime=self.FormatTime(rowRo_rop["FINISH-TIME"])
                                duration=rowRo_rop["DURATION"]
                                rop_obj={
                                        "techNo":techNo,
									    "type":t_type,
                                        "date":t_date,                                      
									    "startTime":startTime,
                                        "finishTime":finishTime,
                                        "duration":duration                 
                                         
                                      }
                                rops.append(rop_obj)      
                        dict_labor_ops= self.GetFilteredDF(df_labor_ops,referNumber,lineCode) 
                        opCodes=[]
                        if len(dict_labor_ops)>0:
                            dict_labor_ops=dict_labor_ops.to_dict('records')
                            for rowLabor_ops in dict_labor_ops: 
                                seq_no=  rowLabor_ops["SEQ-NO"] 
                                pts_seq_nos=rowLabor_ops["PTS-SEQ-NOS"]
                                parts=[]
                                if len(pts_seq_nos) >0:
                                   seqList= str(pts_seq_nos  ).split(',')
                                   parts=self.GetPartsDataList(df_parts,referNumber,lineCode,seqList)                                 
                                opCode=   rowLabor_ops["OP-CODE"]                             
                                desc=  dbhelper.RemoveNewLineCarrigeReturn( rowLabor_ops["SVC-DESC"] )
                                lbr_sale_acct=rowLabor_ops["LBR-SALE-ACCT"]
                                cost=rowLabor_ops["COST$"]
                                sale=rowLabor_ops["SALE$"]
                                actualHrs= rowLabor_ops["ACTUAL-HR"] 
                                hrs=   rowLabor_ops["HR-SOLD"] 
                                tech_no=   rowLabor_ops["TECH-NO"] 
                                cr_co=   rowLabor_ops["CR.CO"] 
                                lbr_sale_ctrl_no=rowLabor_ops["LBR-SALE-CTRL-NO"] 
                                lbr_type=   rowLabor_ops["LBR-TYPE"] 
                                cost_amt=   rowLabor_ops["COST-AMT"] 
                                sale_amt=   rowLabor_ops["SALE-AMT"]  
                                opCodeObj={
                                        "seqNo":seq_no,
                                        "code":opCode,
									    "desc":str(desc).strip(),
                                        "techNo":tech_no,
                                        "lbrType":lbr_type,
									    "actualHrs":actualHrs,
                                        "soldHrs":hrs,
                                        "cost":cost,
                                        "sale":sale,
                                        "lbrCostAmt":cost_amt,
									    "lbrSaleAmt":sale_amt ,
                                        "lbrSaleAcct":lbr_sale_acct,
                                        "lbrSaleCtrlNo":lbr_sale_ctrl_no,
                                        "saleCo":'',
                                        "crCo":cr_co , 
                                        "feeId":''  ,
                                        "discId":'',
                                        "type":'LOP-OPS',
                                        "parts":parts                                                   
                                      }
                                opCodes.append(opCodeObj)

                        dict_ro_mls= self.GetFilteredDF(df_ro_mls,referNumber,lineCode) 
                        roMls=[]
                        if len(dict_ro_mls)>0:
                            dict_ro_mls=dict_ro_mls.to_dict('records')
                            for rowRo_Mls in dict_ro_mls: 
                                seq_no=  rowRo_Mls["MLS-NO"]
                                pts_seq_nos=rowRo_Mls["MCD-NOS"] 
                                parts=[]
                                if len(pts_seq_nos) >0:
                                   seqList= str(pts_seq_nos  ).split(',')
                                   parts=self.GetPartsDataList(df_parts,referNumber,lineCode,seqList)
                                opCode=rowRo_Mls["OP-CODE"]                             
                                desc=dbhelper.RemoveNewLineCarrigeReturn(rowRo_Mls["SVC-DESC"]) 
                                lbr_sale_acct=rowRo_Mls["LBR-SALE-ACCT"]
                                cost=rowRo_Mls["COST$"]
                                sale=rowRo_Mls["SALE$"]
                                sale_co=rowRo_Mls["SALE.CO"] 
                                lbr_sale_ctrl_no=rowRo_Mls["LBR-SALE-CTRL-NO"] 
                                mls_type=rowRo_Mls["MLS-TYPE"] 
                                cost_amt=rowRo_Mls["COST-AMT"] 
                                sale_amt=rowRo_Mls["SALE-AMT"]  
                                fee_id=rowRo_Mls["FEE.ID"] 
                                desc_id=  rowRo_Mls["DISC.ID"]                        
                                mls={
                                        "seqNo":seq_no,
                                        "code":opCode,
									    "desc":str(desc).strip(),
                                        "techNo":'',
                                        "lbrType":mls_type,
									    "actualHrs":'',
                                        "soldHrs":'',
                                        "cost":cost,
                                        "sale":sale,
                                        "lbrCostAmt":cost_amt,
									    "lbrSaleAmt":sale_amt ,
                                        "lbrSaleAcct":lbr_sale_acct,
                                        "lbrSaleCtrlNo":lbr_sale_ctrl_no,
                                        "saleCo":sale_co,
                                        "crCo":'' , 
                                        "feeId":fee_id  ,
                                        "discId":desc_id,
                                        "type":'RO-MLS' ,
                                        "parts":parts                    
                                      }
                                roMls.append(mls) 
                        if len(roMls)>0:    
                           opCodes.extend(roMls)
                        
                        techStory=None
                        dict_story= self.GetFilteredDF(df_story,referNumber,lineCode)
                        if len(dict_story)>0:
                            dict_story=dict_story.to_dict('records')
                            for rowStory in dict_story:  
                                storyText=dbhelper.RemoveNewLineCarrigeReturn(rowStory["TECH-STORY"] )
                               
                                storyTime=self.FormatTime(rowStory["STORY-TIME"])
                                storyDate=self.FormatDateNew(open_dt_str= rowStory["STORY-DATE"],print_time_seconds_str="")
                                techName=dbhelper.RemoveNewLineCarrigeReturn(rowStory['TECH-NAME'])
                                techEmpNo=rowStory['TECH-EMP-NO']                                               
                                techStory={
                                        "techComment":str(storyText).strip(),
									    "techEmpNo":techEmpNo,
                                        "techEmpName":techName,                                      
									    "storyDate":storyDate,
                                        "storyTime":storyTime                                        
                                      }
                                 
                        serviceDetail={
                                    "lineCode":lineCode,
                                    "lineDesc":str(servicePartsDescription).strip(),
                                    "causes":str(causes).strip(),
                                    "lineTotalEstimate":str(estimate),
                                    "operations":opCodes,
                                    "techStory":techStory,
                                    "punchTimes":rops
                                  }
                        servicedetails.append(serviceDetail)
                    #END FOR  
                #end labor condition 
                #lineCode=""
                #servicePartsDescription=None
                #tech_no=None
                dict_ro_mls= self.GetFilteredDF(df_ro_mls,referNumber,'RO') 
                roMls=[]
                if len(dict_ro_mls)>0:
                    #logger.debug("ROMLS with 'RO' LineCode found RO#"+referNumber+" total count="+str(len(dict_ro_mls)))
                   
                    dict_ro_mls=dict_ro_mls.to_dict('records')
                    for rowRo_Mls in dict_ro_mls: 
                        seq_no=  rowRo_Mls["MLS-NO"]
                        pts_seq_nos=rowRo_Mls["MCD-NOS"] 
                        parts=[]
                        if len(pts_seq_nos) >0:
                            seqList= str(pts_seq_nos  ).split(',')
                            parts=self.GetPartsDataList(df_parts,referNumber,'',seqList)
                    
                        opCode=rowRo_Mls["OP-CODE"]                             
                        desc=dbhelper.RemoveNewLineCarrigeReturn(rowRo_Mls["SVC-DESC"]) 
                        lbr_sale_acct=rowRo_Mls["LBR-SALE-ACCT"]
                        cost=rowRo_Mls["COST$"]
                        sale=rowRo_Mls["SALE$"]
                        sale_co=rowRo_Mls["SALE.CO"] 
                        lbr_sale_ctrl_no=rowRo_Mls["LBR-SALE-CTRL-NO"] 
                        mls_type=rowRo_Mls["MLS-TYPE"] 
                        cost_amt=rowRo_Mls["COST-AMT"] 
                        sale_amt=rowRo_Mls["SALE-AMT"]  
                        fee_id=rowRo_Mls["FEE.ID"]  
                        desc_id=  rowRo_Mls["DISC.ID"]                           
                        mls={
                            "seqNo":seq_no,
                            "code":opCode,
                            "desc":str(desc).strip(),
                            "techNo":'',
                            "lbrType":mls_type,
                            "actualHrs":'',
                            "soldHrs":'',
                            "cost":cost,
                            "sale":sale,
                            "lbrCostAmt":cost_amt,
                            "lbrSaleAmt":sale_amt ,
                            "lbrSaleAcct":lbr_sale_acct,
                            "lbrSaleCtrlNo":lbr_sale_ctrl_no,
                            "saleCo":sale_co,
                            "crCo":'' , 
                            "feeId":fee_id  ,
                            "discId":desc_id,
                            "type":'RO-MLS' ,
                            "parts":parts    
                            }
                        roMls.append(mls)  
                
                parts=self.GetPartsDataList(df_parts,referNumber,"",None) 
                stock_no=row['EPDE.STOCK']    
                options=""
                if len(stock_no)>0:
                    options=options+"SOLD-STK:"+stock_no
                if len(dealer_cd)>0:
                    options=options+" DLR:"+dealer_cd
                if len(eng_no)>0:
                    options=options+" ENG:"+eng_no
                OPEN_DATE=self.FormatDateNew(open_dt_str=row["OPEN-DATE"],print_time_seconds_str="")
                CLOSE_DATE=self.FormatDateNew(open_dt_str=row["CLOSED"],print_time_seconds_str="")
                EPDE_DEL_DATE=self.FormatDateNew(open_dt_str=row["EPDE.DEL.DATE"],print_time_seconds_str="")
                PROMISED_DATE=self.FormatDateNew(open_dt_str=row["PROMISED-DATE"],print_time_seconds_str="")
                ro_detail={
                            "client_id": str(client_id),
                            "store_code": str(store_code),
                            "document_id": referNumber,
                            "document_type": 'RO',
                            "saleType": row['SALE-TYPE'],
                            "status":   sts,
                            "openDate": OPEN_DATE,
                            "closeDate": CLOSE_DATE,
                            "openTime":self.FormatTime(row["EPDE.PRINT.TIME"]),
                            "closeTime":self.FormatTime( row['EPDE.CLOSED.TIME']),
                            "amountDue": row['CP-TOTAL-SALE$'],
                            "warrantyDue": row['EPDE.WT.TOTAL'],
                            "comments":dbhelper.RemoveNewLineCarrigeReturn( row['COMMENTS']).strip(),
                            "vehicle": {
                                "vin":row['VIN'],
                                "make":row['MAKE'],
                                "model": row['MODEL'],
                                "year": row['YEAR'],
                                "color":color,
                                "engNo":eng_no,
                                "dlrCd":dealer_cd,
                                "delDate":EPDE_DEL_DATE,
                                "stockNo":stock_no
                            },
                            "customer": {
                                "id": row['EPDE.CUST'],
                                "firstName": dbhelper.RemoveNewLineCarrigeReturn(custFNM),
                                "lastName": dbhelper.RemoveNewLineCarrigeReturn(custLNM),
                                "email":str(row['EPDE.EMAIL']).strip(),
                                "addresses": [
                                    {
                                        "addressLine": [
                                            dbhelper.RemoveNewLineCarrigeReturn(row['CUST-N3'])
                                        ],
                                        "city": row['CITY'],
                                        "state": row['STATE'],
                                        "zip": str(row['ZIP']).strip()
                                    }
                                ],
                                "contacts": [
                                    {
                                        "desc": "Contact Number",
                                        "value": row['EPDE.PHONE'] 
                                    },
                                      {
                                        "desc": "Business Phone",
                                        "value": row['EPDE.BUS'] 
                                    },
                                      {
                                        "desc": "Cell Phone",
                                        "value": row['EPDE.CELL'] 
                                    },
                                      {
                                        "desc": "Home Phone",
                                        "value": row['EPDE.HOME'] 
                                    }
                                ],
                                "company": ""
                            },
                            "employee": {
                                "id": row['SA-R5'],
                                "firstName":dbhelper.RemoveNewLineCarrigeReturn(empFNM),
                                "lastName": dbhelper.RemoveNewLineCarrigeReturn(empLNM)
                            },
                            "hattagnumber": row["TAG-NO"],
                            "contactemail": row['EPDE.EMAIL'],
                            "contactphone": row['EPDE.PHONE'] ,
                            "homephone": row['EPDE.HOME'] ,
                            "cellphone": row['EPDE.CELL'] ,
                            "busphone":row['EPDE.BUS'],
                            "laborTotal":row['CP-LABOR-SALE$'],
                            "partsTotal":row['CP-PART-SALE$'],
                            "miscTotal":row['CP-MISC-SALE$'],
                            "golTotal":row['CP-LUBE-SALE$'], 
                            "salesTaxTotal":row['CP-TAX-SALE$'],
                            "deductibleTotal":row['CP-DEDUCT-SALE$'],
                            "sublTotal":row['CP-SUBL-SALE$'],
                            "schgTotal":row['CP-SCHG-SALE$'],
                            "poNumber":'',
                            "rate":'',
                            "options":options,
                            "payment":payment,
                            "promisedDate":PROMISED_DATE,
                            "promisedTime":self.FormatTime(row['PROMISED-TIME']),
                            "waiter":waiter,
                            "mileageIn":  mileage_in ,
                            "mileageOut":  mileage_out,
                            "prodDate":prod_date,
                            "warExpDate":war_exp_date,
                            "license":license, 
                            "spc_ins": dbhelper.RemoveNewLineCarrigeReturn(row["EPDE.SPC.INS"]),
                            "serviceDetails": servicedetails,
                            "roOperations":roMls ,
                            "roParts":parts,
                            "totalEstimate":str(totalEstimate),
                            "create_date": dbhelper.CovertToString(create_date_only),
                            "create_date_time":create_date,
                            "exp_time":dbhelper.get_time_to_live()                            
                        }         
                
                items.append(ro_detail) 
                if not local:
                    json_string = json.dumps(items)
                    byte_ = json_string.encode("utf-8")
                    size_in_bytes = len(byte_)
                    if size_in_bytes>=240000 or len(items)>= LAMBDA_BATCH_SIZE:
                        batch_items.append(items)
                        items=[]
                
            if not local and len(items)>0:
                batch_items.append(items)

            region=os.environ['REGION']          
            TableName=store_code+"_INVOICE_FILE"
            overwrite_by_pkeys=['document_type', 'document_id']         
            et = datetime.now()
            delta=et-ct
            logger.debug("Build JSON total_seconds="+str(delta.total_seconds()))
            logger.debug("TOTAL batch_items="+str(len(batch_items)))           
            MAX_PROC=4
            BATCH_SIZE=25
            overwrite_by_pkeys=['document_type', 'document_id']
            if items is not None and  len(items)>0:
                attributes_to_compare=[
                            "document_id",
                            "saleType",
                            "status",
                            "openDate",
                            "closeDate",
                            "openTime",
                            "closeTime",
                            "amountDue",
                            "warrantyDue",
                            "comments",
                            "hattagnumber",
                            "contactemail",
                            "contactphone",
                            "homephone",
                            "cellphone",
                            "busphone",
                            "laborTotal",
                            "partsTotal",
                            "miscTotal",
                            "golTotal",
                            "salesTaxTotal",
                            "deductibleTotal",
                            "sublTotal",
                            "schgTotal",
                            "poNumber",
                            "rate",
                            "options",
                            "payment",
                            "promisedDate",
                            "promisedTime",
                            "waiter",
                            "mileageIn",
                            "mileageOut",
                            "prodDate",
                            "warExpDate",
                            "license",
                            "spc_ins",
                            {
                                "vehicle": [
                                    "vin",
                                    "make",
                                    "model",
                                    "year",
                                    "color",
                                    "engNo",
                                    "dlrCd",
                                    "delDate",
                                    "stockNo",
                                ]
                            },
                            {
                                "customer": [
                                    "id",
                                    "firstName",
                                    "lastName",
                                    "email",
                                    {"addresses": ["addressLine", "city", "state", "zip"]},
                                    {"contacts": ["desc", "value"]},
                                    "company",
                                ]
                            },
                            {"employee": ["id", "firstName", "lastName"]},
                            {
                                "serviceDetails": [
                                    "lineCode",
                                    "lineDesc",
                                    "causes",
                                    "lineTotalEstimate",
                                    {
                                        "operations": [
                                            "seqNo",
                                            "code",
                                            "desc",
                                            "techNo",
                                            "lbrType",
                                            "actualHrs",
                                            "soldHrs",
                                            "cost",
                                            "sale",
                                            "lbrCostAmt",
                                            "lbrSaleAmt",
                                            "lbrSaleAcct",
                                            "lbrSaleCtrlNo",
                                            "saleCo",
                                            "crCo",
                                            "feeId",
                                            "discId",
                                            "type",
                                            {
                                                "parts": [
                                                    "seqNo",
                                                    "partNo",
                                                    "desc",
                                                    "qtyOrd",
                                                    "qtyB",
                                                    "cost",
                                                    "sale",
                                                    "list",
                                                    "lbrCostAmt",
                                                    "lbrSaleAmt",
                                                    "lbrListAmt",
                                                    "tSale",
                                                    "total",
                                                    "saleAcct",
                                                    "saleCo",
                                                    "partsNotes",
                                                ]
                                            },
                                        ]
                                    },
                                    {
                                        "techStory": [
                                            "techComment",
                                            "techEmpNo",
                                            "techEmpName",
                                            "storyDate",
                                            "storyTime"
                                        ]
                                    },
                                    {
                                        "punchTimes": [
                                            "techNo",
                                            "type",
                                            "date",
                                            "startTime",
                                            "finishTime",
                                            "duration"
                                        ]
                                    },
                                ]
                            },
                            {
                                "roOperations": [
                                    "seqNo",
                                    "code",
                                    "desc",
                                    "techNo",
                                    "lbrType",
                                    "actualHrs",
                                    "soldHrs",
                                    "cost",
                                    "sale",
                                    "lbrCostAmt",
                                    "lbrSaleAmt",
                                    "lbrSaleAcct",
                                    "lbrSaleCtrlNo",
                                    "saleCo",
                                    "crCo",
                                    "feeId",
                                    "discId",
                                    "type",
                                    {
                                        "parts": [
                                            "seqNo",
                                            "partNo",
                                            "desc",
                                            "qtyOrd",
                                            "qtyB",
                                            "cost",
                                            "sale",
                                            "list",
                                            "lbrCostAmt",
                                            "lbrSaleAmt",
                                            "lbrListAmt",
                                            "tSale",
                                            "total",
                                            "saleAcct",
                                            "saleCo",
                                            "partsNotes"
                                        ]
                                    },
                                ]
                            },
                            {
                                "roParts": [
                                    "seqNo",
                                    "partNo",
                                    "desc",
                                    "qtyOrd",
                                    "qtyB",
                                    "cost",
                                    "sale",
                                    "list",
                                    "lbrCostAmt",
                                    "lbrSaleAmt",
                                    "lbrListAmt",
                                    "tSale",
                                    "total",
                                    "saleAcct",
                                    "saleCo",
                                    "partsNotes"
                                ]
                            },
                            "totalEstimate"
                        ]
                beForFilterCount=len(items)
                logger.debug("CDK Invoice Total Record Count Before Filter=."+str(len(items)))   
                items=dbhelper.GetChangedItems(region=region,tableName=TableName,items=items,attributes_to_compare=attributes_to_compare)
                if items is not None and  len(items)>0:
                        
                    dbhelper.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
                    logger.info("CDK Invoice Data has been inserted successfuly in table ["+TableName+"] Total Filtered Record Count=."+str(len(items))+", Total Parsed Record Count "+str(beForFilterCount))                 
                else:
                    logger.info("CDK Invoice Data has not been inserted in table ["+TableName+"] Filtered Total Record Count=0")
            else:
                logger.info("CDK Invoice  Data has not been inserted in table ["+TableName+"] Total Record Count=0")

            """ if not local:
                dbhelper.submitSaveData(TableName=TableName,items=batch_items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
            else:
                 dbhelper.submitSaveDataLocal(TableName=TableName,items=items,region=region,overwrite_by_pkeys=overwrite_by_pkeys,checkForUpdateExisting=False,MAX_PROC=MAX_PROC,BATCH_SIZE=BATCH_SIZE)
    	    """           
            et = datetime.now()
            delta=et-ct
            logger.debug("after calling lambda total_seconds="+str(delta.total_seconds())) 
            logger.info("Invoice Data has been inserted successfuly in table ["+TableName+"] Record Count=."+str(len(items)))
            return { "operation_status":"SUCCESS","total_item_count":str(recount),"total_item_parsed":str(len(items)),"items":items} 
        except Exception as e:
            # Return failed record's sequence number
            logger.error("Error Occure in Save Invoice data to table ["+TableName+"] in DB..." , exc_info=True)
            ex_type, ex_value, ex_traceback = sys.exc_info()
            return { "operation_status":"FAILED","error_code":-1 ,"error_message":ex_value} 
    @classmethod
    def GetFilteredDF(self,df,roNumber,lineCode):
        if lineCode is not None :
            dict_filtered=df.loc[(df['REFER#'] == roNumber)&(df['LINE-CDS'] == lineCode)] 
        else:
             dict_filtered=df.loc[(df['REFER#'] == roNumber) ]        
        return dict_filtered
    @classmethod
    def GetPartsDataList(self,df_parts,referNumber,lineCode, seqList):
        dict_parts= self.GetPartsFilteredDF(df_parts,referNumber,lineCode,seqList)
        parts=[]
        if len(dict_parts)>0:
            dict_parts=dict_parts.to_dict('records')
            for rowParts in dict_parts :  
                seq_no=  rowParts["SEQ-NO"]
                part_no=rowParts["PART-NO"] 
                desc=dbhelper.RemoveNewLineCarrigeReturn(rowParts["DESC"])                                
                qtyOrd=rowParts["Q.O."] 
                cost=rowParts["COST"] 
                sale=rowParts["SALE"] 
                list1=rowParts["LIST"]                                
                totalSale=rowParts["T-SALE"] 
                totalAmt=rowParts["TOTAL$"] 
                saleAcct=rowParts["SALE-ACCT"] 
                sale_co=rowParts["SALE.CO"] 
                qtyB=rowParts["Q.B."] 
                partsNotes=dbhelper.RemoveNewLineCarrigeReturn(rowParts["EPDE.PARTS.NOTE"] )
                lbrCostAmt=rowParts["COST-AMT"] 
                lbrSaleAmt=rowParts["SALE-AMT"] 
                lbrListAmt=rowParts["LIST-AMT"]   
                part={ 
                        "seqNo":seq_no,
                        "partNo":part_no,
                        "desc":str(desc).strip(),
                        "qtyOrd":qtyOrd,
                        "qtyB":qtyB,
                        "cost":cost,
                        "sale":sale,
                        "list":list1,
                        "lbrCostAmt":lbrCostAmt,
                        "lbrSaleAmt":lbrSaleAmt ,  
                        "lbrListAmt":lbrListAmt ,      
                        "tSale":totalSale,
                        "total":totalAmt,                                         
                        "saleAcct":saleAcct,
                        "saleCo":sale_co,                                      
                        "partsNotes":str(partsNotes).strip()                                                              
                        }
                parts.append(part)
        return parts
    @classmethod
    def GetPartsFilteredDF(self,df,roNumber,lineCode,seqList):
        if roNumber is not None and lineCode is not None and seqList is not None :
            dict_filtered=df.loc[(df['REFER#'] == roNumber)&(df['LINE-CDS'] == lineCode)& (df['SEQ-NO'].isin( seqList))] 
        elif roNumber is not None and lineCode is not None :
            dict_filtered=df.loc[(df['REFER#'] == roNumber)&(df['LINE-CDS'] == lineCode)] 
        else:
             dict_filtered=df.loc[(df['REFER#'] == roNumber) ]        
        return dict_filtered
    def FormatDateNew(open_dt_str,print_time_seconds_str):
        try:
                    
            if open_dt_str is not None and  len(str(open_dt_str).strip())>0:
                open_dt_str=str(open_dt_str).strip() 
                open_dt_str= open_dt_str.replace(" ", "")              
                open_dt_date = datetime.strptime(open_dt_str, "%d%b%y")                
                if print_time_seconds_str is not None and  len(str(print_time_seconds_str).strip())>0:
                    print_time_seconds_str=str(print_time_seconds_str).strip()
                    print_time_seconds_str= print_time_seconds_str.replace(" ", "") 
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
    def FormatPuchDate(dt_str):
        try:                    
            if dt_str is not None and  len(str(dt_str).strip())>0:
                dt_str=str(dt_str).strip()               
                dt_date = datetime.strptime(dt_str, "%m-%d-%y") 
                dt_date_str=dt_date.strftime("%m/%d/%Y")
                return dt_date_str               
            else:
                return ""
        except Exception as e:
            return ""
    def FormatTime(print_time_seconds_str):
        try:                    
            if print_time_seconds_str is not None and  len(str(print_time_seconds_str).strip())>0:
                    print_time_seconds_str=str(print_time_seconds_str).strip() 
                    f_dt = datetime.strptime(print_time_seconds_str, "%H:%M")
                    f_dt=f_dt.strftime("%I:%M %p")
                    return f_dt
            else:
                return ""
        except Exception as e:
            return ""