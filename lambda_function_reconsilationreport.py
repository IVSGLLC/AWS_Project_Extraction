
 
import io

import pytz
from EPDE_CloudWatchLog import CloudWatchLogger 
from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient
from EPDE_PostPayment import PostManger
import datetime
import boto3
import pandas as pd
import boto3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
logger=LogManger()
err_handler=ErrorHandler()
def export_excel(df):
    with io.BytesIO() as buffer:
     with pd.ExcelWriter(buffer) as writer:
        df.to_excel(writer)
     buffer.seek(0)        
     return buffer.getvalue()

def lambda_handler(event, context):
    res_json=""
    resHandler=ResponseHandler()
    eastern=pytz.timezone('US/Eastern')  
    requestStart = datetime.now().astimezone(eastern)
    status="ERROR"  
    stage="prod"
    responseCode=-1   
    try:
        _moduleNM="EPDE_Lambda"
        _functionNM="lambda_handler"
        err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)     
        region=event['REGION']
        tableperstore=1
        tableperstore_post=1
        is_Test=False
        if "IS_TEST" in event:   
           test=str(event["IS_TEST"])
           if test.lower()=='true':
              is_Test=True        
        email_source=event["EMAIL_SOURCE"]           
        app_client= AppClient()
        post_mgr=PostManger(region,tableperstore_post)
        store_grp_resp=app_client.GetStoreGroups(region)
        if store_grp_resp['status']:
            store_groups=store_grp_resp['items']
            for group in store_groups:
                extraInfo=[]
                group_id=group['group_id']
                group_name=group['group_name']
                stores_resp=None
                alerts=group['alerts']
                if 'DAILY_PAYMENT_RECONCILATION' in alerts:   
                    if not is_Test:
                        stores_resp=app_client.GetStores(group_id,region)
                    else:
                        stores_resp=app_client.GetTestStores(group_id,region)    

                    if stores_resp!=None and  stores_resp['status']== True:
                        stores=stores_resp['items']
                        report_summary_list=[]
                        attachments=[]
                        for store in stores:
                            store_code=store['store_code']   
                            create_date=""   
                            issued = datetime.datetime.now()
                            create_date = issued.strftime("%m-%d-%Y")  
                            utc=pytz.utc
                            eastern=pytz.timezone('US/Eastern')
                            date=datetime.datetime.strptime(create_date,"%m-%d-%Y")
                            date_eastern=eastern.localize(date,is_dst=None)
                            date_utc=date_eastern.astimezone(utc)
                            create_date=str(date_utc.timestamp())                    
                            ro_state_resp= post_mgr.GetPostPaymentRereconciliationReport(store_code,create_date,tableperstore) 
                            if ro_state_resp['status']:                                                       
                                report_summary=ro_state_resp['report_summary']
                                report_summary_list.append(report_summary)
                                excel_doc=excelUpdateDT(ro_state_resp['excel_doc'] )                          
                                excelName=ro_state_resp['excel_name']
                                logger.debug("excel_name="+excelName+",excel_doc:"+str(excel_doc)+",total rows="+str(len(excel_doc)))
                                excel_attachment={"excel_doc":excel_doc ,"excel_name":excelName}
                                attachments.append(excel_attachment)                            
                            else:
                                logger.debug("Error found in PostPayment Rereconciliation Report for store_code:"+store_code)                                                    
                                extraInfo.append({"store_code":store_code,"error":ro_state_resp})
                        status="SUCCESS"
                        responseCode=0  
                        if len(report_summary_list)>0:                            
                            send_email_resp= send_email(group,report_summary_list,attachments,region,email_source,is_Test)
                             
                            if send_email_resp["status"]==False:
                               responseCode=-1
                               status="ERROR"   
                            logger.info("send email response:"+str(send_email_resp))
                            extraInfo.append(send_email_resp)
                        else:
                            logger.info("email not send , No active stores in group:"+group_name)
                        try:       
                               
                                aws_account_id = context.invoked_function_arn.split(":")[4]            
                                logEvent={
                                        "instance" :str(aws_account_id),
                                        "app":"EPDE-JOB",
                                        "stage":stage,            
                                        "jobName" :"PostPaymentReconciliation_Email_Job",                                                                  
                                        "group_id":group_id,     
                                        "group_name":group_name,          
                                        "status": status,
                                        "requestTime":requestStart.strftime("%Y-%m-%dT%H:%M:%S%z"),                        
                                        "responseCode":responseCode,
                                        "extraInfo":extraInfo,
                                        "summaryCount":len(report_summary_list),
                                        "attachments":len(attachments)
                                } 
                                cwLogger= CloudWatchLogger()    
                                cwLogger.DoCloudWatchLog('epde/epdejobs',logEvent)
                        except:
                                logger.error("error occured DoCloudWatchLog in lambda_handler",True) 
                        res_json={"message":"Send PostPayment Rereconciliation Report Alert successfully."}     
                    else:
                        error_code=stores_resp['error_code']
                        res_json= resHandler.GetErrorResponseJSON(error_code,None)
                        LogErrorInCloudWatch1(stage=stage,context=context,requestStart=requestStart,res_json=res_json,group_id=group_id,group_name=group_name)
                else:
                    logger.info("email not send , Alert is not active in group:"+group_name)
                """ stores_resp=app_client.GetTestStores(group_id,region)         
                if stores_resp['status']== True:
                    stores=stores_resp['items']
                    report_summary_list=[]
                    attachments=[]
                    for store in stores:
                        store_code=store['store_code']   
                        create_date=""   
                        issued = datetime.datetime.now()
                        create_date = issued.strftime("%m-%d-%Y")
                      
                        ro_state_resp= post_mgr.GetPostPaymentRereconciliationReport(store_code,create_date,tableperstore) 
                        if ro_state_resp['status']:
                            report_summary=ro_state_resp['report_summary']
                            report_summary_list.append(report_summary)
                            excel_doc=excelUpdateDT(ro_state_resp['excel_doc'])
                            excelName=ro_state_resp['excel_name']
                            excel_attachment={"excel_doc":excel_doc ,"excel_name":excelName}
                            attachments.append(excel_attachment)                            
                        else:
                            logger.debug("Error found in PostPayment Rereconciliation Report for store_code:"+client_id) 
                    if len(report_summary_list)>0:
                        send_email_resp= send_email(group,report_summary_list,attachments,region,email_source,is_Test)
                        logger.debug("send email response:"+str(send_email_resp))
                    else:
                        logger.debug("email not send , No active test stores in group:"+group_name)
                
                    res_json={"message":"Send PostPayment Rereconciliation Report Alert successfully."}     
                else:
                    error_code=stores_resp['error_code']
                    res_json= resHandler.GetErrorResponseJSON(error_code,None) """  
        else:
            error_code=store_grp_resp['error_code']
            res_json= resHandler.GetErrorResponseJSON(error_code,None)      
            LogErrorInCloudWatch(stage=stage,context=context,requestStart=requestStart,res_json=res_json)
        return resHandler.GetAPIResponse(res_json) 
    except Exception as e:
            logger.error("error occured in lambda_handler",True)
            res_json= resHandler.GetErrorResponseJSON(313,None)
            LogErrorInCloudWatch(stage=stage,context=context,requestStart=requestStart,res_json=res_json)
            return resHandler.GetAPIResponse(res_json) 
def LogErrorInCloudWatch1(context,stage,requestStart,res_json,group_id,group_name):
    try:        
        aws_account_id = context.invoked_function_arn.split(":")[4]            
        logEvent={
                "instance" :str(aws_account_id),
                "app":"EPDE-JOB",
                "stage":stage,            
                "jobName" :"PostPaymentReconciliation_Email_Job",
                "status": "ERROR",
                "group_id":group_id,     
                "group_name":group_name,  
                "requestTime":requestStart.strftime("%Y-%m-%dT%H:%M:%S%z"),                        
                "extraInfo": res_json ,             
                "responseCode":-1                
        } 
        cwLogger= CloudWatchLogger()    
        cwLogger.DoCloudWatchLog('epde/epdejobs',logEvent)
    except:
        logger.error("error occured DoCloudWatchLog in lambda_handler",True)  
def LogErrorInCloudWatch(context,stage,requestStart,res_json):
    try:        
        aws_account_id = context.invoked_function_arn.split(":")[4]            
        logEvent={
                "instance" :str(aws_account_id),
                "app":"EPDE-JOB",
                "stage":stage,            
                "jobName" :"PostPaymentReconciliation_Email_Job",
                "status": "ERROR",
                "requestTime":requestStart.strftime("%Y-%m-%dT%H:%M:%S%z"),                        
                "extraInfo": res_json ,             
                "responseCode":-1                
        } 
        cwLogger= CloudWatchLogger()    
        cwLogger.DoCloudWatchLog('epde/epdejobs',logEvent)
    except:
        logger.error("error occured DoCloudWatchLog in lambda_handler",True)  
def myFunc(e):
      return e['store_code']
def getTemaplate(table,create_dt,head_bg_color,body_bg_color,group_name):    

    x=f"""<!DOCTYPE html>
        <html>
        <head>
        <title>PostPayment Call Report</title>
        <style>table.blueTable {{
        border: 1px solid {head_bg_color};
        background-color: {body_bg_color};
        width: 100%;
        text-align: left;
        border-collapse: collapse;
        }}
        table.blueTable td, table.blueTable th {{
        border: 1px solid #AAAAAA;
        padding: 3px 2px;
        }}
        table.blueTable tbody td {{
        font-size: 13px;
        }}
        table.blueTable tr:nth-child(even) {{
        background: #D0E4F5;
         }}
        table.blueTable thead {{
        background: {head_bg_color};
        background: -moz-linear-gradient(top, #5592bb 0%, #327cad 66%, {head_bg_color} 100%);
        background: -webkit-linear-gradient(top, #5592bb 0%, #327cad 66%, {head_bg_color} 100%);
        background: linear-gradient(to bottom, #5592bb 0%, #327cad 66%, {head_bg_color} 100%);
        border-bottom: 2px solid #444444;
         }}
        table.blueTable thead th {{
        font-size: 15px;
        font-weight: bold;
        color: #FFFFFF;
        border-left: 2px solid #D0E4F5;
        }}
        table.blueTable thead th:first-child {{
        border-left: none;
        }}

        table.blueTable tfoot {{
        font-size: 14px;
        font-weight: bold;
        color: #FFFFFF;
        background: #D0E4F5;
        background: -moz-linear-gradient(top, #dcebf7 0%, #d4e6f6 66%, #D0E4F5 100%);
        background: -webkit-linear-gradient(top, #dcebf7 0%, #d4e6f6 66%, #D0E4F5 100%);
        background: linear-gradient(to bottom, #dcebf7 0%, #d4e6f6 66%, #D0E4F5 100%);
        border-top: 2px solid #444444;
         }}
        table.blueTable tfoot td {{
        font-size: 14px;
        }}
        table.blueTable tfoot .links {{
        text-align: right;
        }}
        table.blueTable tfoot .links a{{
        display: inline-block;
        background: {head_bg_color};
        color: #FFFFFF;
        padding: 2px 8px;
        border-radius: 5px;
         }}</style>
        </head>
        <body>

        <p>Hi All,</p>
        <p>Summary of EPDE PostPayment Call for {group_name} Stores as of {create_dt}.</p>
        <p>{table}</p>
        </body>
        </html>""" 
    return x
def excelUpdateDT(excel_doc):
    try:
        excel_doc_updated=[]
        for excel_row in excel_doc:
            excel_row['ROPostedOn']=formatDateEST(excel_row['ROPostedOn'])
            excel_row['ROStatusOn']=formatDateEST( excel_row['ROStatusOn'])
            excel_row['Latest_ROStatusOn']=formatDateEST( excel_row['Latest_ROStatusOn'])
            excel_doc_updated.append(excel_row)
        return excel_doc_updated
    except:
        return excel_doc

def formatDateEST(dtStr):
    try:

        dt_request = datetime.datetime.strptime(str(dtStr),"%Y-%m-%d %I:%M:%S %p") 
        estZone = pytz.timezone("US/Eastern")     
        create_date=dt_request.astimezone(estZone).strftime("%m-%d-%Y %I:%M:%S %p")   
        return create_date  
    except:
        return dtStr
def send_email(group,ro_state_list,attachments,region,add_from="sanjay.sahu@ivsgllc.com",istest=False):
    _moduleNM="EPDE_Lambda"
    _functionNM="send_email"
    group_id=group['group_id']
    group_name=group['group_name']  
    
    if istest:
        headColor='#24943A'
        bodyColor='#D4EED1'
    else:
        headColor='#1C6EA4'
        bodyColor='#EEEEEE'

    try:
      				
        to_list=  group['to_list']  
        if istest:
              to_list=  group['test_to_list'] 
        cc_list=  group['cc_list']  
        html_table_header= f"""
                <table class="blueTable">
                <thead>
                    <tr>
                        <th>Store</th>
                        <th>logon</th>
                        <th>Close All Paid RO’s</th>
                        <th>Total Request</th>
                        <th>Posted Closed-C98</th>
                        <th>Posted NotClosed-C97</th>"
                        <th>Posted WarrantyDue</th> 
                        <th>Not Posted</th> 
                        <th>In Queue</th>
                        <th>Timeout</th>
                    </tr>
                </thead>"""
        html_table_row=""
        ro_state_list.sort(key=myFunc)
        for ro_state in ro_state_list:
            store= ro_state['store_code']+"-"+ro_state['store_name']
            close_all_paid_ros=ro_state['close_all_paid_ros']
            log_on=ro_state['log_on']
            total_request=ro_state['total_request']
            total_posted_closed_C98=ro_state['total_posted_closed_C98']
            total_posted_warranty_due=ro_state['total_posted_warranty_due']
            total_not_posted=ro_state['total_not_posted']
            total_in_queue=ro_state['total_in_queue']
            total_posted_not_closed_C97=ro_state['total_posted_not_closed_C97'] 
            total_timeout=ro_state['total_timeout']            
            html_table_row= html_table_row+ f"""<tr>
                        <td>{store}</td>
                        <td>{log_on}</td>
                        <td>{close_all_paid_ros}</td>
                        <td>{total_request}</td>
                        <td>{total_posted_closed_C98}</td>
                        <td>{total_posted_not_closed_C97}</td>
                        <td>{total_posted_warranty_due}</td>
                        <td>{total_not_posted}</td>
                        <td>{total_in_queue}</td>
                        <td>{total_timeout}</td>
                        </tr>"""               
        html_table_footer=" </tbody></table>"
        tab=html_table_header+html_table_row+html_table_footer
        estZone = pytz.timezone("US/Eastern")           
        local_datetime = datetime.datetime.now(estZone) 
        create_date=local_datetime.strftime("%m-%d-%Y %I:%M:%S %p")          
        html=getTemaplate(tab,create_date,headColor,bodyColor,group_name)
        
        subject='EPDE PostPayment Reconciliation Report -'+create_date
        
         
        message = MIMEMultipart()
        message['Subject'] = subject
        message['From'] = add_from
        message['To'] = ','.join(to_list)
        #message['Cc'] = ','.join(cc_list)

        # message body
        part = MIMEText(html, 'html')
        message.attach(part)

        for att in attachments:
            excel_doc=att['excel_doc']
            fileName=att['excel_name']
            df= pd.DataFrame(excel_doc)
            if len(df)>0:
                buff= export_excel(df)
                #logger.debug(buff)            
                part = MIMEApplication(buff)
                part.add_header('Content-Disposition', 'attachment', filename=fileName)
                part.add_header('Content-Type', 'application/vnd.ms-excel; charset=UTF-8')
                message.attach(part)

         
        client = boto3.client('ses', region_name=region)
         
       # destination = { 'ToAddresses' : to_list}
        result = client.send_raw_email(Source = message['From'], Destinations = to_list, RawMessage = {'Data': message.as_string(),})
        logger.debug("send mail result:"+str(result))
        return {"status":True,"group-id":group_id,"group-name":group_name,"message":"email send succesfully","message_id":result['MessageId']}
    except Exception as e:
        rjs= err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 
        return {"status":True,"group-id":group_id,"group-name":group_name,"message":"error in sending email","error_code":rjs['error_code'],"error_message":rjs['error_message']}