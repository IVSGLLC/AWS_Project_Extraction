
import datetime

import pytz
from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient
from EPDE_RO import RepairOrder
import boto3
logger=LogManger()
err_handler=ErrorHandler()
def lambda_handler(event, context):
    res_json=""
    resHandler=ResponseHandler()
    try:
        _moduleNM="EPDE_Lambda"
        _functionNM="lambda_handler"
        err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)             
        app_client= AppClient()
        region=event['REGION']
        email_source=event["EMAIL_SOURCE"]
        is_Test=False
        if "IS_TEST" in event:   
           test=str(event["IS_TEST"])
           if test.lower()=='true':
              is_Test=True          
        tableperstore=1      
        ro_mgr=RepairOrder(region=region,table_Per_Store_RO_Detail=tableperstore)
        store_grp_resp=app_client.GetStoreGroups(region)
        if store_grp_resp['status']:
            store_groups=store_grp_resp['items']
            for group in store_groups:
                group_id=group['group_id']
                group_name=group['group_name']
                alerts=group['alerts']
                if 'DAILY_WIP_STATUS' in alerts:

                    if not is_Test:
                        stores_resp=app_client.GetStores(group_id,region)
                    else:
                        stores_resp=app_client.GetTestStores(group_id,region)           
                    if stores_resp!=None and stores_resp['status']== True:
                        stores=stores_resp['items']
                        ro_state_list=[]
                        for store in stores:
                            store_code=store['store_code']         
                            ro_state_resp= ro_mgr.GetROStatistics(store_code,region) 
                            if ro_state_resp['status']:
                                ro_state=ro_state_resp['ro_state']
                                ro_state_list.append(ro_state)
                            else:
                                logger.debug("Error found in RO sttistices for store_code:"+store_code) 
                        if len(ro_state_list)>0:
                            send_email_resp= send_email(group,ro_state_list,is_Test,email_source)
                            logger.info("send email response:"+str(send_email_resp))
                        else:
                            logger.info("email not send , No active stores in group:"+group_name)
                    
                        res_json={"message":"Send Latest RO Counts Alert successfully."}     
                    else:
                        error_code=stores_resp['error_code']
                        res_json= resHandler.GetErrorResponseJSON(error_code,None)
                else:
                    logger.debug("email not send ,Daily DMS Status Alert not active in group:"+group_name)
                
        else:
            error_code=store_grp_resp['error_code']
            res_json= resHandler.GetErrorResponseJSON(error_code,None)        
        return resHandler.GetAPIResponse(res_json) 
    except Exception as e:
            logger.error("error occured in lambda_handler",True)
            res_json= resHandler.GetErrorResponseJSON(313,None)
            return resHandler.GetAPIResponse(res_json) 
def myFunc(e):
      return e['store_code']
def getTemaplate(table,create_dt,  headColor='#1C6EA4',bodyColor='#EEEEEE',group_name=''):
    x=f"""<!DOCTYPE html>
        <html>
        <head>
        <title>PostPayment Call Report</title>
        <style>table.blueTable {{
        border: 1px solid {headColor};
        background-color: {bodyColor};
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
        background:  {headColor};
        background: -moz-linear-gradient(top, #5592bb 0%, #327cad 66%,  {headColor} 100%);
        background: -webkit-linear-gradient(top, #5592bb 0%, #327cad 66%,  {headColor} 100%);
        background: linear-gradient(to bottom, #5592bb 0%, #327cad 66%,  {headColor} 100%);
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
        background:  {headColor};
        color: #FFFFFF;
        padding: 2px 8px;
        border-radius: 5px;
         }}</style>
        </head>
        <body>

        <p>Hi All,</p>
        <p>EPDE Status for {group_name} Stores as of  {create_dt}.</p>
        <p>{table}</p>
        </body>
        </html>""" 
    return x 
def send_email(group,ro_state_list,istest=False,email_source="sanjay.sahu@ivsgllc.com"):
    _moduleNM="EPDE_Lambda"
    _functionNM="send_email"
    group_id=group['group_id']
    group_name=group['group_name']  
    try:
       #11/07/2021 06:00:04 AM.
        if istest:
            headColor='#24943A'
            bodyColor='#D4EED1'
        else:
            headColor='#1C6EA4'
            bodyColor='#EEEEEE'
                  

        to_list=  group['to_list'] 
        if istest:
              to_list=  group['test_to_list'] 

        #cc_list=  group['cc_list']
        html_table_header= f"""
                <table class="blueTable">
                <thead>
                    <tr>
                        <th>Store</th>
                        <th>logon</th>
                        <th>RO Counts</th>
                        <th>Random RO</th>
                        <th>C94</th>
                        <th>C97</th>
                        <th>C98</th> 
                        <th>I91</th>
                        <th>I98</th>
                        <th>Warranty Due</th> 
                        <th>Status-Blank</th> 
                        <th>Status-Other</th> 
                        <th>Invalid Amount Due</th> 
                        <th>Invalid Warranty</th> 
                       
                    </tr>
                </thead>"""
        html_table_row=""  
        logger.debug(str(ro_state_list))
        ro_state_list.sort(key=myFunc)
        for ro_state in ro_state_list:            
            store= ro_state['store_code']+"-"+ro_state['store_name']
            log_on=ro_state['log_on']
            total_ro=ro_state['total_ro']
            total_warranty_due=ro_state['total_warranty_due']
            total_invalid_amount_due=ro_state['total_invalid_amount_due']
            total_invalid_warranty_due=ro_state['total_invalid_warranty_due']
            status_C97=ro_state['status_C97']
            status_C98=ro_state['status_C98']
            status_C94=ro_state['status_C94']
            status_I98=ro_state['status_I98']
            status_I91=ro_state['status_I91']
            status_blank=ro_state['status_blank']
            status_other=ro_state['status_other']
            random_ro=ro_state['random_ro']
            error_code=ro_state['error_code']
            error_message=ro_state['error_message']
            if error_code==0:
                 html_table_row= html_table_row+ f"""<tr>
                        <td>{store}</td>
                        <td>{log_on}</td>
                        <td>{total_ro}</td>
                        <td>{random_ro}</td>
                        <td>{status_C94}</td>
                        <td>{status_C97}</td>
                        <td>{status_C98}</td>
                        <td>{status_I91}</td>
                        <td>{status_I98}</td>
                        <td>{total_warranty_due}</td>   
                        <td>{status_blank}</td>
                        <td>{status_other}</td>
                        <td>{total_invalid_amount_due}</td>
                        <td>{total_invalid_warranty_due}</td>                      
                        </tr>"""    
            else:
                html_table_row=  html_table_row+ f"""<tr>
                        <td>Record not fetched, error-code={error_code}, error-message={error_message}</td>
                        </tr>"""   
        html_table_footer="</tbody></table></p>"
        tab=html_table_header+html_table_row+html_table_footer
        estZone = pytz.timezone("US/Eastern")           
        local_datetime = datetime.datetime.now(estZone) 
        create_date=local_datetime.strftime("%m-%d-%Y %I:%M:%S %p")  
        html=getTemaplate(tab,create_date,headColor,bodyColor,group_name)
        subject='DMS Service Up!!'
        client = boto3.client('ses')
        response = client.send_email(
                                    Destination={
                                         
                                        'ToAddresses':to_list,
                                    },
                                    Message={
                                        'Body': {
                                            'Html': {
                                                'Charset': 'UTF-8',
                                                'Data': html,
                                            }
                                        },
                                        'Subject': {
                                            'Charset': 'UTF-8',
                                            'Data': subject,
                                        },
                                    },
                                     
                                    Source=email_source
                                     
                                )
        logger.debug("send mail response:"+str(response))
        return {"status":True,"group-id":group_id,"group-name":group_name,"message":"email send succesfully","message_id":response['MessageId']}
    except Exception as e:
        rjs= err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 
        return {"status":True,"group-id":group_id,"group-name":group_name,"message":"error in sending email","error_code":rjs['error_code'],"error_message":rjs['error_message']}