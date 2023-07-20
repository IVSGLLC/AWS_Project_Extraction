
from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient
from EPDE_INVOICE import Invoice
logger=LogManger()
err_handler=ErrorHandler()

def lambda_handler(event, context):
    res_json=""
    auth_json=""
    store_code=""
    resHandler=ResponseHandler()
    try:             
            _moduleNM="EPDE_Lambda"
            _functionNM="lambda_function_invoice_list"
            err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)
            app_client= AppClient()
            config=app_client.GetConfiguration(event=event)
            region=config['REGION']
            tablePerStore=1
            auth_json=None            
            auth_detail_resp=app_client.GetAuthenticationDetail(event) 
            if auth_detail_resp['status']== True:
               auth_json=auth_detail_resp['auth_json']
               maintenanace_resp=app_client.isMaintenanceTime(config)
               if maintenanace_resp['status']:  
                  res_json= maintenanace_resp['resjson'] 
                  logger.debug("Maintenance Time is running API call not allowed...")
                  return resHandler.GetAPIResponse(res_json) 
               validate_resp=app_client.ValidateStoreDetail(region,event)
               if validate_resp['status']==False: 
                    logger.debug("STORE NOT FOUND...")
                    error_code=validate_resp['error_code']
                    res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)   
                    return resHandler.GetAPIResponse(res_json) 
               else:
                    if 'store_code' in validate_resp:
                        store_code=validate_resp['store_code']
               #client_id=auth_detail_resp['client_id']  
               #queryStringParameters= event.get("queryStringParameters")     
               #document_type=  queryStringParameters["documentType"]
               document_type='RO' 
               logger.debug("store_code="+store_code+",document_type="+document_type)                    
               invoice_manager= Invoice(region,tablePerStore)       
               validateResp=invoice_manager.Validate_InvoiceList_inputs(document_type,auth_json)
               if validateResp['responseCode']== 0:
                    logger.info("before call GetInvoiceList")
                    queryStringParameters= event.get("queryStringParameters")       
                    try:
                            limit=int( queryStringParameters["limit"])
                            

                    except:
                            limit=-1
                           
                    try:
                            
                            lastEvaluatedKey= queryStringParameters["lastEvaluatedKey"]

                    except:
                          
                            lastEvaluatedKey=None 
                    ro_resp=invoice_manager.GetInvoiceList(store_code,document_type,lastEvaluatedKey,limit)
                    logger.info("after call GetInvoiceList")
                    if ro_resp['status']== True: 
                            items=ro_resp['items']
                            LastEvaluatedKey=None
                            try:
                              LastEvaluatedKey=ro_resp['LastEvaluatedKey'] 
                            except:
                                ""
                            logger.info("before call prepareInvoiceList")
                            pre_resp=invoice_manager.prepareInvoiceList(items)
                            logger.info("after  call prepareInvoiceList")
                            if pre_resp['status']== True:
                               docs=pre_resp['invoiceList']
                               res_json={"responseCode": 0,
                                         "documentList":docs,
                                         "auth_token":auth_json,
                                         "limit":limit,
                                         "lastEvaluatedKey":LastEvaluatedKey
                                         }                                 
                            else:
                                logger.debug("Error while making invoiceList.")
                                error_code=pre_resp['error_code']
                                res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)
                           
                    else:
                        logger.debug("Invalid RO /RO not Found..")
                        error_code=ro_resp['error_code']
                        res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)
               else:
                    logger.debug("Invalid Request..")
                    res_json= validateResp
            else:
                logger.debug("Auth detail not found...")
                auth_json=auth_detail_resp['auth_json']
                error_code=auth_detail_resp['error_code']
                res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)                
            return resHandler.GetAPIResponse(res_json)
    except Exception as e:             
            logger.error("error occured in lambda_handler",True)
            res_json= resHandler.GetErrorResponseJSON(313,auth_json)
            return resHandler.GetAPIResponse(res_json)

 