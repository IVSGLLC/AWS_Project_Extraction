
from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient
from EPDE_PARTS import PartsInvoice
logger=LogManger()
err_handler=ErrorHandler()

def lambda_handler(event, context):
    res_json=""
    auth_json=""
    store_code=""
    resHandler=ResponseHandler()
    try:
          
            _moduleNM="EPDE_Lambda"
            _functionNM="lambda_function_parts_invoice_detail_file"
            err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)
            app_client= AppClient()
            config=app_client.GetConfiguration(event=event)
            region=config['REGION']
            tablePerStore=1
            ro_manager= PartsInvoice(region,tablePerStore)  
            auth_json=None            
            auth_detail_resp=app_client.GetAuthenticationDetail(event) 
            if auth_detail_resp['status']:
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
               queryStringParameters= event.get("queryStringParameters")       
               pathParameters= event.get("pathParameters") 
               try:      
                 document_id=  pathParameters["invoiceNumber"]
               except KeyError:
                    try:      
                        document_id=  pathParameters["partsInvoiceNumber"]
                    except KeyError:
                            try:      
                                 document_id=  pathParameters["roNumber"]
                            except KeyError:
                                    pass
               fileType='PDF' 
               try:      
                      fileType=queryStringParameters["fileType"] 
               except Exception:
                      fileType='PDF'
                      pass  
               document_type='PARTS' 
               copyType='CUSTOMER'
               try:      
                      copyType=queryStringParameters["copyType"] 
               except Exception:
                      copyType='CUSTOMER'
                      pass
               logger.debug("store_code="+store_code+",partsInvoiceNumber="+str(document_id)+",document_type="+document_type+",fileType="+fileType)                    
               validateResp=ro_manager.Validate_PartsInvoiceDetailPDF_inputs(document_id,document_type,fileType,auth_json,copyType)
               if validateResp['responseCode']== 0:
                    ro_resp=ro_manager.GetPartsInvoiceDetail(store_code,document_id,document_type)
                    if ro_resp['status']== True: 
                            item=ro_resp['item']
                           
                            pre_resp=ro_manager.preparePartsInvoiceFile(config,item,fileType,auth_json,copyType)
                            if pre_resp['status']== True:
                               fileData=pre_resp['partsInvoice'] 
                               #logger.info('fileData='+str(fileData))
                               if fileType.upper()=='HTML':
                                   return resHandler.GetHtmlResponse(fileData)
                               elif fileType.upper()=='PDF':
                                   return resHandler.GetPDFResponse(fileData,document_id)
                               else:
                                   res_json=fileData 
                            else:
                                logger.debug("Error while making invoice response.")
                                error_code=pre_resp['error_code']
                                res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)
                           
                    else:
                        logger.debug("Invalid partsInvoice /partsInvoice not Found..")
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

 