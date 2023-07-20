
from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient
from EPDE_SALESCUST import SalesCustomer
logger=LogManger()
err_handler=ErrorHandler()

def lambda_handler(event, context):
    res_json=""
    auth_json=None     
    resHandler=ResponseHandler()
    store_code=""
    try:
             
            _moduleNM="EPDE_Lambda"
            _functionNM="salescustomer_lambda_handler"
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
               pathParameters= event.get("pathParameters")               
               customer_number=  pathParameters["customerNumber"]
               logger.debug("store_code="+store_code+",customer_number="+str(customer_number))                    
               salescust_manager= SalesCustomer(region,tablePerStore)       
               validateResp=salescust_manager.Validate_SalesCustDetail_inputs(customer_number=customer_number,auth_json=auth_json)
               if validateResp['responseCode']== 0:
                    salscust_resp=salescust_manager.GetSalesCustomerDetail(store_code,customer_number)
                    if salscust_resp['status']== True: 
                            items=salscust_resp['items']
                            pre_resp=salescust_manager.prepareSalesCustomerResponse(items,auth_json)
                            if pre_resp['status']== True:
                               res_json=pre_resp['salesCustomerDetail']                               
                            else:
                                logger.debug("Error while making documenmtList.")
                                error_code=pre_resp['error_code']
                                res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)
                           
                    else:
                        logger.debug("Invalid Customer number  /Customer number not Found..")
                        error_code=salscust_resp['error_code']
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

 