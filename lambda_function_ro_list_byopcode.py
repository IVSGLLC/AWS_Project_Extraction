
from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient
from EPDE_RO import RepairOrder
logger=LogManger()
err_handler=ErrorHandler()

def lambda_handler(event, context):
    res_json=""
    store_code=""
    resHandler=ResponseHandler()
    try:
            _moduleNM="EPDE_Lambda"
            _functionNM="lambda_handler_ro_byopcodes"
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
               document_type= 'RO'  
               queryStringParameters= event.get("queryStringParameters")       
               try:
                    limit=int( queryStringParameters["limit"])
               except:
                    limit=-1

               limit=-1          
               try:
                         
                    lastEvaluatedKey= queryStringParameters["lastEvaluatedKey"]

               except:
                         
                    lastEvaluatedKey=None

               try:
                    opCode=str( queryStringParameters["opcode"])                        

               except:
                    opCode=None

               logger.debug("store_code="+store_code+",opCode="+str(opCode)+",document_type="+document_type)                    
               ro_manager= RepairOrder(region,tablePerStore)       
               validateResp=ro_manager.Validate_ROListByOpcode_inputs(store_code,region,opCode,document_type,auth_json)
               if validateResp['responseCode']== 0:
                    
                    opCodes=validateResp['opcodes']
                    ro_resp=ro_manager.GetROListByOpcode(store_code,document_type,opCodes,lastEvaluatedKey,limit)
                    if ro_resp['status']== True: 
                            items=ro_resp['items']
                            LastEvaluatedKey=None
                            try:
                              LastEvaluatedKey=ro_resp['LastEvaluatedKey'] 
                            except:
                                ""
                            pre_resp=ro_manager.prepareDocumentListByOpcodes(items)
                            if pre_resp['status']== True:
                               docs=pre_resp['documentList']                               
                               res_json={"responseCode": 0,
                                         "documentList":docs,
                                         "auth_token":auth_json,
                                         "limit":limit,
                                         "lastEvaluatedKey":LastEvaluatedKey
                                         } 
                               logger.debug("store_code="+store_code+"_RO_LIST_BYOPCODES_JSON :"+str(res_json)) 
                            else:
                                logger.debug("Error while making documenmtListByOpcodes.")
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

 