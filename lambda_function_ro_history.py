from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_ROHISTORY import ROHistory
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient

logger=LogManger()
err_handler=ErrorHandler()

def lambda_handler(event, context):
     res_json=""
     resHandler=ResponseHandler()
     try:
          _moduleNM="EPDE_REPAIR_ORDERS_HISTORY_Lambda"
          _functionNM="lambda_handler"
          err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)
          app_client= AppClient()
          config=app_client.GetConfiguration(event=event)
          region=config['REGION']
          tablePerStore=1
          auth_json=None    
          store_code=None        
          auth_detail_resp=app_client.GetAuthenticationDetail(event) 
          if auth_detail_resp['status']== True:
               auth_json=auth_detail_resp['auth_json']
               maintenanace_resp=app_client.isMaintenanceTime(config)
               if maintenanace_resp['status']:  
                    res_json= maintenanace_resp['resjson'] 
                    logger.debug("Maintenance Time is running API call not allowed...")
                    return resHandler.GetAPIResponse(res_json) 
               #client_id=auth_detail_resp['client_id']  
               validate_resp=app_client.ValidateStoreDetail(region,event)
               if validate_resp['status']==False: 
                    logger.debug("STORE NOT FOUND...")
                    error_code=validate_resp['error_code']
                    res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)   
                    return resHandler.GetAPIResponse(res_json) 
               else:
                    if 'store_code' in validate_resp:
                        store_code=validate_resp['store_code']
               queryStringParameters= event.get("queryStringParameters") 
               noFilters=True
               vinList=None 
               customerNo=None
               try:
                    vinList= queryStringParameters["vin"]
                    noFilters=False                    
               except:
                    vinList=None                
               document_type= 'ROHIST'
              
               try:
                    customerNo= queryStringParameters["customer"]
                    noFilters=False        
               except:
                    customerNo=None
                              
               try:
                    fromDate= queryStringParameters["fromDate"]
                    noFilters=False        
               except:
                    fromDate=None
               try:
                    toDate= queryStringParameters["toDate"]
                    noFilters=False        
               except:
                    toDate=None
               try:
                    limit=int( queryStringParameters["limit"])
               except:
                    limit=-1                         
               try:                         
                    lastEvaluatedKey= queryStringParameters["lastEvaluatedKey"]
               except:                         
                    lastEvaluatedKey=None 

               logger.debug("store_code="+store_code+",document_type="+document_type+",vinList="+str(vinList)+",customerNo="+str(customerNo)+",fromDate="+str(fromDate)+",toDate="+str(toDate))                    
               roist_manager= ROHistory(region,tablePerStore)       
               validateResp=roist_manager.Validate_inputs(store_code=store_code,vinList=vinList,customerNo=customerNo,fromDate=fromDate,toDate=toDate,auth_json=auth_json)
               if validateResp['responseCode']== 0:                               
                    if  noFilters :
                        ro_resp=roist_manager.GetROHistoryList(store_code,document_type,lastEvaluatedKey,limit)
                    else:      
                         ro_resp=roist_manager.GetFilteredListNew(store_code=store_code,vinList=vinList,customerNo=customerNo,fromDate=fromDate,toDate=toDate,last_key=lastEvaluatedKey,page_size=limit)                     
                     
                         #ro_resp=roist_manager.GetFilteredList(store_code=store_code,vinList=vinList,customerNo=customerNo,fromDate=fromDate,toDate=toDate,last_key=lastEvaluatedKey,page_size=limit)                     
                          
                    if ro_resp['status']== True: 
                              items=ro_resp['items']
                              LastEvaluatedKey=None
                              try:
                                   LastEvaluatedKey=ro_resp['LastEvaluatedKey'] 
                              except:
                                   ""    
                                
                              pre_resp=roist_manager.prepareOutputJSONListNew(items=items,fromDate=fromDate,toDate=toDate)
                         
                              #pre_resp=roist_manager.prepareOutputJSONList(items=items,fromDate=fromDate,toDate=toDate)
                                   
                              if pre_resp['status']== True:
                                   docs=pre_resp['items']                                   
                                   res_json={"responseCode": 0,
                                             "results":docs,
                                             "auth_token":auth_json,
                                             "limit":limit,
                                             "lastEvaluatedKey":LastEvaluatedKey
                                             } 
                                   logger.info("store_code="+store_code+"_RO_HISTORYLIST_JSON :"+str(res_json)) 
                              else:
                                   logger.debug("Error while making inventoryStatusList.")
                                   error_code=pre_resp['error_code']
                                   res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)
                    else:
                         logger.debug("Record/s not Found..")
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

 