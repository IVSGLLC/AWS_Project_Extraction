from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient
from EPDE_INVENTORY import Inventory
logger=LogManger()
err_handler=ErrorHandler()

def lambda_handler(event, context):
     res_json=""
     resHandler=ResponseHandler()
     try:
          _moduleNM="EPDE_INVENTORY_NOW_Lambda"
          _functionNM="lambda_handler"
          err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)
          #logger.debug("event="+str(event))
          #logger.debug("context="+str(context))
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
               
               vinList=None       
               gpsServicePlan=None
               fromDate=None
               toDate=None
               stockFilter=None
               fromEntryDate=None
               toEntryDate=None
               inventoryNow='True'
               queryStringParameters= event.get("queryStringParameters") 
               noFilters=True
               try:
                    limit=int( queryStringParameters["limit"])
               except:
                    limit=-1                         
               try:                         
                    lastEvaluatedKey= queryStringParameters["lastEvaluatedKey"]
               except:                         
                    lastEvaluatedKey=None
               activeServicePlans=[]
               logger.debug("store_code="+store_code+",toDate="+str(toDate)+",fromDate="+str(fromDate)+",plan="+str(gpsServicePlan)+",vinList="+str(vinList)+",stockFilter="+str(stockFilter)+",fromEntryDate="+str(fromEntryDate)+",toEntryDate="+str(toEntryDate)+",inventoryNow="+str(inventoryNow))                    
               inventory_manager= Inventory(region,tablePerStore)       
               ro_resp=inventory_manager.GetFilteredListNow(store_code=store_code,last_key=lastEvaluatedKey,page_size=limit,inventoryNow=inventoryNow)                     
               if ro_resp['status']== True: 
                    items=ro_resp['items']
                    LastEvaluatedKey=None
                    try:
                         LastEvaluatedKey=ro_resp['LastEvaluatedKey'] 
                    except:
                         ""
                    pre_resp=inventory_manager.prepareListForInventoryNow(items=items,inventoryNow=inventoryNow)
                    if pre_resp['status']== True:
                         docs=pre_resp['items']                                   
                         res_json={"responseCode": 0,
                                   "results":docs,
                                   "auth_token":auth_json,
                                   "limit":limit,
                                   "lastEvaluatedKey":LastEvaluatedKey
                                   } 
                         logger.info("store_code="+store_code+"_INVENTORY_NOW_LIST_JSON :"+str(res_json)) 
                    else:
                         logger.debug("Error while making inventoryNowList.")
                         error_code=pre_resp['error_code']
                         res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)
               else:
                    logger.debug("vin/s not Found..")
                    error_code=ro_resp['error_code']
                    res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)               
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

 