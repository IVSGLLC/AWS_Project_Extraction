 
from EPDE_DEAL import Deal
from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient 
logger=LogManger()
err_handler=ErrorHandler()

def lambda_handler(event, context):
     res_json=""
     resHandler=ResponseHandler()
     try:
          logger.debug("event="+str(event))
          _moduleNM="EPDE_DEALS_Lambda"
          _functionNM="lambda_handler"
          err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)
          app_client= AppClient()
          config=app_client.GetConfiguration(event=event)
          region=config['REGION']
          tablePerStore=1
          #store_code=app_client.GetStoreCode(event=event)
          auth_json=None            
          auth_detail_resp=app_client.GetAuthenticationDetail(event) 
          store_code=None
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
               queryStringParameters= event.get("queryStringParameters") 
               noFilters=True
               vin=None 
               try:
                    vin= queryStringParameters["vin"]
                    noFilters=False                    
               except:
                    vin=None                
               document_type= 'DEAL'               
               try:
                    plan= queryStringParameters["plan"]
                    noFilters=False        
               except:
                    plan=None
                
               try:
                    status= queryStringParameters["status"]
                    noFilters=False        
               except:
                    status=None
               try:
                    _type= queryStringParameters["type"]
                    noFilters=False        
               except:
                    _type=None
               
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
               activeServicePlans=[]
               logger.debug("store_code="+store_code+",toDate="+str(toDate)+",fromDate="+str(fromDate)+",plan="+str(plan)+",vin="+str(vin))                    
               deal_manager= Deal(region,tablePerStore)       
               validateResp=deal_manager.Validate_inputs(store_code=store_code,vinList=vin,plan=plan,status=status,_type=_type,fromDate=fromDate,toDate=toDate,document_type=document_type,auth_json=auth_json)
               if validateResp['responseCode']== 0:
                    activeServicePlans=validateResp["activeServicePlans"]
                    activeStatusList=validateResp["activeStatusList"]
                    activeSaleTypeList=validateResp["activeSaleTypeList"]
                    isFiltered=False
                    if  noFilters :
                        ro_resp=deal_manager.GetDealList(store_code,document_type,lastEvaluatedKey,limit)
                    else:
                         isFiltered=True                        
                         ro_resp=deal_manager.GetFilteredList(store_code=store_code,document_type=document_type,vinList=vin,status=status,plan=plan,_type=_type,fromDate=fromDate,toDate=toDate,activeServicePlans=activeServicePlans,activeStatus=activeStatusList,activeSaleTypes=activeSaleTypeList,last_key=lastEvaluatedKey,page_size=limit)
                    if ro_resp['status']== True: 
                              items=ro_resp['items']
                              LastEvaluatedKey=None
                              try:
                                   LastEvaluatedKey=ro_resp['LastEvaluatedKey'] 
                              except:
                                   ""
                              pre_resp=deal_manager.prepareDealList(items=items,isFiltered=isFiltered)
                              if pre_resp['status']== True:
                                   docs=pre_resp['items']                                   
                                   res_json={
                                             "responseCode": 0,
                                             "results":docs,
                                             "auth_token":auth_json,
                                             "limit":limit,
                                             "lastEvaluatedKey":LastEvaluatedKey
                                             } 
                                   logger.info("store_code="+store_code+"_DEAL_LIST_JSON :"+str(res_json)) 
                              else:
                                   logger.debug("Error while making dealList.")
                                   error_code=pre_resp['error_code']
                                   res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)
                    else:
                         logger.debug("vin/s not Found..")
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

 