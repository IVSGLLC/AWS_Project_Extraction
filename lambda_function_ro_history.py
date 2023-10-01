import datetime
import pytz
from EPDE_CloudWatchLog import CloudWatchLogger
from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_ROHISTORY import ROHistory
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient
from EPDE_Validator import RequestValidator

logger=LogManger()
err_handler=ErrorHandler()

def lambda_handler(event, context):    
     resHandler=ResponseHandler()
     eastern=pytz.timezone('US/Eastern')  
     requestStart = datetime.now().astimezone(eastern)
     res_json=""
     store_code=""
     status="ERROR"    
     document_type= 'ROHIST'
     tablePerStore=1 
     noFilters=True
     vinList=None 
     customerNo=None
     limit=-1
     lastEvaluatedKey=None 
     recordCounts=-1
     try:
          
               validator=RequestValidator(resHandler)     
               res_json=validator.Validate()
               if res_json["responseCode"]  == -1:
                    res_json=res_json["res_json"]
                    return resHandler.GetAPIResponse(res_json)     

               auth_json=res_json['auth_json']
               store_code=res_json['store_code']
               region=res_json['REGION']


               queryStringParameters= event.get("queryStringParameters") 
              
               if "vin" in queryStringParameters:
                    vinList= queryStringParameters["vin"]
                    noFilters=False 
               if "customer" in queryStringParameters:
                    customerNo= queryStringParameters["customer"]
                    noFilters=False 
               if "fromDate" in queryStringParameters:
                    fromDate= queryStringParameters["fromDate"]
                    noFilters=False 
               if "toDate" in queryStringParameters:
                    toDate= queryStringParameters["toDate"]
                    noFilters=False 

               if "limit" in queryStringParameters:
                    limit=int( queryStringParameters["limit"])
               if "lastEvaluatedKey" in queryStringParameters:
                    lastEvaluatedKey= queryStringParameters["lastEvaluatedKey"]     
                    

               logger.debug("store_code="+store_code+",document_type="+document_type+",vinList="+str(vinList)+",customerNo="+str(customerNo)+",fromDate="+str(fromDate)+",toDate="+str(toDate))                    
               roist_manager= ROHistory(region,tablePerStore)       
               validateResp=roist_manager.Validate_inputs(store_code=store_code,vinList=vinList,customerNo=customerNo,fromDate=fromDate,toDate=toDate,auth_json=auth_json)
               if validateResp['responseCode']== -1:                    
                    logger.debug("Invalid Request..")
                    res_json= validateResp
                    return resHandler.GetAPIResponse(res_json)     
               
               if  noFilters :
                    ro_resp=roist_manager.GetROHistoryList(store_code,document_type,lastEvaluatedKey,limit)
               else:      
                    ro_resp=roist_manager.GetFilteredListNew(store_code=store_code,vinList=vinList,customerNo=customerNo,fromDate=fromDate,toDate=toDate,last_key=lastEvaluatedKey,page_size=limit)                     
               if ro_resp['status']== False: 
                    logger.debug("Record/s not Found..")
                    error_code=ro_resp['error_code']
                    res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)
                    return resHandler.GetAPIResponse(res_json)
               
               items=ro_resp['items']
               LastEvaluatedKey=None
               if "lastEvaluatedKey" in ro_resp:
                  LastEvaluatedKey=ro_resp['LastEvaluatedKey'] 

               pre_resp=roist_manager.prepareOutputJSONListNew(items=items,fromDate=fromDate,toDate=toDate)
               if pre_resp['status']== False:
                    logger.debug("Error while making inventoryStatusList.")
                    error_code=pre_resp['error_code']
                    res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)
                    return resHandler.GetAPIResponse(res_json)
               status="SUCCESS"
               docs=pre_resp['items']                                   
               res_json={"responseCode": 0,
                         "results":docs,
                         "auth_token":auth_json,
                         "limit":limit,
                         "lastEvaluatedKey":LastEvaluatedKey
                         } 
               recordCounts=len(docs) 
               logger.debug("store_code="+store_code+",Total RO-HISTORY:"+str(recordCounts)) 
               return resHandler.GetAPIResponse(res_json)
     except Exception as e:             
            logger.error("error occured in lambda_handler",True)
            res_json= resHandler.GetErrorResponseJSON(313,auth_json)
            return resHandler.GetAPIResponse(res_json)

     finally: 
          try:        
               aws_account_id = context.invoked_function_arn.split(":")[4] 
               time_difference=datetime.now().astimezone(eastern)-requestStart
               time_difference_in_milliseconds = int(time_difference.total_seconds() * 1000) 
               responseCode= 0
               errorMessage=""
               errorCode=0
               if status=="ERROR":
                    responseCode= res_json["responseCode"]
                    errorList= res_json["errorList"]               
                    errorMessage=errorList["code"]
                    errorCode=errorList["message"]

               param_json={
                    "queryStringParameters":event['queryStringParameters'],
                    "pathParameters":event['pathParameters']
               }               
               apiParameters=resHandler.ConvertJsonToString(resjson=param_json)
               logEvent={
                    "instance" :str(aws_account_id),
                    "app":"EPDE-API-GATEWAY",
                    "sourceIP":event["requestContext"]["identity"]["sourceIp"],
                    "apiId":event['requestContext']['apiId'],   
                    "stage":event['requestContext']['stage'],            
                    "resourcePath" :event['requestContext']['resourcePath'],
                    "apiName" :"GetROHistory",
                    "httpMethod" :event['requestContext']['httpMethod'],
                    "apiParameters" :apiParameters,
                    "storeCode":store_code,          
                    "status": status,
                    "requestTime":requestStart.strftime("%Y-%m-%dT%H:%M:%S%z"),  
                    "totalProcessingTimeMS": time_difference_in_milliseconds,                
                    "message": errorMessage,
                    "code": errorCode,
                    "responseCode":responseCode,
                    "totalFetchRecords": recordCounts,
                    "responseSize": len(res_json.encode('utf-8')) ,
                    "apiType" :"public"              
               } 
               cwLogger= CloudWatchLogger()    
               cwLogger.DoCloudWatchLog('epde/apiLog',logEvent)
          except:
               logger.error("error occured DoCloudWatchLog in lambda_handler",True)