
import datetime
import pytz
from EPDE_CloudWatchLog import CloudWatchLogger
from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_Response import ResponseHandler
from EPDE_RO import RepairOrder
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
     recordCounts=-1
     open_date=None
     document_type=None
     limit=-1
     lastEvaluatedKey=None
     try:  
          tablePerStore=1  
          validator=RequestValidator(resHandler)     
          res_json=validator.Validate()
          if res_json["responseCode"]  == -1:
             return resHandler.GetAPIResponse(res_json)     

          auth_json=res_json['auth_json']
          store_code=res_json['store_code']
          region=res_json['REGION']
          
          queryStringParameters= event.get("queryStringParameters")
         
          if "openDateTime" in queryStringParameters:  
               open_date= queryStringParameters["openDateTime"]
          if "documentType" in queryStringParameters:  
               document_type=  queryStringParameters["documentType"]
          if "limit" in queryStringParameters:  
               limit=  queryStringParameters["limit"]
          if "lastEvaluatedKey" in queryStringParameters:  
               lastEvaluatedKey=  queryStringParameters["lastEvaluatedKey"]                            
          logger.debug("QUERY PARAM store_code="+store_code+",open_date="+str(open_date)+",document_type="+document_type +",limit="+str(limit)+",lastEvaluatedKey="+lastEvaluatedKey)                    
          
          ro_manager= RepairOrder(region,tablePerStore)       
          validateResp=ro_manager.Validate_ROList_inputs(open_date,document_type,auth_json)
          if validateResp['responseCode'] != 0:
               logger.debug("Invalid Request..")
               res_json= validateResp
               return resHandler.GetAPIResponse(res_json)
          
          ro_resp=ro_manager.GetROList(store_code,document_type,open_date,lastEvaluatedKey,limit)
          if ro_resp['status']== False  :                
               logger.debug("Invalid RO /RO not Found..")
               error_code=ro_resp['error_code']
               res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)
               return resHandler.GetAPIResponse(res_json)
         
          items=ro_resp['items']
          LastEvaluatedKey=None
          if "lastEvaluatedKey" in ro_resp:  
               LastEvaluatedKey=ro_resp['LastEvaluatedKey'] 

          pre_resp=ro_manager.prepareDocumentList(items)
          if pre_resp['status']== False:
               logger.debug("Error while making documenmtList.")
               error_code=pre_resp['error_code']
               res_json= resHandler.GetErrorResponseJSON(error_code,auth_json) 
               return resHandler.GetAPIResponse(res_json)
                  
          docs=pre_resp['documentList']
          status="SUCCESS"
          res_json={
                    "responseCode": 0,
                    "documentList":docs,
                    "auth_token":auth_json,
                    "limit":limit,
                    "lastEvaluatedKey":LastEvaluatedKey
                    }
          recordCounts=len(docs) 
          logger.debug("store_code="+store_code+",Total RO:"+str(recordCounts)) 
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
                    "apiName" :"GetROList",
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
    