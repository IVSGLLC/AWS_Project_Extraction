
import datetime
import pytz
from EPDE_CloudWatchLog import CloudWatchLogger
from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient
from EPDE_SALESCUST import SalesCustomer
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
    document_type=None
    tablePerStore=1 

    try:
        validator=RequestValidator(resHandler)     
        res_json=validator.Validate()
        if res_json["responseCode"]  == -1:
            return resHandler.GetAPIResponse(res_json)     

        auth_json=res_json['auth_json']
        store_code=res_json['store_code']
        region=res_json['REGION']

        customer_number=None 
        pathParameters= event.get("pathParameters") 
        if "customerNumber" in pathParameters:
            customer_number=  pathParameters["customerNumber"]
        
        logger.debug("store_code="+store_code+",customer_number="+str(customer_number)) 
        salescust_manager= SalesCustomer(region,tablePerStore)       
        validateResp=salescust_manager.Validate_SalesCustDetail_inputs(customer_number=customer_number,auth_json=auth_json)
        if validateResp['responseCode']== -1:
            logger.debug("Invalid Request..")
            res_json= validateResp  
            return resHandler.GetAPIResponse(res_json) 
        
        salscust_resp=salescust_manager.GetSalesCustomerDetail(store_code,customer_number)
        if salscust_resp['status']== False: 
            logger.debug("Invalid Customer number  /Customer number not Found..")
            error_code=salscust_resp['error_code']
            res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)
            return resHandler.GetAPIResponse(res_json) 
        items=salscust_resp['items']
        pre_resp=salescust_manager.prepareSalesCustomerResponse(items,auth_json)
        if pre_resp['status']== False:
            logger.debug("Error while making SalesCustomerResponse.")
            error_code=pre_resp['error_code']
            res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)     
            return resHandler.GetAPIResponse(res_json)               
        
        res_json=pre_resp['salesCustomerDetail']
        logger.debug("store_code="+store_code+", SALESCUST_DETAIL_JSON :"+str(res_json))    
        status="SUCCESS"                               
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
                    "apiName" :"GetSalesCustomerDetail",
                    "httpMethod" :event['requestContext']['httpMethod'],
                    "apiParameters" :apiParameters,
                    "storeCode":store_code,          
                    "status": status,
                    "requestTime":requestStart.strftime("%Y-%m-%dT%H:%M:%S%z"),  
                    "totalProcessingTimeMS": time_difference_in_milliseconds,                
                    "message": errorMessage,
                    "code": errorCode,
                    "responseCode":responseCode,                    
                    "responseSize": len(res_json.encode('utf-8')) ,
                    "apiType" :"public"              
               } 
               cwLogger= CloudWatchLogger()    
               cwLogger.DoCloudWatchLog('epde/apiLog',logEvent)
          except:
               logger.error("error occured DoCloudWatchLog in lambda_handler",True)
 