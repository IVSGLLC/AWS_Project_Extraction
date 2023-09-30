
import datetime
import pytz
from EPDE_CloudWatchLog import CloudWatchLogger
from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient
from EPDE_INVOICE import Invoice
from EPDE_Validator import RequestValidator
logger=LogManger()
err_handler=ErrorHandler()

def lambda_handler(event, context):
        resHandler=ResponseHandler()
        tablePerStore=1       
        auth_json=None        
        res_json=""
        store_code=""
        status="ERROR"
        recordCounts=-1      
        eastern=pytz.timezone('US/Eastern')  
        requestStart = datetime.now().astimezone(eastern)
        limit=-1
        lastEvaluatedKey=None
        region=""
        try:             
                validator=RequestValidator(resHandler)     
                res_json=validator.Validate()
                if res_json["responseCode"]  == -1:
                        return resHandler.GetAPIResponse(res_json)     

                auth_json=res_json['auth_json']
                store_code=res_json['store_code']
                region=res_json['REGION']
               
                queryStringParameters= event.get("queryStringParameters")               
                if "limit" in queryStringParameters:  
                        limit=  queryStringParameters["limit"]
                if "lastEvaluatedKey" in queryStringParameters:  
                        lastEvaluatedKey=  queryStringParameters["lastEvaluatedKey"]  
               
                document_type='RO' 
                logger.debug("store_code="+store_code+",document_type="+document_type+",limit="+str(limit)+",lastEvaluatedKey="+lastEvaluatedKey)                    
                
                invoice_manager= Invoice(region,tablePerStore)       
                validateResp=invoice_manager.Validate_InvoiceList_inputs(document_type,auth_json)
                if validateResp['responseCode']== -1:
                        logger.debug("Invalid Request..")
                        res_json= validateResp
                        return resHandler.GetAPIResponse(res_json)    
                
                ro_resp=invoice_manager.GetInvoiceList(store_code,document_type,lastEvaluatedKey,limit)
                if ro_resp['status']== False: 
                        logger.debug("Invalid RO /RO not Found..")
                        error_code=ro_resp['error_code']
                        res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)
                        return resHandler.GetAPIResponse(res_json)
                
                items=ro_resp['items']
                LastEvaluatedKey=None
                try:
                        LastEvaluatedKey=ro_resp['LastEvaluatedKey'] 
                except:
                        ""
                            
                pre_resp=invoice_manager.prepareInvoiceList(items)                           
                if pre_resp['status']== False:
                        logger.debug("Error while making invoiceList.")
                        error_code=pre_resp['error_code']
                        res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)       
                        return resHandler.GetAPIResponse(res_json)
                docs=pre_resp['invoiceList']
                recordCounts=len(docs)
                status="SUCCESS"
                res_json={"responseCode": 0,
                        "documentList":docs,
                        "auth_token":auth_json,
                        "limit":limit,
                        "lastEvaluatedKey":LastEvaluatedKey
                        }                                 
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
                    "apiName" :"GetRODetailPDFList",
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
    
 