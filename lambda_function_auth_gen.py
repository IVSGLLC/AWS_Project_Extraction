import pytz
from EPDE_CloudWatchLog import CloudWatchLogger
from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient 
import requests 
import datetime
import base64 as b64
logger=LogManger()
err_handler=ErrorHandler()
def lambda_handler(event, context):
    status="ERROR" 
    res_json=""
    api_key=None
    resHandler=ResponseHandler()
    eastern=pytz.timezone('US/Eastern')  
    requestStart = datetime.now().astimezone(eastern)
    store_code=None
    body=None
    try:           
            grant_type=True
            isPasswordClient=False
            stage=event['requestContext']['stage']
            access_secret=None
            try:
                body= event['body']
                if str(body).lower().startswith("grant-type") or str(body).lower().startswith("grant_type"):
                    grant_type=True
                    if str(body).lower() != "grant-type=password" and str(body).lower() != "grant_type=password":
                        isPasswordClient=False
                        length_gt=len("grant-type=")                              
                        access_secret=str(body).strip()[length_gt:]
                        #logger.debug("as="+access_secret)
                    else:  
                        access_secret="password"
                        #logger.debug("as="+access_secret)
                        isPasswordClient=True
                else:
                    grant_type=False 
            except:
                grant_type=False

            if grant_type==False:
                logger.debug("Grant Type not found/Invalid...")                  
                res_json= {
                            "responseCode": -1,
                            "errorList": [
                                {
                                    "code": 361,
                                    "message": "unsupported grant type"
                                }
                            ]
                        }
                return resHandler.GetAPIResponse(res_json)
            access_token=None
            try:
                Authorization=event['headers']['Authorization']                        
                auth_token=Authorization.split(" ")
                if len(auth_token)>=2:
                    token_type=auth_token[0]  
                    if str(token_type).lower() !="bearer":
                        access_token=None
                    else:
                        access_token=auth_token[1] 
                else:
                    access_token=Authorization                    
            except:
                access_token=None
            if access_token is None:
                logger.debug("API Key not found...")                  
                res_json= resHandler.GetErrorResponseJSON(331,None)
                return resHandler.GetAPIResponse(res_json)
          
            app_client= AppClient()
            config=app_client.GetConfiguration(event)
            region=config['REGION']
            store_detail_resp=None
            api_key=access_token
            if isPasswordClient:
                store_detail_resp=app_client.GetStoreDetailByAPIKey(access_token,region)
                if store_detail_resp['status']== True and "item" in store_detail_resp:
                    if "store" in store_detail_resp["item"]:          
                        store_code= store_detail_resp["item"]["store"]
            else:
                store_detail_resp=app_client.GetStoreDetailByClientId(access_token,region) 
                if store_detail_resp['status']== True and "item" in store_detail_resp:
                    if "store_code" in store_detail_resp["item"]:  
                        store_code= store_detail_resp["item"]["store_code"]
                    #if "stores" in store_detail_resp["item"]:  
                        #store_code= store_detail_resp["item"]["stores"]     
                

            if store_detail_resp['status']== False:
                        logger.debug("Store detail not found...")                
                        error_code=store_detail_resp['error_code']
                        res_json= resHandler.GetErrorResponseJSON(error_code,None)
                        return resHandler.GetAPIResponse(res_json)
            else:
                item =store_detail_resp['item']
                is_active=item['is_active']
                on_prod=item['on_prod']                                
                if is_active and ((stage == 'test' and on_prod == False) or (stage == 'prod' and on_prod == True)) :
                    if isPasswordClient:
                        client_id=item['client_id']
                        client_secret=item['client_secret'] 
                    else:
                        client_id=access_token
                        client_secret=access_secret  
                else:
                    logger.debug("Store not active / not accessible to given stage")                
                    res_json= resHandler.GetErrorResponseJSON(339,None)  
                    return resHandler.GetAPIResponse(res_json)  
           

            url=config['TOKEN_API_ENDPOINT']
            sample_string=client_id+":"+client_secret
            sample_string_bytes = sample_string.encode("ascii")
            base64_bytes = b64.b64encode(sample_string_bytes)
            base64_string = base64_bytes.decode("ascii")
            #logger.debug("base64_string:"+base64_string)
            authrization_value = 'Basic '+base64_string
            payload = "grant_type=client_credentials&client_id="+client_id
            headers = {
                        'authorization': authrization_value,
                        'content-type': "application/x-www-form-urlencoded"
                        
                        }
            logger.debug("Cognito payload  payload:"+str(payload))
            logger.debug("Cognito token  headers:"+str(headers))                        
            api_response = requests.request("POST", url, data=payload, headers=headers)
            logger.debug("Cognito token endpoint  (url="+url+") json :"+str(api_response.json()))
            api_responseJson=api_response.json()  
            logger.debug("Cognito api_response.status_code:"+str(api_response.status_code))                      
            if api_response.status_code ==200:
                status="SUCCESS"
                token_type=api_responseJson['token_type']
                access_token=api_responseJson['access_token']
                expires_in=api_responseJson['expires_in']
                issued = datetime.datetime.now()
                time_change = datetime.timedelta(seconds=expires_in)
                expires = issued + time_change                            
                x = issued.astimezone().strftime("%a, %d %b %G %H:%M:%S GMT")
                y = expires.astimezone().strftime("%a, %d %b %G %H:%M:%S GMT")                    
                res_json={
                                "access_token": access_token,
                                "token_type": token_type,
                                "expires_in": expires_in,
                                ".issued": str(x),
                                ".expires": str(y)
                                }
            else:
                res_json= resHandler.GetErrorResponseJSON(401,None)                   
            return resHandler.GetAPIResponse(res_json)
            
    except Exception as e:
            logger.error("error occured in lambda_handler",True)
            res_json= resHandler.GetErrorResponseJSON(313,None)
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
                    "pathParameters":event['pathParameters'],
                    "body":event['body'],                    
                    "Authorization":event['headers']['Authorization']                                 
               }               
               apiParameters=resHandler.ConvertJsonToString(resjson=param_json)
               logEvent={
                    "instance" :str(aws_account_id),
                    "app":"EPDE-API-GATEWAY",
                    "sourceIP":event["requestContext"]["identity"]["sourceIp"],
                    "apiId":event['requestContext']['apiId'],   
                    "stage":event['requestContext']['stage'],            
                    "resourcePath" :event['requestContext']['resourcePath'],
                    "apiName" :"GenerateToken",
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
                    "apiType" :"public" ,
                    "apiKey" :api_key 
                                 
               } 
               cwLogger= CloudWatchLogger()    
               cwLogger.DoCloudWatchLog('epde/apiLog',logEvent)
          except:
               logger.error("error occured DoCloudWatchLog in lambda_handler",True)
 