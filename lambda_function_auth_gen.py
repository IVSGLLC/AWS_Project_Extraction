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
    #logger.debug("event="+str(event))    
    res_json=""
    resHandler=ResponseHandler()
    try:
            _moduleNM="EPDE_Lambda"
            _functionNM="lambda_handler"
            err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM) 
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
            app_client= AppClient()
            config=app_client.GetConfiguration(event)
            region=config['REGION']
            store_detail_resp=None
            if isPasswordClient:
                store_detail_resp=app_client.GetStoreDetailByAPIKey(access_token,region) 
            else:
                store_detail_resp=app_client.GetStoreDetailByClientId(access_token,region) 

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
