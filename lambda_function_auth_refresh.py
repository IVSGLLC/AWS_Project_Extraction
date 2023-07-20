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
    res_json=""
    resHandler=ResponseHandler()
    try:
        _moduleNM="EPDE_Lambda"
        _functionNM="lambda_handler"
        err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)             
        app_client= AppClient()
        config=app_client.GetConfiguration(event)
        url=config['TOKEN_API_ENDPOINT']
        region=config['REGION']
        auth_resp=app_client.GetAuthenticationDetail(event) 
        stage=event['requestContext']['stage']
        if auth_resp['status']== True:
            client_id=auth_resp['client_id']
            store_detail_resp=app_client.GetAppClientDetailByClientId(client_id,region)
            if store_detail_resp['status']== True:                         
                item =store_detail_resp['item']
                is_active=item['is_active']
                on_prod=item['on_prod']                             
                if is_active and ((stage == 'test' and on_prod == False) or (stage == 'prod' and on_prod == True)) :

                    client_secret=item['client_secret']                 
                    sample_string=client_id+":"+client_secret
                    sample_string_bytes = sample_string.encode("ascii")
                    base64_bytes = b64.b64encode(sample_string_bytes)
                    base64_string = base64_bytes.decode("ascii")
                    logger.debug("base64_string:"+base64_string)
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
                        logger.debug("AUTH Endpoint error....api_response:"+str(api_response))
                        res_json= resHandler.GetErrorResponseJSON(313,None)
                else:
                        logger.debug("Store not active / not accessible to given stage")                
                        res_json= resHandler.GetErrorResponseJSON(339,None)
            else:
                logger.debug("Store detail not found by client id...")                
                error_code=store_detail_resp['error_code']
                res_json= resHandler.GetErrorResponseJSON(error_code,None)     
        else:
            logger.debug("auth  json not found...")                
            error_code=auth_resp['error_code']
            res_json= resHandler.GetErrorResponseJSON(error_code,None)            
             
        return resHandler.GetAPIResponse(res_json)
    except Exception as e:
            logger.error("error occured in lambda_handler",True)
            res_json= resHandler.GetErrorResponseJSON(313,None)
            return resHandler.GetAPIResponse(res_json) 
