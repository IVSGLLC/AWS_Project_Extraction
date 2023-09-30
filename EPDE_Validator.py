
from EPDE_Client import AppClient
from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_Response import ResponseHandler

class RequestValidator(object):

    def __init__(self,resHandler):        
        resHandler=resHandler 
        logger=LogManger()   
                
    @classmethod   
    def Validate(self,event):        
        app_client= AppClient()
        config=app_client.GetConfiguration(event=event)
        region=config['REGION']
        auth_detail_resp=app_client.GetAuthenticationDetail(event) 
        if auth_detail_resp['status']== False:
            self.logger.debug("Auth detail not found...")
            auth_json=auth_detail_resp['auth_json']
            error_code=auth_detail_resp['error_code']
            res_json= self.resHandler.GetErrorResponseJSON(error_code,auth_json)    
            return { "responseCode": -1,"store_code":"","res_json":res_json,"REGION":region,"config":config}
              
        validate_resp=app_client.ValidateStoreDetail(region,event)
        if validate_resp['status']==False: 
            self.logger.debug("STORE NOT FOUND...")
            error_code=validate_resp['error_code']
            res_json= self.resHandler.GetErrorResponseJSON(error_code,auth_json)   
            return { "responseCode": -1,"auth_json":auth_json,"store_code":"","res_json":res_json,"REGION":region,"config":config}
        
        auth_json=auth_detail_resp['auth_json']
        store_code=validate_resp['store_code']

        maintenanace_resp=app_client.isMaintenanceTime(config)
        if maintenanace_resp['status']:  
            res_json= maintenanace_resp['resjson']             
            self.logger.debug("Maintenance Time is running API call not allowed...")
            return { "responseCode": -1,"auth_json":auth_json,"store_code":store_code,"res_json":res_json,"REGION":region,"config":config}

        return { "responseCode": 0,"auth_json":auth_json,"store_code":store_code,"REGION":region,"config":config,"res_json":""}
