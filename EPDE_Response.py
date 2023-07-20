
import base64
from decimal import Decimal
from EPDE_Logging import LogManger
from EPDE_Error import ErrorHandler
import json
class fakefloat(float):
    def __init__(self, value):
        self._value = value
    def __repr__(self):
        return str(self._value)

def defaultencode(o):
    if isinstance(o, Decimal):
        # Subclass float with custom repr?
        return fakefloat(o)
    raise TypeError(repr(o) + " is not JSON serializable")
#This module is responsible to handle EPDE API response
class ResponseHandler(object):
    logger=LogManger() 
    err_handler=ErrorHandler()
    
    @classmethod
    def GetAPIResponse(self,resjson):
        _moduleNM="ResponseHandler"
        _functionNM="GetAPIResponse"
        #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM) 
        json_body=json.dumps(resjson, default=defaultencode, separators=(',', ':')) 
        responseDict= {
            "statusCode": 200,
            "isBase64Encoded": False,
            "body": json_body,
            "headers": {
                "Content-Type": "application/json"
                }
            }
        #self.logger.debug("APIResponse:"+str(responseDict))  
        return responseDict
    @classmethod
    def GetPDFResponse(self,pdfdoc,ro):
        _moduleNM="ResponseHandler"
        _functionNM="GetPDFResponse"
        responseDict= {
            "statusCode": 200,
            "isBase64Encoded": True,
            "body":base64.b64encode(pdfdoc).decode('utf-8') ,
            "headers": {
                "Content-Type": "application/pdf"
                
                }
            }
        #self.logger.debug("GetPDFResponse:"+str(responseDict))  
        return responseDict
    @classmethod
    def GetHtmlResponse(self,htmdoc):
        _moduleNM="ResponseHandler"
        _functionNM="GetHtmlResponse"
        #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM) 
        
        responseDict= {
            "statusCode": 200,
            "isBase64Encoded": False,
            "body": htmdoc,
            "headers": {
                "Content-Type": "text/html"
                }
            }
        #self.logger.debug("GetHtmlResponse:"+str(responseDict))  
        return responseDict
    @classmethod
    def GetErrorResponseJSON(self,code,auth_json=None):
        _moduleNM="ResponseHandler"
        _functionNM="GetErrorResponseJSON"
        try:
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)  
            msg=self.err_handler.GetErrorMessage(code)
            
            if not auth_json is None:
                return {
                        "responseCode": -1,"errorList": [
                                {
                                "code": code,
                                "message": msg
                                }
                            ],"auth_token":auth_json}
            else:
                return {
                        "responseCode": -1,"errorList": [
                            {
                            "code": code,
                            "message": msg
                            }
                        ]}
        except Exception as e:
              comm_err=self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
              return {
                        "responseCode": -1,"errorList": [
                            {
                            "code": comm_err['error_code'],
                            "message": comm_err['error_message']
                            }
                        ]}

    @classmethod
    def GetFormattedErrorResponseJSON(self,code,auth_json={},args=[]):
        _moduleNM="ResponseHandler"
        _functionNM="GetFormattedErrorResponseJSON"
        try:
            #self.err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)  
            msg=self.err_handler.GetErrorFormattedMessage(code,args=args)
            
            if not auth_json is None:
                return {
                            "responseCode": -1,"errorList": [
                                {
                                "code": code,
                                "message": msg
                                }
                            ],"auth_token":auth_json}
            else:
                return {
                        "responseCode": -1,"errorList": [
                            {
                            "code": code,
                            "message": msg
                            }
                        ]}
        except Exception as e:
               comm_err=self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
               return {
                        "responseCode": -1,"errorList": [
                            {
                            "code": comm_err['error_code'],
                            "message": comm_err['error_message']
                            }
                        ]}
              
        