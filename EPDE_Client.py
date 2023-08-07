
import datetime
from EPDE_Logging import LogManger
import boto3
from boto3.dynamodb.conditions import Attr, Key
from EPDE_Error import ErrorHandler
from EPDE_Response import ResponseHandler

#This module is responsible to handle EPDE API Client
class AppClient(object):
    logger=LogManger()   
    err_handler=ErrorHandler()
    """ @classmethod
    def GetStoreDetailByCode(self,store_code,region):
        try:
            _moduleNM="AppClient"
            _functionNM="GetStoreDetailByCode"
            self.logger.debug("GetStoreDetailByCode>> store_code:"+str(store_code)+", region:"+str(region))
            dynamodb = boto3.resource('dynamodb', region_name=region)
            TableName="EPDE_CLIENTS"
            table = dynamodb.Table(TableName)
            response = table.query(KeyConditionExpression=Key('store_code').eq(store_code),IndexName='store_code_index')
            self.logger.debug("response:"+str(response))
            if response['Count'] > 0:
                items = response['Items']
                item=items[0]
                if item['is_active']:
                    return { "status":True,"item": item } 
                else:
                    return self.err_handler.HandleAppError(331,moduleNM=_moduleNM,functionNM=_functionNM)   
            else:
                return self.err_handler.HandleAppError(339,moduleNM=_moduleNM,functionNM=_functionNM)   
        except Exception as e:
               return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) """
            
    @classmethod
    def GetStoreDetailByAPIKey(self,api_key,region):
        try:
            _moduleNM="AppClient"
            _functionNM="GetStoreDetailByAPIKey"
            self.logger.debug("GetStoreDetailByAPIKey >> api_key:"+str(api_key)+", region:"+str(region))
            dynamodb = boto3.resource('dynamodb', region_name=region)
            TableName="EPDE_API_KEYS"
            table = dynamodb.Table(TableName)
            response = table.query(KeyConditionExpression=Key('api_key').eq(api_key))
            self.logger.debug("response:"+str(response))
            if response['Count'] > 0:
                items = response['Items']
                item=items[0]
                if item['is_active']:
                    stResponse=self.GetStoreDetailByClientId(item['client_id'],region)
                    if stResponse['status']:
                       item=stResponse['item']
                       return { "status":True,"item": item } 
                    else:
                      return self.err_handler.HandleAppError(331,moduleNM=_moduleNM,functionNM=_functionNM) 
                else:
                    return self.err_handler.HandleAppError(331,moduleNM=_moduleNM,functionNM=_functionNM)   
            else:
                return self.err_handler.HandleAppError(339,moduleNM=_moduleNM,functionNM=_functionNM)   
        except Exception as e:
               return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
    
    @classmethod
    def GetStoreDetailByClientId(self,client_id,region):
        try:
            _moduleNM="AppClient"
            _functionNM="GetStoreDetailByClientId"
            self.logger.debug("GetStoreDetailByClientId >> client_id:"+str(client_id)+", region:"+str(region))           
            dynamodb = boto3.resource('dynamodb', region_name=region)
            TableName="EPDE_CLIENTS"
            table = dynamodb.Table(TableName)
            response = table.get_item(Key={'client_id': client_id})
            self.logger.debug("response:"+str(response))
            try:
                item = response['Item']
                if item['is_active']:
                    return { "status":True,"item": item } 
                else:
                    return self.err_handler.HandleAppError(331,moduleNM=_moduleNM,functionNM=_functionNM)   
            except KeyError as kerr:
                return self.err_handler.HandleAppError(339,moduleNM=_moduleNM,functionNM=_functionNM)              
        except Exception as e:
               return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
    @classmethod
    def GetAppClientDetailByClientId(self,client_id,region):
        try:
            _moduleNM="AppClient"
            _functionNM="GetAppClientDetailByClientId"
            self.logger.debug("GetAppClientDetailByClientId >> client_id:"+str(client_id)+", region:"+str(region))           
            dynamodb = boto3.resource('dynamodb', region_name=region)
            TableName="EPDE_CLIENTS"
            table = dynamodb.Table(TableName)
            response = table.get_item(Key={'client_id': client_id})
            self.logger.debug("response:"+str(response))
            try:
                item = response['Item']
                if item['is_active']:
                    return { "status":True,"item": item } 
                else:
                    return self.err_handler.HandleAppError(331,moduleNM=_moduleNM,functionNM=_functionNM)   
            except KeyError as kerr:
                return self.err_handler.HandleAppError(339,moduleNM=_moduleNM,functionNM=_functionNM)              
        except Exception as e:
               return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
    
    @classmethod
    def GetStoreDetail(self,store_code,region):
        try:
            _moduleNM="AppClient"
            _functionNM="GetStoreDetail"
            self.logger.debug("GetStoreDetail>> store_code:"+str(store_code)+", region:"+str(region))
            dynamodb = boto3.resource('dynamodb', region_name=region)
            TableName="EPDE_STORES"
            table = dynamodb.Table(TableName)
            response = table.query(KeyConditionExpression=Key('store_code').eq(store_code))
            self.logger.debug("response:"+str(response))
            if response['Count'] > 0:
                items = response['Items']
                item=items[0]
                if item['is_active']:
                    return { "status":True,"item": item } 
                else:
                    return self.err_handler.HandleAppError(331,moduleNM=_moduleNM,functionNM=_functionNM)   
            else:
                return self.err_handler.HandleAppError(339,moduleNM=_moduleNM,functionNM=_functionNM)   
        except Exception as e:
               return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
            
    @classmethod
    def ValidateStoreDetail(self,region,event):
        try:
            client_id=self.GetClient_Id(event)
            storecode=self.GetStoreCode(event)
            _moduleNM="AppClient"
            _functionNM="ValidateStoreDetail"
            self.logger.debug("ValidateStoreDetail >> client_id:"+str(client_id)+", region:"+str(region))           
            dynamodb = boto3.resource('dynamodb', region_name=region)
            TableName="EPDE_CLIENTS"
            table = dynamodb.Table(TableName)
            response = table.get_item(Key={'client_id': client_id})
            #self.logger.debug("response:"+str(response))
            try:
                item = response['Item']
                if item['is_active']:
                    if storecode is not None and len(storecode.strip())>0 :
                        if 'stores' in item  and list(item['stores']).__contains__(storecode):
                          self.logger.debug("from new store_code:"+str(storecode))
                          return { "status":True,"store_code": storecode } 
                        else:
                          return self.err_handler.HandleAppError(401,moduleNM=_moduleNM,functionNM=_functionNM)    
                    else:
                        if 'store_code' in item and len(item["store_code"])>0:
                            self.logger.debug("from old store_code:"+str(item["store_code"]))
                            return { "status":True,"store_code": item["store_code"] } 
                        else:
                            return self.err_handler.HandleAppError(401,moduleNM=_moduleNM,functionNM=_functionNM)    
                else:
                    return self.err_handler.HandleAppError(331,moduleNM=_moduleNM,functionNM=_functionNM)   
            except KeyError as kerr:
                return self.err_handler.HandleAppError(339,moduleNM=_moduleNM,functionNM=_functionNM)              
        except Exception as e:
               return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)


    @classmethod
    def GetAuthenticationDetail(self,event):
        _moduleNM="AppClient"
        _functionNM="GetStoreDetail"         
        try:            
            client_id = self.GetClient_Id(event)
            auth_json=self.GetAuthJson(event)
            return {'status':True,'auth_json':auth_json, 'client_id':client_id} 
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM)
    
    @classmethod
    def GetStoreGroups(self,region):
        _moduleNM="AppClient"
        _functionNM="GetStoreGroups"         
        try:                          
            self.logger.debug("GetStoreGroups>> region:"+str(region))
            dynamodb = boto3.resource('dynamodb', region_name=region)
            TableName="EPDE_CUSTOMER_GROUP"
            table = dynamodb.Table(TableName)
            response = table.scan(FilterExpression= Attr('is_active').eq(True) )
            self.logger.debug("response:"+str(response))
            try:
                items = response['Items']                
                return { "status":True,"items": items }
            except:
                return { "status":True,"items": [] }
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 
    @classmethod
    def GetStores(self,group_id,region):
        _moduleNM="AppClient"
        _functionNM="GetStores"         
        try:
                          
            self.logger.debug("GetStores>>")           
            dynamodb = boto3.resource('dynamodb', region_name=region)
            TableName="EPDE_STORES"
            table = dynamodb.Table(TableName)
            response = table.scan(
                FilterExpression= Attr('is_active').eq(True) & Attr('group_id').eq(group_id) & Attr('on_prod').eq(True) )
            self.logger.debug("response:"+str(response))
            try:
                    items = response['Items']                
                    while 'LastEvaluatedKey' in response:
                        table.scan(
                            FilterExpression= Attr('is_active').eq(True) & Attr('group_id').eq(group_id) & Attr('on_prod').eq(True),
                            ExclusiveStartKey=response['LastEvaluatedKey']
                            )
                        try:
                            items.update(response['Items'])
                        except:
                            ""
                    return { "status":True,"items": items }
            except:
                    return { "status":True,"items": [] } 
           
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 
    @classmethod
    def GetAllStores(self,group_id,region):
        _moduleNM="AppClient"
        _functionNM="GetAllStores"         
        try:
                          
            self.logger.debug("GetAllStores>>")           
            dynamodb = boto3.resource('dynamodb', region_name=region)
            TableName="EPDE_STORES"
            table = dynamodb.Table(TableName)
            response = table.scan(
                FilterExpression= Attr('is_active').eq(True) & Attr('group_id').eq(group_id)  )
            self.logger.debug("response:"+str(response))
            try:
                    items = response['Items']                
                    while 'LastEvaluatedKey' in response:
                        table.scan(
                            FilterExpression= Attr('is_active').eq(True) & Attr('group_id').eq(group_id) ,
                            ExclusiveStartKey=response['LastEvaluatedKey']
                            )
                        try:
                            items.update(response['Items'])
                        except:
                            ""
                    return { "status":True,"items": items }
            except:
                    return { "status":True,"items": [] } 
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 
    @classmethod
    def GetTestStores(self,group_id,region):
        _moduleNM="AppClient"
        _functionNM="GetTestStores"         
        try:                          
            self.logger.debug("GetTestStores>>")           
            dynamodb = boto3.resource('dynamodb', region_name=region)
            TableName="EPDE_STORES"
            table = dynamodb.Table(TableName)
            response = table.scan(
                FilterExpression= Attr('is_active').eq(True) & Attr('group_id').eq(group_id) & Attr('on_prod').eq(False))
            self.logger.debug("response:"+str(response))
            try:
                    items = response['Items']                
                    while 'LastEvaluatedKey' in response:
                        table.scan(
                            FilterExpression= Attr('is_active').eq(True) & Attr('group_id').eq(group_id) & Attr('on_prod').eq(False) ,
                            ExclusiveStartKey=response['LastEvaluatedKey']
                            )
                        try:
                            items.update(response['Items'])
                        except:
                            ""
                    return { "status":True,"items": items }
            except:
                    return { "status":True,"items": [] } 
        except Exception as e:
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 
    @classmethod
    def GetAccessToken(self,event):
        access_token=None
        try:
            Authorization=event['headers']['Authorization']                     
            auth_token=Authorization.split(" ")
            token_type=auth_token[0]  
            if str(token_type).lower() !="bearer":
                access_token=None
            else:
                access_token=auth_token[1] 
        except:
            access_token=None

        return access_token
    @classmethod
    def GetStoreCode(self,event):
        storecode=None
        try:
            storecode=event['headers']['storecode']                     
            
        except:
            storecode=None

        return storecode
    @classmethod
    def GetClient_Id(self,event):
        client_id=None
        try:
            client_id=event['requestContext']['authorizer']['claims']['client_id']    
        except:
            client_id=None
        return client_id
    @classmethod
    def GetIssued(self,event):
        iat=None
        try:
            iat=event['requestContext']['authorizer']['claims']['iat']    
        except:
            iat=None
        return iat
    @classmethod
    def GetExpires(self,event):
        exp=None
        try:
            exp=event['requestContext']['authorizer']['claims']['exp']             
        except:
            exp=None
        return exp
    @classmethod
    def GetExpiresIn(self,event):
        exp_str=self.GetExpires(event)
        iat_str=self.GetIssued(event)
        #Sat Nov 13 07:42:06 UTC 2021
        exp_date_time_obj = datetime.datetime.strptime(exp_str, '%a %b %d  %H:%M:%S %Z %Y')
        iat_date_time_obj = datetime.datetime.strptime(iat_str, '%a %b %d %H:%M:%S %Z %Y')
        delta=exp_date_time_obj-iat_date_time_obj
        return delta.seconds
    @classmethod
    def GetAuthJson(self,event):
        access_token= self.GetAccessToken(event)
        expires_in=self.GetExpiresIn(event)
        iat=self.GetIssued(event)
        iat= datetime.datetime.strptime(iat, '%a %b %d  %H:%M:%S %Z %Y').strftime('%a, %d %b %G %H:%M:%S GMT') 
        exp=self.GetExpires(event)
        exp= datetime.datetime.strptime(exp, '%a %b %d  %H:%M:%S %Z %Y').strftime('%a, %d %b %G %H:%M:%S GMT') 
       
        return {
                        "access_token": access_token,
                        "token_type": "Bearer",
                        "expires_in": expires_in,
                        ".issued": iat,
                        ".expires": exp
            } 
    @classmethod
    def GetConfiguration(self,event):
        stageVariables=None
        try:
            stageVariables=event['stageVariables']     
        except:
            stageVariables=None
        return stageVariables
    """  @classmethod
    def isCaptureTime(self,config,auth=None):
        self.logger.info("isMaintenanceTime>> x:"+str(config)) 
        response=False    
        resjson=""  
        try:
            x = datetime.datetime.now().time()
           
            self.logger.debug("isMaintenanceTime>> current time:"+str(x)) 
            maintenanace_start_time=config['API_MAINTENANCE_START_TIME']
            start = datetime.datetime.strptime(maintenanace_start_time, '%H:%M').time()
           
            maintenanace_end_time=config['API_MAINTENANCE_END_TIME']  
            end = datetime.datetime.strptime(maintenanace_end_time, '%H:%M').time()           
           
            self.logger.debug("isMaintenanceTime>> x:"+str(x))     
            if start <= end:
                response =(start <= x <= end)
            else:
                response =(start <= x or x <= end)
           
            if response:
                args=[]
                args.append(maintenanace_start_time)
                args.append(maintenanace_end_time)
                res_handler=ResponseHandler()
                resjson=res_handler.GetFormattedErrorResponseJSON(333,auth,args)  
                              
        except:
                self.logger.error("isMaintenanceTime error>>",True)  
                res_handler=ResponseHandler()
                resjson=res_handler.GetErrorResponseJSON(312,auth)   

        return {"status":response,"resjson":resjson}  """
    @classmethod
    def isMaintenanceTime(self,config,auth=None):
        self.logger.debug("isMaintenanceTime>> x:"+str(config)) 
        response=False    
        resjson=""  
        try:
            x = datetime.datetime.now().time()
           
            #self.logger.debug("isMaintenanceTime>> current time:"+str(x)) 
            maintenanace_start_time=config['API_MAINTENANCE_START_TIME']
            start = datetime.datetime.strptime(maintenanace_start_time, '%H:%M').time()
           
            maintenanace_end_time=config['API_MAINTENANCE_END_TIME']  
            end = datetime.datetime.strptime(maintenanace_end_time, '%H:%M').time()           
           
            #self.logger.debug("isMaintenanceTime>> x:"+str(x))     
            if start <= end:
                response =(start <= x <= end)
            else:
                response =(start <= x or x <= end)
           
            if response:
                args=[]
                args.append(maintenanace_start_time)
                args.append(maintenanace_end_time)
                res_handler=ResponseHandler()
                resjson=res_handler.GetFormattedErrorResponseJSON(333,auth,args)  
                              
        except:
                self.logger.error("isMaintenanceTime error>>",True)  
                res_handler=ResponseHandler()
                resjson=res_handler.GetErrorResponseJSON(312,auth)   

        return {"status":response,"resjson":resjson} 
