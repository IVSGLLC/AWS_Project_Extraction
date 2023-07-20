
from EPDE_Deposits import DepositsManager
from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_PostAccounting import AccountingManager
from EPDE_PostPayment import PostManger
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient
from EPDE_BatchCloseRO import BatchCloseRO
logger=LogManger()
err_handler=ErrorHandler()

def lambda_handler(event, context):
    res_json=""
    resHandler=ResponseHandler()
    store_code=""
    try:
            _moduleNM="EPDE_Lambda"
            _functionNM="lambda_handler_post_request_status"
            err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)
            app_client= AppClient()
            config=app_client.GetConfiguration(event=event)
            region=config['REGION']    
            auth_json=None            
            auth_detail_resp=app_client.GetAuthenticationDetail(event) 
            if auth_detail_resp['status']:
                auth_json=auth_detail_resp['auth_json']
                maintenanace_resp=app_client.isMaintenanceTime(config)
                if maintenanace_resp['status']:  
                    res_json= maintenanace_resp['resjson'] 
                    logger.debug("Maintenance Time is running API call not allowed...")
                    return resHandler.GetAPIResponse(res_json) 
                validate_resp=app_client.ValidateStoreDetail(region,event)
                if validate_resp['status']==False: 
                    logger.debug("STORE NOT FOUND...")
                    error_code=validate_resp['error_code']
                    res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)   
                    return resHandler.GetAPIResponse(res_json) 
                else:
                    if 'store_code' in validate_resp:
                        store_code=validate_resp['store_code']
                #client_id=auth_detail_resp['client_id']  
                queryStringParameters= event.get("queryStringParameters")       
                pathParameters= event.get("pathParameters")       
                file_id=  pathParameters["fileId"]
                request_type=queryStringParameters["requestType"] 
                logger.debug("store_code="+store_code+",file_id="+str(file_id)+",request_type="+request_type)                    
                
                if (file_id is None ) or file_id == "" or len(file_id.strip())==0:
                        res_json= resHandler.GetErrorResponseJSON(code=353,auth_json=auth_json) 
                        return resHandler.GetAPIResponse(res_json)     
                if (request_type is None ) or request_type == "" or len(request_type.strip())==0:
                        res_json=  resHandler.GetErrorResponseJSON(code=354,auth_json=auth_json)  
                        return resHandler.GetAPIResponse(res_json) 
                if request_type != "PAYMENTS" and request_type != "DEPOSITS" and request_type != "BATCHCLOSERO":
                    list=[ str(request_type)]
                    res_json=  resHandler.GetFormattedErrorResponseJSON(code=355,auth_json=auth_json,args=list)
                    return resHandler.GetAPIResponse(res_json) 
                entry=None
                if request_type == "PAYMENTS" :                   
                    tableperstore_post=1
                    payment_manager= PostManger(region,tableperstore_post)   
                    entry=payment_manager.GetPostPaymentEntry(store_code=store_code,fileId=file_id) 
                  
                    if entry['status'] :  
                        item= entry['item']  
                        res_json= {
                        "request_status":str(item['request_status']),
                        "request_json": str(item['request_json']),
                        "response_json":str( item['response_json']),
                        "response_time":str(item['res_time']),
                        "document_id":str(item['document_id']),
                        "ro_status":str(item['ro_status'])
                        }                       
                elif request_type == "DEPOSITS" : 
                    tableperstore_post_deposit=1
                    deposit_manager= DepositsManager(region,tableperstore_post_deposit) 
                    entry=deposit_manager.GetDepositsEntry(store_code=store_code,fileId=file_id) 
                   
                    if entry['status'] : 
                        item= entry['item']  
                        res_json= {
                        "request_status":str(item['request_status']),
                        "request_json": str(item['request_json']),
                        "response_json":str( item['response_json']),
                        "response_time":str(item['res_time']),
                        "depositReferenceNumber":str(item['deposit_ref_number'])
                        } 
                elif request_type == "BATCHCLOSERO" : 
                    tableperstore_post_batchclosero=1
                    batch_manager= BatchCloseRO(region,tableperstore_post_batchclosero) 
                    entry=batch_manager.GetBatchCloseROEntry(store_code=store_code,fileId=file_id)                    
                    if entry['status'] : 
                        item= entry['item']  
                        res_json= {
                        "request_status":str(item['request_status']),
                        "request_json": str(item['request_json']),
                        "response_json":str( item['response_json']),
                        "response_time":str(item['res_time'])                        
                        }       
                elif request_type == "ACCOUNTING" : 
                    
                    acct_manager= AccountingManager(region,1) 
                    entry=acct_manager.GetPostAccountingEntry(store_code=store_code,fileId=file_id)                    
                    if entry['status'] : 
                        item= entry['item']  
                        res_json= {
                        "request_status":str(item['request_status']),
                        "request_json": str(item['request_json']),
                        "response_json":str( item['response_json']),
                        "response_time":str(item['res_time']),
                        "accountingReferenceNumber":str(item['accounting_ref_number'])
                        }        
                if entry['status'] :
                    logger.debug("res_json="+str(res_json))    
                    return resHandler.GetAPIResponse(res_json)    
                else:
                    logger.debug("Invalid File id..")
                    error_code= entry['error_code']
                    logger.debug("error_code="+str(error_code))    
                    res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)      
            else:
                logger.debug("Auth detail not found...")
                auth_json=auth_detail_resp['auth_json']
                error_code=auth_detail_resp['error_code']
                res_json= resHandler.GetErrorResponseJSON(error_code,auth_json)                 
          
            
    except Exception as e:             
            logger.error("error occured in lambda_handler_post_request_status",True)
            res_json= resHandler.GetErrorResponseJSON(313,auth_json)
    
    return resHandler.GetAPIResponse(res_json)

 