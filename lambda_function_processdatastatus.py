
from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_Response import ResponseHandler
from EPDE_Client import AppClient
import boto3
logger=LogManger()
err_handler=ErrorHandler()

def lambda_handler(event, context):
    res_json=""
    resHandler=ResponseHandler()
    store_code=""
    try:
            _moduleNM="EPDE_Lambda"
            _functionNM="lambda_function_processdatastatus"
            err_handler.appInfo(moduleNM=_moduleNM,functionNM=_functionNM)
            app_client= AppClient()
            config=app_client.GetConfiguration(event=event)
            region='us-east-1'
            if 'REGION' in config:
                region=config['REGION']               
            pathParameters= event.get("pathParameters")       
            partitionKey=  pathParameters["partitionKey"]
            queryStringParameters= event.get("queryStringParameters") 
            store_code=None 
            if 'queryStringParameters' in event and 'storeCode' in event.get("queryStringParameters"):
                store_code=queryStringParameters["storeCode"] 
            
            logger.debug("processdatastatus partitionKey="+str(partitionKey)+",store_code="+str(store_code))
            if (partitionKey is None ) or partitionKey == "" or len(partitionKey.strip())==0:
                 list=[ 'partitionKey']
                 res_json= resHandler.GetFormattedErrorResponseJSON(code=384,auth_json=None,args=list) 
            elif (store_code is None ) or store_code == "" or len(store_code.strip())==0:
                 list=[ 'storeCode']
                 res_json= resHandler.GetFormattedErrorResponseJSON(code=384,auth_json=None,args=list)
            else:
                dynamodb = boto3.resource('dynamodb', region_name=region)
                TableName=store_code+"_EXTRACT_LOG"
                table = dynamodb.Table(TableName)
                response = table.get_item(Key={'file_id': partitionKey},ConsistentRead=False)
                logger.debug("processdatastatus response="+str(response))
                if response is not None and 'Item' in response:
                    item = response['Item']
                    res_json= { "responseCode":0,"processDetail": item }                         
                else:
                    res_json= resHandler.GetErrorResponseJSON(385,None)
            
    except Exception as e:             
            logger.error("error occured in lambda_function_processdatastatus",True)
            res_json= resHandler.GetErrorResponseJSON(313,None)
   
    return resHandler.GetAPIResponse(res_json)
 