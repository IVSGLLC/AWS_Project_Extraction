from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
from EPDE_Response import ResponseHandler
import boto3 
import datetime
from boto3.dynamodb.conditions import Attr
logger=LogManger()
err_handler=ErrorHandler()
            
def lambda_handler(event, context):
    res_json=""
    resHandler=ResponseHandler()
    try:         
        region=str(event['REGION'])
        store_code=event['store_code']        
        logger.info("Purgedata started for store_code_str:"+str(store_code)) 
        payments_days=int(event['payments_days'])
        deposits_days=int(event['deposits_days'])
        batchclosero_days=int(event['batchclosero_days'])  
        extractlog_days=int(event['extractlog_days'])   
        accounting_days=int(event['accounting_days']) 
        delete_batch(region,store_code,payments_days,deposits_days,batchclosero_days,extractlog_days,accounting_days)
        res_json={"message":"Purge Process completed for store_code="+store_code}
        logger.info("Purgedata Completed for store_code:"+str(store_code)) 
        return resHandler.GetAPIResponse(res_json) 
    except Exception as e:
            logger.error("error occured in lambda_handler purgedata",True)
            res_json= resHandler.GetErrorResponseJSON(313,None)
            return resHandler.GetAPIResponse(res_json) 
 
def delete_batch(region,store_code_str,payments_days,deposits_days,batchclosero_days,extractlog_days,accounting_days):
        dynamodb = boto3.resource('dynamodb', region_name=region)
        table_names = [table.name for table in dynamodb.tables.all()]  
        try:              
            store_list = store_code_str.split(",")
            for store_code in store_list:
                try:  
                    tableName=store_code+"_WIP_FILE" 
                    if  tableName in table_names:
                        truncateTable(tableName,dynamodb) 
                    else:
                        logger.info("WIP TABLE NOT FOUND :"+str(tableName))
                except Exception :
                    logger.error("Error found truncate WIP table",True)
                try:   
           
                    tableName=store_code+"_SALESCUST_FILE"    
                    if  tableName in table_names:
                        truncateTable(tableName,dynamodb) 
                    else:
                        logger.info("SALESCUST TABLE NOT FOUND :"+str(tableName)) 
                except Exception :
                        logger.error("Error found truncate SALESCUST table",True)
                try:    
                    
                    tableName=store_code+"_INVOICE_FILE"
                    if  tableName in table_names:
                        truncateTable(tableName,dynamodb)  
                    else:
                        logger.info("INVOICE TABLE NOT FOUND :"+str(tableName)) 
                except Exception :
                        logger.error("Error found truncate INVOICE table",True)
                try:   
                
                    tableName=store_code+"_PARTS_FILE"     
                    if  tableName in table_names:
                        truncateTable(tableName,dynamodb)  
                    else:
                        logger.info("PARTS TABLE NOT FOUND :"+str(tableName)) 
                except Exception :
                        logger.error("Error found truncate PARTS table",True)

                try:
                    
                    tableName=store_code+"_POST_PAYMENT"
                    if  tableName in table_names:
                        truncateTableFilter(tableName,dynamodb,payments_days) 
                    else:
                        logger.info("POST_PAYMENT TABLE NOT FOUND :"+str(tableName)) 
                    #truncateTable(tableName,dynamodb)  
                except Exception :
                        logger.error("Error found truncate POST_PAYMENT table",True)
                try:
                
                    tableName=store_code+"_POST_DEPOSIT"      
                    if  tableName in table_names:
                        truncateTableFilter(tableName,dynamodb,deposits_days) 
                    else:
                        logger.info("POST_DEPOSIT  TABLE NOT FOUND :"+str(tableName)) 
                    #truncateTable(tableName,dynamodb)  
                except Exception :
                        logger.error("Error found truncate POST_DEPOSIT table",True)
                try:
                    tableName=store_code+"_BATCH_CLOSERO"
                    if  tableName in table_names:
                        truncateTableFilter(tableName,dynamodb,batchclosero_days) 
                    else:
                        logger.info("BATCH_CLOSERO  TABLE NOT FOUND :"+str(tableName)) 
                    
                except Exception :
                        logger.error("Error found truncate BATCH_CLOSERO table",True)
                try:
                    tableName=store_code+"_EXTRACT_LOG"            
                    if  tableName in table_names:
                        truncateTableFilter(tableName,dynamodb,extractlog_days) 
                    else:
                        logger.info("EXTRACT_LOG  TABLE NOT FOUND :"+str(tableName)) 
                    
                except Exception :
                        logger.error("Error found truncate EXTRACT_LOG table",True)
                try:
                    tableName=store_code+"_POST_ACCOUNTING"            
                    if  tableName in table_names:
                        truncateTableFilter(tableName,dynamodb,accounting_days) 
                    else:
                        logger.info("POST_ACCOUNTING  TABLE NOT FOUND :"+str(tableName)) 
                    
                except Exception :
                        logger.error("Error found truncate POST_ACCOUNTING table",True)


        except Exception :
                logger.error("Error found delete_batch",True)
        
        
def truncateTable(tableName,dynamo):
    try:
        table = dynamo.Table(tableName)    
        #get the table keys
        tableKeyNames = [key.get("AttributeName") for key in table.key_schema]

        #Only retrieve the keys for each item in the table (minimize data transfer)
        projectionExpression = ", ".join('#' + key for key in tableKeyNames)
        expressionAttrNames = {'#'+key: key for key in tableKeyNames}
        
        counter = 0
        page = table.scan(ProjectionExpression=projectionExpression, ExpressionAttributeNames=expressionAttrNames)
        with table.batch_writer() as batch:
            while page["Count"] > 0:
                counter += page["Count"]
                # Delete items in batches
                for itemKeys in page["Items"]:
                    batch.delete_item(Key=itemKeys)
                # Fetch the next page
                if 'LastEvaluatedKey' in page:
                    page = table.scan(
                        ProjectionExpression=projectionExpression, ExpressionAttributeNames=expressionAttrNames,
                        ExclusiveStartKey=page['LastEvaluatedKey'])
                else:
                    break
        logger.info("Total Row Deleted "+str(counter)+" In Table:"+tableName)
    except Exception:
        logger.error("error occured in truncateTable In Table:"+tableName,True) 

def truncateTableFilter(tableName,dynamo,retain_days):
    try:
        table = dynamo.Table(tableName)    
        #get the table keys
        tableKeyNames = [key.get("AttributeName") for key in table.key_schema]

        #Only retrieve the keys for each item in the table (minimize data transfer)
        projectionExpression = ", ".join('#' + key for key in tableKeyNames)
        expressionAttrNames = {'#'+key: key for key in tableKeyNames}
        ct = datetime.datetime.now()
        d = ct - datetime.timedelta(days=retain_days)
        #logger.debug("retain_days="+str(retain_days))
        create_date =d.strftime("%Y-%m-%d")
        
        req_ts = d.timestamp()
        logger.debug("create_date="+str(create_date))     
        
        counter = 0
        page = table.scan(ProjectionExpression=projectionExpression, ExpressionAttributeNames=expressionAttrNames,FilterExpression= Attr('create_date').lte(str(create_date)))
        logger.debug("page="+str(page))
        with table.batch_writer() as batch:
            while page["Count"] > 0:
                counter += page["Count"]
                # Delete items in batches
                for itemKeys in page["Items"]:
                    batch.delete_item(Key=itemKeys)
                # Fetch the next page
                if 'LastEvaluatedKey' in page:
                    page = table.scan(
                        ProjectionExpression=projectionExpression, ExpressionAttributeNames=expressionAttrNames,FilterExpression= Attr('req_ts').lt(req_ts)
                        ,ExclusiveStartKey=page['LastEvaluatedKey'])
                else:
                    break
        logger.info("Total Filtered Row Deleted "+str(counter)+" In Table:"+tableName)
    except Exception:
        logger.error("error occured in truncateTableFilter In Table:"+tableName,True)   
