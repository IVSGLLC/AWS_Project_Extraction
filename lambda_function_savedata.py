import logging
import os
import boto3
import concurrent.futures 
from InventoryDBHelper import InventoryDBHelper
from DBHelper import DBHelper
from ROHistoryDBHelper import ROHistoryDBHelper
loglevel =int(os.environ['LOG_LEVEL'])
logger = logging.getLogger('lambda_function_savedata')
logger.setLevel(loglevel)
def lambda_handler(event, context):
    #1 Read the input parameters
    TableName = event['tableName']
    overwrite_by_pkeys= event['overwrite_by_pkeys']
    items   = event['items']
    #dbHelper=DBHelper()
    #input=dbHelper.decompressBytesToString(items)
    #items = json.loads(input) 
    #logger.debug("Items after decompress="+str(items))
    MAX_PROC=4
    if 'MAX_PROC' in event:
        MAX_PROC=int(event['MAX_PROC'])
    BATCH_SIZE=25
    if 'BATCH_SIZE' in event:
        BATCH_SIZE=int(event['BATCH_SIZE'])
    region='us-east-1'
    if 'region' in event:
        region=event['region']
    checkForUpdateExisting='False'
    if 'checkForUpdateExisting' in event:
        checkForUpdateExisting=event['checkForUpdateExisting']

    logger.debug("region="+str(region)+",MAX_PROC="+str(MAX_PROC)+",BATCH_SIZE="+str(BATCH_SIZE)+",TableName="+str(TableName)+",checkForUpdateExisting="+str(checkForUpdateExisting))
    logger.debug("Items to be inserted in DB count="+str(len(items)))
    logger.debug("Items overwrite_by_pkeys="+str(overwrite_by_pkeys))   

    dynamodb = boto3.resource('dynamodb', region_name=region)     
    results=[]
    futures=[]
    res=None
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_PROC) as executor:
        batches = chunks(items,BATCH_SIZE)  
        count=1              
        for batch in batches:
            #logger.debug("submit batch-"+str(count)+" size="+str(len(batch)))  
            count=count+1
            futures.append(executor.submit(load_batch,dynamodb, TableName,overwrite_by_pkeys, batch,checkForUpdateExisting))
        for future in concurrent.futures.as_completed(futures):
            #res=future.result()
            #logger.debug("batch future result="+str(res))
            results.append(future.result())
            
    #4 Format and return the result
    return {
         'Success' :   True
        
    }
     
def chunks(lst, n):       
        for i in range(0, len(lst), n):
            yield lst[i:i + n]       
def load_batch(dynamodb,table_name,pkeys, batch_list,checkForUpdateExisting):
        logger.debug("load_batch -checkForUpdateExisting="+str(checkForUpdateExisting)+" batch_list size="+str(len(batch_list)))  
        if str(checkForUpdateExisting).lower()=='true':
           updated_batch_list=get_updated_list(dynamodb,table_name,batch_list)
        else:
            updated_batch_list=batch_list
        try:
            table = dynamodb.Table(table_name)
            with table.batch_writer(overwrite_by_pkeys=pkeys) as batch:
                for item in updated_batch_list:
                    batch.put_item(
                        Item=item
                    )
            #logger.debug("after batchwrite ")  
        except Exception:
            logger.exception("load_batch Couldn't load data into table %s.", table_name)
            raise
 
  
def get_updated_list(dynamodb,table_name,batch_list):
    #logger.debug("get_updated_list   batch_list size="+str(len(batch_list))+",table_name="+table_name)  
    #logger.debug("get_updated_list   batch_list json="+str((batch_list)))  
    if  str(table_name).endswith("_INVENTORY_FILE")==True:
        invHelper= InventoryDBHelper()
        return invHelper.handleInventoryTableDataUpdate(dynamodb=dynamodb,batch_list=batch_list,table_name=table_name)
    elif  str(table_name).endswith("_ROHISTORY_FILE")==True:
        rohistHelper= ROHistoryDBHelper()
        return rohistHelper.handleROHistoryTableDataUpdate(dynamodb=dynamodb,batch_list=batch_list,table_name=table_name)
    else:
        return batch_list  
