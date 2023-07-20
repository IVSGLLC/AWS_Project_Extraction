import os
import sys
from EPDE_Client import AppClient
import boto3 
import traceback
def getWIPTableName(client_id):
        #tablePerStoreRoDetail=int(os.environ['TABLE_PER_STORE_RO_DETAIL']) 
        TableName=client_id+"_WIP_FILE"
        return TableName
def getPartsTableName(client_id):
        #tablePerStoreRoDetail=int(os.environ['TABLE_PER_STORE_PARTS']) 
        #if tablePerStoreRoDetail == 1:
        TableName=client_id+"_PARTS_FILE"
        #else:
         #       TableName="COMMON_PARTS_FILE"
        return TableName
def getInvoiceTableName(client_id):
        """ tablePerStoreRoDetail=int(os.environ['TABLE_PER_STORE_INVOICE']) 
        if tablePerStoreRoDetail == 1:
                TableName=client_id+"_INVOICE_FILE"
        else:
                TableName="COMMON_INVOICE_FILE" """
        TableName=client_id+"_INVOICE_FILE"
        return TableName

def getPostPaymentTableName(client_id):
        """ tablePerStorePostPayment=int(os.environ['TABLE_PER_STORE_POSTPAYMENT'])
        if tablePerStorePostPayment == 1:
                TableName=client_id+"_POST_PAYMENT"
        else:
                TableName="COMMON_POST_PAYMENT" """
        TableName=client_id+"_POST_PAYMENT"
        return TableName  
def getPostDepositTableName(client_id):
        """  tablePerStorePostPayment=int(os.environ['TABLE_PER_STORE_POSTDEPOSIT'])
        if tablePerStorePostPayment == 1:
                TableName=client_id+"_POST_DEPOSIT"
        else:
                TableName="COMMON_POST_DEPOSIT" """
        TableName=client_id+"_POST_DEPOSIT"
        return TableName  
def getSalesCustTableName(client_id):
        """ tablePerStoreSalescustDetail=int(os.environ['TABLE_PER_STORE_SALESCUST']) 
        if tablePerStoreSalescustDetail == 1:
                TableName=client_id+"_SALESCUST_FILE"
        else:
                TableName="COMMON_SALESCUST_FILE" """
        TableName=client_id+"_SALESCUST_FILE"
        return TableName
def getBatchCloseROTableName(client_id):
        """  tablePerStorePostPayment=int(os.environ['TABLE_PER_STORE_POSTDEPOSIT'])
        if tablePerStorePostPayment == 1:
                TableName=client_id+"_POST_DEPOSIT"
        else:
                TableName="COMMON_POST_DEPOSIT" """
        TableName=client_id+"_BATCH_CLOSERO"
        return TableName
def lambda_handler(event, context):
    
    try:
        app_client= AppClient()
        region=os.environ['REGION']
        grp_resp=app_client.GetStoreGroups(region)
        if grp_resp['status']:
           groups=grp_resp['items']  
           print("groups:"+str(groups))            
           for group in groups:
                stores_resp=app_client.GetAllStores(group['group_id'],region) 
                    
                if stores_resp['status']== True:  
                   stores=stores_resp['items']   
                   for store in stores:
                        store_code=store['store_code']    
                        group_id=group['group_id']  
                        if group_id=='TKN' :
                                tableName=getWIPTableName(store_code)
                                #DeleteTable(tableName) 
                                CreateTable(tableName,"document_type","document_id") 

                                tableName=getInvoiceTableName(store_code)
                                #DeleteTable(tableName) 
                                CreateTable(tableName,"document_type","document_id")

                                tableName=getPartsTableName(store_code)
                                #DeleteTable(tableName) 
                                CreateTable(tableName,"document_type","document_id") 

                                tableName=getPostDepositTableName(store_code)
                                #DeleteTable(tableName) 
                                CreateTablesSingleK(tableName,"file_id") 

                                tableName=getPostPaymentTableName(store_code)
                                #DeleteTable(tableName) 
                                CreateTablesSingleK(tableName,"file_id") 

                                tableName= store_code+"_ROHISTORY_FILE"
                                #DeleteTable(tableName) 
                                CreateTable(tableName,"document_type","document_id") 

                                tableName= getSalesCustTableName(store_code)
                                #DeleteTable(tableName) 
                                CreateTable(tableName,"buyer_number","deal_number") 

                                tableName=getBatchCloseROTableName(store_code)
                                #DeleteTable(tableName) 
                                CreateTablesSingleK(tableName,"file_id") 
                                ""
                        elif group_id=='COL' :
                                ""
                                #tableName= store_code+"_ROHISTORY_FILE"
                                 #DeleteTable(tableName) 
                                #CreateTable(tableName,"document_type","document_id") 

                                #tableName= store_code+"_DEAL_FILE"
                                 #DeleteTable(tableName) 
                                #CreateTable(tableName,"document_type","document_id") 

                               # tableName= store_code+"_INVENTORY_FILE"
                                 #DeleteTable(tableName) 
                                #CreateTable(tableName,"document_type","document_id") 

            
                else:
                    print("error while fetching stores...")
                    
        else:
            print("error while fetching group...")
            
        
        print("Table Creation Process completed....")
        return {"message":"operation completed...."}
    except Exception as e:
            traceback.print_exc()
            print("error occured in lambda_handler",True)
            return {"message":"operation not completed...."}
           

def DeleteTable(table_name):
    client = boto3.client('dynamodb')
    response = client.delete_table(
    TableName=table_name
    )
    print("Table deleted:"+table_name)
    return response 

def CreateTable(table_name,column_name,sort_key):
        try:
            print("Inside CreateTable table_name:"+table_name)   
           
            dynamodb = boto3.client('dynamodb')
            existing_tables = dynamodb.list_tables()['TableNames']
            if table_name not in existing_tables:
                dynamodb = boto3.resource('dynamodb')
                tableObject = dynamodb.create_table(
                TableName=table_name,
                    KeySchema=[
                    
                        {
                            'AttributeName': column_name,
                            'KeyType': 'HASH'
                        },
                          {
                            'AttributeName': sort_key,
                            'KeyType': 'RANGE'
                        }
                    ],
                    AttributeDefinitions=[
                 
                        {
                            'AttributeName': column_name,
                            'AttributeType': 'S'
                        },
                        {
                            'AttributeName': sort_key,
                            'AttributeType': 'S'
                        }

                    ],
                    BillingMode='PAY_PER_REQUEST',
                )
               # tableObject.wait_until_exists()  
                return { "operation_status":"SUCCESS", "table_status":True}                
            else:
                print("Table ["+table_name+"] already exist")
                dynamodb = boto3.resource('dynamodb')
                tableObject = dynamodb.Table(table_name)
                return { "operation_status":"SUCCESS", "table_status":tableObject.table_status}  
                  
        except Exception as e:
            # Return failed record's sequence number
            print("Error Occure in CreateTable ["+table_name+"] in DB..." , exc_info=True)
            ex_type, ex_value, ex_traceback = sys.exc_info()
            return { "operation_status":"FAILED","error_code":-1 ,"error_message":ex_value} 
def CreateTablesSingleK(table_name,column_name):
        try:
            print("Inside CreateTable table_name:"+table_name)   
           
            dynamodb = boto3.client('dynamodb')
            existing_tables = dynamodb.list_tables()['TableNames']
            if table_name not in existing_tables:
                dynamodb = boto3.resource('dynamodb')
                tableObject = dynamodb.create_table(
                TableName=table_name,
                    KeySchema=[
                    
                        {
                            'AttributeName': column_name,
                            'KeyType': 'HASH'
                        } 
                    ],
                    AttributeDefinitions=[
                 
                        {
                            'AttributeName': column_name,
                            'AttributeType': 'S'
                        } 

                    ],
                    BillingMode='PAY_PER_REQUEST',
                )
               # tableObject.wait_until_exists()  
                return { "operation_status":"SUCCESS", "table_status":True}                
            else:
                print("Table ["+table_name+"] already exist")
                dynamodb = boto3.resource('dynamodb')
                tableObject = dynamodb.Table(table_name)
                return { "operation_status":"SUCCESS", "table_status":tableObject.table_status}  
                  
        except Exception as e:
            # Return failed record's sequence number
            print("Error Occure in CreateTable ["+table_name+"] in DB..." , exc_info=True)
            ex_type, ex_value, ex_traceback = sys.exc_info()
            return { "operation_status":"FAILED","error_code":-1 ,"error_message":ex_value} 
def truncateTable(tableName,dynamo):
    
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
    print("Total Row Deleted "+str(counter)+" In Table:"+tableName)
            
