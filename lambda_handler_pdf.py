from datetime import datetime
import json
import random
import string
import boto3
import os
import subprocess
from typing import Optional
from EPDE_Logging import LogManger
import boto3
from botocore.exceptions import ClientError
from  EPDE_SQS import  SQSManager
from EPDE_Client import AppClient
# Set up logging
logger=LogManger()
 
# Get the s3 client
s3 = boto3.client('s3')

def download_s3_file(bucket: str, file_key: str) -> str:
    """Downloads a file from s3 to `/tmp/[File Key]`.

    Args:
        bucket (str): Name of the bucket where the file lives.
        file_key (str): The file key of the file in the bucket.

    Returns:
        The local file name as a string.
    """
    local_filename = f'/tmp/{file_key}'
    s3.download_file(Bucket=bucket, Key=file_key, Filename=local_filename)
    logger.info('Downloaded HTML file to %s' % local_filename)

    return local_filename
def ReadS3File(bucketName, fileName):
        local_filename = download_s3_file(bucketName, fileName)
        html_string=""
        with open(local_filename, 'r') as f:
           html_string= f.read()
        try:
            os.unlink(local_filename)
        except FileNotFoundError:
            pass
        return html_string
def GetFileId():
        # initializing size of string 
        N = 10        
        # using random.choices()
        # generating random strings 
        res = ''.join(random.choices(string.ascii_uppercase +
                                    string.digits, k = N))
        return res
def upload_file_to_s3(bucket: str, filename: str) -> Optional[str]:
    """Uploads the generated PDF to s3.

    Args:
        bucket (str): Name of the s3 bucket to upload the PDF to.
        filename (str): Location of the file to upload to s3.

    Returns:
        The file key of the file in s3 if the upload was successful.
        If the upload failed, then `None` will be returned.
    """
    file_key = None
    try:
        file_key = filename.replace('/tmp/', '')
        s3.upload_file(Filename=filename, Bucket=bucket, Key=file_key)
        logger.info('Successfully uploaded the PDF to %s as %s'
                    % (bucket, file_key))
    except ClientError as e:
        logger.error('Failed to upload file to s3.')
        logger.error(e)
        file_key = None

    return file_key
def truncateTable(tableName):
    try:
        dynamo = boto3.resource('dynamodb', region_name='us-east-1')
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
     

def lambda_handler(event, context):
    try:
        """ acct_sqs= SQSManager(QueueName= "TEST_EPDE_PostAccounting_Request.fifo",region="us-east-1",MaxMsgCount=10,waitTimeInSecond=20)   
         
        stores=['GCP0001','GCP0002','GCP0003','GCP0004','GCP0005','GCP0006','GCP0007']
        if len(stores)>0: 
                for store_code in stores: 
                   
                    fileId=GetFileId()
                    acct_msg={
                        "dealerCode":store_code,
                        "api":"postaccounting",
                        "fileId":fileId,
                        "accountingReferenceNumber":"ACCT12345",
                        "totalAccountingAmount":"1000",
                        "accountingDetails":[
                           { "referenceNumber":"1111",
                            "amount":"500",
                            "department":"DEPRT-1"
                           },
                           { "referenceNumber":"2222",
                            "amount":"500",
                            "department":"DEPRT-2"
                           }
                        ]
                    }                     
                    sqs_resp=acct_sqs.send_message(msg_body=acct_msg,MessageGroupId=store_code,MessageDeduplicationId=fileId)
 """                                
        """ items=[]
        TableName='53567u5li0b1vhmf25ia3caj8m_POST_DEPOSIT'
        truncateTable(TableName)
        TableName='591cqr23oai664onjsemodccmd_POST_DEPOSIT'
        truncateTable(TableName)
        TableName='4q05to17sjdlhq7tunftnsvthn_POST_DEPOSIT'
        truncateTable(TableName)
        TableName='1a5kliv0kfccu16njhg3cr62l6_POST_DEPOSIT'
        truncateTable(TableName)
        TableName='fb02ip8q28tf0o5hb8i9rigs5_POST_DEPOSIT'
        truncateTable(TableName)
        TableName='117uiob0tdbdkpu63chjqh83k9_POST_DEPOSIT'
        truncateTable(TableName)
        TableName='5jrg3mob17mh840j0tssfhm23d_POST_DEPOSIT'
        truncateTable(TableName)
        TableName='4nn4p8gc0npeu6k3k07mr40n57_POST_DEPOSIT'
        truncateTable(TableName)
        TableName='480aidb6b15hgi6q1i9537vsrd_POST_DEPOSIT'
        truncateTable(TableName)
        TableName='5fq8o4o32f0t8d7460upaaa8f2_POST_DEPOSIT'
        truncateTable(TableName)
        TableName='lo5n865e685kfhn225n43fii6_POST_DEPOSIT'
        truncateTable(TableName)
        TableName='3co9fke2cjvqjfb4t9ag9lldfb_POST_DEPOSIT'
        truncateTable(TableName)
        TableName='7a7v230mnk3m87vrf50efdjhbd_POST_DEPOSIT'
        truncateTable(TableName)
        TableName='1p49iuotg1rlg0bb9062gdr61t_POST_DEPOSIT'
        truncateTable(TableName)
        TableName='5f182ccm3rljpk3djdj7fgljp2_POST_DEPOSIT'
        truncateTable(TableName)
        TableName='1it7h04i0iis2h9n9jc3pmsvj2_POST_DEPOSIT'
        truncateTable(TableName)
        TableName='53567u5li0b1vhmf25ia3caj8m_POST_PAYMENT'
        truncateTable(TableName)
        TableName='591cqr23oai664onjsemodccmd_POST_PAYMENT'
        truncateTable(TableName)
        TableName='4q05to17sjdlhq7tunftnsvthn_POST_PAYMENT'
        truncateTable(TableName)
        TableName='1a5kliv0kfccu16njhg3cr62l6_POST_PAYMENT'
        truncateTable(TableName)
        TableName='fb02ip8q28tf0o5hb8i9rigs5_POST_PAYMENT'
        truncateTable(TableName)
        TableName='117uiob0tdbdkpu63chjqh83k9_POST_PAYMENT'
        truncateTable(TableName)
        TableName='5jrg3mob17mh840j0tssfhm23d_POST_PAYMENT'
        truncateTable(TableName)
        TableName='4nn4p8gc0npeu6k3k07mr40n57_POST_PAYMENT'
        truncateTable(TableName)
        TableName='480aidb6b15hgi6q1i9537vsrd_POST_PAYMENT'
        truncateTable(TableName)
        TableName='5fq8o4o32f0t8d7460upaaa8f2_POST_PAYMENT'
        truncateTable(TableName)
        TableName='lo5n865e685kfhn225n43fii6_POST_PAYMENT'
        truncateTable(TableName)
        TableName='3co9fke2cjvqjfb4t9ag9lldfb_POST_PAYMENT'
        truncateTable(TableName)
        TableName='7a7v230mnk3m87vrf50efdjhbd_POST_PAYMENT'
        truncateTable(TableName)
        TableName='1p49iuotg1rlg0bb9062gdr61t_POST_PAYMENT'
        truncateTable(TableName)
        TableName='5f182ccm3rljpk3djdj7fgljp2_POST_PAYMENT'
        truncateTable(TableName)
        TableName='1it7h04i0iis2h9n9jc3pmsvj2_POST_PAYMENT'
        truncateTable(TableName) """
        """  TableName='53567u5li0b1vhmf25ia3caj8m_INVOICE_FILE'
        truncateTable(TableName)
        TableName='591cqr23oai664onjsemodccmd_INVOICE_FILE'
        truncateTable(TableName)
        TableName='4q05to17sjdlhq7tunftnsvthn_INVOICE_FILE'
        truncateTable(TableName)
        TableName='1a5kliv0kfccu16njhg3cr62l6_INVOICE_FILE'
        truncateTable(TableName)
        TableName='fb02ip8q28tf0o5hb8i9rigs5_INVOICE_FILE'
        truncateTable(TableName)
        TableName='117uiob0tdbdkpu63chjqh83k9_INVOICE_FILE'
        truncateTable(TableName)
        TableName='5jrg3mob17mh840j0tssfhm23d_INVOICE_FILE'
        truncateTable(TableName)
        TableName='4nn4p8gc0npeu6k3k07mr40n57_INVOICE_FILE'
        truncateTable(TableName)
        TableName='480aidb6b15hgi6q1i9537vsrd_INVOICE_FILE'
        truncateTable(TableName)
        TableName='5fq8o4o32f0t8d7460upaaa8f2_INVOICE_FILE'
        truncateTable(TableName)
        TableName='lo5n865e685kfhn225n43fii6_INVOICE_FILE'
        truncateTable(TableName)
        TableName='3co9fke2cjvqjfb4t9ag9lldfb_INVOICE_FILE'
        truncateTable(TableName)
        TableName='7a7v230mnk3m87vrf50efdjhbd_INVOICE_FILE'
        truncateTable(TableName)
        TableName='1p49iuotg1rlg0bb9062gdr61t_INVOICE_FILE'
        truncateTable(TableName)
        TableName='5f182ccm3rljpk3djdj7fgljp2_INVOICE_FILE'
        truncateTable(TableName)
        TableName='1it7h04i0iis2h9n9jc3pmsvj2_INVOICE_FILE'
        truncateTable(TableName)
        TableName='53567u5li0b1vhmf25ia3caj8m_WIP_FILE'
        truncateTable(TableName)
        TableName='591cqr23oai664onjsemodccmd_WIP_FILE'
        truncateTable(TableName)
        TableName='4q05to17sjdlhq7tunftnsvthn_WIP_FILE'
        truncateTable(TableName)
        TableName='1a5kliv0kfccu16njhg3cr62l6_WIP_FILE'
        truncateTable(TableName)
        TableName='fb02ip8q28tf0o5hb8i9rigs5_WIP_FILE'
        truncateTable(TableName)
        TableName='117uiob0tdbdkpu63chjqh83k9_WIP_FILE'
        truncateTable(TableName)
        TableName='5jrg3mob17mh840j0tssfhm23d_WIP_FILE'
        truncateTable(TableName)
        TableName='4nn4p8gc0npeu6k3k07mr40n57_WIP_FILE'
        truncateTable(TableName)
        TableName='480aidb6b15hgi6q1i9537vsrd_WIP_FILE'
        truncateTable(TableName)
        TableName='5fq8o4o32f0t8d7460upaaa8f2_WIP_FILE'
        truncateTable(TableName)
        TableName='lo5n865e685kfhn225n43fii6_WIP_FILE'
        truncateTable(TableName)
        TableName='3co9fke2cjvqjfb4t9ag9lldfb_WIP_FILE'
        truncateTable(TableName)
        TableName='7a7v230mnk3m87vrf50efdjhbd_WIP_FILE'
        truncateTable(TableName)
        TableName='1p49iuotg1rlg0bb9062gdr61t_WIP_FILE'
        truncateTable(TableName)
        TableName='5f182ccm3rljpk3djdj7fgljp2_WIP_FILE'
        truncateTable(TableName)
        TableName='1it7h04i0iis2h9n9jc3pmsvj2_WIP_FILE'
        truncateTable(TableName)
        TableName='53567u5li0b1vhmf25ia3caj8m_PARTS_FILE'
        truncateTable(TableName)
        TableName='591cqr23oai664onjsemodccmd_PARTS_FILE'
        truncateTable(TableName)
        TableName='4q05to17sjdlhq7tunftnsvthn_PARTS_FILE'
        truncateTable(TableName)
        TableName='1a5kliv0kfccu16njhg3cr62l6_PARTS_FILE'
        truncateTable(TableName)
        TableName='fb02ip8q28tf0o5hb8i9rigs5_PARTS_FILE'
        truncateTable(TableName)
        TableName='117uiob0tdbdkpu63chjqh83k9_PARTS_FILE'
        truncateTable(TableName)
        TableName='5jrg3mob17mh840j0tssfhm23d_PARTS_FILE'
        truncateTable(TableName)
        TableName='4nn4p8gc0npeu6k3k07mr40n57_PARTS_FILE'
        truncateTable(TableName)
        TableName='480aidb6b15hgi6q1i9537vsrd_PARTS_FILE'
        truncateTable(TableName)
        TableName='5fq8o4o32f0t8d7460upaaa8f2_PARTS_FILE'
        truncateTable(TableName)
        TableName='lo5n865e685kfhn225n43fii6_PARTS_FILE'
        truncateTable(TableName)
        TableName='3co9fke2cjvqjfb4t9ag9lldfb_PARTS_FILE'
        truncateTable(TableName)
        TableName='7a7v230mnk3m87vrf50efdjhbd_PARTS_FILE'
        truncateTable(TableName)
        TableName='1p49iuotg1rlg0bb9062gdr61t_PARTS_FILE'
        truncateTable(TableName)
        TableName='5f182ccm3rljpk3djdj7fgljp2_PARTS_FILE'
        truncateTable(TableName)
        TableName='1it7h04i0iis2h9n9jc3pmsvj2_PARTS_FILE'
        truncateTable(TableName)
        TableName='53567u5li0b1vhmf25ia3caj8m_INVOICE_FILE'
        truncateTable(TableName)
        TableName='591cqr23oai664onjsemodccmd_INVOICE_FILE'
        truncateTable(TableName)
        TableName='4q05to17sjdlhq7tunftnsvthn_INVOICE_FILE'
        truncateTable(TableName)
        TableName='1a5kliv0kfccu16njhg3cr62l6_INVOICE_FILE'
        truncateTable(TableName)
        TableName='fb02ip8q28tf0o5hb8i9rigs5_INVOICE_FILE'
        truncateTable(TableName)
        TableName='117uiob0tdbdkpu63chjqh83k9_INVOICE_FILE'
        truncateTable(TableName)
        TableName='5jrg3mob17mh840j0tssfhm23d_INVOICE_FILE'
        truncateTable(TableName)
        TableName='4nn4p8gc0npeu6k3k07mr40n57_INVOICE_FILE'
        truncateTable(TableName)
        TableName='480aidb6b15hgi6q1i9537vsrd_INVOICE_FILE'
        truncateTable(TableName)
        TableName='5fq8o4o32f0t8d7460upaaa8f2_INVOICE_FILE'
        truncateTable(TableName)
        TableName='lo5n865e685kfhn225n43fii6_INVOICE_FILE'
        truncateTable(TableName)
        TableName='3co9fke2cjvqjfb4t9ag9lldfb_INVOICE_FILE'
        truncateTable(TableName)
        TableName='7a7v230mnk3m87vrf50efdjhbd_INVOICE_FILE'
        truncateTable(TableName)
        TableName='1p49iuotg1rlg0bb9062gdr61t_INVOICE_FILE'
        truncateTable(TableName)
        TableName='5f182ccm3rljpk3djdj7fgljp2_INVOICE_FILE'
        truncateTable(TableName)
        TableName='1it7h04i0iis2h9n9jc3pmsvj2_INVOICE_FILE'
        truncateTable(TableName) """
    except Exception as e:
            print(e)
    return {
        'status': 200,
        'file_key': '',
    }
 