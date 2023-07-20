
import os
import datetime
import base64
from EPDE_Logging import LogManger
from EPDE_S3 import S3Manager
logger=LogManger()

def lambda_handler(event, context):
       
    try:
            
            region=os.environ['REGION']
            folder= os.environ['RAW_FILE_S3_FOLDER_NAME']
            bucketName=os.environ['S3_BUCKET_NAME']           
            s3=S3Manager(region)
            issued = datetime.datetime.now()
            folder_suffix = issued.strftime("%Y/%m/%d") 
            for record in event['Records']:        
                #logger.info("record"+str(record))
                base64_string = record["body"]
                b64decode=base64.b64decode(base64_string)   
                raw_file_data=b64decode.decode('utf-8')
                fileName=folder+"/"+folder_suffix+"/"+ str(record['messageAttributes']['filename']['stringValue'])+".txt"
                s3.uploadFile(bucketName=bucketName,fileName=fileName,content=raw_file_data)
                     
            
    except Exception as e:
            logger.error("error occured in lambda_handler",True)
           
