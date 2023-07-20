import os
import boto3
from EPDE_Error import ErrorHandler
from EPDE_Logging import LogManger
class S3Manager(object):
    region="us-east-1"
    logger=LogManger()   
    err_handler=ErrorHandler()     
    def __init__(self,region="us-east-1"):        
        S3Manager.region=region        
        
    def uploadFile(self,bucketName,fileName,content):
        _moduleNM="S3Manager"
        _functionNM="uploadFile"
        try:
            s3 = boto3.resource('s3',region_name=S3Manager.region)
            b_content = bytes(content, 'utf-8')
            s3.Object(bucketName, fileName).put(Body=b_content)            
            return {"status":True}
        except Exception as e:
            self.logger.error("Error Occure in S3Manager uploadFile......." , True)
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 
    
    def downloadFile(self,bucketName,fileName):
        _moduleNM="S3Manager"
        _functionNM="downloadFile"
        try:
            s3 = boto3.resource('s3',region_name=S3Manager.region)
            obj = s3.Object(bucketName, fileName)
            body = obj.get()['Body'].read()
            self.logger.debug("body"+str(body))           
            return {"status":True,"data":body}
        except Exception as e:
            self.logger.error("Error Occure in S3Manager downloadFile......." , True)
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 
    def deleteFile(self,bucketName,fileName):
        _moduleNM="S3Manager"
        _functionNM="deleteFile"
        try:
            s3 = boto3.resource('s3',region_name=S3Manager.region)
            obj = s3.Object(bucketName, fileName)
            obj.delete()
            self.logger.debug("fileName deleted"+fileName)           
            return {"status":True}
        except Exception as e:
            self.logger.error("Error Occure in S3Manager deleteFile......." , True)
            return self.err_handler.HandleGeneralError(moduleNM=_moduleNM,functionNM=_functionNM) 

    def read_s3_file(self,bucketName, fileName):
            html_string=None
            try:
                local_filename = self.download_s3_file(bucketName, fileName)
                html_string=""
                with open(local_filename, 'r') as f:
                 html_string= f.read()
                try:
                    os.unlink(local_filename)
                except FileNotFoundError:
                    pass
            except Exception:
                html_string=None
            return html_string

    def download_s3_file(self,bucketName: str, fileName: str) -> str:
        """Downloads a file from s3 to `/tmp/[File Key]`.

        Args:
            bucket (str): Name of the bucket where the file lives.
            file_key (str): The file key of the file in the bucket.

        Returns:
            The local file name as a string.
        """
        head,tail = os.path.split(fileName)
        s3 = boto3.client('s3')
        local_filename = f'/tmp/{tail}'
        try:
            os.unlink(local_filename)
        except FileNotFoundError:
            pass
        s3.download_file(Bucket=bucketName, Key=fileName, Filename=local_filename)
        self.logger.debug('Downloaded HTML file to %s' % local_filename)

        return local_filename
    def delete_all_objects_from_s3_folder(self,bucket_name,folderPrefix):
        """
        This function deletes all files in a folder from S3 bucket
        :return: None
        """
        try:
            s3_client = boto3.client("s3")

            # First we list all files in folder
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folderPrefix)

            files_in_folder = response["Contents"]
            files_to_delete = []
            # We will create Key array to pass to delete_objects function
            for f in files_in_folder:
                files_to_delete.append({"Key": f["Key"]})

            # This will delete all files in a folder
            response = s3_client.delete_objects(
                Bucket=bucket_name, Delete={"Objects": files_to_delete}
            )
            self.logger.info('delete_all_objects_from_s3_folder %s' % folderPrefix) 
        except :
            self.logger.error('error in delete_all_objects_from_s3_folder ',True) 