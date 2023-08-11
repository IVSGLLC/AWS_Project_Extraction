from decimal import Decimal
import json
import os
from EPDE_Logging import LogManger
import boto3
from datetime import datetime
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
class CloudWatchLogger(object):
    logger=LogManger() 
    @classmethod
    def DoCloudWatchLog(self,log_group_name,message_dict):
        try:   
            isCloudWatchLog=False
            try:
                isCloudWatchLog= str(os.environ['CLOUD_WATCH_LOG'] )=='True'
            except:
                isCloudWatchLog=False
            if isCloudWatchLog==True and message_dict is not None and len(message_dict)>0:    
                current_datetime = datetime.now()
                formatted_datetime = current_datetime.strftime('%Y%m%d%H%M%S%f')
                client = boto3.client('logs')
                #
                # Get a list of existing log groups
                response = client.describe_log_groups()      
                # Check if the log group exists in the response
                isExist=False
                for log_group in response['logGroups']:            
                    if log_group['logGroupName'] == log_group_name:               
                        isExist=True
                        break
                if isExist == False: 
                    response = client.create_log_group(logGroupName=log_group_name)
                response = client.create_log_stream( logGroupName=log_group_name,logStreamName=formatted_datetime)
                current_time_utc = datetime.utcnow()
                timestamp_milliseconds = int(current_time_utc.timestamp() * 1000)
                message_json_str=json.dumps(message_dict, default=defaultencode, separators=(',', ':')) 
                response = client.put_log_events(
                                logGroupName=log_group_name,
                                logStreamName=formatted_datetime,
                                logEvents=[
                                    {
                                        'timestamp': timestamp_milliseconds,
                                        'message':   message_json_str 
                                    }
                                ]                         
                            )
        except Exception as e:
            self.logger.error("error occured in DoCloudWatchLog",True)