from EPDE_Logging import LogManger
import boto3 
logger=LogManger()            
def lambda_handler(event, context):
    try:
        WORKSPACE_IDS=None
        WORKSPACE_OPERATION="RESTART"
        if 'WORKSPACE_OPERATION' in event and event['WORKSPACE_OPERATION'] is not None and len(event['WORKSPACE_OPERATION'])>1:
            WORKSPACE_OPERATION=event['WORKSPACE_OPERATION']

        if 'WORKSPACE_IDS' in event and event['WORKSPACE_IDS'] is not None and len(event['WORKSPACE_IDS'])>1:
            WORKSPACE_IDS=event['WORKSPACE_IDS']
            wksp_arr=WORKSPACE_IDS.split(',')
            client = boto3.client('workspaces')   
            logger.info(str(WORKSPACE_OPERATION)+" WORKSPACE IDS  :"+str(WORKSPACE_IDS))  
            rebootReqList=[]
            for wspId in wksp_arr:
                req= {
                    'WorkspaceId': wspId
                }
                rebootReqList.append(req)
            if WORKSPACE_OPERATION=='RESTART':
                response = client.reboot_workspaces(
                            RebootWorkspaceRequests=rebootReqList
                        ) 
                if 'FailedRequests ' in response:
                    logger.error("Operation status -RESTART WorkSpace with below failed requests",True)
                    for FailedRequests in response['FailedRequests']:
                        logger.error("WorkSpace Failed Requests  :"+str(FailedRequests),True)
                else:
                    logger.error("Operation status - RESTART WorkSpace Successfully",True) 
            elif WORKSPACE_OPERATION=='STOP':
                response = client.stop_workspaces(
                            RebootWorkspaceRequests=rebootReqList
                        ) 
                if 'FailedRequests ' in response:
                    logger.error("Operation status - STOP WorkSpace with below failed requests",True)
                    for FailedRequests in response['FailedRequests']:
                        logger.error("WorkSpace Failed Requests  :"+str(FailedRequests),True)
                else:
                    logger.error("Operation status - STOP WorkSpace Successfully",True) 
            elif WORKSPACE_OPERATION=='START':
                response = client.start_workspaces(
                            RebootWorkspaceRequests=rebootReqList
                        ) 
                if 'FailedRequests ' in response:
                    logger.error("Operation status -START WorkSpace with below failed requests",True)
                    for FailedRequests in response['FailedRequests']:
                        logger.error("WorkSpace Failed Requests  :"+str(FailedRequests),True)
                else:
                    logger.error("Operation status - START WorkSpace Successfully",True) 
            else:
                 logger.error("Operation status - No Matching Operation Provided...",True) 

        else:
            logger.error("Operation status - No Workspace ID configured...",True) 
    except Exception as e:
            logger.error("error occured in Reboot WorkSpace",True)
 