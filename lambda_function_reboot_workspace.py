from EPDE_Logging import LogManger
import boto3 
logger=LogManger()            
def lambda_handler(event, context):
    try:
        
        if 'WORKSPACE_IDS' in event and event['WORKSPACE_IDS'] is not None and len(event['WORKSPACE_IDS'])>1:
            WORKSPACE_IDS=event['WORKSPACE_IDS']
            wksp_arr=WORKSPACE_IDS.split(',')
            client = boto3.client('workspaces')   
            logger.info("Reboot WorkSpace Ids  :"+str(WORKSPACE_IDS))  
            rebootReqList=[]
            for wspId in wksp_arr:
                req= {
                    'WorkspaceId': wspId
                }
                rebootReqList.append(req)

            response = client.reboot_workspaces(
                        RebootWorkspaceRequests=rebootReqList
                    ) 
            if 'FailedRequests ' in response:
                logger.error("Reboot WorkSpace Completed with below failed requests",True)
                for FailedRequests in response['FailedRequests']:
                    logger.error("Reboot WorkSpace Failed Requests  :"+str(FailedRequests),True)
            else:
                logger.error("Reboot WorkSpace Completed Successfully",True) 
        else:
            logger.error("Reboot WorkSpace Completed - No Workspace ID configured...",True) 
    except Exception as e:
            logger.error("error occured in Reboot WorkSpace",True)
 