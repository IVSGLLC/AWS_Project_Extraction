 
 
items=[{'id':123,'vehicle_vin':"rtytyuu"},{'id':123,'vehicle_vin':None},{'id':123}]
for item in items:
 vin=item.get('vehicle_vin')
 print('vin='+str(vin))

dealers=['GCP0031','GCP0032','GCP0033','GCP0034','GCP0035','GCP0036','GCP0037','GCP0038','GCP0039']
for dealerCode in dealers:
    TableName=dealerCode+'_WIP_FILE'
    truncateTable(TableName)
    TableName=dealerCode+'_PARTS_FILE'
    truncateTable(TableName)
    TableName=dealerCode+'_SALESCUST_FILE'
    truncateTable(TableName)  
    