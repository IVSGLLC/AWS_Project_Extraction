 
 
items=[{'id':123,'vehicle_vin':"rtytyuu"},{'id':123,'vehicle_vin':None},{'id':123}]
for item in items:
 vin=item.get('vehicle_vin')
 print('vin='+str(vin))
    