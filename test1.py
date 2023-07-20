import json


def compare_dict_listtype_json_objects(obj1, obj2,key, attr):
        if key in attr:
            parts_attributes= attr[key]
            parts_List1=obj1.get(key)
            parts_List2=obj2.get(key) 
            filtered_parts_list = filter_changed_objects(  list1=parts_List1,list2= parts_List2,attributes= parts_attributes)
            if filtered_parts_list is not None and len(filtered_parts_list)>0:
               return False
        return True
  
def compare_dict_dicttype_json_objects(obj1, obj2,key, attr):
    if isinstance(obj1, dict):
        if type(obj1) != type(obj2):
            return False 
        attributes= attr[key]
        obj11=obj1.get(key)
        obj22=obj2.get(key)
        if compare_json_objects(obj11, obj22, attributes) == False:
            return False
    return True

def compare_json_objects(obj1, obj2, attributes):
        for attr in attributes:
            if isinstance(attr, dict):
                if "partsList" in attr:
                    if compare_dict_listtype_json_objects(obj1, obj2,"partsList",attr) ==False:
                        return False
                if "punchTimes" in attr:
                    if compare_dict_listtype_json_objects(obj1, obj2,"punchTimes",attr) ==False:
                        return False     
                if "parts" in attr:
                    if compare_dict_listtype_json_objects(obj1, obj2,"parts",attr) ==False:
                        return False         
                if "roOperations" in attr:
                    if compare_dict_listtype_json_objects(obj1, obj2,"roOperations",attr) ==False:
                        return False  
                if "roParts" in attr:
                    if compare_dict_listtype_json_objects(obj1, obj2,"roParts",attr) ==False:
                        return False     
                if "serviceDetails" in attr:
                    if compare_dict_listtype_json_objects(obj1, obj2,"serviceDetails",attr) ==False:
                        return False                   
                if "contacts" in attr:
                    if compare_dict_listtype_json_objects(obj1, obj2,"contacts",attr) ==False:
                        return False 
                    
                if "addresses" in attr:
                    
                    if compare_dict_listtype_json_objects(obj1, obj2, "addresses",attr) ==False:
                        return False  

                if "operations" in attr:
                    if compare_dict_listtype_json_objects(obj1, obj2,"operations",attr) ==False:
                        return False  
                if "soldTo" in attr:
                    if compare_dict_dicttype_json_objects(obj1, obj2, "soldTo",attr) ==False:
                       return False       
                if "shipTo" in attr:
                    if compare_dict_dicttype_json_objects(obj1, obj2, "shipTo",attr) ==False:
                       return False       
                    
                if "vehicle" in attr:
                    if compare_dict_dicttype_json_objects(obj1, obj2, "vehicle",attr) ==False:
                        return False                          
                if "customer" in attr:
                    if compare_dict_dicttype_json_objects(obj1, obj2, "customer",attr) ==False:
                        return False 
                
                if "techStory" in attr:
                    if compare_dict_dicttype_json_objects(obj1, obj2, "techStory",attr) ==False:
                        return False                                     
                if "employee" in attr:
                    if compare_dict_dicttype_json_objects(obj1, obj2, "employee",attr) ==False:
                        return False       
            else: 
                if isinstance(obj1, dict):
                    if type(obj1) != type(obj2):
                        return False                    
                    if obj1.get(attr) != obj2.get(attr):
                        return False   
                else:
                    if isinstance(obj1, list):
                        if len(obj1) != len(obj2):
                            return False                    
                        if obj1.get(attr) != obj2.get(attr):
                            return False   
                    else:                 
                        if obj1.get(attr) != obj2.get(attr):
                            return False
        return True
    
def filter_similar_objects(list1, list2, attributes):
        similar_objects = []
        for obj1 in list1:
            for obj2 in list2:
                if compare_json_objects(obj1, obj2, attributes):
                    similar_objects.append(obj1)
        return similar_objects
def filter_changed_objects(list1, list2, attributes):
        filtered_list = []
        for obj1 in list1:
            found_similar = False
            for obj2 in list2:
                if compare_json_objects(obj1, obj2, attributes):
                    found_similar = True
                    break
            if not found_similar:
                filtered_list.append(obj1)
        return filtered_list
def GetChangedItems(region, tableName,items, attributes_to_compare,oldItems):
        if True:
            filtered_list=[]
            isFiltered=False
            if items is not None and len(items)>0:
                isFiltered=False
                try:
                    
                   
                        
                        if oldItems is not None and len(oldItems)>0:
                            filtered_list = filter_changed_objects( list1=items,list2= oldItems,attributes= attributes_to_compare)
                            isFiltered=True
                            if filtered_list is not None and len(filtered_list)>0:
                                try:
                                    result = json.dumps(items)
                                    #s3.deleteFile(bucketName=bucketName,fileName=fileName)
                                     
                                    print("Uploaded Latest JSON Data to S3 fileName:"+str(tableName))
                                except:
                                    print("Error in Upload Latest JSON Data to S3 fileName:"+str(tableName),True)
                    
                except:
                   print("Error in read  JSON Data from S3 fileName:"+str(tableName),True)                 
                
            if isFiltered==False:
                filtered_list=items
            print("GetChangedItems isFiltered:"+str(isFiltered))  
            return   filtered_list
        else:
             return   items
        

# Example usage
 
list1=[
    {
        "client_id": "6v5fbdqhv61n0pfsm38r3odp5u",
        "store_code": "GCPTEST",
        "document_id": "1093008",
        "document_type": "RO",
        "saleType": "RO",
        "status": "C98",
        "openDate": "01/04/2020",
        "closeDate": "01/11/2020",
        "openTime": "11:43 AM",
        "closeTime": "",
        "amountDue": "",
        "warrantyDue": "",
        "comments": "",
        "vehicle": {
            "vin": "JTJBM7FX3G5143769",
            "make": "LEXUS",
            "model": "GX460",
            "year": "16",
            "color": "SILVER",
            "engNo": "4.6_Liter",
            "dlrCd": "64504",
            "delDate": "01/11/2020",
            "stockNo": "LP200009",
        },
        "customer": {
            "id": "L213",
            "firstName": "VERONICA MICHELLE",
            "lastName": "HELDEN",
            "email": "DAVEANDVERONICA@GMAIL.COM",
            "addresses": [
                {
                    "addressLine": ["7312 HARTSHORNE SQUARE"],
                    "city": "ALEXANDRIA",
                    "state": "VA",
                    "zip": "22315",
                }
            ],
            "contacts": [
                {"desc": "Contact Number", "value": "7033146481"},
                {"desc": "Business Phone", "value": ""},
                {"desc": "Cell Phone", "value": "7033146481"},
                {"desc": "Home Phone", "value": "7033146481"},
            ],
            "company": "",
        },
        "employee": {
            "id": "5231",
            "firstName": "CHRISTOPHER COREY",
            "lastName": "MERRIWEATHER",
        },
        "hattagnumber": "T0009",
        "contactemail": "DAVEANDVERONICA@GMAIL.COM",
        "contactphone": "7033146481",
        "homephone": "7033146481",
        "cellphone": "7033146481",
        "busphone": "",
        "laborTotal": "",
        "partsTotal": "",
        "miscTotal": "",
        "golTotal": "",
        "salesTaxTotal": "",
        "deductibleTotal": "",
        "sublTotal": "",
        "schgTotal": "",
        "poNumber": "",
        "rate": "",
        "options": "SOLD-STK:LP200009 DLR:64504 ENG:4.6_Liter",
        "payment": "CASH",
        "promisedDate": "01/08/2020",
        "promisedTime": "11:00 PM",
        "waiter": False,
        "mileageIn": "35633",
        "mileageOut": "",
        "prodDate": "",
        "warExpDate": "",
        "license": "",
        "spc_ins": "",
        "serviceDetails": [
            {
                "lineCode": "A",
                "lineDesc": "CERTIFIED PRE-OWNED PDI",
                "causes": "",
                "lineTotalEstimate": "",
                "operations": [
                    {
                        "seqNo": "1",
                        "code": "001014",
                        "desc": "CERTIFIED PRE-OWNED PDI",
                        "techNo": "",
                        "lbrType": "IUC",
                        "actualHrs": "0.00",
                        "soldHrs": "2.00",
                        "cost": "",
                        "sale": "",
                        "lbrCostAmt": "",
                        "lbrSaleAmt": "",
                        "lbrSaleAcct": "46300",
                        "lbrSaleCtrlNo": "",
                        "saleCo": "",
                        "crCo": "240",
                        "feeId": "",
                        "discId": "",
                        "type": "LOP-OPS",
                        "parts": [
                            {
                                "seqNo": "2",
                                "partNo": "B1700",
                                "desc": "WASHER FLUID",
                                "qtyOrd": "0",
                                "qtyB": "0",
                                "cost": "3.20",
                                "sale": "4.16",
                                "list": "5.29",
                                "lbrCostAmt": "320",
                                "lbrSaleAmt": "416",
                                "lbrListAmt": "529",
                                "tSale": "0.00",
                                "total": "",
                                "saleAcct": "48100",
                                "saleCo": "240",
                                "partsNotes": "",
                            }
                        ],
                    }
                ],
                "techStory": {
                    "techComment": "108259 200 installed new sop amplifier assembly, verified sound is now working, note: car was taken apart before, placed all bolts and clip back in place, some missing clip in rear trunk covers, SOMEONE PREVIOUSLY HAD REMOVED ALL COVERS AND CLIPS, NO OTHER VISIBLE DAMAGE FOUND AT THIS TIME.",
                    "techEmpNo": "5483",
                    "techEmpName": "ALVELAR CASTRO,ALVARO",
                    "storyDate": "05/24/2023",
                    "storyTime": "02:11 PM",
                },
                "punchTimes": [
                    {
                        "techNo": "5483",
                        "type": "W",
                        "date": "05/10/2023",
                        "startTime": "09:56 PM",
                        "finishTime": "09:57 PM",
                        "duration": "0.02",
                    } 
                ],
            } 
        ],
        "roOperations": [
            {
                "seqNo": "1",
                "code": "001014",
                "desc": "CERTIFIED PRE-OWNED PDI",
                "techNo": "",
                "lbrType": "IUC",
                "actualHrs": "0.00",
                "soldHrs": "2.00",
                "cost": "",
                "sale": "",
                "lbrCostAmt": "",
                "lbrSaleAmt": "",
                "lbrSaleAcct": "46300",
                "lbrSaleCtrlNo": "",
                "saleCo": "",
                "crCo": "240",
                "feeId": "",
                "discId": "",
                "type": "LOP-OPS",
                "parts": [
                    {
                        "seqNo": "2",
                        "partNo": "B1700",
                        "desc": "WASHER FLUID",
                        "qtyOrd": "0",
                        "qtyB": "0",
                        "cost": "3.20",
                        "sale": "4.16",
                        "list": "5.29",
                        "lbrCostAmt": "320",
                        "lbrSaleAmt": "416",
                        "lbrListAmt": "529",
                        "tSale": "0.00",
                        "total": "",
                        "saleAcct": "48100",
                        "saleCo": "240",
                        "partsNotes": "",
                    }
                ],
            }
        ],
        "roParts": [
            {
                "seqNo": "2",
                "partNo": "B1700",
                "desc": "WASHER FLUID",
                "qtyOrd": "0",
                "qtyB": "0",
                "cost": "3.20",
                "sale": "4.16",
                "list": "5.29",
                "lbrCostAmt": "320",
                "lbrSaleAmt": "416",
                "lbrListAmt": "529",
                "tSale": "0.00",
                "total": "",
                "saleAcct": "48100",
                "saleCo": "240",
                "partsNotes": "",
            }
        ],
        "totalEstimate": "0",
        "create_date": "2023-06-22",
        "create_date_time": "2023-06-22 09:46:36 AM",
    }
	]

list2=[
    {
        "client_id": "6v5fbdqhv61n0pfsm38r3odp5u",
        "store_code": "GCPTEST",
        "document_id": "1093008",
        "document_type": "RO",
        "saleType": "RO",
        "status": "C98",
        "openDate": "01/04/2020",
        "closeDate": "01/11/2020",
        "openTime": "11:43 AM",
        "closeTime": "",
        "amountDue": "",
        "warrantyDue": "",
        "comments": "",
        "vehicle": {
            "vin": "JTJBM7FX3G5143769",
            "make": "LEXUS",
            "model": "GX460",
            "year": "16",
            "color": "SILVER",
            "engNo": "4.6_Liter",
            "dlrCd": "64504",
            "delDate": "01/11/2020",
            "stockNo": "LP200009",
        },
        "customer": {
            "id": "L213",
            "firstName": "VERONICA MICHELLE",
            "lastName": "HELDEN",
            "email": "DAVEANDVERONICA@GMAIL.COM",
            "addresses": [
                {
                    "addressLine": ["7312 HARTSHORNE SQUARE"],
                    "city": "ALEXANDRIA",
                    "state": "VA",
                    "zip": "22315",
                }
            ],
            "contacts": [
                {"desc": "Contact Number", "value": "7033146481"},
                {"desc": "Business Phone", "value": ""},
                {"desc": "Cell Phone", "value": "7033146481"},
                {"desc": "Home Phone", "value": "7033146481"},
            ],
            "company": "",
        },
        "employee": {
            "id": "5231",
            "firstName": "CHRISTOPHER COREY",
            "lastName": "MERRIWEATHER",
        },
        "hattagnumber": "T0009",
        "contactemail": "DAVEANDVERONICA@GMAIL.COM",
        "contactphone": "7033146481",
        "homephone": "7033146481",
        "cellphone": "7033146481",
        "busphone": "",
        "laborTotal": "",
        "partsTotal": "",
        "miscTotal": "",
        "golTotal": "",
        "salesTaxTotal": "",
        "deductibleTotal": "",
        "sublTotal": "",
        "schgTotal": "",
        "poNumber": "",
        "rate": "",
        "options": "SOLD-STK:LP200009 DLR:64504 ENG:4.6_Liter",
        "payment": "CASH",
        "promisedDate": "01/08/2020",
        "promisedTime": "11:00 PM",
        "waiter": False,
        "mileageIn": "35633",
        "mileageOut": "",
        "prodDate": "",
        "warExpDate": "",
        "license": "",
        "spc_ins": "",
        "serviceDetails": [
            {
                "lineCode": "A",
                "lineDesc": "CERTIFIED PRE-OWNED PDI",
                "causes": "",
                "lineTotalEstimate": "",
                "operations": [
                    {
                        "seqNo": "1",
                        "code": "001014",
                        "desc": "CERTIFIED PRE-OWNED PDI",
                        "techNo": "",
                        "lbrType": "IUC",
                        "actualHrs": "0.00",
                        "soldHrs": "2.00",
                        "cost": "",
                        "sale": "",
                        "lbrCostAmt": "",
                        "lbrSaleAmt": "",
                        "lbrSaleAcct": "46300",
                        "lbrSaleCtrlNo": "",
                        "saleCo": "",
                        "crCo": "240",
                        "feeId": "",
                        "discId": "",
                        "type": "LOP-OPS",
                        "parts": [
                            {
                                "seqNo": "2",
                                "partNo": "B1700",
                                "desc": "WASHER FLUID",
                                "qtyOrd": "0",
                                "qtyB": "0",
                                "cost": "3.20",
                                "sale": "4.16",
                                "list": "5.29",
                                "lbrCostAmt": "320",
                                "lbrSaleAmt": "416",
                                "lbrListAmt": "529",
                                "tSale": "0.00",
                                "total": "",
                                "saleAcct": "48100",
                                "saleCo": "240",
                                "partsNotes": "",
                            }
                        ],
                    }
                ],
                "techStory": {
                    "techComment": "108259 200 installed new sop amplifier assembly, verified sound is now working, note: car was taken apart before, placed all bolts and clip back in place, some missing clip in rear trunk covers, SOMEONE PREVIOUSLY HAD REMOVED ALL COVERS AND CLIPS, NO OTHER VISIBLE DAMAGE FOUND AT THIS TIME.",
                    "techEmpNo": "5483",
                    "techEmpName": "ALVELAR CASTRO,ALVARO",
                    "storyDate": "05/24/2023",
                    "storyTime": "02:11 PM",
                },
                "punchTimes": [
                    {
                        "techNo": "5483",
                        "type": "W",
                        "date": "05/10/2023",
                        "startTime": "09:56 PM",
                        "finishTime": "09:57 PM",
                        "duration": "0.02",
                    } 
                ],
            } 
        ],
        "roOperations": [
            {
                "seqNo": "1",
                "code": "001014",
                "desc": "CERTIFIED PRE-OWNED PDI",
                "techNo": "",
                "lbrType": "IUC",
                "actualHrs": "0.00",
                "soldHrs": "2.00",
                "cost": "",
                "sale": "",
                "lbrCostAmt": "",
                "lbrSaleAmt": "",
                "lbrSaleAcct": "46300",
                "lbrSaleCtrlNo": "",
                "saleCo": "",
                "crCo": "240",
                "feeId": "",
                "discId": "",
                "type": "LOP-OPS",
                "parts": [
                    {
                        "seqNo": "2",
                        "partNo": "B1700",
                        "desc": "WASHER FLUID",
                        "qtyOrd": "0",
                        "qtyB": "0",
                        "cost": "3.20",
                        "sale": "4.16",
                        "list": "5.29",
                        "lbrCostAmt": "320",
                        "lbrSaleAmt": "416",
                        "lbrListAmt": "529",
                        "tSale": "0.00",
                        "total": "",
                        "saleAcct": "48100",
                        "saleCo": "240",
                        "partsNotes": "",
                    }
                ],
            }
        ],
        "roParts": [
            {
                "seqNo": "2",
                "partNo": "B1700",
                "desc": "WASHER FLUID",
                "qtyOrd": "0",
                "qtyB": "0",
                "cost": "3.20",
                "sale": "4.16",
                "list": "5.29",
                "lbrCostAmt": "320",
                "lbrSaleAmt": "416",
                "lbrListAmt": "529",
                "tSale": "0.00",
                "total": "",
                "saleAcct": "48100",
                "saleCo": "240",
                "partsNotes": "",
            }
        ],
        "totalEstimate": "0",
        "create_date": "2023-06-22",
        "create_date_time": "2023-06-22 09:46:36 AM",
    },
    {
        "client_id": "6v5fbdqhv61n0pfsm38r3odp5u",
        "store_code": "GCPTEST",
        "document_id": "1093009",
        "document_type": "RO",
        "saleType": "RO",
        "status": "C98",
        "openDate": "01/04/2020",
        "closeDate": "01/11/2020",
        "openTime": "11:43 AM",
        "closeTime": "",
        "amountDue": "",
        "warrantyDue": "",
        "comments": "",
        "vehicle": {
            "vin": "JTJBM7FX3G5143769",
            "make": "LEXUS",
            "model": "GX460",
            "year": "16",
            "color": "SILVER",
            "engNo": "4.6_Liter",
            "dlrCd": "64504",
            "delDate": "01/11/2020",
            "stockNo": "LP200009",
        },
        "customer": {
            "id": "L213",
            "firstName": "VERONICA MICHELLE",
            "lastName": "HELDEN",
            "email": "DAVEANDVERONICA@GMAIL.COM",
            "addresses": [
                {
                    "addressLine": ["7312 HARTSHORNE SQUARE"],
                    "city": "ALEXANDRIA",
                    "state": "VA",
                    "zip": "22315",
                }
            ],
            "contacts": [
                {"desc": "Contact Number", "value": "7033146481"},
                {"desc": "Business Phone", "value": ""},
                {"desc": "Cell Phone", "value": "7033146481"},
                {"desc": "Home Phone", "value": "7033146481"},
            ],
            "company": "",
        },
        "employee": {
            "id": "5231",
            "firstName": "CHRISTOPHER COREY",
            "lastName": "MERRIWEATHER",
        },
        "hattagnumber": "T0009",
        "contactemail": "DAVEANDVERONICA@GMAIL.COM",
        "contactphone": "7033146481",
        "homephone": "7033146481",
        "cellphone": "7033146481",
        "busphone": "",
        "laborTotal": "",
        "partsTotal": "",
        "miscTotal": "",
        "golTotal": "",
        "salesTaxTotal": "",
        "deductibleTotal": "",
        "sublTotal": "",
        "schgTotal": "",
        "poNumber": "",
        "rate": "",
        "options": "SOLD-STK:LP200009 DLR:64504 ENG:4.6_Liter",
        "payment": "CASH",
        "promisedDate": "01/08/2020",
        "promisedTime": "11:00 PM",
        "waiter": False,
        "mileageIn": "35633",
        "mileageOut": "",
        "prodDate": "",
        "warExpDate": "",
        "license": "",
        "spc_ins": "",
        "serviceDetails": [
            {
                "lineCode": "A",
                "lineDesc": "CERTIFIED PRE-OWNED PDI",
                "causes": "",
                "lineTotalEstimate": "",
                "operations": [
                    {
                        "seqNo": "1",
                        "code": "001014",
                        "desc": "CERTIFIED PRE-OWNED PDI",
                        "techNo": "",
                        "lbrType": "IUC",
                        "actualHrs": "0.00",
                        "soldHrs": "2.00",
                        "cost": "",
                        "sale": "",
                        "lbrCostAmt": "",
                        "lbrSaleAmt": "",
                        "lbrSaleAcct": "46300",
                        "lbrSaleCtrlNo": "",
                        "saleCo": "",
                        "crCo": "240",
                        "feeId": "",
                        "discId": "",
                        "type": "LOP-OPS",
                        "parts": [
                            {
                                "seqNo": "2",
                                "partNo": "B1700",
                                "desc": "WASHER FLUID",
                                "qtyOrd": "0",
                                "qtyB": "0",
                                "cost": "3.20",
                                "sale": "4.16",
                                "list": "5.29",
                                "lbrCostAmt": "320",
                                "lbrSaleAmt": "416",
                                "lbrListAmt": "529",
                                "tSale": "0.00",
                                "total": "",
                                "saleAcct": "48100",
                                "saleCo": "240",
                                "partsNotes": "",
                            }
                        ],
                    }
                ],
                "techStory": {
                    "techComment": "108259 200 installed new sop amplifier assembly, verified sound is now working, note: car was taken apart before, placed all bolts and clip back in place, some missing clip in rear trunk covers, SOMEONE PREVIOUSLY HAD REMOVED ALL COVERS AND CLIPS, NO OTHER VISIBLE DAMAGE FOUND AT THIS TIME.",
                    "techEmpNo": "5483",
                    "techEmpName": "ALVELAR CASTRO,ALVARO",
                    "storyDate": "05/24/2023",
                    "storyTime": "02:11 PM",
                },
                "punchTimes": [
                    {
                        "techNo": "5483",
                        "type": "W",
                        "date": "05/10/2023",
                        "startTime": "09:56 PM",
                        "finishTime": "09:57 PM",
                        "duration": "0.02",
                    } 
                ],
            } 
        ],
        "roOperations": [
            {
                "seqNo": "1",
                "code": "001014",
                "desc": "CERTIFIED PRE-OWNED PDI",
                "techNo": "",
                "lbrType": "IUC",
                "actualHrs": "0.00",
                "soldHrs": "2.00",
                "cost": "",
                "sale": "",
                "lbrCostAmt": "",
                "lbrSaleAmt": "",
                "lbrSaleAcct": "46300",
                "lbrSaleCtrlNo": "",
                "saleCo": "",
                "crCo": "240",
                "feeId": "",
                "discId": "",
                "type": "LOP-OPS",
                "parts": [
                    {
                        "seqNo": "2",
                        "partNo": "B1700",
                        "desc": "WASHER FLUID",
                        "qtyOrd": "0",
                        "qtyB": "0",
                        "cost": "3.20",
                        "sale": "4.16",
                        "list": "5.29",
                        "lbrCostAmt": "320",
                        "lbrSaleAmt": "416",
                        "lbrListAmt": "529",
                        "tSale": "0.00",
                        "total": "",
                        "saleAcct": "48100",
                        "saleCo": "240",
                        "partsNotes": "",
                    }
                ],
            }
        ],
        "roParts": [
            {
                "seqNo": "2",
                "partNo": "B1700",
                "desc": "WASHER FLUID",
                "qtyOrd": "0",
                "qtyB": "0",
                "cost": "3.20",
                "sale": "4.16",
                "list": "5.29",
                "lbrCostAmt": "320",
                "lbrSaleAmt": "416",
                "lbrListAmt": "529",
                "tSale": "0.00",
                "total": "",
                "saleAcct": "48100",
                "saleCo": "240",
                "partsNotes": "",
            }
        ],
        "totalEstimate": "0",
        "create_date": "2023-06-22",
        "create_date_time": "2023-06-22 09:46:36 AM",
    }
	]

attributes_to_compare=[
    "document_id",
    "saleType",
    "status",
    "openDate",
    "closeDate",
    "openTime",
    "closeTime",
    "amountDue",
    "warrantyDue",
    "comments",
    "hattagnumber",
    "contactemail",
    "contactphone",
    "homephone",
    "cellphone",
    "busphone",
    "laborTotal",
    "partsTotal",
    "miscTotal",
    "golTotal",
    "salesTaxTotal",
    "deductibleTotal",
    "sublTotal",
    "schgTotal",
    "poNumber",
    "rate",
    "options",
    "payment",
    "promisedDate",
    "promisedTime",
    "waiter",
    "mileageIn",
    "mileageOut",
    "prodDate",
    "warExpDate",
    "license",
    "spc_ins",
    {
        "vehicle": [
            "vin",
            "make",
            "model",
            "year",
            "color",
            "engNo",
            "dlrCd",
            "delDate",
            "stockNo",
        ]
    },
    {
        "customer": [
            "id",
            "firstName",
            "lastName",
            "email",
            {"addresses": ["addressLine", "city", "state", "zip"]},
            {"contacts": ["desc", "value"]},
            "company",
        ]
    },
    {"employee": ["id", "firstName", "lastName"]},
    {
        "serviceDetails": [
            "lineCode",
            "lineDesc",
            "causes",
            "lineTotalEstimate",
            {
                "operations": [
                    "seqNo",
                    "code",
                    "desc",
                    "techNo",
                    "lbrType",
                    "actualHrs",
                    "soldHrs",
                    "cost",
                    "sale",
                    "lbrCostAmt",
                    "lbrSaleAmt",
                    "lbrSaleAcct",
                    "lbrSaleCtrlNo",
                    "saleCo",
                    "crCo",
                    "feeId",
                    "discId",
                    "type",
                    {
                        "parts": [
                            "seqNo",
                            "partNo",
                            "desc",
                            "qtyOrd",
                            "qtyB",
                            "cost",
                            "sale",
                            "list",
                            "lbrCostAmt",
                            "lbrSaleAmt",
                            "lbrListAmt",
                            "tSale",
                            "total",
                            "saleAcct",
                            "saleCo",
                            "partsNotes",
                        ]
                    },
                ]
            },
            {
                "techStory": [
                    "techComment",
                    "techEmpNo",
                    "techEmpName",
                    "storyDate",
                    "storyTime"
                ]
            },
            {
                "punchTimes": [
                    "techNo",
                    "type",
                    "date",
                    "startTime",
                    "finishTime",
                    "duration"
                ]
            },
        ]
    },
    {
        "roOperations": [
            "seqNo",
            "code",
            "desc",
            "techNo",
            "lbrType",
            "actualHrs",
            "soldHrs",
            "cost",
            "sale",
            "lbrCostAmt",
            "lbrSaleAmt",
            "lbrSaleAcct",
            "lbrSaleCtrlNo",
            "saleCo",
            "crCo",
            "feeId",
            "discId",
            "type",
            {
                "parts": [
                    "seqNo",
                    "partNo",
                    "desc",
                    "qtyOrd",
                    "qtyB",
                    "cost",
                    "sale",
                    "list",
                    "lbrCostAmt",
                    "lbrSaleAmt",
                    "lbrListAmt",
                    "tSale",
                    "total",
                    "saleAcct",
                    "saleCo",
                    "partsNotes"
                ]
            },
        ]
    },
    {
        "roParts": [
            "seqNo",
            "partNo",
            "desc",
            "qtyOrd",
            "qtyB",
            "cost",
            "sale",
            "list",
            "lbrCostAmt",
            "lbrSaleAmt",
            "lbrListAmt",
            "tSale",
            "total",
            "saleAcct",
            "saleCo",
            "partsNotes"
        ]
    },
    "totalEstimate"
]

beForFilterCount=len(list1)
print("CDK PARTS Total Record Count Before Filter=."+str(len(list1)))   
items=GetChangedItems( region="",tableName="",items=list2,attributes_to_compare=attributes_to_compare,oldItems=list1)
print("CDK PARTS Total Record Count After Filter=."+str((items)))   