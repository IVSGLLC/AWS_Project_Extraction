import calendar
from decimal import Decimal
import gzip
 
from logging import error
from operator import itemgetter
from sqlite3 import Date
import time
from tokenize import Double
from turtle import pd
from xmlrpc.client import DateTime
from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.conditions import Key 
import json
import numpy as np
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta
from boto3.dynamodb.types import TypeDeserializer
import pandas as pad
import pytz
class fakefloat(float):
    def __init__(self, value):
        self._value = value
    def __repr__(self):
        return str(self._value)
from datetime import datetime, timedelta

def get_time_until_next_target(target_hour, target_minute):
    current_time = datetime.now()
    eastern=pytz.timezone('US/Eastern')
    utc=pytz.utc
    
    current_time_eastern=eastern.localize(current_time,is_dst=None)
    target_time_eastern = current_time_eastern.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
    # If the target time has already passed for today, calculate for tomorrow
    if current_time_eastern > target_time_eastern:
        target_time_eastern = target_time_eastern + timedelta(days=1)
   
    target_time_utc=target_time_eastern.astimezone(utc)
    current_time_utc=current_time_eastern.astimezone(utc)
    time_until_next_target_utc = target_time_utc - current_time_utc
    ttl_seconds = int((time_until_next_target_utc).total_seconds())
    return ttl_seconds
def get_time_to_live():
        target_hour=3
        target_minute=55        
        current_time = datetime.now()
        print(current_time)
        eastern=pytz.timezone('US/Eastern')
        utc=pytz.utc        
        #current_time_eastern=eastern.localize(current_time,is_dst=None)
        current_time_eastern = current_time.astimezone(eastern)
        print(current_time_eastern)
        target_time_eastern = current_time_eastern.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
        print(target_time_eastern)
        # If the target time has already passed for today, calculate for tomorrow
        
        if current_time_eastern > target_time_eastern:
            target_time_eastern = target_time_eastern + timedelta(days=1)
    
        target_time_utc=target_time_eastern.astimezone(utc)
        print(target_time_utc)
        epoch_time = int(target_time_utc.timestamp())
        #current_time_utc=current_time_eastern.astimezone(utc)
        #time_until_next_target_utc = target_time_utc - current_time_utc
        #ttl_seconds = int((target_time_utc).total_seconds())
        return epoch_time
# Example usage
target_hour = 4
target_minute = 0
remaining_time = get_time_until_next_target(target_hour, target_minute)


def convert_to_ttl_utc_seconds(target_datetime):
    date = datetime.now()
    utc=pytz.utc
    eastern=pytz.timezone('US/Eastern')
    
    date_eastern=eastern.localize(date,is_dst=None)
    date_utc=date_eastern.astimezone(utc)
    create_date=str(date_utc.timestamp())
     
    current_time = datetime.utcnow()
    ttl_seconds = int((target_datetime - current_time).total_seconds())
    return ttl_seconds

# Example usage
#target_datetime = datetime(2023, 7, 31, 10, 30, 0)  # Replace with your desired date and time in UTC
#ttl_utc_seconds = convert_to_ttl_utc_seconds(target_datetime)
print(get_time_to_live())

print(f"Remaining time until {target_hour:02d}:{target_minute:02d}: {remaining_time}")

""" def defaultencode(o):
    if isinstance(o, Decimal):
        # Subclass float with custom repr?
        return fakefloat(o)
    raise TypeError(repr(o) + " is not JSON serializable")
def GetLender(acct,gl_accts):
        print("acct="+str(acct)+",gl_accts="+str(gl_accts))
        desc=None
        if acct is not None and gl_accts is not None and len(gl_accts)>0:
            acct_list = [gl_acct for gl_acct in gl_accts if (gl_acct["acct_no"] == acct)]
            if acct_list is not None and  len(acct_list)>0:
                desc= acct_list[0]["acct_desc"]                           
        return desc
 """# Set up logging
#for i = 6 to 1510 Step 60	
""" def appendInt(num):
    if num > 9:
        secondToLastDigit = str(num)[-2]
        if secondToLastDigit == '1':
            return 'th'
    lastDigit = num % 10
    if (lastDigit == 1):
        return 'st'
    elif (lastDigit == 2):
        return 'nd'
    elif (lastDigit == 3):
        return 'rd'
    else:
        return 'th'
def FormatDateNew(open_dt_str,print_time_seconds_str):
        try:
                    
            if open_dt_str is not None and  len(str(open_dt_str).strip())>0:
                open_dt_str=str(open_dt_str).strip()               
                open_dt_date = datetime.strptime(open_dt_str, "%d%b%y")                
                if print_time_seconds_str is not None and  len(str(print_time_seconds_str).strip())>0:
                    print_time_seconds_str=str(print_time_seconds_str) 
                    f_dt = datetime.strptime(open_dt_str+' '+print_time_seconds_str, "%d%b%y %H:%M")
                    f_dt=f_dt.strftime("%m/%d/%Y %I:%M %p")
                    return f_dt
                else:
                        f_dt=open_dt_date
                        f_dt=f_dt.strftime("%m/%d/%Y")
                        return f_dt               
            else:
                return ""
        except Exception as e:
            return ""
def FormatDate(open_dt_str,print_time_seconds_str):
        try:
            open_dt_str=str(open_dt_str)
           # if loglevel==logging.DEBUG:
              #  logger.debug("open_dt_str:"+open_dt_str)
         
            print_time_seconds_str=str(print_time_seconds_str)
           # if loglevel==logging.DEBUG:
               # logger.debug("print_time_seconds_str:"+print_time_seconds_str)
            if len(open_dt_str)>0:
                open_dt_date = datetime.strptime(open_dt_str, "%d%b%y")
                print_time_seconds_str=print_time_seconds_str.strip()
                if len(print_time_seconds_str)>0:
                    pt_arr=print_time_seconds_str.split(" ")
                    if len(pt_arr)>=2:
                        print_time_seconds_str=pt_arr[1]
                        p_added_seconds = timedelta(seconds=int(print_time_seconds_str))
                        f_dt=open_dt_date+p_added_seconds
                        f_dt=f_dt.strftime("%Y-%m-%d %I:%M:%S %p").lower()
                        return f_dt
                    else:
                        f_dt=open_dt_date
                        f_dt=f_dt.strftime("%Y-%m-%d").lower()
                        return f_dt

                else:
                    f_dt=open_dt_date
                    f_dt=f_dt.strftime("%Y-%m-%d").lower()
                    return f_dt
            else:
                return ""
        except Exception as e:
            #logger.error("Error Occure in FormatDate ..." , exc_info=True)
            return ""
 """""" FromDate = datetime.now()			 
for i in range(6,1510,60):
    ToDate=FromDate 				
    print("r_date_to="+str(ToDate) ) 
    datetime.now().__add__()				
    #FromDate=DateAdd("d",-i,datetime.now())
    #r_date_from=formateDate(FromDate) 				  """
""" open_dt_str="15FEB23"
 
print_time_seconds_str="20135 20135 30240 20135"
 
#print("r_date_from="+str(FormatDate(open_dt_str,print_time_seconds_str))) 
stkType='USED2'
documentList=[{'documentId':"a1","amount":23},{'documentId':"1","amount":23},{'documentId':"A1","amount":23},{'documentId':"11","amount":23},{'documentId':"2","amount":23}]
documentList=[]
documentList=sorted(documentList, key=itemgetter('documentId'))    
#print("list="+str((documentList)) )

epoch_time = int(time.time()) 
now = datetime.now()

current_time = now.strftime("%y-%m-%d %H:%M:%S")
print("Current Time =", current_time) """
#print(calendar.timegm(t.timetuple()))
 
#t=datetime.datetime(2021, 7, 7, 1, 2, 1)
#print(calendar.timegm(now.timetuple()))
#close file
 
""" lienHolderName=None
gl_accts=[{'acct_no': '121000', 'acct_type': 'L', 'acct_desc': 'N/P - NEW VEH & DEMOS'}, {'acct_no': '121010', 'acct_type': 'L', 'acct_desc': 'N/P - USED VEHICLES'}]
lienHolderName=GetLender('121010',gl_accts) 
print(lienHolderName)
lienHolderAmount='-39,753.42'
lienHolderAmount_decimal = Decimal(lienHolderAmount.replace(',','')) 
print(lienHolderAmount_decimal)
if lienHolderAmount_decimal<0:
   lienHolderAmount_decimal= lienHolderAmount_decimal*-1
print("1;;;;"+str(lienHolderAmount_decimal)) """
""" text_file = open("D:\ssahu\CodeCommit\SOW\CCDI_CDS\GCP0001_ROHIST_NEWFILE_20220830_222.txt", "r")
file_name ="D:\ssahu\CodeCommit\SOW\CCDI_CDS\GCP0001_ROHIST_NEWFILE_20220830_222.json"
#read whole file to a string
fileDataAsStr = text_file.read()
#close file
text_file.close()
ct = datetime.now()
create_date = ct.strftime("%m-%d-%Y %I:%M:%S %p")
create_date_only = ct.strftime("%m-%d-%Y")
df_final=pad.DataFrame([]) 
try:
    parser= ROHistoryParser()
    df_final=parser.parse(fileDataAsStr)
    df=df_final["ro-hist"]
    df_lines=df_final["hist-opcodes"]
    RO_DATA_1= df_lines.groupby(["RO"])
    HIST_GROUPED_BY_VIN = df.groupby(["SERIAL"])
    outputJSONArray=[]
    for VIN, VIN_DATA in HIST_GROUPED_BY_VIN:
        vin_records=VIN_DATA.to_dict('records')
        vechicle={
            "delDate": vin_records[0]["DELIVERY.DATE"],
            "color":vin_records[0]["COLOR"],
            "year":vin_records[0]["YEAR"],
            "dlrCd": vin_records[0]["DEALER-CD"],
            "vin": VIN,
            "make": "",
            "model":vin_records[0]["MODEL"],
            "engNo": "",
            "stockNo": vin_records[0]['STOCK-NO'],
        }
        VIN_GROUPED_BY_CUST = VIN_DATA.groupby(["CUST-NBR"])
        for CUST, CUST_DATA in VIN_GROUPED_BY_CUST:
            cust_records=CUST_DATA.to_dict('records') 
            fullName=cust_records[0]['CUST-NAME']             
            customer= {
                "id": CUST,
                "firstName":fullName,
                "lastName": fullName,
                "addresses": [
                    {
                        "zip": cust_records[0]['ZIP'],
                        "state":cust_records[0]['STATE'],
                        "addressLine": [
                           cust_records[0]['ADDRESS']
                        ],
                        "city": cust_records[0]['CITY']
                    }
                ],
                "company": "",               
                "email": cust_records[0]['EMAIL'],
                "contacts": [
                    {
                        "value": "",
                        "desc": "Contact Number"
                    },
                    {
                        "value": cust_records[0]['B-PHONE'],
                        "desc": "Business Phone"
                    },
                    {
                        "value": cust_records[0]['CELL-PHONE'],
                        "desc": "Cell Phone"
                    },
                    {
                        "value": cust_records[0]['H-PHONE'],
                        "desc": "Home Phone"
                    }
                ]
            }           
            ro_list=[]          
            CUST_RO = CUST_DATA.groupby(["RO"])
            for RO, RO_DATA in CUST_RO:
                ro_records=RO_DATA.to_dict('records')                  
                ro_item={
                    "ro":RO,
                    "status": "",
                    "openDate": ro_records[0]["OPENED"],
                    "openTime": "",
                    "closeDate":  ro_records[0]["CLOSE-DATE"],
                    "closeTime": "",
                    "amountDue": ro_records[0]["RO-TOTAL"],
                    "warrantyDue": "",
                }
                RO_DATA1=RO_DATA_1.get_group(RO)
                RO_LINES = RO_DATA1.groupby(["LINE-CODE"])
                serviceDetails=[]
                for LINE_CODE, LINES_DATA in RO_LINES:
                    lines_records=LINES_DATA.to_dict('records') 
                    serviceLine_item={
                        "lineCode": LINE_CODE,
                        "lineDesc": "",
                        "causes": "",
                        "techStory": ""
                    }                    
                    LINE_OPERATIONS = LINES_DATA.groupby(["OP-CODES"])
                    operations=[]
                    for OP_CODE, OPCODE_DATA in LINE_OPERATIONS:
                        opcode_records=OPCODE_DATA.to_dict('records')
                        seqNo=1 
                        opcode_item={
                            "soldHrs": "",
                            "code":OP_CODE,
                            "cost": "",
                            "seqNo": seqNo,
                            "lbrSaleAmt":opcode_records[0]["LBR-SALE-AMTS"],
                            "type": "LOP-OPS",
                            "feeId": "",
                            "lbrSaleCtrlNo": "",
                            "sale": "",
                            "discId": "",
                            "saleCo": "",
                            "crCo": "",
                            "lbrCostAmt":str(opcode_records[0]['LBR-COST$']).strip(),
                            "techNo": "",
                            "lbrType": str(opcode_records[0]['LBR-TYPES']).strip(),
                            "actualHrs": str(opcode_records[0]['OP-CODES']).strip(),
                            "desc": str(opcode_records[0]['OP-DESC']).strip(),
                            "lbrSaleAcct": ''
                        }
                        operations.append(opcode_item)
                    #end for operations
                    serviceLine_item["operations"]=operations
                    serviceDetails.append(serviceLine_item)
                #end for service lines
                ro_item["serviceDetails"]=serviceDetails
                ro_list.append(ro_item)
            #end for ro-item
            outputJSON_item={
                            "document_id": VIN+"_"+CUST,
                            "document_type": "HISTORY",
                            "vin":VIN,
                            "customer":CUST,
                            "vechicle":vechicle,
                            "customer":customer,
                            "roList":ro_list,
                            "createDateTime":str(create_date)                           
                            } 
            outputJSONArray.append(outputJSON_item)
            print(str(outputJSON_item))
     
    et = datetime.now()
    t=(et-ct)
    print(t.total_seconds()) 
    #outputJSONArray.to_csv(file_name, sep='\t', encoding='utf-8')
    with open(file_name, 'w') as fout:
        json_dumps_str = json.dumps(outputJSONArray, indent=4)
        print(json_dumps_str, file=fout)
    
     
    print("Inventory Parsing done succesfully...")
except Exception as ex:
       print("Error Occure in parse_DMS_Inventory_File ...",exc_info=True) """
         
