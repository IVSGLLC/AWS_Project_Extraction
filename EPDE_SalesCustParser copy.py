from ast import If
import json
import numpy as np
import pandas as pad
import io
import logging 
import os

loglevel = int(os.environ['LOG_LEVEL'] )
logger = logging.getLogger('SalesCustParser')
logger.setLevel(loglevel)
 
class SalesCustParser():

    def parse(self,fileDataAsStr):
        logger.debug("inside parse_DMS_SalesCust_File...")

        txtfil = io.StringIO(fileDataAsStr)   
        
        row = txtfil.readline() 
        idx = 0
     
        salescust_detail : bool =False
        dict_detail={'DEAL-NUMBER':[],
        'BUYER-NUMBER':[],
        'BUYER-NAME':[],
        'DATE-SOLD':[],        
        'VIN':[],
        'YEAR':[],
        'MAKE':[],
        'MODEL':[],
        'STATUS':[],
        'CASH-DOWN':[],
        'SOLD-AGE':[] ,
        'BUYER-EMAIL':[] ,
        'BUYER-EMAIL-DESC':[]        
        }
        #DEAL-NUMBER........ BUYER-NUMBER....... BUYER-NAME............... DATE-SOLD.......... SERIAL-NO........ MAKE...... MODEL..... YEAR S. CASH-DOWN.......... SOLD-AGE
        #DEAL-NUMBER........ BUYER-NUMBER....... BUYER-NAME............... DATE-SOLD.......... SERIAL-NO........ MAKE...... MODEL..... YEAR S. CASH-DOWN.......... DAYS
        salescust_detail_line=str('DEAL-NUMBER........ BUYER-NUMBER....... BUYER-NAME............... DATE-SOLD').split("^")
        
        #ITEMS LISTED.
        line_end_text=str('ITEMS LISTED.^ITEM LISTED.').split("^")
        #ITEMS SELECTED^DEAL-NUMBER........ BUYER-NUMBER.......
        
        skip_line_contains_list=str("ITEMS SELECTED^DEAL-NUMBER........ BUYER-NUMBER.......^' NOT ON FILE").split("^")
        skip_line_contains_list.append('adp (')
        skip_line_contains_list.append('                               DESC')
        #>SORT^PAGE^:GET-LIST FI.WIP^:SSELECT FI-WIP WITH STATUS "F"^>SAVE-LIST FI.WIP^login^password
        skip_line_startwith_list=str(':^>').split("^")
        skip_line_startwith_list.append('login:')
        skip_line_startwith_list.append('password:')
        skip_line_startwith_list.append('>SORT')
        skip_line_startwith_list.append('>LIST')
        skip_line_startwith_list.append(':SSELECT')
        skip_line_startwith_list.append(':SELECT')
        skip_line_startwith_list.append('PAGE')
        skip_line_startwith_list.append('GET-LIST FI.WIP')
        skip_line_startwith_list.append(':SSELECT FI-WIP WITH STATUS "F"')
        skip_line_startwith_list.append(':SELECT FI-WIP WITH STATUS "F"')
        skip_line_startwith_list.append('>SAVE-LIST FI.WI')
        
        account_line=[]     
        while row:
            row = txtfil.readline()
            if row.__contains__('adp (') and row.strip().endswith(')'):
                account_line.append(row.strip())      
                
            if row.startswith(tuple(salescust_detail_line)):
                salescust_detail=True   
            if list(filter(row.__contains__, line_end_text)) != []:
                if(salescust_detail==True): salescust_detail=False  

            if salescust_detail==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                if len(row[0:19].strip())==0 :
                    if len(row[20:39].strip())>0:
                        dict_detail["BUYER-NUMBER"][(len(dict_detail["BUYER-NUMBER"])-1)] = (dict_detail["BUYER-NUMBER"][(len(dict_detail["BUYER-NUMBER"])-1)] + str(row[20:39].strip())) 
                    if len(row[40:65].strip())>0:
                        dict_detail["BUYER-NAME"][(len(dict_detail["BUYER-NAME"])-1)] = (dict_detail["BUYER-NAME"][(len(dict_detail["BUYER-NAME"])-1)] + str(row[40:65].rstrip()))
                    if len(row[104:114].strip())>0:
                        dict_detail["MAKE"][(len(dict_detail["MAKE"])-1)] = (dict_detail["MAKE"][(len(dict_detail["MAKE"])-1)] + str(row[104:114].rstrip()))
                    if len(row[115:125].strip())>0:
                        dict_detail["MODEL"][(len(dict_detail["MODEL"])-1)] = (dict_detail["MODEL"][(len(dict_detail["MODEL"])-1)] + str(row[115:125].rstrip()))
                    if len(row[154:194].strip())>0:
                        dict_detail["BUYER-EMAIL"][(len(dict_detail["BUYER-EMAIL"])-1)] = (dict_detail["BUYER-EMAIL"][(len(dict_detail["BUYER-EMAIL"])-1)] + str(row[154:194].rstrip()))
                    if len(row[195:205].strip())>0:
                        dict_detail["BUYER-EMAIL-DESC"][(len(dict_detail["BUYER-EMAIL-DESC"])-1)] = (dict_detail["BUYER-EMAIL-DESC"][(len(dict_detail["BUYER-EMAIL-DESC"])-1)] + str(row[195:205].rstrip()))
                else:
                    dict_detail["DEAL-NUMBER"].append(str(row[0:19].strip()))
                    dict_detail["BUYER-NUMBER"].append(str(row[20:39].strip()))
                    dict_detail["BUYER-NAME"].append(str(row[40:65]))
                    dict_detail["DATE-SOLD"].append(str(row[66:85].strip()))                   
                    dict_detail["VIN"].append(str(row[86:103].strip()))
                    dict_detail["MAKE"].append(str(row[104:114]))
                    dict_detail["MODEL"].append(str(row[115:125]))
                    dict_detail["YEAR"].append(str(row[126:130].strip()))
                    dict_detail["STATUS"].append(str(row[131:133].strip()))
                    dict_detail["CASH-DOWN"].append(str(row[134:153].strip()))
                    dict_detail["BUYER-EMAIL"].append(str(row[154:194]))
                    dict_detail["BUYER-EMAIL-DESC"].append(str(row[195:205]))
                    dict_detail["SOLD-AGE"].append(str(row[206:].strip()))
                   

        if loglevel==logging.DEBUG:
            logger.debug("Parse Completed...dict_detail :"+str(dict_detail))
             
         #covert dict to dataframe
        df_detail = pad.DataFrame(dict_detail)        
         
         
     
        df_final=pad.DataFrame([])
        #Merge dataframe together  
        df_final = df_detail.replace('\r\n',' ', regex=True)    
        df_final = df_final.replace('\n',' ', regex=True)
        df_final = df_final.replace('\r',' ', regex=True)
        df_final = df_final.replace(np.nan,'', regex=True) 
        #if loglevel==logging.DEBUG:
            #logger.debug(str(df_final))
        logger.debug("Parsing done succesfully...")
        return {'df_final':df_final,'account_line':account_line}
    def parse_TK_SALESCUST(self,fileDataAsStr):
        logger.debug("TEKION SalesCust Parsing done succesfully...") 
        JSON_obj = json.loads(fileDataAsStr)         
        return JSON_obj    