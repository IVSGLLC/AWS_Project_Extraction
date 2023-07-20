import json
import numpy as np
import pandas as pad
import io
import logging 
import os

loglevel = int(os.environ['LOG_LEVEL'] )
logger = logging.getLogger('PartsInvoiceParser')
logger.setLevel(loglevel)
 
class PartsParser():

    def parse(self,fileDataAsStr):

        logger.debug("inside PartsParser parse...")
        df_final=pad.DataFrame([]) 
        txtfil = io.StringIO(fileDataAsStr)           
        row = txtfil.readline()
        idx = 0
        #REFER#.. PO#..... SHIPPED OPEN-DATE CUST.... CUSTOMER LINE1.......... CUSTOMER LINE2.......... CUSTOMER LINE3.......... CUSTOMER LINE4.......... SHIP TO LINE1........... SHIP TO LINE2........... SHIP TO LINE3...........
        #REFER# PO# SHIPPED OPEN-DATE CUST CUST-N1 CUST-N2 CUST-N3 CUST-N4 SHIP-N1 SHIP-N2 SHIP-N3
        wip_1 : bool =False
        dict_WIP_1={'REFER#':[],
                    'SHIPPED':[],
                    'OPEN-DATE':[],
                    'CUST':[],
                    'CUST-N1':[],
                    'CUST-N2':[],
                    'CUST-N3':[],
                    'CUST-N4':[],
                    'SHIP-N1':[],
                    'SHIP-N2':[],
                    'SHIP-N3':[] ,
                    'CLOSE-DATE':[],  
                    'STATUS':[]                        
                    }
       #REFER#.. SALES TYPE.... CP TAX.. TOTAL$...... CP PTS.. CP SUB.. SALES TYPE.... PO#..... PHONE....... PP
       #                        SALE $                SALE $   SALE $   
       
       # REFER# SALE-TYPE CP-TAX-SALE$ TOTAL$ CP-PART-SALE$ CP-SUBL-SALE$ HPHONE ID-SUPP           
        wip_2 : bool =False
        dict_WIP_2={'REFER#':[],
                     'SALE-TYPE':[],
                     'CP-TAX-SALE$':[],
                     'TOTAL$':[],
                     'CP-PART-SALE$':[],
                     'CP-SUBL-SALE$':[],
                     'PO#':[],
                     'HPHONE':[] ,
                     'PPFLAG':[],
                     'EMAIL':[]
                   }
                   
        #REF# Q.O. Q.S. Q.B. PART-NO DESC LIST DISCOUNT T-SALE BIN FREIGHT SVIA EMP# ID-SUPP
        # REFER#.... Q.O. Q.S. Q.B. PART NO......... DESC.............. LIST.... DISCNT SALE.... BIN..... FREIGHT. SHIP VIA EMP#...
        parts:bool =False
        dict_parts={'REFER#':[],
                     'Q.O.':[],
                     'Q.S.':[],
                     'Q.B.':[],
                     'PART-NO':[],
                     'DESC':[],
                     'LIST':[] ,
                     'DISCOUNT':[],
                     'T-SALE':[],
                     'BIN':[],
                     'FREIGHT':[],
                     'SVIA':[],
                     'EMP#':[],
                     'SALE':[]
                   }
        SKIP_LINE_CONTAINS_TEXT="SORT WIP REFER# SHIPPED OPEN-DATE CUST^ ITEMS SELECTED^GET-LIST^REFER#.. SHIPPED OPEN-DATE^ITEM SELECTED^ITEMS SELECTED^ITEM LISTED^ITEMS LISTED^REFER# SALE-TYPE^SALE $             SALE $   SALE $^CATALOGED;^PARTS REF# Q.O. Q.S. Q.B. ^Q.O. Q.S. Q.B. PART NO......... DESC...^COUNT PRIVLIB^' NOT ON FILE"
        SKIP_LINE_STARTWITH_TEXT='>LIST^PAGE^:SORT^:^login^password^>SAVE-LIST^>^>SORT^:SSELECT^REFER#.. SHIPPED OPEN-DATE^REFER#.. SALES TYPE.... CP TAX^REFER#.... Q.O. Q.S. Q.B. PART NO'
        LINE_END='ITEMS LISTED.^ITEM LISTED.'
        ENG_WIP1_START='REFER#.. SHIPPED OPEN-DATE CUST..'
        ENG_WIP2_START='REFER#.. SALES TYPE.... CP TAX'
        ENG_PARTS_START='REFER#.... Q.O. Q.S. Q.B. PART NO......... DESC'
        END_OF_FILE='ITEMS COUNTED.'
        skip_line_contains_list=str(SKIP_LINE_CONTAINS_TEXT).split("^")
        skip_line_startwith_list=str(SKIP_LINE_STARTWITH_TEXT).split("^")
        line_end_text=str(LINE_END).split("^")
        wip_1_line=str(ENG_WIP1_START).split("^")
        wip_2_line=str(ENG_WIP2_START).split("^")         
        parts_line=str(ENG_PARTS_START).split("^")
        eof=str(END_OF_FILE) 
        account_line=[]      
        while row:
            row = txtfil.readline()   
            if row.__contains__('adp (') and row.strip().endswith(')'):
                account_line.append(row.strip())  
                       
            if row.startswith(tuple(wip_1_line)):
                wip_1=True                
            if row.startswith(tuple(wip_2_line)):
                wip_2=True   
            if row.startswith(tuple(parts_line)):
                parts=True                  
                            
            if list(filter(row.__contains__, line_end_text)) != []:
                #logger.info("if line_end_text:True") 
                if(wip_1==True): 
                    wip_1=False 
                   
                if(wip_2==True):
                     wip_2=False                      
                
                if(parts==True): 
                    parts=False                      
            

            if row.strip() != "" and wip_1==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                #logger.info("wip_1 true found:"+str(row))    
                if len(row[0:8].strip())==0 :
                    
                    if len(row[36:60].strip())>0:
                        dict_WIP_1["CUST-N1"][(len(dict_WIP_1["CUST-N1"])-1)] = (dict_WIP_1["CUST-N1"][(len(dict_WIP_1["CUST-N1"])-1)] + str(row[36:60].rstrip())) 
                    if len(row[61:85].strip())>0:
                        dict_WIP_1["CUST-N2"][(len(dict_WIP_1["CUST-N2"])-1)] = (dict_WIP_1["CUST-N2"][(len(dict_WIP_1["CUST-N2"])-1)] + str(row[61:85].rstrip())) 
                    if len(row[86:110].strip())>0:
                        dict_WIP_1["CUST-N3"][(len(dict_WIP_1["CUST-N3"])-1)] = (dict_WIP_1["CUST-N3"][(len(dict_WIP_1["CUST-N3"])-1)] + str(row[86:110].rstrip())) 
                    if len(row[111:135].strip())>0:
                        dict_WIP_1["CUST-N4"][(len(dict_WIP_1["CUST-N4"])-1)] = (dict_WIP_1["CUST-N4"][(len(dict_WIP_1["CUST-N4"])-1)] + str(row[111:135].rstrip())) 
                    
                    if len(row[136:160].strip())>0:
                        dict_WIP_1["SHIP-N1"][(len(dict_WIP_1["SHIP-N1"])-1)] = (dict_WIP_1["SHIP-N1"][(len(dict_WIP_1["SHIP-N1"])-1)] + str(row[136:160].rstrip())) 
                    if len(row[161:185].strip())>0:
                        dict_WIP_1["SHIP-N2"][(len(dict_WIP_1["SHIP-N2"])-1)] = (dict_WIP_1["SHIP-N2"][(len(dict_WIP_1["SHIP-N2"])-1)] + str(row[161:185].rstrip())) 
                    if len(row[186:210].strip())>0:
                        dict_WIP_1["SHIP-N3"][(len(dict_WIP_1["SHIP-N3"])-1)] = (dict_WIP_1["SHIP-N3"][(len(dict_WIP_1["SHIP-N3"])-1)] + str(row[186:210].rstrip())) 

                else:
                        dict_WIP_1["REFER#"].append(str(row[0:8].strip()))                       
                        dict_WIP_1["SHIPPED"].append(str(row[9:16].strip()))
                        dict_WIP_1["OPEN-DATE"].append(str(row[17:26].strip()))
                        dict_WIP_1["CUST"].append(str(row[27:35].strip()))
                        dict_WIP_1["CUST-N1"].append(str(row[36:60]))
                        dict_WIP_1["CUST-N2"].append(str(row[61:85]))
                        dict_WIP_1["CUST-N3"].append(str(row[86:110]))
                        dict_WIP_1["CUST-N4"].append(str(row[111:135]))
                        dict_WIP_1["SHIP-N1"].append(str(row[136:160]))  
                        dict_WIP_1["SHIP-N2"].append(str(row[161:185])) 
                        dict_WIP_1["SHIP-N3"].append(str(row[186:210])) 
                        dict_WIP_1["CLOSE-DATE"].append(str(row[211:221].strip())) 
                        dict_WIP_1["STATUS"].append(str(row[222:].strip()))  
                        
 
            if row.strip() != "" and wip_2==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                if len(row[0:8].strip())==0 :
                    if len(row[73:84].strip())>0:
                        dict_WIP_2["PO#"][(len(dict_WIP_2["PO#"])-1)] = (dict_WIP_2["PO#"][(len(dict_WIP_2["PO#"])-1)] + str(row[73:84].strip())) 
                    if len(row[103:].strip())>0:
                        dict_WIP_2["EMAIL"][(len(dict_WIP_2["EMAIL"])-1)] = (dict_WIP_2["EMAIL"][(len(dict_WIP_2["EMAIL"])-1)] + str(row[103:].strip()))                         
                else:
                    dict_WIP_2["REFER#"].append(str(row[0:8].strip()))
                    dict_WIP_2["SALE-TYPE"].append(str(row[9:23].strip()))
                    dict_WIP_2["CP-TAX-SALE$"].append(str(row[24:32].strip()))
                    dict_WIP_2["TOTAL$"].append(str(row[33:42].strip())) 
                    dict_WIP_2["CP-PART-SALE$"].append(str(row[43:51].strip()))
                    dict_WIP_2["CP-SUBL-SALE$"].append(str(row[52:60].strip()))
                    po=str(row[61:84].strip())
                    po=po.replace(row[9:23].strip(), "",1)
                    dict_WIP_2["PO#"].append(po)
                    dict_WIP_2["HPHONE"].append(str(row[85:97].strip()))
                    dict_WIP_2["PPFLAG"].append(str(row[98:100].strip()))
                    dict_WIP_2["EMAIL"].append(str(row[103:].strip()))
                   
                      
                    
            if row.strip() != "" and parts==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                if len(row[0:10].strip())==0 :
                
                    if  len(row[26:42].strip())>0:
                        #if len(row[43:61].strip())>0:
                         #  dict_parts["DESC"][(len(dict_parts["DESC"])-1)] = (dict_parts["DESC"][(len(dict_parts["DESC"])-1)] + str(row[43:61]))
                        #else:
                         #   if len(row[87:95].strip())>0: 
                          #     dict_parts["BIN"][(len(dict_parts["BIN"])-1)] = (dict_parts["BIN"][(len(dict_parts["BIN"])-1)] + str(row[87:95]))   
                    #else:
                        dict_parts["PART-NO"][(len(dict_parts["PART-NO"])-1)] = (dict_parts["PART-NO"][(len(dict_parts["PART-NO"])-1)] + str(row[26:42]))
                    if len(row[87:95].strip())>0: 
                               dict_parts["BIN"][(len(dict_parts["BIN"])-1)] = (dict_parts["BIN"][(len(dict_parts["BIN"])-1)] + str(row[87:95]))   
                    if len(row[43:61].strip())>0:
                           dict_parts["DESC"][(len(dict_parts["DESC"])-1)] = (dict_parts["DESC"][(len(dict_parts["DESC"])-1)] +' '+ str(row[43:61]))
                    if len(row[105:113].strip())>0:
                           dict_parts["SVIA"][(len(dict_parts["SVIA"])-1)] = (dict_parts["SVIA"][(len(dict_parts["SVIA"])-1)] +' '+ str(row[105:113]))
                        
                else:                     

                    dict_parts["REFER#"].append(str(row[0:10].strip()))
                    dict_parts["Q.O."].append(str(row[11:15].strip()))
                    dict_parts["Q.S."].append(str(row[16:20].strip()))
                    dict_parts["Q.B."].append(str(row[21:25].strip()))
                    dict_parts["PART-NO"].append(str(row[26:42].strip()))
                    dict_parts["DESC"].append(str(row[43:61]))
                    dict_parts["LIST"].append(str(row[62:70].strip()))
                    dict_parts["DISCOUNT"].append(str(row[71:77].strip()))
                    dict_parts["T-SALE"].append(str(row[78:86].strip()))
                    dict_parts["BIN"].append(str(row[87:95].strip()))
                    dict_parts["FREIGHT"].append(str(row[96:104].strip()))
                    dict_parts["SVIA"].append(str(row[105:113]))
                    dict_parts["EMP#"].append(str(row[114:121].strip()))
                    dict_parts["SALE"].append(str(row[122:].strip()))
                    #dict_parts["SALE"].append(str(""))
            
            idx += 1

        #END WHILE
        if loglevel==logging.DEBUG:
            logger.debug("Parse Completed...dict_WIP_1 :"+str((dict_WIP_1)))
            logger.debug("Parse Completed...dict_WIP_2:"+str((dict_WIP_2)))
            logger.debug("Parse Completed...dict_parts:"+str((dict_parts)))
         #covert dict to dataframe
               
        dict_WIP_1 = pad.DataFrame(dict_WIP_1)        
        dict_WIP_2 = pad.DataFrame(dict_WIP_2)
        dict_parts = pad.DataFrame(dict_parts)      
        df_final=pad.DataFrame([])
        #Merge dataframe together
        if len(dict_WIP_1)>0 and len(dict_WIP_2)>0:
            df_merge_1_2 = pad.merge(dict_WIP_1,dict_WIP_2, on='REFER#', how='left')
            #df_final = df_merge_1_2.replace('\r\n',' ', regex=True)
            #df_final = df_final.replace('\n',' ', regex=True)
            #df_final = df_final.replace('\r',' ', regex=True)
            df_final = df_merge_1_2.replace(np.nan,'', regex=True) 
        dict_parts = dict_parts.replace(np.nan,'', regex=True)   
           
        logger.debug("Parts Parsing done succesfully...") 
        return {"ro":df_final,"parts":dict_parts,'account_line':account_line}           
    def parse_TK_PARTS(self,fileDataAsStr):         
        logger.debug("TEKION Parts Parsing done succesfully...") 
        JSON_obj = json.loads(fileDataAsStr)         
        return JSON_obj
    #MR INTEGRATION
    def parse_MR_PARTS(self,fileDataAsStr):         
        logger.debug("inside parse_MR_PARTS...")   
        JSON_obj = json.loads(fileDataAsStr)         
        return JSON_obj
