import numpy as np
import pandas as pad
import io
import logging 
import os

 
loglevel = int(os.environ['LOG_LEVEL'] )
#loglevel=10
logger = logging.getLogger('ROHistoryParser')
logger.setLevel(loglevel)
 
class ROHistoryParser():
    @classmethod
    def parse(self,fileDataAsStr):
        logger.debug("inside ROHistoryParser parse...")
        df_final=pad.DataFrame([]) 
        txtfil = io.StringIO(fileDataAsStr)           
        row = txtfil.readline()
        idx = 0
       
        #LIST HISTORY VEH-ID RO ROI.LOGON STOCK-NO SERIAL VEH-LAST-SVC-DATE CUST-LAST-SVC-DATE WARR-EXP-DATE COLOR STOCK.TYPE OPENED YEAR ID-SUPP                                                                                                                                                                                     SALE $          
        #VEH-ID.... RO..... ROI....... STOCK-NO.. SERIAL........... VEH-LAST-SVC-DATE CUST-LAST-SERV-DATE WARR-EXP-DATE COLOR..... STOCK-TYPE OPENED. YR
        hist_1 : bool =False
        DICT_HIST_1={'VEH-ID':[],
                    'RO':[],
                    'ROI.LOGON':[],
                    'STOCK-NO':[],
                    'SERIAL':[],
                    'VEH-LAST-SVC-DATE':[],
                    'CUST-LAST-SVC-DATE':[],
                    'WARR-EXP-DATE':[],
                    'COLOR':[],
                    'STOCK.TYPE':[],
                    'OPENED':[],
                    'YEAR':[]                             
                    }
        #>LIST HISTORY VEH-ID RO DELIVERY.DATE DEALER-CD SOLD.DATE MODEL MILEAGE EMAIL MAKE LABOR$ PARTS$ ID-SUPPID
        #VEH-ID.... RO..... DELIVERY DEALER-CD. SOLD.... MODEL......................... MILEAGE EMAIL............................................. MAKE........... LABOR$.. PARTS$..
        #           DATE                DATE                                                                                                                                
        hist_2 : bool =False
        DICT_HIST_2={'VEH-ID':[],
                    'RO':[],
                    'DELIVERY.DATE':[],
                    'DEALER-CD':[],
                    'SOLD.DATE':[],
                    'MODEL':[],
                    'MILEAGE':[]  ,
                    'EMAIL':[] ,
                    'MAKE':[]              
                    } 
        #LIST HISTORY VEH-ID RO LINE-CODE OP-CODES OP-DESC LBR-TYPES LBR-SALE-AMTS PTS-SALE-AMTS MISC-SALE-AMTS LBR-COST$ PTS-COST$ COMMENT ID-SUPP
        #VEH-ID.... RO..... LINE OP-CODES.. OPERATION DESCRIPTION.... LBR-TYPE LBR-SALE-AMTS PTS-SALE-AMTS MISC-SALE-AMTS LBR-COST$.. PTS-COST$.. COMMENT.............
        hist_opcodes : bool =False
        DICT_HIST_OPCODES={'VEH-ID':[],
                    'RO':[],
                    'LINE-CODE':[],
                    'OP-CODES':[],
                    'OP-DESC':[],
                    'LBR-TYPES':[],
                    'LBR-SALE-AMTS':[]  ,
                    'PTS-SALE-AMTS':[]   ,
                    'MISC-SALE-AMTS':[]  , 
                    'LBR-COST$':[]   ,
                    'PTS-COST$':[] ,  
                    'COMMENT':[]                   
                    } 
        #LIST HISTORY VEH-ID RO CUST-NO CUST-NAME ADDRESS CITY STATE ZIP BUS-PHONE HOME-PHONE CELL.PHONE SA-NO CLOSE-DATE RO-AMOUNT ID-SUPP
        #VEH-ID.... RO..... CUST-NO. CUSTOMER................. CUST-ADDRESS....................... CUST-CITY........... CUST-STATE CUST-ZIP.. BUS....... HOME...... CELL...... SA-NO CLOSE-DATE RO-AMOUNT...
        #                    NAME                                                                                                     PHONE      PHONE      PHONE                                   
        hist_3 : bool =False
        DICT_HIST_3={'VEH-ID':[],
                    'RO':[],
                    'CUST-NBR':[],
                    'CUST-NAME':[],
                    'ADDRESS':[],
                    'CITY':[],
                    'STATE':[] ,
                    'ZIP':[] ,
                    'H-PHONE':[],
                    'B-PHONE':[],
                    'CELL-PHONE':[],                    
                    'SA-NO':[],
                    'CLOSE-DATE':[],
                    'RO-TOTAL':[],
                    } 
        #LIST HISTORY-LINES VEHID RO  IP-SUBL-SALE$ IP-SUBL-COST$ IP-LABOR-COST$ IP-LABOR-SALE$ IP-PART-COST$ IP-PART-SALE$ ID-SUPP
        #VEHID............ RO...... IP SUB.. IP SUB.. IP LBR.. IP LBR.. IP PTS.. IP PTS..
        #                           SALE $   COST $   COST $   SALE $   COST $   SALE $
        hist_lines_1 : bool =False
        DICT_HIST_LINES_1={
                    'VEH-ID':[],
                    'RO':[],
                    'IP-SUBL-SALE$':[],
                    'IP-SUBL-COST$':[],
                    'IP-LABOR-COST$':[],
                    'IP-LABOR-SALE$':[],
                    'IP-PART-COST$':[],
                    'IP-PART-SALE$':[]                   
                    } 
        #LIST HISTORY-LINES VEHID RO COMEBACK DENIED-BY EST IP-SOLD-HRS IP-ACTUAL-HRS RESNO ID-SUPP
        #VEHID............ RO...... CBK DENIED BY ESTIMATE.. IP SOLD. IP ACT.. RESNO... WP SUB.. WP SUB..
        #                                                    HRS      HRS               SALE $   COST $                                                   HRS      HRS              

        hist_lines_2 : bool =False
        DICT_HIST_LINES_2={
                    'VEH-ID':[],
                    'RO':[],
                    'COMEBACK':[],
                    'DENIED-BY':[] ,
                    'EST':[] ,
                    'IP-SOLD-HRS':[],
                    'IP-ACTUAL-HRS':[],
                    'RESNO':[] ,
                    'WP-SUBL-SALE$':[],
                    'WP-SUBL-COST$':[]
                    } 
        #LIST HISTORY-LINES VEHID RO CP-SUBL-SALE$ CP-SUBL-COST$ CP-TOTAL-SALE$ CP-TOTAL-COST$  WP-TOTAL-COST$ WP-TOTAL-SALE$ ID-SUPP
        #VEHID............ RO...... CP SUB.. CP SUB.. CP TOT.. CP TOT.. WP TOT.. WP TOT..
        #                           SALE $   COST $   SALE $   COST $   COST $   SALE $  
        hist_lines_3 : bool =False
        DICT_HIST_LINES_3={'VEH-ID':[],
                    'RO':[],
                    'CP-SUBL-SALE$':[],           
                    'CP-SUBL-COST$':[],
                    'CP-TOTAL-SALE$':[],
                    'CP-TOTAL-COST$':[] ,
                    'WP-TOTAL-COST$':[] ,
                    'WP-TOTAL-SALE$':[]                     
                    } 
        #LIST HISTORY-LINES VEHID RO IP-TOTAL-SALE$ IP-TOTAL-COST$ CP-SOLD-HRS CP-ACTUAL-HRS CP-LABOR-COST$ CP-LABOR-SALE$ CP-PART-COST$ ID-SUPP
        #VEHID............ RO...... IP TOT.. IP TOT.. CP SOLD. CP ACT.. CP LBR.. CP LBR.. CP PTS..
        #                           SALE $   COST $   HRS      HRS      COST $   SALE $   COST $ 
        hist_lines_4 : bool =False
        DICT_HIST_LINES_4={'VEH-ID':[],
                    'RO':[],
                    'IP-TOTAL-SALE$':[],           
                    'IP-TOTAL-COST$':[],
                    'CP-SOLD-HOURS':[],
                    'CP-ACTUAL-HRS':[],
                    'CP-LABOR-COST$':[] ,
                    'CP-LABOR-SALE$':[] ,
                    'CP-PART-COST$':[]                     
                    } 
        #LIST HISTORY-LINES VEHID RO CP-PART-SALE$ WP-SOLD-HRS WP-ACTUAL-HRS WP-LABOR-COST$ WP-LABOR-SALE$ WP-PART-COST$ WP-PART-SALE$ ID-SUPP
        #VEHID............ RO...... CP PTS.. WP SOLD. WP ACT.. WP LBR.. WP LBR.. WP PTS.. WP PTS..
        #                           SALE $   HRS      HRS      COST $   SALE $   COST $   SALE $  
        hist_lines_5 : bool =False
        DICT_HIST_LINES_5={'VEH-ID':[],
                    'RO':[],
                    'CP-PART-SALE$':[],           
                    'WP-SOLD-HOURS':[],
                    'WP-ACTUAL-HRS':[],
                    'WP-LABOR-COST$':[] ,
                    'WP-LABOR-SALE$':[] ,
                    'WP-PART-COST$':[] ,
                    'WP-PART-SALE$':[]                     
                    } 
        #LIST HISTORY-LINES VEHID RO LINE-CODE CAUSE ID-SUPP
        #VEHID............ RO...... LINE CAUSE...............
        hist_lineCodes : bool =False
        DICT_HIST_LINESCODES={'VEH-ID':[],
                    'RO':[],
                    'LINE-CODE':[],
                    'CAUSE':[]                   
                    } 
        #LIST WIP-HISTORY NF-VEHID RO# PART-LOP-LN-CD PART-OP-CODE PART-NUM PART-DESC PART-QTYORD PART-COST PART-SALE ID-SUPP
        #NF-VEHID.. RO.#.... LOP-LINE-CD OP-CODE............. PARTNUMBER................ PARTDESCRIPTION..... QTYORD PARTCOST PARTSALE
        hist_parts : bool =False
        DICT_HIST_PARTS={'VEH-ID':[],
                    'RO':[],
                    'LINE-CODE':[],
                    'OP-CODES':[], 
                    'PART-NUM':[],   
                    'PART-DESC':[],   
                    'PART-QTYORD':[],   
                    'PART-COST':[],   
                    'PART-SALE':[],                 
                    }  
        skip_line_startwith_list=[]
        skip_line_startwith_list.append(':GET-LIST')
        skip_line_startwith_list.append('>LIST')
        skip_line_startwith_list.append('>SORT')
        skip_line_startwith_list.append(':')
        skip_line_startwith_list.append(':SSELECT')        
        skip_line_startwith_list.append('>')
        skip_line_startwith_list.append('TERM 300,0')

        skip_line_startwith_list.append('VEH-ID.... RO..... ROI....... STOCK-NO.. SERIAL........... VEH-LAST-SVC-DATE')
        skip_line_startwith_list.append('VEH-ID.... RO..... DELIVERY DEALER-CD. SOLD.... MODEL....')
        skip_line_startwith_list.append('VEH-ID.... RO..... LINE OP-CODES.. OPERATION DESCRIPTION....')
        skip_line_startwith_list.append('VEH-ID.... RO..... CUST-NO. CUSTOMER................. CUST-ADDRESS.')
        skip_line_startwith_list.append('VEHID............ RO...... IP SUB.. IP SUB.. IP LBR.. IP LBR..')
        skip_line_startwith_list.append('VEHID............ RO...... CBK DENIED BY ESTIMATE.. IP SOLD. IP ACT..')
        skip_line_startwith_list.append('VEHID............ RO...... CP SUB.. CP SUB.. CP TOT.. CP TOT.. WP TOT..')
        skip_line_startwith_list.append('VEHID............ RO...... IP TOT.. IP TOT.. CP SOLD. CP ACT.. CP LBR.. CP LBR..')
        skip_line_startwith_list.append('VEHID............ RO...... CP PTS.. WP SOLD. WP ACT.. WP LBR.. WP LBR.. WP PTS..')
        skip_line_startwith_list.append('VEHID............ RO...... LINE CAUSE...............')
        skip_line_startwith_list.append('NF-VEHID.. RO.#.... LOP-LINE-CD OP-CODE............. PARTNUMBER................ PARTDESCRIPTION..... QTYORD')
 
        history_1=['VEH-ID.... RO..... ROI....... STOCK-NO.. SERIAL........... VEH-LAST-SVC-DATE']
        history_2=['VEH-ID.... RO..... DELIVERY DEALER-CD. SOLD.... MODEL......']
        history_opcodes=['VEH-ID.... RO..... LINE OP-CODES.. OPERATION DESCRIPTION....']
        history_3=['VEH-ID.... RO..... CUST-NO. CUSTOMER................. CUST-ADDRESS.']
        history_lines_1=['VEHID............ RO...... IP SUB.. IP SUB.. IP LBR.. IP LBR..']
        history_lines_2=['VEHID............ RO...... CBK DENIED BY ESTIMATE.. IP SOLD. IP ACT..']
        history_lines_3=['VEHID............ RO...... CP SUB.. CP SUB.. CP TOT.. CP TOT.. WP TOT..']
        history_lines_4=['VEHID............ RO...... IP TOT.. IP TOT.. CP SOLD. CP ACT.. CP LBR.. CP LBR..']
        history_lines_5=['VEHID............ RO...... CP PTS.. WP SOLD. WP ACT.. WP LBR.. WP LBR.. WP PTS..']
        history_linecodes=['VEHID............ RO...... LINE CAUSE...............']
        history_parts=['NF-VEHID.. RO.#.... LOP-LINE-CD OP-CODE............. PARTNUMBER................ PARTDESCRIPTION']
         
        skip_line_contains_list=[]
        skip_line_contains_list.append('ITEMS SELECTED')
        skip_line_contains_list.append('ITEM SELECTED')
        skip_line_contains_list.append('ITEM LISTED')
        skip_line_contains_list.append('ITEMS LISTED')
        skip_line_contains_list.append('FRAMES USED.')
        skip_line_contains_list.append('CATALOGED;')        
        skip_line_contains_list.append("' NOT ON FILE")
        skip_line_contains_list.append("COUNT PRIVLIB")
        skip_line_contains_list.append("ITEMS COUNTED")
        skip_line_contains_list.append("ITEM COUNTED")
        skip_line_contains_list.append(' adp (') 
        skip_line_contains_list.append('[m[H[J  PAGE') 
        skip_line_contains_list.append('                   LOGON')
        skip_line_contains_list.append('DATE                DATE')
        skip_line_contains_list.append('NAME                                                                                                     PHONE      PHONE      PHONE')
        skip_line_contains_list.append('SALE $   COST $   COST $   SALE $   COST $   SALE $') 
        skip_line_contains_list.append('HRS      HRS               SALE $   COST $')
        skip_line_contains_list.append('SALE $   COST $   SALE $   COST $   COST $   SALE $')
        skip_line_contains_list.append('SALE $   COST $   HRS      HRS      COST $   SALE $   COST $')
        skip_line_contains_list.append('SALE $   HRS      HRS      COST $   SALE $   COST $   SALE $')


        line_end_text=['ITEMS LISTED.']       
        eof='ITEMS COUNTED'
        account_line=[]     
        while row:
            row = txtfil.readline()
            if row.__contains__('adp (') and row.strip().endswith(')'):
                account_line.append(row.strip())      
            if row.startswith(tuple(history_1)):
                hist_1=True            
            if row.startswith(tuple(history_2)):
                hist_2=True 

            if row.startswith(tuple(history_opcodes)):
                hist_opcodes=True    
            
            if row.startswith(tuple(history_3)):
                hist_3=True 
            
            if row.startswith(tuple(history_lines_1)):
                hist_lines_1=True 
            if row.startswith(tuple(history_lines_2)):
                hist_lines_2=True 
            if row.startswith(tuple(history_lines_3)):
                hist_lines_3=True 
            if row.startswith(tuple(history_lines_4)):
                hist_lines_4=True 
            if row.startswith(tuple(history_lines_5)):
                hist_lines_5=True  
            if row.startswith(tuple(history_linecodes)):
                hist_lineCodes=True   
            if row.startswith(tuple(history_parts)):
                hist_parts=True      

            if list(filter(row.__contains__, line_end_text)) != []:
                
                if(hist_1==True): 
                    hist_1=False                   
                if(hist_2==True):
                    hist_2=False  
                    
                if(hist_opcodes==True):
                    hist_opcodes=False 

                if(hist_3==True):
                    hist_3=False  

                if(hist_lines_1==True):
                    hist_lines_1=False 
                if(hist_lines_2==True):
                    hist_lines_2=False 
                if(hist_lines_3==True):
                    hist_lines_3=False 
                if(hist_lines_4==True):
                    hist_lines_4=False 
                if(hist_lines_5==True):
                    hist_lines_5=False 

                if(hist_lineCodes==True):
                    hist_lineCodes=False

                if(hist_parts==True):
                    hist_parts=False    

            if row.strip() != "" and hist_1==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                     
                if len(row[0:18].strip())==0 :

                    if len(row[19:29].strip())>0:
                       DICT_HIST_1["ROI.LOGON"][(len(DICT_HIST_1["ROI.LOGON"])-1)] = (DICT_HIST_1["ROI.LOGON"][(len(DICT_HIST_1["ROI.LOGON"])-1)] + str(row[19:29].strip()))
                    if len(row[30:40].strip())>0:
                       DICT_HIST_1["STOCK-NO"][(len(DICT_HIST_1["STOCK-NO"])-1)] = (DICT_HIST_1["STOCK-NO"][(len(DICT_HIST_1["STOCK-NO"])-1)] + str(row[30:40].strip()))
                    if len(row[111:121].strip())>0:
                       DICT_HIST_1["COLOR"][(len(DICT_HIST_1["COLOR"])-1)] = (DICT_HIST_1["COLOR"][(len(DICT_HIST_1["COLOR"])-1)] + str(row[111:121].rstrip()))
                    if len(row[122:132].strip())>0:
                       DICT_HIST_1["STOCK.TYPE"][(len(DICT_HIST_1["STOCK.TYPE"])-1)] = (DICT_HIST_1["STOCK.TYPE"][(len(DICT_HIST_1["STOCK.TYPE"])-1)] + str(row[122:132].rstrip()))
                          
                else:
                    DICT_HIST_1["VEH-ID"].append(str(row[0:10].strip()))
                    DICT_HIST_1["RO"].append(str(row[11:18].strip()))
                    DICT_HIST_1["ROI.LOGON"].append(str(row[19:29].strip()))
                    DICT_HIST_1["STOCK-NO"].append(str(row[30:40].strip()))
                    DICT_HIST_1["SERIAL"].append(str(row[41:58].strip()))
                    DICT_HIST_1["VEH-LAST-SVC-DATE"].append(str(row[59:76].strip()))
                    DICT_HIST_1["CUST-LAST-SVC-DATE"].append(str(row[77:96].strip()))
                    DICT_HIST_1["WARR-EXP-DATE"].append(str(row[97:110].strip()))
                    DICT_HIST_1["COLOR"].append(str(row[111:121]))
                    DICT_HIST_1["STOCK.TYPE"].append(str(row[122:132].strip()))
                    DICT_HIST_1["OPENED"].append(str(row[133:140].strip()))
                    DICT_HIST_1["YEAR"].append(str(row[141:].strip()))
        
            if row.strip() != "" and hist_2==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                   
                if len(row[0:18].strip())==0 :

                    if len(row[48:78].strip())>0:
                           DICT_HIST_2["MODEL"][(len(DICT_HIST_2["MODEL"])-1)] = (DICT_HIST_2["MODEL"][(len(DICT_HIST_2["MODEL"])-1)] + str(row[48:78].rstrip()))
                    if len(row[87:137].strip())>0:
                           DICT_HIST_2["EMAIL"][(len(DICT_HIST_2["EMAIL"])-1)] = (DICT_HIST_2["EMAIL"][(len(DICT_HIST_2["EMAIL"])-1)] + str(row[87:137].rstrip()))
                    if len(row[138:153].strip())>0:
                           DICT_HIST_2["MAKE"][(len(DICT_HIST_2["MAKE"])-1)] = (DICT_HIST_2["MAKE"][(len(DICT_HIST_2["MAKE"])-1)] + str(row[138:153].rstrip()))
                          
                else:
                    DICT_HIST_2["VEH-ID"].append(str(row[0:10].strip()))
                    DICT_HIST_2["RO"].append(str(row[11:18].strip()))
                    DICT_HIST_2["DELIVERY.DATE"].append(str(row[19:27].strip()))
                    DICT_HIST_2["DEALER-CD"].append(str(row[28:38].strip()))
                    DICT_HIST_2["SOLD.DATE"].append(str(row[39:47].strip()))
                    DICT_HIST_2["MODEL"].append(str(row[48:78]))
                    DICT_HIST_2["MILEAGE"].append(str(row[79:86].strip()))
                    DICT_HIST_2["EMAIL"].append(str(row[87:137].strip()))
                    DICT_HIST_2["MAKE"].append(str(row[138:].strip()))
                   
            
            if row.strip() != "" and hist_opcodes==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                
                           
                if len(row[0:18].strip())==0 :
                   
                    if  len(row[19:23].strip())==0:
                        if len(row[24:34].strip())>0:
                           DICT_HIST_OPCODES["OP-CODES"][(len(DICT_HIST_OPCODES["OP-CODES"])-1)] = (DICT_HIST_OPCODES["OP-CODES"][(len(DICT_HIST_OPCODES["OP-CODES"])-1)] + str(row[24:34].strip()))
                        if len(row[35:60].strip())>0:
                           DICT_HIST_OPCODES["OP-DESC"][(len(DICT_HIST_OPCODES["OP-DESC"])-1)] = (DICT_HIST_OPCODES["OP-DESC"][(len(DICT_HIST_OPCODES["OP-DESC"])-1)] + str(row[35:60].rstrip()))
                        if len(row[61:69].strip())>0:
                           DICT_HIST_OPCODES["LBR-TYPES"][(len(DICT_HIST_OPCODES["LBR-TYPES"])-1)] = (DICT_HIST_OPCODES["LBR-TYPES"][(len(DICT_HIST_OPCODES["LBR-TYPES"])-1)] + str(row[61:69].rstrip()))
                        if len(row[137:].strip())>0:
                           DICT_HIST_OPCODES["COMMENT"][(len(DICT_HIST_OPCODES["COMMENT"])-1)] = (DICT_HIST_OPCODES["COMMENT"][(len(DICT_HIST_OPCODES["COMMENT"])-1)] + str(row[137:157].rstrip()))
                        
                    else:
                            DICT_HIST_OPCODES["VEH-ID"].append(DICT_HIST_OPCODES["VEH-ID"][(len(DICT_HIST_OPCODES["VEH-ID"])-1)])
                            DICT_HIST_OPCODES["RO"].append(DICT_HIST_OPCODES["RO"][(len(DICT_HIST_OPCODES["RO"])-1)])
                            DICT_HIST_OPCODES["LINE-CODE"].append(str(row[19:23].strip()))
                            DICT_HIST_OPCODES["OP-CODES"].append(str(row[24:34].strip()))
                            DICT_HIST_OPCODES["OP-DESC"].append(str(row[35:60]))                            
                            DICT_HIST_OPCODES["LBR-TYPES"].append(str(row[61:69]))
                            DICT_HIST_OPCODES["LBR-SALE-AMTS"].append(str(row[70:83].strip()))
                            DICT_HIST_OPCODES["PTS-SALE-AMTS"].append(str(row[84:97].strip()))
                            DICT_HIST_OPCODES["MISC-SALE-AMTS"].append(str(row[98:112].strip()))                            
                            DICT_HIST_OPCODES["LBR-COST$"].append(str(row[113:124].strip()))
                            DICT_HIST_OPCODES["PTS-COST$"].append(str(row[125:136].strip()))
                            DICT_HIST_OPCODES["COMMENT"].append(str(row[137:]))  
                        
                else:    
                                 
                    DICT_HIST_OPCODES["VEH-ID"].append(str(row[0:10].strip()))
                    DICT_HIST_OPCODES["RO"].append(str(row[11:18].strip()))
                    DICT_HIST_OPCODES["LINE-CODE"].append(str(row[19:23].strip()))
                    DICT_HIST_OPCODES["OP-CODES"].append(str(row[24:34].strip()))
                    DICT_HIST_OPCODES["OP-DESC"].append(str(row[35:60]))                            
                    DICT_HIST_OPCODES["LBR-TYPES"].append(str(row[61:69]))
                    DICT_HIST_OPCODES["LBR-SALE-AMTS"].append(str(row[70:83].strip()))
                    DICT_HIST_OPCODES["PTS-SALE-AMTS"].append(str(row[84:97].strip()))
                    DICT_HIST_OPCODES["MISC-SALE-AMTS"].append(str(row[98:112].strip()))                            
                    DICT_HIST_OPCODES["LBR-COST$"].append(str(row[113:124].strip()))
                    DICT_HIST_OPCODES["PTS-COST$"].append(str(row[125:136].strip()))
                    DICT_HIST_OPCODES["COMMENT"].append(str(row[137:]))  
                  
            if row.strip() != "" and hist_3==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                
                if len(row[0:27].strip())==0 :

                    if len(row[28:53].strip())>0:
                           DICT_HIST_3["CUST-NAME"][(len(DICT_HIST_3["CUST-NAME"])-1)] = (DICT_HIST_3["CUST-NAME"][(len(DICT_HIST_3["CUST-NAME"])-1)] + str(row[28:53].rstrip()))
                    if len(row[54:89].strip())>0:
                           DICT_HIST_3["ADDRESS"][(len(DICT_HIST_3["ADDRESS"])-1)] = (DICT_HIST_3["ADDRESS"][(len(DICT_HIST_3["ADDRESS"])-1)] + str(row[54:89].rstrip()))
                    if len(row[90:110].strip())>0:
                           DICT_HIST_3["CITY"][(len(DICT_HIST_3["CITY"])-1)] = (DICT_HIST_3["CITY"][(len(DICT_HIST_3["CITY"])-1)] + str(row[90:110].rstrip()))
                         
                else:
                    
                   
                    DICT_HIST_3["VEH-ID"].append(str(row[0:10].strip()))
                    DICT_HIST_3["RO"].append(str(row[11:18].strip()))
                    DICT_HIST_3["CUST-NBR"].append(str(row[19:27].strip()))
                    DICT_HIST_3["CUST-NAME"].append(str(row[28:53]))
                    DICT_HIST_3["ADDRESS"].append(str(row[54:89]))
                    DICT_HIST_3["CITY"].append(str(row[90:110]))
                    DICT_HIST_3["STATE"].append(str(row[111:121].strip()))
                    DICT_HIST_3["ZIP"].append(str(row[122:132].strip()))
                    DICT_HIST_3["B-PHONE"].append(str(row[133:143].strip()))
                    DICT_HIST_3["H-PHONE"].append(str(row[144:154].strip()))
                    DICT_HIST_3["CELL-PHONE"].append(str(row[155:165].strip()))
                    DICT_HIST_3["SA-NO"].append(str(row[166:171].strip()))
                    DICT_HIST_3["CLOSE-DATE"].append(str(row[172:182].strip()))
                    DICT_HIST_3["RO-TOTAL"].append(str(row[183:].strip()))

            if row.strip() != "" and hist_lines_1==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                
                    DICT_HIST_LINES_1["VEH-ID"].append(str(row[0:17].strip()))
                    DICT_HIST_LINES_1["RO"].append(str(row[18:26].strip()))
                    DICT_HIST_LINES_1["IP-SUBL-SALE$"].append(str(row[27:35].strip())) 
                    DICT_HIST_LINES_1["IP-SUBL-COST$"].append(str(row[36:44].strip())) 
                    DICT_HIST_LINES_1["IP-LABOR-COST$"].append(str(row[45:53].strip()))  
                    DICT_HIST_LINES_1["IP-LABOR-SALE$"].append(str(row[54:62].strip()))  
                    DICT_HIST_LINES_1["IP-PART-COST$"].append(str(row[63:71].strip()))  
                    DICT_HIST_LINES_1["IP-PART-SALE$"].append(str(row[72:].strip()))  

            if row.strip() != "" and hist_lines_2==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                if len(row[0:26].strip())==0 :

                    if len(row[31:40].strip())>0:
                           DICT_HIST_LINES_2["DENIED-BY"][(len(DICT_HIST_LINES_2["DENIED-BY"])-1)] = (DICT_HIST_LINES_2["DENIED-BY"][(len(DICT_HIST_LINES_2["DENIED-BY"])-1)] + str(row[31:40].rstrip()))
                      
                else:
                     
                    DICT_HIST_LINES_2["VEH-ID"].append(str(row[0:17].strip()))
                    DICT_HIST_LINES_2["RO"].append(str(row[18:26].strip()))
                    DICT_HIST_LINES_2["COMEBACK"].append(str(row[27:30].strip()))
                    DICT_HIST_LINES_2["DENIED-BY"].append(str(row[31:40]))
                    DICT_HIST_LINES_2["EST"].append(str(row[41:51].strip()))
                    DICT_HIST_LINES_2["IP-SOLD-HRS"].append(str(row[52:60].strip()))
                    DICT_HIST_LINES_2["IP-ACTUAL-HRS"].append(str(row[61:69].strip()))                    
                    DICT_HIST_LINES_2["RESNO"].append(str(row[70:78].strip()))
                    DICT_HIST_LINES_2["WP-SUBL-SALE$"].append(str(row[79:87].strip())) 
                    DICT_HIST_LINES_2["WP-SUBL-COST$"].append(str(row[88:].strip())) 

            if row.strip() != "" and hist_lines_3==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                
                    DICT_HIST_LINES_3["VEH-ID"].append(str(row[0:17].strip()))
                    DICT_HIST_LINES_3["RO"].append(str(row[18:26].strip()))
                    DICT_HIST_LINES_3["CP-SUBL-SALE$"].append(str(row[27:35].strip())) 
                    DICT_HIST_LINES_3["CP-SUBL-COST$"].append(str(row[36:44].strip())) 
                    DICT_HIST_LINES_3["CP-TOTAL-SALE$"].append(str(row[45:53].strip())) 
                    DICT_HIST_LINES_3["CP-TOTAL-COST$"].append(str(row[54:62].strip())) 
                    DICT_HIST_LINES_3["WP-TOTAL-COST$"].append(str(row[63:71].strip())) 
                    DICT_HIST_LINES_3["WP-TOTAL-SALE$"].append(str(row[72:].strip())) 
                                 
           
            if row.strip() != "" and hist_lines_4==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                     
                    DICT_HIST_LINES_4["VEH-ID"].append(str(row[0:17].strip()))
                    DICT_HIST_LINES_4["RO"].append(str(row[18:26].strip()))
                    DICT_HIST_LINES_4["IP-TOTAL-SALE$"].append(str(row[27:35].strip()))  
                    DICT_HIST_LINES_4["IP-TOTAL-COST$"].append(str(row[36:44].strip()))  
                    DICT_HIST_LINES_4["CP-SOLD-HOURS"].append(str(row[45:53].strip()))  
                    DICT_HIST_LINES_4["CP-ACTUAL-HRS"].append(str(row[54:62].strip()))  
                    DICT_HIST_LINES_4["CP-LABOR-COST$"].append(str(row[63:71].strip()))  
                    DICT_HIST_LINES_4["CP-LABOR-SALE$"].append(str(row[72:80].strip()))  
                    DICT_HIST_LINES_4["CP-PART-COST$"].append(str(row[81:].strip()))  

                    
            
            if row.strip() != "" and hist_lines_5==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
             
                    DICT_HIST_LINES_5["VEH-ID"].append(str(row[0:17].strip()))
                    DICT_HIST_LINES_5["RO"].append(str(row[18:26].strip()))
                    DICT_HIST_LINES_5["CP-PART-SALE$"].append(str(row[27:35].strip()))    
                    DICT_HIST_LINES_5["WP-SOLD-HOURS"].append(str(row[36:44].strip()))  
                    DICT_HIST_LINES_5["WP-ACTUAL-HRS"].append(str(row[45:53].strip()))  
                    DICT_HIST_LINES_5["WP-LABOR-COST$"].append(str(row[54:62].strip()))  
                    DICT_HIST_LINES_5["WP-LABOR-SALE$"].append(str(row[63:71].strip()))  
                    DICT_HIST_LINES_5["WP-PART-COST$"].append(str(row[72:80].strip()))  
                    DICT_HIST_LINES_5["WP-PART-SALE$"].append(str(row[81:].strip())) 

            if row.strip() != "" and hist_lineCodes==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                
                           
                if len(row[0:26].strip())==0 :
                   
                    if  len(row[27:31].strip())==0:
                        if len(row[32:].strip())>0:
                           DICT_HIST_LINESCODES["CAUSE"][(len(DICT_HIST_LINESCODES["CAUSE"])-1)] = (DICT_HIST_LINESCODES["CAUSE"][(len(DICT_HIST_LINESCODES["CAUSE"])-1)] + str(row[32:].rstrip()))
                        
                    else:
                            DICT_HIST_LINESCODES["VEH-ID"].append(DICT_HIST_LINESCODES["VEH-ID"][(len(DICT_HIST_LINESCODES["VEH-ID"])-1)])
                            DICT_HIST_LINESCODES["RO"].append(DICT_HIST_LINESCODES["RO"][(len(DICT_HIST_LINESCODES["RO"])-1)])
                            DICT_HIST_LINESCODES["LINE-CODE"].append(str(row[27:31].strip()))
                            DICT_HIST_LINESCODES["CAUSE"].append(str(row[32:]))
                           
                        
                else:                     
                    DICT_HIST_LINESCODES["VEH-ID"].append(str(row[0:17].strip()))
                    DICT_HIST_LINESCODES["RO"].append(str(row[18:26].strip()))
                    DICT_HIST_LINESCODES["LINE-CODE"].append(str(row[27:31].strip()))
                    DICT_HIST_LINESCODES["CAUSE"].append(str(row[32:]))  
            
            if row.strip() != "" and hist_parts==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                
                           
                if len(row[0:31].strip())==0 :
                   
                     
                    if len(row[32:52].strip())>0:
                        DICT_HIST_PARTS["OP-CODES"][(len(DICT_HIST_PARTS["OP-CODES"])-1)] = (DICT_HIST_PARTS["OP-CODES"][(len(DICT_HIST_PARTS["OP-CODES"])-1)] + str(row[32:52].strip()))
                    if len(row[53:79].strip())>0:
                        DICT_HIST_PARTS["PART-NUM"][(len(DICT_HIST_PARTS["PART-NUM"])-1)] = (DICT_HIST_PARTS["PART-NUM"][(len(DICT_HIST_PARTS["PART-NUM"])-1)] + str(row[53:79].strip()))
                    if len(row[80:100].strip())>0:
                        DICT_HIST_PARTS["PART-DESC"][(len(DICT_HIST_PARTS["PART-DESC"])-1)] = (DICT_HIST_PARTS["PART-DESC"][(len(DICT_HIST_PARTS["PART-DESC"])-1)] + str(row[80:100].rstrip()))
                   
                        
                else:    
                       
                    DICT_HIST_PARTS["VEH-ID"].append(str(row[0:10].strip()))
                    DICT_HIST_PARTS["RO"].append(str(row[11:19].strip()))
                    DICT_HIST_PARTS["LINE-CODE"].append(str(row[20:31].strip()))
                    DICT_HIST_PARTS["OP-CODES"].append(str(row[32:52].strip()))
                    DICT_HIST_PARTS["PART-NUM"].append(str(row[53:79].strip()))                            
                    DICT_HIST_PARTS["PART-DESC"].append(str(row[80:100]))
                    DICT_HIST_PARTS["PART-QTYORD"].append(str(row[101:107].strip()))
                    DICT_HIST_PARTS["PART-COST"].append(str(row[108:116].strip()))
                    DICT_HIST_PARTS["PART-SALE"].append(str(row[117:].strip()))                            
                     

            idx += 1

        #END WHILE
        if loglevel==logging.DEBUG:
            logger.debug("Parse Completed...DICT_HIST_1 :"+str((DICT_HIST_1)))
            logger.debug("Parse Completed...DICT_HIST_2 :"+str((DICT_HIST_2)))
            logger.debug("Parse Completed...DICT_HIST_OPCODES :"+str((DICT_HIST_OPCODES)))
            logger.debug("Parse Completed...DICT_HIST_3 :"+str((DICT_HIST_3)))
            logger.debug("Parse Completed...DICT_HIST_LINES_1 :"+str((DICT_HIST_LINES_1)))
            logger.debug("Parse Completed...DICT_HIST_LINES_2 :"+str((DICT_HIST_LINES_2)))
            logger.debug("Parse Completed...DICT_HIST_LINES_3 :"+str((DICT_HIST_LINES_3)))
            logger.debug("Parse Completed...DICT_HIST_LINES_4 :"+str((DICT_HIST_LINES_4)))
            logger.debug("Parse Completed...DICT_HIST_LINES_5 :"+str((DICT_HIST_LINES_5)))  
            logger.debug("Parse Completed...DICT_HIST_LINESCODES :"+str((DICT_HIST_LINESCODES)))  
            logger.debug("Parse Completed...DICT_HIST_PARTS :"+str((DICT_HIST_PARTS)))  
            
         #covert dict to dataframe
        #print("Parse Completed...DICT_HIST_LINES_1 :"+str((DICT_HIST_LINES_1)))       
        DICT_HIST_1 = pad.DataFrame(DICT_HIST_1)        
        DICT_HIST_2 = pad.DataFrame(DICT_HIST_2)
        DICT_HIST_OPCODES = pad.DataFrame(DICT_HIST_OPCODES)
        DICT_HIST_3 = pad.DataFrame(DICT_HIST_3)
        DICT_HIST_LINES_1 = pad.DataFrame(DICT_HIST_LINES_1)
        DICT_HIST_LINES_2 = pad.DataFrame(DICT_HIST_LINES_2)
        DICT_HIST_LINES_3 = pad.DataFrame(DICT_HIST_LINES_3)
        DICT_HIST_LINES_4 = pad.DataFrame(DICT_HIST_LINES_4)
        DICT_HIST_LINES_5 = pad.DataFrame(DICT_HIST_LINES_5) 
        DICT_HIST_LINESCODES=pad.DataFrame(DICT_HIST_LINESCODES) 
        DICT_HIST_PARTS=pad.DataFrame(DICT_HIST_PARTS) 
        
        df_final=pad.DataFrame([])

        #Merge dataframe together
        if len(DICT_HIST_1)>0:
            df_final=DICT_HIST_1
        if len(df_final)>0 and len(DICT_HIST_2)>0:
            df_final = pad.merge(df_final,DICT_HIST_2, on=['VEH-ID','RO'], how='left')
        if len(df_final)>0 and len(DICT_HIST_3)>0:
            df_final = pad.merge(df_final,DICT_HIST_3, on=['VEH-ID','RO'], how='left')

        df_final_lines=   pad.DataFrame([]) 

        if len(DICT_HIST_LINES_1)>0:
            df_final_lines=DICT_HIST_LINES_1
        if len(df_final_lines)>0 and len(DICT_HIST_LINES_2)>0:
            df_final_lines = pad.merge(df_final_lines,DICT_HIST_LINES_2, on=['VEH-ID','RO'], how='left')
        if len(df_final_lines)>0 and len(DICT_HIST_LINES_3)>0:
            df_final_lines = pad.merge(df_final_lines,DICT_HIST_LINES_3, on=['VEH-ID','RO'], how='left')
        if len(df_final_lines)>0 and len(DICT_HIST_LINES_4)>0:
            df_final_lines = pad.merge(df_final_lines,DICT_HIST_LINES_4, on=['VEH-ID','RO'], how='left')
        if len(df_final_lines)>0 and len(DICT_HIST_LINES_5)>0:
            df_final_lines = pad.merge(df_final_lines,DICT_HIST_LINES_5, on=['VEH-ID','RO'], how='left')
         
        if len(df_final)>0 and len(df_final_lines)>0:
            df_final = pad.merge(df_final,df_final_lines, on=['VEH-ID','RO'], how='left')        
            
        df_final = df_final.replace(np.nan,'', regex=True) 
        #DICT_HIST_OPCODES = DICT_HIST_OPCODES.replace(np.nan,'', regex=True) 
        #DICT_HIST_LINESCODES = DICT_HIST_LINESCODES.replace(np.nan,'', regex=True) 
       
        if len(DICT_HIST_OPCODES)>0 and len(DICT_HIST_PARTS)>0:
            DICT_HIST_OPCODES = pad.merge(DICT_HIST_OPCODES,DICT_HIST_PARTS, on=['VEH-ID','RO','LINE-CODE','OP-CODES'], how='left')
        if len(DICT_HIST_OPCODES)>0 and len(DICT_HIST_LINESCODES)>0:
            DICT_HIST_OPCODES = pad.merge(DICT_HIST_OPCODES,DICT_HIST_LINESCODES, on=['VEH-ID','RO','LINE-CODE'], how='left')
        
        DICT_HIST_OPCODES = DICT_HIST_OPCODES.replace(np.nan,'', regex=True)    
         
        logger.debug("ro history Parsing done succesfully...")        
        return {"ro-hist":df_final,"hist-opcodes":DICT_HIST_OPCODES,'account_line':account_line}
    @classmethod
    def parseV1(self,fileDataAsStr):
        logger.debug("inside ROHistoryParser parseV1...")
        df_final=pad.DataFrame([]) 
        txtfil = io.StringIO(fileDataAsStr)           
        row = txtfil.readline()
        idx = 0
       
        #LIST HISTORY VEH-ID RO ROI.LOGON STOCK-NO SERIAL VEH-LAST-SVC-DATE CUST-LAST-SVC-DATE WARR-EXP-DATE COLOR STOCK.TYPE OPENED YEAR ID-SUPP                                                                                                                                                                                     SALE $          
        #VEH-ID.. RO.... ROI....... STOCK-NO.. SERIAL........... VEH-LAST-SVC-DATE CUST-LAST-SERV-DATE WARR-EXP-DATE COLOR..... STOCK-TYPE OPENED. YR
        #                LOGON  
        hist_1 : bool =False
        DICT_HIST_1={'VEH-ID':[],
                    'RO':[],
                    'ROI.LOGON':[],
                    'STOCK-NO':[],
                    'SERIAL':[],
                    'VEH-LAST-SVC-DATE':[],
                    'CUST-LAST-SVC-DATE':[],
                    'WARR-EXP-DATE':[],
                    'COLOR':[],
                    'STOCK.TYPE':[],
                    'OPENED':[],
                    'YEAR':[]                             
                    }
        #>LIST HISTORY VEH-ID RO DELIVERY.DATE DEALER-CD SOLD.DATE MODEL MILEAGE EMAIL MAKE LABOR$ PARTS$ ID-SUPPID
        #VEH-ID.. RO.... DELIVERY DEALER-CD. SOLD.... MODEL......................... MILEAGE EMAIL............................................. MAKE...........
        #                DATE                DATE                                                                                                              
        hist_2 : bool =False
        DICT_HIST_2={'VEH-ID':[],
                    'RO':[],
                    'DELIVERY.DATE':[],
                    'DEALER-CD':[],
                    'SOLD.DATE':[],
                    'MODEL':[],
                    'MILEAGE':[]  ,
                    'EMAIL':[] ,
                    'MAKE':[]              
                    } 
        #LIST HISTORY VEH-ID RO LINE-CODE OP-CODES OP-DESC LBR-TYPES LBR-SALE-AMTS PTS-SALE-AMTS MISC-SALE-AMTS LBR-COST$ PTS-COST$ COMMENT ID-SUPP
        #VEH-ID.. RO.... LINE OP-CODES.. OPERATION DESCRIPTION.... LBR-TYPE LBR-SALE-AMTS PTS-SALE-AMTS MISC-SALE-AMTS LBR-COST$.. PTS-COST$.. COMMENT.............
        hist_opcodes : bool =False
        DICT_HIST_OPCODES={'VEH-ID':[],
                    'RO':[],
                    'LINE-CODE':[],
                    'OP-CODES':[],
                    'OP-DESC':[],
                    'LBR-TYPES':[],
                    'LBR-SALE-AMTS':[]  ,
                    'PTS-SALE-AMTS':[]   ,
                    'MISC-SALE-AMTS':[]  , 
                    'LBR-COST$':[]   ,
                    'PTS-COST$':[] ,  
                    'COMMENT':[]                   
                    } 
        #LIST HISTORY VEH-ID RO CUST-NO CUST-NAME ADDRESS CITY STATE ZIP BUS-PHONE HOME-PHONE CELL.PHONE SA-NO CLOSE-DATE RO-AMOUNT ID-SUPP
        #VEH-ID.. RO.... CUST-NO. CUSTOMER................. CUST-ADDRESS....................... CUST-CITY........... CUST-STATE CUST-ZIP.. BUS....... HOME...... CELL...... SA-NO CLOSE-DATE RO-AMOUNT...
        #                         NAME                                                                                                     PHONE      PHONE      PHONE                                   

        hist_3 : bool =False
        DICT_HIST_3={'VEH-ID':[],
                    'RO':[],
                    'CUST-NBR':[],
                    'CUST-NAME':[],
                    'ADDRESS':[],
                    'CITY':[],
                    'STATE':[] ,
                    'ZIP':[] ,
                    'H-PHONE':[],
                    'B-PHONE':[],
                    'CELL-PHONE':[],                    
                    'SA-NO':[],
                    'CLOSE-DATE':[],
                    'RO-TOTAL':[],
                    } 
        #LIST HISTORY-LINES VEHID RO  IP-SUBL-SALE$ IP-SUBL-COST$ IP-LABOR-COST$ IP-LABOR-SALE$ IP-PART-COST$ IP-PART-SALE$ ID-SUPP
        #VEHID............ RO...... IP SUB.. IP SUB.. IP LBR.. IP LBR.. IP PTS.. IP PTS..
        #                           SALE $   COST $   COST $   SALE $   COST $   SALE $
        hist_lines_1 : bool =False
        DICT_HIST_LINES_1={
                    'VEH-ID':[],
                    'RO':[],
                    'IP-SUBL-SALE$':[],
                    'IP-SUBL-COST$':[],
                    'IP-LABOR-COST$':[],
                    'IP-LABOR-SALE$':[],
                    'IP-PART-COST$':[],
                    'IP-PART-SALE$':[]                   
                    } 
        #LIST HISTORY-LINES VEHID RO COMEBACK DENIED-BY EST IP-SOLD-HRS IP-ACTUAL-HRS RESNO ID-SUPP
        #VEHID............ RO...... CBK DENIED BY ESTIMATE.. IP SOLD. IP ACT.. RESNO... WP SUB.. WP SUB..
        #                                                    HRS      HRS               SALE $   COST $                                                   HRS      HRS              

        hist_lines_2 : bool =False
        DICT_HIST_LINES_2={
                    'VEH-ID':[],
                    'RO':[],
                    'COMEBACK':[],
                    'DENIED-BY':[] ,
                    'EST':[] ,
                    'IP-SOLD-HRS':[],
                    'IP-ACTUAL-HRS':[],
                    'RESNO':[] ,
                    'WP-SUBL-SALE$':[],
                    'WP-SUBL-COST$':[]
                    } 
        #LIST HISTORY-LINES VEHID RO CP-SUBL-SALE$ CP-SUBL-COST$ CP-TOTAL-SALE$ CP-TOTAL-COST$  WP-TOTAL-COST$ WP-TOTAL-SALE$ ID-SUPP
        #VEHID............ RO...... CP SUB.. CP SUB.. CP TOT.. CP TOT.. WP TOT.. WP TOT..
        #                           SALE $   COST $   SALE $   COST $   COST $   SALE $  
        hist_lines_3 : bool =False
        DICT_HIST_LINES_3={'VEH-ID':[],
                    'RO':[],
                    'CP-SUBL-SALE$':[],           
                    'CP-SUBL-COST$':[],
                    'CP-TOTAL-SALE$':[],
                    'CP-TOTAL-COST$':[] ,
                    'WP-TOTAL-COST$':[] ,
                    'WP-TOTAL-SALE$':[]                     
                    } 
        #LIST HISTORY-LINES VEHID RO IP-TOTAL-SALE$ IP-TOTAL-COST$ CP-SOLD-HRS CP-ACTUAL-HRS CP-LABOR-COST$ CP-LABOR-SALE$ CP-PART-COST$ ID-SUPP
        #VEHID............ RO...... IP TOT.. IP TOT.. CP SOLD. CP ACT.. CP LBR.. CP LBR.. CP PTS..
        #                           SALE $   COST $   HRS      HRS      COST $   SALE $   COST $ 
        hist_lines_4 : bool =False
        DICT_HIST_LINES_4={'VEH-ID':[],
                    'RO':[],
                    'IP-TOTAL-SALE$':[],           
                    'IP-TOTAL-COST$':[],
                    'CP-SOLD-HOURS':[],
                    'CP-ACTUAL-HRS':[],
                    'CP-LABOR-COST$':[] ,
                    'CP-LABOR-SALE$':[] ,
                    'CP-PART-COST$':[]                     
                    } 
        #LIST HISTORY-LINES VEHID RO CP-PART-SALE$ WP-SOLD-HRS WP-ACTUAL-HRS WP-LABOR-COST$ WP-LABOR-SALE$ WP-PART-COST$ WP-PART-SALE$ ID-SUPP
        #VEHID............ RO...... CP PTS.. WP SOLD. WP ACT.. WP LBR.. WP LBR.. WP PTS.. WP PTS..
        #                           SALE $   HRS      HRS      COST $   SALE $   COST $   SALE $  
        hist_lines_5 : bool =False
        DICT_HIST_LINES_5={'VEH-ID':[],
                    'RO':[],
                    'CP-PART-SALE$':[],           
                    'WP-SOLD-HOURS':[],
                    'WP-ACTUAL-HRS':[],
                    'WP-LABOR-COST$':[] ,
                    'WP-LABOR-SALE$':[] ,
                    'WP-PART-COST$':[] ,
                    'WP-PART-SALE$':[]                     
                    } 
        #LIST HISTORY-LINES VEHID RO LINE-CODE CAUSE ID-SUPP
        #VEHID............ RO...... LINE CAUSE...............
        hist_lineCodes : bool =False
        DICT_HIST_LINESCODES={'VEH-ID':[],
                    'RO':[],
                    'LINE-CODE':[],
                    'CAUSE':[]                   
                    } 
        #LIST WIP-HISTORY NF-VEHID RO# PART-LOP-LN-CD PART-OP-CODE PART-NUM PART-DESC PART-QTYORD PART-COST PART-SALE ID-SUPP
        #NF-VEHID.. RO.#.... LOP-LINE-CD OP-CODE............. PARTNUMBER................ PARTDESCRIPTION..... QTYORD PARTCOST PARTSALE
        hist_parts : bool =False
        DICT_HIST_PARTS={'VEH-ID':[],
                    'RO':[],
                    'LINE-CODE':[],
                    'OP-CODES':[], 
                    'PART-NUM':[],   
                    'PART-DESC':[],   
                    'PART-QTYORD':[],   
                    'PART-COST':[],   
                    'PART-SALE':[],                 
                    } 
        skip_line_startwith_list=[]
        skip_line_startwith_list.append(':GET-LIST')
        skip_line_startwith_list.append('>LIST')
        skip_line_startwith_list.append('>SORT')
        skip_line_startwith_list.append(':')
        skip_line_startwith_list.append(':SSELECT')        
        skip_line_startwith_list.append('>')
        skip_line_startwith_list.append('TERM 300,0')

        skip_line_startwith_list.append('VEH-ID.. RO.... ROI....... STOCK-NO.. SERIAL........... VEH-LAST-SVC-DATE')
        skip_line_startwith_list.append('VEH-ID.. RO.... DELIVERY DEALER-CD. SOLD.... MODEL.............')
        skip_line_startwith_list.append('VEH-ID.. RO.... LINE OP-CODES.. OPERATION DESCRIPTION....')
        skip_line_startwith_list.append('VEH-ID.. RO.... CUST-NO. CUSTOMER................. CUST-ADDRESS...')
        skip_line_startwith_list.append('VEHID............ RO...... IP SUB.. IP SUB.. IP LBR.. IP LBR..')
        skip_line_startwith_list.append('VEHID............ RO...... CBK DENIED BY ESTIMATE.. IP SOLD. IP ACT..')
        skip_line_startwith_list.append('VEHID............ RO...... CP SUB.. CP SUB.. CP TOT.. CP TOT.. WP TOT..')
        skip_line_startwith_list.append('VEHID............ RO...... IP TOT.. IP TOT.. CP SOLD. CP ACT.. CP LBR.. CP LBR..')
        skip_line_startwith_list.append('VEHID............ RO...... CP PTS.. WP SOLD. WP ACT.. WP LBR.. WP LBR.. WP PTS..')
        skip_line_startwith_list.append('VEHID............ RO...... LINE CAUSE...............')
        skip_line_startwith_list.append('NF-VEHID.. RO.#.... LOP-LINE-CD OP-CODE............. PARTNUMBER................ PARTDESCRIPTION..... QTYORD')

        history_1=['VEH-ID.. RO.... ROI....... STOCK-NO.. SERIAL........... VEH-LAST-SVC-DATE']
        history_2=['VEH-ID.. RO.... DELIVERY DEALER-CD. SOLD.... MODEL.............']
        history_opcodes=['VEH-ID.. RO.... LINE OP-CODES.. OPERATION DESCRIPTION....']
        history_3=['VEH-ID.. RO.... CUST-NO. CUSTOMER................. CUST-ADDRESS...']

        history_lines_1=['VEHID............ RO...... IP SUB.. IP SUB.. IP LBR.. IP LBR..']
        history_lines_2=['VEHID............ RO...... CBK DENIED BY ESTIMATE.. IP SOLD. IP ACT..']
        history_lines_3=['VEHID............ RO...... CP SUB.. CP SUB.. CP TOT.. CP TOT.. WP TOT..']
        history_lines_4=['VEHID............ RO...... IP TOT.. IP TOT.. CP SOLD. CP ACT.. CP LBR.. CP LBR..']
        history_lines_5=['VEHID............ RO...... CP PTS.. WP SOLD. WP ACT.. WP LBR.. WP LBR.. WP PTS..']
        history_linecodes=['VEHID............ RO...... LINE CAUSE...............']
        history_parts=['NF-VEHID.. RO.#.... LOP-LINE-CD OP-CODE............. PARTNUMBER................ PARTDESCRIPTION'] 

        skip_line_contains_list=[]
        skip_line_contains_list.append('ITEMS SELECTED')
        skip_line_contains_list.append('ITEM SELECTED')
        skip_line_contains_list.append('ITEM LISTED')
        skip_line_contains_list.append('ITEMS LISTED')
        skip_line_contains_list.append('FRAMES USED.')
        skip_line_contains_list.append('CATALOGED;')        
        skip_line_contains_list.append("' NOT ON FILE")
        skip_line_contains_list.append("COUNT PRIVLIB")
        skip_line_contains_list.append("ITEMS COUNTED")
        skip_line_contains_list.append("ITEM COUNTED")
        skip_line_contains_list.append(' adp (') 
        skip_line_contains_list.append('[m[H[J  PAGE') 
        skip_line_contains_list.append('       LOGON')
        skip_line_contains_list.append('DATE                DATE')
        skip_line_contains_list.append('NAME                                                                                                     PHONE      PHONE      PHONE')
        skip_line_contains_list.append('SALE $   COST $   COST $   SALE $   COST $   SALE $') 
        skip_line_contains_list.append('HRS      HRS               SALE $   COST $')
        skip_line_contains_list.append('SALE $   COST $   SALE $   COST $   COST $   SALE $')
        skip_line_contains_list.append('SALE $   COST $   HRS      HRS      COST $   SALE $   COST $')
        skip_line_contains_list.append('SALE $   HRS      HRS      COST $   SALE $   COST $   SALE $')


        line_end_text=['ITEMS LISTED.']       
        eof='ITEMS COUNTED'
        account_line=[]     
        while row:
            row = txtfil.readline()
            if row.__contains__('adp (') and row.strip().endswith(')'):
                account_line.append(row.strip())      
                   
            if row.startswith(tuple(history_1)):
                hist_1=True            
            if row.startswith(tuple(history_2)):
                hist_2=True 

            if row.startswith(tuple(history_opcodes)):
                hist_opcodes=True    
            
            if row.startswith(tuple(history_3)):
                hist_3=True 
            
            if row.startswith(tuple(history_lines_1)):
                hist_lines_1=True 
            if row.startswith(tuple(history_lines_2)):
                hist_lines_2=True 
            if row.startswith(tuple(history_lines_3)):
                hist_lines_3=True 
            if row.startswith(tuple(history_lines_4)):
                hist_lines_4=True 
            if row.startswith(tuple(history_lines_5)):
                hist_lines_5=True  
            if row.startswith(tuple(history_linecodes)):
                hist_lineCodes=True     
            if row.startswith(tuple(history_parts)):
                hist_parts=True     
            if list(filter(row.__contains__, line_end_text)) != []:
                
                if(hist_1==True): 
                    hist_1=False                   
                if(hist_2==True):
                    hist_2=False  
                    
                if(hist_opcodes==True):
                    hist_opcodes=False 

                if(hist_3==True):
                    hist_3=False  

                if(hist_lines_1==True):
                    hist_lines_1=False 
                if(hist_lines_2==True):
                    hist_lines_2=False 
                if(hist_lines_3==True):
                    hist_lines_3=False 
                if(hist_lines_4==True):
                    hist_lines_4=False 
                if(hist_lines_5==True):
                    hist_lines_5=False 

                if(hist_lineCodes==True):
                    hist_lineCodes=False     
                if(hist_parts==True):
                    hist_parts=False   
            if row.strip() != "" and hist_1==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                     
                if len(row[0:15].strip())==0 :

                    if len(row[16:26].strip())>0:
                       DICT_HIST_1["ROI.LOGON"][(len(DICT_HIST_1["ROI.LOGON"])-1)] = (DICT_HIST_1["ROI.LOGON"][(len(DICT_HIST_1["ROI.LOGON"])-1)] + str(row[16:26].strip()))
                    if len(row[27:37].strip())>0:
                       DICT_HIST_1["STOCK-NO"][(len(DICT_HIST_1["STOCK-NO"])-1)] = (DICT_HIST_1["STOCK-NO"][(len(DICT_HIST_1["STOCK-NO"])-1)] + str(row[27:37].strip()))
                    if len(row[108:118].strip())>0:
                       DICT_HIST_1["COLOR"][(len(DICT_HIST_1["COLOR"])-1)] = (DICT_HIST_1["COLOR"][(len(DICT_HIST_1["COLOR"])-1)] + str(row[108:118].rstrip()))
                    if len(row[119:129].strip())>0:
                       DICT_HIST_1["STOCK.TYPE"][(len(DICT_HIST_1["STOCK.TYPE"])-1)] = (DICT_HIST_1["STOCK.TYPE"][(len(DICT_HIST_1["STOCK.TYPE"])-1)] + str(row[119:129].rstrip()))
                          
                else:
                    DICT_HIST_1["VEH-ID"].append(str(row[0:8].strip()))
                    DICT_HIST_1["RO"].append(str(row[9:15].strip()))
                    DICT_HIST_1["ROI.LOGON"].append(str(row[16:26].strip()))
                    DICT_HIST_1["STOCK-NO"].append(str(row[27:37].strip()))
                    DICT_HIST_1["SERIAL"].append(str(row[38:55].strip()))
                    DICT_HIST_1["VEH-LAST-SVC-DATE"].append(str(row[56:73].strip()))
                    DICT_HIST_1["CUST-LAST-SVC-DATE"].append(str(row[75:93].strip()))
                    DICT_HIST_1["WARR-EXP-DATE"].append(str(row[94:107].strip()))
                    DICT_HIST_1["COLOR"].append(str(row[108:118]))
                    DICT_HIST_1["STOCK.TYPE"].append(str(row[119:129].strip()))
                    DICT_HIST_1["OPENED"].append(str(row[130:137].strip()))
                    DICT_HIST_1["YEAR"].append(str(row[138:].strip()))
        
            if row.strip() != "" and hist_2==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                   
                if len(row[0:15].strip())==0 :

                    if len(row[45:75].strip())>0:
                           DICT_HIST_2["MODEL"][(len(DICT_HIST_2["MODEL"])-1)] = (DICT_HIST_2["MODEL"][(len(DICT_HIST_2["MODEL"])-1)] + str(row[45:75].rstrip()))
                    if len(row[84:134].strip())>0:
                           DICT_HIST_2["EMAIL"][(len(DICT_HIST_2["EMAIL"])-1)] = (DICT_HIST_2["EMAIL"][(len(DICT_HIST_2["EMAIL"])-1)] + str(row[84:134].rstrip()))
                    if len(row[135:150].strip())>0:
                           DICT_HIST_2["MAKE"][(len(DICT_HIST_2["MAKE"])-1)] = (DICT_HIST_2["MAKE"][(len(DICT_HIST_2["MAKE"])-1)] + str(row[135:].rstrip()))
                          
                else:
                    DICT_HIST_2["VEH-ID"].append(str(row[0:8].strip()))
                    DICT_HIST_2["RO"].append(str(row[9:15].strip()))
                    DICT_HIST_2["DELIVERY.DATE"].append(str(row[16:24].strip()))
                    DICT_HIST_2["DEALER-CD"].append(str(row[25:35].strip()))
                    DICT_HIST_2["SOLD.DATE"].append(str(row[36:44].strip()))
                    DICT_HIST_2["MODEL"].append(str(row[45:75]))
                    DICT_HIST_2["MILEAGE"].append(str(row[76:83].strip()))
                    DICT_HIST_2["EMAIL"].append(str(row[84:134].strip()))
                    DICT_HIST_2["MAKE"].append(str(row[135:].strip()))
                   
            
            if row.strip() != "" and hist_opcodes==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                
                           
                if len(row[0:15].strip())==0 :
                   
                    if  len(row[16:20].strip())==0:
                        if len(row[21:31].strip())>0:
                           DICT_HIST_OPCODES["OP-CODES"][(len(DICT_HIST_OPCODES["OP-CODES"])-1)] = (DICT_HIST_OPCODES["OP-CODES"][(len(DICT_HIST_OPCODES["OP-CODES"])-1)] + str(row[21:31].strip()))
                        if len(row[32:57].strip())>0:
                           DICT_HIST_OPCODES["OP-DESC"][(len(DICT_HIST_OPCODES["OP-DESC"])-1)] = (DICT_HIST_OPCODES["OP-DESC"][(len(DICT_HIST_OPCODES["OP-DESC"])-1)] + str(row[32:57].rstrip()))
                        if len(row[58:66].strip())>0:
                           DICT_HIST_OPCODES["LBR-TYPES"][(len(DICT_HIST_OPCODES["LBR-TYPES"])-1)] = (DICT_HIST_OPCODES["LBR-TYPES"][(len(DICT_HIST_OPCODES["LBR-TYPES"])-1)] + str(row[58:66].rstrip()))
                        if len(row[134:].strip())>0:
                           DICT_HIST_OPCODES["COMMENT"][(len(DICT_HIST_OPCODES["COMMENT"])-1)] = (DICT_HIST_OPCODES["COMMENT"][(len(DICT_HIST_OPCODES["COMMENT"])-1)] + str(row[134:].rstrip()))
                        
                    else:
                            DICT_HIST_OPCODES["VEH-ID"].append(DICT_HIST_OPCODES["VEH-ID"][(len(DICT_HIST_OPCODES["VEH-ID"])-1)])
                            DICT_HIST_OPCODES["RO"].append(DICT_HIST_OPCODES["RO"][(len(DICT_HIST_OPCODES["RO"])-1)])
                            DICT_HIST_OPCODES["LINE-CODE"].append(str(row[16:20].strip()))
                            DICT_HIST_OPCODES["OP-CODES"].append(str(row[21:31].strip()))
                            DICT_HIST_OPCODES["OP-DESC"].append(str(row[32:57]))                            
                            DICT_HIST_OPCODES["LBR-TYPES"].append(str(row[58:66]))
                            DICT_HIST_OPCODES["LBR-SALE-AMTS"].append(str(row[67:80].strip()))
                            DICT_HIST_OPCODES["PTS-SALE-AMTS"].append(str(row[81:94].strip()))
                            DICT_HIST_OPCODES["MISC-SALE-AMTS"].append(str(row[95:109].strip()))                            
                            DICT_HIST_OPCODES["LBR-COST$"].append(str(row[110:121].strip()))
                            DICT_HIST_OPCODES["PTS-COST$"].append(str(row[122:133].strip()))
                            DICT_HIST_OPCODES["COMMENT"].append(str(row[134:]))  
                        
                else:    
                                 
                    DICT_HIST_OPCODES["VEH-ID"].append(str(row[0:8].strip()))
                    DICT_HIST_OPCODES["RO"].append(str(row[9:15].strip()))
                    DICT_HIST_OPCODES["LINE-CODE"].append(str(row[16:20].strip()))
                    DICT_HIST_OPCODES["OP-CODES"].append(str(row[21:31].strip()))
                    DICT_HIST_OPCODES["OP-DESC"].append(str(row[32:57]))                            
                    DICT_HIST_OPCODES["LBR-TYPES"].append(str(row[58:66]))
                    DICT_HIST_OPCODES["LBR-SALE-AMTS"].append(str(row[67:80].strip()))
                    DICT_HIST_OPCODES["PTS-SALE-AMTS"].append(str(row[81:94].strip()))
                    DICT_HIST_OPCODES["MISC-SALE-AMTS"].append(str(row[95:109].strip()))                            
                    DICT_HIST_OPCODES["LBR-COST$"].append(str(row[110:121].strip()))
                    DICT_HIST_OPCODES["PTS-COST$"].append(str(row[122:133].strip()))
                    DICT_HIST_OPCODES["COMMENT"].append(str(row[134:]))  
                  
            if row.strip() != "" and hist_3==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                
                if len(row[0:24].strip())==0 :

                    if len(row[25:50].strip())>0:
                           DICT_HIST_3["CUST-NAME"][(len(DICT_HIST_3["CUST-NAME"])-1)] = (DICT_HIST_3["CUST-NAME"][(len(DICT_HIST_3["CUST-NAME"])-1)] + str(row[25:50].rstrip()))
                    if len(row[51:86].strip())>0:
                           DICT_HIST_3["ADDRESS"][(len(DICT_HIST_3["ADDRESS"])-1)] = (DICT_HIST_3["ADDRESS"][(len(DICT_HIST_3["ADDRESS"])-1)] + str(row[51:86].rstrip()))
                    if len(row[87:107].strip())>0:
                           DICT_HIST_3["CITY"][(len(DICT_HIST_3["CITY"])-1)] = (DICT_HIST_3["CITY"][(len(DICT_HIST_3["CITY"])-1)] + str(row[87:107].rstrip()))
                         
                else:
                    
                   
                    DICT_HIST_3["VEH-ID"].append(str(row[0:8].strip()))
                    DICT_HIST_3["RO"].append(str(row[9:15].strip()))
                    DICT_HIST_3["CUST-NBR"].append(str(row[16:24].strip()))
                    DICT_HIST_3["CUST-NAME"].append(str(row[25:50]))
                    DICT_HIST_3["ADDRESS"].append(str(row[51:86]))
                    DICT_HIST_3["CITY"].append(str(row[87:107]))
                    DICT_HIST_3["STATE"].append(str(row[108:118].strip()))
                    DICT_HIST_3["ZIP"].append(str(row[119:129].strip()))
                    DICT_HIST_3["B-PHONE"].append(str(row[130:140].strip()))
                    DICT_HIST_3["H-PHONE"].append(str(row[141:151].strip()))
                    DICT_HIST_3["CELL-PHONE"].append(str(row[152:162].strip()))
                    DICT_HIST_3["SA-NO"].append(str(row[163:168].strip()))
                    DICT_HIST_3["CLOSE-DATE"].append(str(row[169:179].strip()))
                    DICT_HIST_3["RO-TOTAL"].append(str(row[180:].strip()))

            if row.strip() != "" and hist_lines_1==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                
                    DICT_HIST_LINES_1["VEH-ID"].append(str(row[0:17].strip()))
                    DICT_HIST_LINES_1["RO"].append(str(row[18:26].strip()))
                    DICT_HIST_LINES_1["IP-SUBL-SALE$"].append(str(row[27:35].strip())) 
                    DICT_HIST_LINES_1["IP-SUBL-COST$"].append(str(row[36:44].strip())) 
                    DICT_HIST_LINES_1["IP-LABOR-COST$"].append(str(row[45:53].strip()))  
                    DICT_HIST_LINES_1["IP-LABOR-SALE$"].append(str(row[54:62].strip()))  
                    DICT_HIST_LINES_1["IP-PART-COST$"].append(str(row[63:71].strip()))  
                    DICT_HIST_LINES_1["IP-PART-SALE$"].append(str(row[72:].strip()))  

            if row.strip() != "" and hist_lines_2==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                if len(row[0:26].strip())==0 :

                    if len(row[31:40].strip())>0:
                           DICT_HIST_LINES_2["DENIED-BY"][(len(DICT_HIST_LINES_2["DENIED-BY"])-1)] = (DICT_HIST_LINES_2["DENIED-BY"][(len(DICT_HIST_LINES_2["DENIED-BY"])-1)] + str(row[31:40].rstrip()))
                      
                else:
                     
                    DICT_HIST_LINES_2["VEH-ID"].append(str(row[0:17].strip()))
                    DICT_HIST_LINES_2["RO"].append(str(row[18:26].strip()))
                    DICT_HIST_LINES_2["COMEBACK"].append(str(row[27:30].strip()))
                    DICT_HIST_LINES_2["DENIED-BY"].append(str(row[31:40]))
                    DICT_HIST_LINES_2["EST"].append(str(row[41:51].strip()))
                    DICT_HIST_LINES_2["IP-SOLD-HRS"].append(str(row[52:60].strip()))
                    DICT_HIST_LINES_2["IP-ACTUAL-HRS"].append(str(row[61:69].strip()))                    
                    DICT_HIST_LINES_2["RESNO"].append(str(row[70:78].strip()))
                    DICT_HIST_LINES_2["WP-SUBL-SALE$"].append(str(row[79:87].strip())) 
                    DICT_HIST_LINES_2["WP-SUBL-COST$"].append(str(row[88:].strip())) 

            if row.strip() != "" and hist_lines_3==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                
                    DICT_HIST_LINES_3["VEH-ID"].append(str(row[0:17].strip()))
                    DICT_HIST_LINES_3["RO"].append(str(row[18:26].strip()))
                    DICT_HIST_LINES_3["CP-SUBL-SALE$"].append(str(row[27:35].strip())) 
                    DICT_HIST_LINES_3["CP-SUBL-COST$"].append(str(row[36:44].strip())) 
                    DICT_HIST_LINES_3["CP-TOTAL-SALE$"].append(str(row[45:53].strip())) 
                    DICT_HIST_LINES_3["CP-TOTAL-COST$"].append(str(row[54:62].strip())) 
                    DICT_HIST_LINES_3["WP-TOTAL-COST$"].append(str(row[63:71].strip())) 
                    DICT_HIST_LINES_3["WP-TOTAL-SALE$"].append(str(row[72:].strip())) 
                                 
           
            if row.strip() != "" and hist_lines_4==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                     
                    DICT_HIST_LINES_4["VEH-ID"].append(str(row[0:17].strip()))
                    DICT_HIST_LINES_4["RO"].append(str(row[18:26].strip()))
                    DICT_HIST_LINES_4["IP-TOTAL-SALE$"].append(str(row[27:35].strip()))  
                    DICT_HIST_LINES_4["IP-TOTAL-COST$"].append(str(row[36:44].strip()))  
                    DICT_HIST_LINES_4["CP-SOLD-HOURS"].append(str(row[45:53].strip()))  
                    DICT_HIST_LINES_4["CP-ACTUAL-HRS"].append(str(row[54:62].strip()))  
                    DICT_HIST_LINES_4["CP-LABOR-COST$"].append(str(row[63:71].strip()))  
                    DICT_HIST_LINES_4["CP-LABOR-SALE$"].append(str(row[72:80].strip()))  
                    DICT_HIST_LINES_4["CP-PART-COST$"].append(str(row[81:].strip()))  

                    
            
            if row.strip() != "" and hist_lines_5==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
             
                    DICT_HIST_LINES_5["VEH-ID"].append(str(row[0:17].strip()))
                    DICT_HIST_LINES_5["RO"].append(str(row[18:26].strip()))
                    DICT_HIST_LINES_5["CP-PART-SALE$"].append(str(row[27:35].strip()))    
                    DICT_HIST_LINES_5["WP-SOLD-HOURS"].append(str(row[36:44].strip()))  
                    DICT_HIST_LINES_5["WP-ACTUAL-HRS"].append(str(row[45:53].strip()))  
                    DICT_HIST_LINES_5["WP-LABOR-COST$"].append(str(row[54:62].strip()))  
                    DICT_HIST_LINES_5["WP-LABOR-SALE$"].append(str(row[63:71].strip()))  
                    DICT_HIST_LINES_5["WP-PART-COST$"].append(str(row[72:80].strip()))  
                    DICT_HIST_LINES_5["WP-PART-SALE$"].append(str(row[81:].strip())) 

            if row.strip() != "" and hist_lineCodes==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                
                           
                if len(row[0:26].strip())==0 :
                   
                    if  len(row[27:31].strip())==0:
                        if len(row[32:].strip())>0:
                           DICT_HIST_LINESCODES["CAUSE"][(len(DICT_HIST_LINESCODES["CAUSE"])-1)] = (DICT_HIST_LINESCODES["CAUSE"][(len(DICT_HIST_LINESCODES["CAUSE"])-1)] + str(row[32:].rstrip()))
                        
                    else:
                            DICT_HIST_LINESCODES["VEH-ID"].append(DICT_HIST_LINESCODES["VEH-ID"][(len(DICT_HIST_LINESCODES["VEH-ID"])-1)])
                            DICT_HIST_LINESCODES["RO"].append(DICT_HIST_LINESCODES["RO"][(len(DICT_HIST_LINESCODES["RO"])-1)])
                            DICT_HIST_LINESCODES["LINE-CODE"].append(str(row[27:31].strip()))
                            DICT_HIST_LINESCODES["CAUSE"].append(str(row[32:]))
                           
                        
                else:                     
                    DICT_HIST_LINESCODES["VEH-ID"].append(str(row[0:17].strip()))
                    DICT_HIST_LINESCODES["RO"].append(str(row[18:26].strip()))
                    DICT_HIST_LINESCODES["LINE-CODE"].append(str(row[27:31].strip()))
                    DICT_HIST_LINESCODES["CAUSE"].append(str(row[32:]))  
            
            if row.strip() != "" and hist_parts==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                
                           
                if len(row[0:31].strip())==0 :
                   
                     
                    if len(row[32:52].strip())>0:
                        DICT_HIST_PARTS["OP-CODES"][(len(DICT_HIST_PARTS["OP-CODES"])-1)] = (DICT_HIST_PARTS["OP-CODES"][(len(DICT_HIST_PARTS["OP-CODES"])-1)] + str(row[32:52].strip()))
                    if len(row[53:79].strip())>0:
                        DICT_HIST_PARTS["PART-NUM"][(len(DICT_HIST_PARTS["PART-NUM"])-1)] = (DICT_HIST_PARTS["PART-NUM"][(len(DICT_HIST_PARTS["PART-NUM"])-1)] + str(row[53:79].strip()))
                    if len(row[80:100].strip())>0:
                        DICT_HIST_PARTS["PART-DESC"][(len(DICT_HIST_PARTS["PART-DESC"])-1)] = (DICT_HIST_PARTS["PART-DESC"][(len(DICT_HIST_PARTS["PART-DESC"])-1)] + str(row[80:100].rstrip()))
                   
                        
                else:    
                       
                    DICT_HIST_PARTS["VEH-ID"].append(str(row[0:10].strip()))
                    DICT_HIST_PARTS["RO"].append(str(row[11:19].strip()))
                    DICT_HIST_PARTS["LINE-CODE"].append(str(row[20:31].strip()))
                    DICT_HIST_PARTS["OP-CODES"].append(str(row[32:52].strip()))
                    DICT_HIST_PARTS["PART-NUM"].append(str(row[53:79].strip()))                            
                    DICT_HIST_PARTS["PART-DESC"].append(str(row[80:100]))
                    DICT_HIST_PARTS["PART-QTYORD"].append(str(row[101:107].strip()))
                    DICT_HIST_PARTS["PART-COST"].append(str(row[108:116].strip()))
                    DICT_HIST_PARTS["PART-SALE"].append(str(row[117:].strip()))                            
                    
            idx += 1

        #END WHILE
        if loglevel==logging.DEBUG:
            logger.debug("ParseV1 Completed...DICT_HIST_1 :"+str((DICT_HIST_1)))
            logger.debug("ParseV1 Completed...DICT_HIST_2 :"+str((DICT_HIST_2)))
            logger.debug("ParseV1 Completed...DICT_HIST_OPCODES :"+str((DICT_HIST_OPCODES)))
            logger.debug("Parsev1 Completed...DICT_HIST_3 :"+str((DICT_HIST_3)))
            logger.debug("Parsev1 Completed...DICT_HIST_LINES_1 :"+str((DICT_HIST_LINES_1)))
            logger.debug("ParseV1 Completed...DICT_HIST_LINES_2 :"+str((DICT_HIST_LINES_2)))
            logger.debug("ParseV1 Completed...DICT_HIST_LINES_3 :"+str((DICT_HIST_LINES_3)))
            logger.debug("ParseV1 Completed...DICT_HIST_LINES_4 :"+str((DICT_HIST_LINES_4)))
            logger.debug("ParseV1 Completed...DICT_HIST_LINES_5 :"+str((DICT_HIST_LINES_5)))  
            logger.debug("ParseV1 Completed...DICT_HIST_LINESCODES :"+str((DICT_HIST_LINESCODES)))  
            logger.debug("ParseV1 Completed...DICT_HIST_PARTS :"+str((DICT_HIST_PARTS)))  
            
         #covert dict to dataframe
        #print("Parse Completed...DICT_HIST_LINES_1 :"+str((DICT_HIST_LINES_1)))       
        DICT_HIST_1 = pad.DataFrame(DICT_HIST_1)        
        DICT_HIST_2 = pad.DataFrame(DICT_HIST_2)
        DICT_HIST_OPCODES = pad.DataFrame(DICT_HIST_OPCODES)
        DICT_HIST_3 = pad.DataFrame(DICT_HIST_3)
        DICT_HIST_LINES_1 = pad.DataFrame(DICT_HIST_LINES_1)
        DICT_HIST_LINES_2 = pad.DataFrame(DICT_HIST_LINES_2)
        DICT_HIST_LINES_3 = pad.DataFrame(DICT_HIST_LINES_3)
        DICT_HIST_LINES_4 = pad.DataFrame(DICT_HIST_LINES_4)
        DICT_HIST_LINES_5 = pad.DataFrame(DICT_HIST_LINES_5) 
        DICT_HIST_LINESCODES=pad.DataFrame(DICT_HIST_LINESCODES) 
        DICT_HIST_PARTS=pad.DataFrame(DICT_HIST_PARTS) 
        df_final=pad.DataFrame([])

        #Merge dataframe together
        if len(DICT_HIST_1)>0:
            df_final=DICT_HIST_1
        if len(df_final)>0 and len(DICT_HIST_2)>0:
            df_final = pad.merge(df_final,DICT_HIST_2, on=['VEH-ID','RO'], how='left')
        if len(df_final)>0 and len(DICT_HIST_3)>0:
            df_final = pad.merge(df_final,DICT_HIST_3, on=['VEH-ID','RO'], how='left')

        df_final_lines=   pad.DataFrame([]) 

        if len(DICT_HIST_LINES_1)>0:
            df_final_lines=DICT_HIST_LINES_1
        if len(df_final_lines)>0 and len(DICT_HIST_LINES_2)>0:
            df_final_lines = pad.merge(df_final_lines,DICT_HIST_LINES_2, on=['VEH-ID','RO'], how='left')
        if len(df_final_lines)>0 and len(DICT_HIST_LINES_3)>0:
            df_final_lines = pad.merge(df_final_lines,DICT_HIST_LINES_3, on=['VEH-ID','RO'], how='left')
        if len(df_final_lines)>0 and len(DICT_HIST_LINES_4)>0:
            df_final_lines = pad.merge(df_final_lines,DICT_HIST_LINES_4, on=['VEH-ID','RO'], how='left')
        if len(df_final_lines)>0 and len(DICT_HIST_LINES_5)>0:
            df_final_lines = pad.merge(df_final_lines,DICT_HIST_LINES_5, on=['VEH-ID','RO'], how='left')
         
        if len(df_final)>0 and len(df_final_lines)>0:
            df_final = pad.merge(df_final,df_final_lines, on=['VEH-ID','RO'], how='left')        
            
        df_final = df_final.replace(np.nan,'', regex=True) 
        #DICT_HIST_OPCODES = DICT_HIST_OPCODES.replace(np.nan,'', regex=True) 
        #DICT_HIST_LINESCODES = DICT_HIST_LINESCODES.replace(np.nan,'', regex=True) 

        if len(DICT_HIST_OPCODES)>0 and len(DICT_HIST_PARTS)>0:
            DICT_HIST_OPCODES = pad.merge(DICT_HIST_OPCODES,DICT_HIST_PARTS, on=['VEH-ID','RO','LINE-CODE','OP-CODES'], how='left')
            
        if len(DICT_HIST_OPCODES)>0 and len(DICT_HIST_LINESCODES)>0:
            DICT_HIST_OPCODES = pad.merge(DICT_HIST_OPCODES,DICT_HIST_LINESCODES, on=['VEH-ID','RO','LINE-CODE'], how='left')
        
        DICT_HIST_OPCODES = DICT_HIST_OPCODES.replace(np.nan,'', regex=True)    
         
        logger.debug("ro history Parsing done succesfully v1...")        
        return {"ro-hist":df_final,"hist-opcodes":DICT_HIST_OPCODES,'account_line':account_line}
    