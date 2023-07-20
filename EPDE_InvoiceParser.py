import numpy as np
import pandas as pad
import io
import logging 
import os

loglevel = int(os.environ['LOG_LEVEL'] )
logger = logging.getLogger('InvoiceParser')
logger.setLevel(loglevel)
 
class InvoiceParser():
    @classmethod
    def parse(self,fileDataAsStr):
        logger.debug("inside InvoiceParser parse...")
        df_final=pad.DataFrame([]) 
        txtfil = io.StringIO(fileDataAsStr)           
        row = txtfil.readline()
        idx = 0
       
        #LIST WIP REFER# SALE-TYPE RO-STATUS OPEN-DATE EPDE.PRINT.TIME CLOSED SA-R5 SA-NAME COMMENTS VIN YEAR MAKE MODEL CP-SUBL-SALE$ TAG-NO ID-SUPP
        #REFER#.. SALES TYPE.... STATUS OPEN-DATE EPDE.PRINT.TIME CLOSED. SA-R.. SA-NAME.................. COMMENTS........................... SERIAL NO........ YR MAKE........... MODEL..... CP SUB.. TAG-NO.
        #                                                                                                                                                                                      SALE $          
        wip_1 : bool =False
        dict_WIP_1={'REFER#':[],
                    'SALE-TYPE':[],
                    'RO-STATUS':[],
                    'OPEN-DATE':[],
                    'EPDE.PRINT.TIME':[],
                    'CLOSED':[],
                    'SA-R5':[],
                    'SA-NAME':[],
                    'COMMENTS':[],
                    'VIN':[],
                    'YEAR':[],
                    'MAKE':[],
                    'MODEL':[],
                    'CP-SUBL-SALE$':[],
                    'TAG-NO':[]                  
                    }
        #LIST WIP REFER# CP-LABOR-SALE$ CP-PART-SALE$ CP-MISC-SALE$ CP-LUBE-SALE$ CP-TAX-SALE$ CP-DEDUCT-SALE$ CP-TOTAL-SALE$ CP-SCHG-SALE$ ID-SUPP
        #REFER#.. CP LBR.. CP PTS.. CP MISC. CP LUBE. CP TAX.. CP DED.. CP TOT.. CP SCHG.
        #         SALE $   SALE $   SALE $   SALE $   SALE $   SALE $   SALE $   SALE $  
        wip_2 : bool =False
        dict_WIP_2={'REFER#':[],
                     'CP-LABOR-SALE$':[],
                     'CP-PART-SALE$':[],
                     'CP-MISC-SALE$':[],
                     'CP-LUBE-SALE$':[],
                     'CP-TAX-SALE$':[],
                     'CP-DEDUCT-SALE$':[],
                     'CP-TOTAL-SALE$':[],
                     'CP-SCHG-SALE$':[]
                   }
        #LIST WIP REFER# EPDE.CUST CUST-N1 CUST-N3 CITY-STATE-ZIP EPDE.WT.TOTAL PROMISED-DATE PROMISED-TIME EPDE.DEL.DATE EPDE.CLOSED.TIME ID-SUPP
        #REFER#.. EPDE.CUST... CUSTOMER LINE1.......... CUSTOMER LINE3.......... CITY-STATE-ZIP..................... WP TOT.. PROMISED-DATE PROMISED-TIME DEL.DATE EPDE.CLOSED.TIME
        #                                                                                                            SALE $                                                                                                                                                            SALE $                                                   
        wip_3 : bool =False
        dict_WIP_3={'REFER#':[],
                     'EPDE.CUST':[],
                     'CUST-N1':[],
                     'CUST-N3':[],
                     'CITY':[],
                     'STATE':[],
                     'ZIP':[],
                     'EPDE.WT.TOTAL':[],
                     'PROMISED-DATE':[],
                     'PROMISED-TIME':[],
                     'EPDE.DEL.DATE':[],
                     'EPDE.CLOSED.TIME':[] 
                    }
        
        #LIST WIP REFER# EPDE.STOCK EPDE.EMAIL EPDE.PHONE EPDE.SPC.INS EPDE.HOME.PHONE EPDE.BUS.PHONE EPDE.CELL.PHONE EPDE.DLR EPDE.ENG.NO ID-SUPP
        #REFER#.. STOCK#...... EMAIL.............................. PHONE....... SPECIAL-INSTRUCTIONS............... HOME-PHONE.. BUS-PHONE... CELL-PHONE.. DEALER-CD. ENG-NO....
      
        wip_4 : bool =False
        dict_WIP_4={ 'REFER#':[],
                     'EPDE.STOCK':[],
                     'EPDE.EMAIL':[],
                     'EPDE.PHONE':[],
                     "EPDE.CELL":[],
                     "EPDE.BUS":[],
                     "EPDE.HOME":[],
                     "EPDE.SPC.INS":[],
                     "EPDE.DLR":[],
                     "EPDE.ENG.NO":[]
                   }        
        
        #LIST LABOR RONUM LINE-CDS SVC-DESCS MILEAGE-IN MILEAGE-OUT LICENSE COLOR PAY-TYPE CAUSES PROD-DATE WARR-EXP-DATE WAITER ID-SUPP
        #RONUM... LC SERVICE DESCRIPTION................ MILEAGE-IN MILEAGE-OUT LICENSE... COLOR..... PAY-TYPE.. CAUSES............................. PROD-DATE WARR-EXP-DATE WAITER....

        labor: bool =False
        dict_labor={ 'REFER#':[],                     
                     'LINE-CDS':[],
                     'SVC-DESCS':[],
                     'MILEAGE-IN':[],
                     'MILEAGE-OUT':[],
                     'LICENSE':[],
                     'COLOR':[],
                     'PAY-TYPE':[],
                     'CAUSES':[],
                     'PROD-DATE':[],
                     'WARR-EXP-DATE':[],
                     'WAITER':[]                                 
                   }
        #LIST RO-PUNCH-TIMES LINES TECH TYPE DATE START-TIME FINISH-TIME DUR
        #RO-PUNCH-TIMES LINE(S)... TECH-NO TYPE. DATE...... START-TIME FINISH-TIME DUR.        
        ro_rop: bool =False            
        dict_rop={
                  'REFER#':[],
                  'LINE-CDS':[],
                  'TECH-NO':[],
                  'TYPE':[],
                  'DATE':[],
                  'START-TIME':[],
                  'FINISH-TIME':[],
                  'DURATION':[]
        }
        #LIST LABOR-OPS RO LINE-CD OP-CODE SVC-DESC LBR-SALE-ACCT COST$ SALE$ ACTUAL-HR HR-SOLD TECH-NO CR.CO LBR-SALE-CTRL-NO LBR-TYPE 2 3 ID-SUPP 
        #RO...... LC OP-CODE... SERVICE DESCRIPTION................ LBR-SALE-ACCT COST$... SALE$... ACTUAL-HR HR-SOLD TECH. CR. LBR-SALE-CTRL-NO LBR-TYPE A/AMC S/NAME....
        labor_ops:bool =False
        dict_labor_ops={'REFER#':[],
                     'LINE-CDS':[],
                     'OP-CODE':[],
                     'SVC-DESC':[],
                     'LBR-SALE-ACCT':[],
                     'COST$':[],
                     'SALE$':[],
                     'ACTUAL-HR':[],
                     'HR-SOLD':[] ,
                     'TECH-NO':[],
                     'CR.CO' :[],
                     'LBR-SALE-CTRL-NO':[],                     
                     'LBR-TYPE':[] ,
                     'COST-AMT' :[],
                     'SALE-AMT' :[] 
                       }
        #LIST LABOR-OPS RO LINE-CD LOP-SEQ-NO OP-CODE PTS-SEQ-NOS ID-SUPP
        #RO...... LC LOP OP-CODE... PTS
        #            SEQ            SEQ
        labor_ops_seq:bool =False
        dict_labor_ops_seq={
                     'REFER#':[],
                     'LINE-CDS':[],
                     'OP-CODE':[],
                     'SEQ-NO':[],
                     'PTS-SEQ-NOS':[]
                      }
        #LIST RO-MLS RO LINE-CD OP-CODE SVC-DESC LBR-SALE-ACCT COST$ SALE$ SALE.CO LBR-SALE-CTRL-NO FEE.ID 1 2 3 MLS-NO MCD-NOS ID-SUPP
        #RO...... LC OP-CODE... SERVICE DESCRIPTION................ LBR-SALE-ACCT COST$... SALE$... SLE LBR-SALE-CTRL-NO FEE ID.............. D/CODE.. A/AMC S/NAME.... MLS MCD-NOS
        #                                                                                 CO                                                                 
        ro_mls:bool =False
        dict_ro_mls={'REFER#':[],
                     'LINE-CDS':[],
                     'OP-CODE':[],
                     'SVC-DESC':[],
                     'LBR-SALE-ACCT':[],
                     'COST$':[],
                     'SALE$':[],
                     'SALE.CO':[],
                     'LBR-SALE-CTRL-NO':[],  
                     'FEE.ID':[] ,
                     'MLS-TYPE':[],
                     'COST-AMT' :[],                                      
                     'SALE-AMT':[] ,
                     'MLS-NO':[],
                     'MCD-NOS' :[] ,
                     'DISC.ID':[]             
                     
                   }
        #LIST PARTS REF# 15 PART-NO DESC Q.O. COST SALE LIST T-SALE TOTAL$ SALE-ACCT SALE.CO Q.B. EPDE.PARTS.COMMENT 8 9 11 LINE ID-SUPP
        #REFER#.... LINE...... PART NO......... DESC.............. Q.O. COST... SALE.... LIST.... SALE.... TOTAL$.. SALE-ACCT SLE Q.B. EPDE.PARTS.NOTE............... V/CORR.... V/TYP V/MIN LINE
        parts:bool =False
        dict_parts={'REFER#':[],
                     'LINE-CDS':[],
                     'PART-NO':[],
                     'DESC':[],
                     'Q.O.':[],
                     'COST':[] ,
                     'SALE':[],
                     'LIST':[] ,                    
                     'T-SALE':[] ,                   
                     'TOTAL$':[] ,
                     'SALE-ACCT':[] ,
                     'SALE.CO':[] ,
                     'Q.B.':[] ,
                     'EPDE.PARTS.NOTE':[],
                     'COST-AMT':[],
                     'SALE-AMT':[],
                     'LIST-AMT':[],
                     'SEQ-NO':[]
                   }
        #LIST STORY RO LINE-CD TEXT TIME DATE EMP-NAME EMP-NO ID-SUPP
        #RO...... LC TECH STORY................................... TIME.. DATE..... NAME................ EMP-NO
        story:bool =False
        dict_story={'REFER#':[],
                     'LINE-CDS':[],
                     'TECH-STORY':[],
                     'STORY-TIME':[],
                     'STORY-DATE':[],
                     'TECH-NAME':[] ,
                     'TECH-EMP-NO':[]                     
                   }
        #LIST SV.LINE.EST RO.PARENT LINE.CODE LINE.EST.TTL ID-SUPP
        #RO...... LINE TOTAL$.....
        estimate:bool=False
        dict_est={   'REFER#':[],
                     'LINE-CDS':[],
                     'TOTAL':[]
                }

        skip_line_startwith_list=[]
        skip_line_startwith_list.append(':GET-LIST')
        skip_line_startwith_list.append('>SORT')
        skip_line_startwith_list.append(':')
        skip_line_startwith_list.append(':SSELECT')        
        skip_line_startwith_list.append('>')
        skip_line_startwith_list.append('REFER#.. SALES TYPE.... STATUS OPEN-DATE EPDE.PRINT.TIME')
        skip_line_startwith_list.append('REFER#.. CP LBR.. CP PTS.. CP MISC.')
        skip_line_startwith_list.append('         SALE $   SALE $   SALE $   SALE $   SALE $   SALE $   SALE $   SALE $')
        skip_line_startwith_list.append('REFER#.. EPDE.CUST... CUSTOMER LINE1..........')
        skip_line_startwith_list.append('                                                                                                            SALE $')
        skip_line_startwith_list.append('REFER#.. STOCK#...... EMAIL.............................. PHONE....... SPECIAL-INSTRUCTIONS............... HOME-')
        skip_line_startwith_list.append('RONUM... LC SERVICE DESCRIPTION................')
        skip_line_startwith_list.append('RO-PUNCH-TIMES LINE(S)... TECH-NO TYPE. DATE...... START-TIME FINISH-TIME')
        skip_line_startwith_list.append('RO...... LC OP-CODE... SERVICE DESCRIPTION................ LBR-SALE-ACCT')
        skip_line_startwith_list.append('                                                                                                              NO   CO')
        skip_line_startwith_list.append('RO...... LC OP-CODE... SERVICE DESCRIPTION................ LBR-SALE-ACCT')
        skip_line_startwith_list.append('                                                                                           CO')
        skip_line_startwith_list.append('REFER#.... LINE...... PART NO......... DESC.............. Q.O.')
        skip_line_startwith_list.append('                                                                                                                     CO')
        skip_line_startwith_list.append('RO...... LC TECH STORY................................... TIME..')
        skip_line_startwith_list.append('            SEQ            SEQ')
        skip_line_startwith_list.append('RO...... LC LOP OP-CODE... PTS')
        skip_line_startwith_list.append('RO...... LINE TOTAL$.....')

        skip_line_contains_list=[]
        skip_line_contains_list.append('ITEMS SELECTED')
        skip_line_contains_list.append('ITEMS LISTED')
        skip_line_contains_list.append('FRAMES USED.')
        skip_line_contains_list.append('CATALOGED;')
        skip_line_contains_list.append('    NO   CO')
        skip_line_contains_list.append('       SALE $')
        skip_line_contains_list.append('SALE $   SALE $   SALE $   SALE $   SALE $   SALE $   SALE $   SALE $')
        #skip_line_contains_list.append(' PAGE    ')
        skip_line_contains_list.append("' NOT ON FILE")
        skip_line_contains_list.append('SEQ            SEQ')

       
        line_end_text=['ITEMS LISTED.']
 
        wip_1_line=['REFER#.. SALES TYPE.... STATUS OPEN-DATE EPDE.PRINT.TIME CLOSED. SA-R']
        wip_2_line=['REFER#.. CP LBR.. CP PTS.. CP MISC.']
        wip_3_line=['REFER#.. EPDE.CUST... CUSTOMER LINE1..........']
        wip_4_line=['REFER#.. STOCK#...... EMAIL.............................. PHONE....... SPECIAL-INSTRUCTIONS']
        
        labor_line=['RONUM... LC SERVICE DESCRIPTION................ MILEAGE-IN']
        
        ro_rop_line=['RO-PUNCH-TIMES LINE(S)... TECH-NO TYPE. DATE...... START-TIME FINISH-TIME']
        
        labor_ops_line=['RO...... LC OP-CODE... SERVICE DESCRIPTION................ LBR-SALE-ACCT COST$... SALE$... ACTUAL-HR']
        ro_mls_line=['RO...... LC OP-CODE... SERVICE DESCRIPTION................ LBR-SALE-ACCT COST$... SALE$... SLE LBR-SALE-CTRL-NO']
        parts_line=['REFER#.... LINE...... PART NO......... DESC.............. Q.O. COST... SALE....']
        story_line=['RO...... LC TECH STORY................................... TIME..']
        labor_ops_seq_line=['RO...... LC LOP OP-CODE... PTS']
        est_line=['RO...... LINE TOTAL$.....']
        eof='ITEMS COUNTED'
        account_line=[] 
        while row:
            row = txtfil.readline()
            if row.__contains__('adp (') and row.strip().endswith(')'):
                 account_line.append(row.strip())   
                       
            if row.startswith(tuple(wip_1_line)):
                wip_1=True 
               
            if row.startswith(tuple(wip_2_line)):
                wip_2=True    
             
            if row.startswith(tuple(wip_3_line)):
                wip_3=True  

            if row.startswith(tuple(wip_4_line)):
                wip_4=True  

            if row.startswith(tuple(labor_line)):
                labor=True 

            if row.startswith(tuple(ro_rop_line)):
                ro_rop=True 

            if row.startswith(tuple(labor_ops_line)):
                labor_ops=True  

            if row.startswith(tuple(ro_mls_line)):
                ro_mls=True 

            if row.startswith(tuple(parts_line)):
                parts=True  

            if row.startswith(tuple(story_line)):
                story=True         
            if row.startswith(tuple(labor_ops_seq_line)):
                labor_ops_seq=True  
            if row.startswith(tuple(est_line)):
                estimate=True  

            if list(filter(row.__contains__, line_end_text)) != []:
                #logger.info("if line_end_text:True") 
                if(wip_1==True): 
                    wip_1=False 
                   
                if(wip_2==True):
                    wip_2=False  
                    
                if(wip_3==True):
                    wip_3=False 

                if(wip_4==True):
                    wip_4=False   

                if(labor==True): 
                    labor=False 

                if(ro_rop==True):
                    ro_rop=False 

                if(labor_ops==True):
                    labor_ops=False 

                if(ro_mls==True):
                    ro_mls=False 

                if(parts==True): 
                    parts=False 

                if(story==True): 
                    story=False
                         
                if(labor_ops_seq==True):
                    labor_ops_seq=False 

                if(estimate==True):
                    estimate=False 

            if row.strip() != "" and wip_1==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                #logger.info("wip_1 true found:"+str(row))    
                if len(row[0:8].strip())==0 :

                    if len(row[24:30].strip())>0:
                        sts=dict_WIP_1["RO-STATUS"][(len(dict_WIP_1["RO-STATUS"])-1)]
                        if len(sts.strip())>0:
                          dict_WIP_1["RO-STATUS"][(len(dict_WIP_1["RO-STATUS"])-1)] = (dict_WIP_1["RO-STATUS"][(len(dict_WIP_1["RO-STATUS"])-1)]+"," + str(row[24:30].strip())) 
                        else:
                          dict_WIP_1["RO-STATUS"][(len(dict_WIP_1["RO-STATUS"])-1)] = (dict_WIP_1["RO-STATUS"][(len(dict_WIP_1["RO-STATUS"])-1)] + str(row[24:30].strip())) 
                    if len(row[72:97].strip())>0:
                        dict_WIP_1["SA-NAME"][(len(dict_WIP_1["SA-NAME"])-1)] = (dict_WIP_1["SA-NAME"][(len(dict_WIP_1["SA-NAME"])-1)] + str(row[72:97])) 
                    if len(row[98:133].strip())>0:
                        dict_WIP_1["COMMENTS"][(len(dict_WIP_1["COMMENTS"])-1)] = (dict_WIP_1["COMMENTS"][(len(dict_WIP_1["COMMENTS"])-1)] +' '+ str(row[98:133])) 
                    if len(row[155:170].strip())>0:
                        dict_WIP_1["MAKE"][(len(dict_WIP_1["MAKE"])-1)] = (dict_WIP_1["MAKE"][(len(dict_WIP_1["MAKE"])-1)] + str(row[155:165].strip())) 
                    if len(row[166:176].strip())>0:
                        dict_WIP_1["MODEL"][(len(dict_WIP_1["MODEL"])-1)] = (dict_WIP_1["MODEL"][(len(dict_WIP_1["MODEL"])-1)] + str(row[166:176].strip()))
                         
                else:

                        dict_WIP_1["REFER#"].append(str(row[0:8].strip()))
                        dict_WIP_1["SALE-TYPE"].append(str(row[9:23].strip()))
                        dict_WIP_1["RO-STATUS"].append(str(row[24:30].strip()))
                        dict_WIP_1["OPEN-DATE"].append(str(row[31:40].strip()))
                        dict_WIP_1["EPDE.PRINT.TIME"].append(str(row[41:56].strip()))
                        dict_WIP_1["CLOSED"].append(str(row[57:64].strip()))
                        dict_WIP_1["SA-R5"].append(str(row[65:71].strip()))  
                        dict_WIP_1["SA-NAME"].append(str(row[72:97])) 
                        dict_WIP_1["COMMENTS"].append(str(row[98:133])) 
                        dict_WIP_1["VIN"].append(str(row[134:151].strip())) 
                        dict_WIP_1["YEAR"].append(str(row[152:154].strip())) 
                        dict_WIP_1["MAKE"].append(str(row[155:165].strip())) 
                        dict_WIP_1["MODEL"].append(str(row[166:176].strip())) 
                        dict_WIP_1["CP-SUBL-SALE$"].append(str(row[177:185].strip())) 
                        dict_WIP_1["TAG-NO"].append(str(row[186:].strip())) 
                         

            if row.strip() != "" and wip_2==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                dict_WIP_2["REFER#"].append(str(row[0:8].strip()))
                dict_WIP_2["CP-LABOR-SALE$"].append(str(row[9:17].strip()))
                dict_WIP_2["CP-PART-SALE$"].append(str(row[18:26].strip()))
                dict_WIP_2["CP-MISC-SALE$"].append(str(row[27:35].strip()))
                dict_WIP_2["CP-LUBE-SALE$"].append(str(row[36:44].strip()))
                dict_WIP_2["CP-TAX-SALE$"].append(str(row[45:53].strip()))
                dict_WIP_2["CP-DEDUCT-SALE$"].append(str(row[54:62].strip()))  
                dict_WIP_2["CP-TOTAL-SALE$"].append(str(row[63:71].strip())) 
                dict_WIP_2["CP-SCHG-SALE$"].append(str(row[72:].strip()))                
                 
                       
            if row.strip() != "" and  wip_3==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                if len(row[0:8].strip())==0 and len(row[9:21].strip())==0:
                    if len(row[22:46].strip())>0:
                        dict_WIP_3["CUST-N1"][(len(dict_WIP_3["CUST-N1"])-1)] = (dict_WIP_3["CUST-N1"][(len(dict_WIP_3["CUST-N1"])-1)] + str(row[22:46] ))
                    if len(row[47:71].strip())>0:
                        dict_WIP_3["CUST-N3"][(len(dict_WIP_3["CUST-N3"])-1)] = (dict_WIP_3["CUST-N3"][(len(dict_WIP_3["CUST-N3"])-1)] + str(row[47:71] )) 
                    if len(row[72:107].strip())>0:
                        dict_WIP_3["ZIP"][(len(dict_WIP_3["ZIP"])-1)] = (dict_WIP_3["ZIP"][(len(dict_WIP_3["ZIP"])-1)] + str(row[72:107].strip())) 
                     
                else:
                    dict_WIP_3["REFER#"].append(str(row[0:8].strip()))
                    dict_WIP_3["EPDE.CUST"].append(str(row[9:21].strip()))
                    dict_WIP_3["CUST-N1"].append(str(row[22:46] ))
                    dict_WIP_3["CUST-N3"].append(str(row[47:71] ))
                    #CITY-STATE-ZIP
                    city=""
                    state=""
                    zip=""
                    if len(row[72:107].strip())>0 :
                            csz=row[72:107].strip()
                            #if loglevel==logging.DEBUG:
                             #   logger.debug("..csz:"+str(csz))
                            arr_csz=csz.split(',')
                            if len(arr_csz)>=2:
                                city=arr_csz[0]
                                sz=arr_csz[1]
                                arr_sz=sz.strip().split(' ')
                                if len(arr_sz)>=2:
                                    state=arr_sz[0]
                                    zip=arr_sz[1]
                                else:
                                    state=sz
                                    zip="" 
                            else:
                                arr_csz=csz.strip().split(' ')
                                if len(arr_csz)>=3:
                                    city=arr_csz[0]
                                    state=arr_csz[1]
                                    zip=arr_csz[2]
                                else:  
                                    if len(arr_csz)>=2:
                                        city=arr_csz[0]
                                        state=arr_csz[1] 
                                        zip=""
                                    else: 
                                        city=csz
                                        state=""
                                        zip="" 
                                                          
                    dict_WIP_3["CITY"].append(str(city))
                    dict_WIP_3["STATE"].append(str(state))
                    dict_WIP_3["ZIP"].append(str(zip)) 
                    dict_WIP_3["EPDE.WT.TOTAL"].append(str(row[108:116].strip()))
                    dict_WIP_3["PROMISED-DATE"].append(str(row[117:130].strip()))
                    dict_WIP_3["PROMISED-TIME"].append(str(row[131:144].strip())) 
                    dict_WIP_3["EPDE.DEL.DATE"].append(str(row[145:153].strip()))
                    dict_WIP_3["EPDE.CLOSED.TIME"].append(str(row[154:].strip())) 
 
                    #if loglevel==logging.DEBUG:
                                #logger.debug("city:"+str(city)+",state:"+str(state)+",zip:"+str(zip))
            if row.strip() != "" and wip_4==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                #logger.info("wip_4 true found:"+str(row))    
                if len(row[0:8].strip())==0 :
                    if len(row[9:21].strip())>0:
                        dict_WIP_4["EPDE.STOCK"][(len(dict_WIP_4["EPDE.STOCK"])-1)] = (dict_WIP_4["EPDE.STOCK"][(len(dict_WIP_4["EPDE.STOCK"])-1)] + str(row[9:21].strip())) 
                    if len(row[22:57].strip())>0:
                        dict_WIP_4["EPDE.EMAIL"][(len(dict_WIP_4["EPDE.EMAIL"])-1)] = (dict_WIP_4["EPDE.EMAIL"][(len(dict_WIP_4["EPDE.EMAIL"])-1)] + str(row[22:57].strip())) 
                    if len(row[71:106].strip())>0:
                        dict_WIP_4["EPDE.SPC.INS"][(len(dict_WIP_4["EPDE.SPC.INS"])-1)] = (dict_WIP_4["EPDE.SPC.INS"][(len(dict_WIP_4["EPDE.SPC.INS"])-1)] +' '+ str(row[71:106]))      
                    if len(row[146:156].strip())>0:
                        dict_WIP_4["EPDE.DLR"][(len(dict_WIP_4["EPDE.DLR"])-1)] = (dict_WIP_4["EPDE.DLR"][(len(dict_WIP_4["EPDE.DLR"])-1)] + str(row[146:156].strip()))          
                    if len(row[157:].strip())>0:
                        dict_WIP_4["EPDE.ENG.NO"][(len(dict_WIP_4["EPDE.ENG.NO"])-1)] = (dict_WIP_4["EPDE.ENG.NO"][(len(dict_WIP_4["EPDE.ENG.NO"])-1)] + str(row[157:].strip()))          
                        
                else: 
                        dict_WIP_4["REFER#"].append(str(row[0:8].strip()))
                        dict_WIP_4["EPDE.STOCK"].append(str(row[9:21].strip()))
                        dict_WIP_4["EPDE.EMAIL"].append(str(row[22:57].strip()))
                        dict_WIP_4["EPDE.PHONE"].append(str(row[58:70].strip()))
                        dict_WIP_4["EPDE.SPC.INS"].append(str(row[71:106])) 
                        dict_WIP_4["EPDE.HOME"].append(str(row[107:119].strip())) 
                        dict_WIP_4["EPDE.BUS"].append(str(row[120:132].strip())) 
                        dict_WIP_4["EPDE.CELL"].append(str(row[133:145].strip())) 
                        dict_WIP_4["EPDE.DLR"].append(str(row[146:156].strip())) 
                        dict_WIP_4["EPDE.ENG.NO"].append(str(row[157:].strip())) 
                       
                      
                        
                         
                                  
            if row.strip() != "" and labor==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
             
                if len(row[0:8].strip())==0 :
                   
                    if  len(row[9:11].strip())==0:
                        if len(row[12:47].strip())>0:
                           dict_labor["SVC-DESCS"][(len(dict_labor["SVC-DESCS"])-1)] = (dict_labor["SVC-DESCS"][(len(dict_labor["SVC-DESCS"])-1)] +' '+ str(row[12:47]))
                        if len(row[104:139].strip())>0:
                           dict_labor["CAUSES"][(len(dict_labor["CAUSES"])-1)] = (dict_labor["CAUSES"][(len(dict_labor["CAUSES"])-1)] +' '+ str(row[104:139]))  
                        if len(row[82:92].strip())>0:
                           dict_labor["COLOR"][(len(dict_labor["COLOR"])-1)] = (dict_labor["COLOR"][(len(dict_labor["COLOR"])-1)] + str(row[82:92].strip()))    
                        if len(row[93:103].strip())>0:
                           dict_labor["PAY-TYPE"][(len(dict_labor["PAY-TYPE"])-1)] = (dict_labor["PAY-TYPE"][(len(dict_labor["PAY-TYPE"])-1)] + str(row[93:103]))          
                      
                    else:
                           dict_labor["REFER#"].append(dict_labor["REFER#"][(len(dict_labor["REFER#"])-1)])
                           dict_labor["LINE-CDS"].append(str(row[9:11].strip()))
                           dict_labor["SVC-DESCS"].append(str(row[12:47]))
                           dict_labor["CAUSES"].append(str(row[104:139])) 
                           dict_labor["MILEAGE-IN"].append(dict_labor["MILEAGE-IN"][(len(dict_labor["MILEAGE-IN"])-1)])
                           dict_labor["MILEAGE-OUT"].append(dict_labor["MILEAGE-OUT"][(len(dict_labor["MILEAGE-OUT"])-1)])
                           dict_labor["LICENSE"].append(dict_labor["LICENSE"][(len(dict_labor["LICENSE"])-1)])
                           dict_labor["COLOR"].append(dict_labor["COLOR"][(len(dict_labor["COLOR"])-1)])
                           dict_labor["PAY-TYPE"].append(dict_labor["PAY-TYPE"][(len(dict_labor["PAY-TYPE"])-1)])
                           dict_labor["PROD-DATE"].append(dict_labor["PROD-DATE"][(len(dict_labor["PROD-DATE"])-1)])
                           dict_labor["WARR-EXP-DATE"].append(dict_labor["WARR-EXP-DATE"][(len(dict_labor["WARR-EXP-DATE"])-1)])
                           dict_labor["WAITER"].append(dict_labor["WAITER"][(len(dict_labor["WAITER"])-1)])
                           
                else:                     
                    dict_labor["REFER#"].append(str(row[0:8].strip()))
                    dict_labor["LINE-CDS"].append(str(row[9:11].strip()))
                    dict_labor["SVC-DESCS"].append(str(row[12:47]))
                    dict_labor["MILEAGE-IN"].append(str(row[48:58].strip()))
                    dict_labor["MILEAGE-OUT"].append(str(row[59:70].strip()))
                    dict_labor["LICENSE"].append(str(row[71:81].strip()))
                    dict_labor["COLOR"].append(str(row[82:92].strip()))
                    dict_labor["PAY-TYPE"].append(str(row[93:103].strip()))
                    dict_labor["CAUSES"].append(str(row[104:139]))
                    dict_labor["PROD-DATE"].append(str(row[140:149].strip()))
                    dict_labor["WARR-EXP-DATE"].append(str(row[150:163].strip()))
                    dict_labor["WAITER"].append(str(row[164:].strip()))

            if row.strip() != "" and ro_rop==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                if len(row[0:14].strip())==0 :
                   
                    if  len(row[15:25].strip())==0:
                        if len(row[26:33].strip())>0:
                           dict_rop["TECH-NO"][(len(dict_rop["TECH-NO"])-1)] = (dict_rop["TECH-NO"][(len(dict_rop["TECH-NO"])-1)] + str(row[26:33].strip()))
                        
                    else:
                            dict_rop["REFER#"].append(dict_rop["REFER#"][(len(dict_rop["REFER#"])-1)])
                            dict_rop["LINE-CDS"].append(str(row[15:25].strip()))
                            dict_rop["TECH-NO"].append(str(row[26:33].strip()))
                            dict_rop["TYPE"].append(str(row[34:39].strip()))
                            dict_rop["DATE"].append(str(row[40:50].strip()))
                            dict_rop["START-TIME"].append(str(row[51:61].strip()))
                            dict_rop["FINISH-TIME"].append(str(row[62:73].strip()))
                            dict_rop["DURATION"].append(str(row[74:].strip()))
                                   
                else:                     
                    dict_rop["REFER#"].append(str(row[0:14].strip()))
                    dict_rop["LINE-CDS"].append(str(row[15:25].strip()))
                    dict_rop["TECH-NO"].append(str(row[26:33].strip()))
                    dict_rop["TYPE"].append(str(row[34:39].strip()))
                    dict_rop["DATE"].append(str(row[40:50].strip()))
                    dict_rop["START-TIME"].append(str(row[51:61].strip()))
                    dict_rop["FINISH-TIME"].append(str(row[62:73].strip()))
                    dict_rop["DURATION"].append(str(row[74:].strip()))
                  
                          

            if row.strip() != "" and row.strip() != "NO   CO" and labor_ops==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
             
                if len(row[0:8].strip())==0 :
                   
                    if  len(row[9:11].strip())==0:
                        if len(row[23:58].strip())>0:
                           dict_labor_ops["SVC-DESC"][(len(dict_labor_ops["SVC-DESC"])-1)] = (dict_labor_ops["SVC-DESC"][(len(dict_labor_ops["SVC-DESC"])-1)] +' '+ str(row[23:58]))
                        if len(row[109:114].strip())>0:
                           dict_labor_ops["TECH-NO"][(len(dict_labor_ops["TECH-NO"])-1)] = (dict_labor_ops["TECH-NO"][(len(dict_labor_ops["TECH-NO"])-1)] + str(row[109:114].strip()))   
                        if len(row[12:22].strip())>0:
                           dict_labor_ops["OP-CODE"][(len(dict_labor_ops["OP-CODE"])-1)] = (dict_labor_ops["OP-CODE"][(len(dict_labor_ops["OP-CODE"])-1)] + str(row[12:22].strip()))       
                        if len(row[145:150].strip())>0:
                           dict_labor_ops["COST-AMT"][(len(dict_labor_ops["COST-AMT"])-1)] = (dict_labor_ops["COST-AMT"][(len(dict_labor_ops["COST-AMT"])-1)] + str(row[145:150].strip()))
                        if len(row[151:].strip())>0:
                           dict_labor_ops["SALE-AMT"][(len(dict_labor_ops["SALE-AMT"])-1)] = (dict_labor_ops["SALE-AMT"][(len(dict_labor_ops["SALE-AMT"])-1)] + str(row[151:].strip()))
   
                else:
                    dict_labor_ops["REFER#"].append(str(row[0:8].strip()))
                    dict_labor_ops["LINE-CDS"].append(str(row[9:11].strip()))
                    dict_labor_ops["OP-CODE"].append(str(row[12:22].strip()))
                    dict_labor_ops["SVC-DESC"].append(str(row[23:58]))
                    dict_labor_ops["LBR-SALE-ACCT"].append(str(row[59:72].strip()))
                    dict_labor_ops["COST$"].append(str(row[73:81].strip()))
                    dict_labor_ops["SALE$"].append(str(row[82:90].strip()))
                    dict_labor_ops["ACTUAL-HR"].append(str(row[91:100].strip()))
                    dict_labor_ops["HR-SOLD"].append(str(row[101:108].strip()))
                    dict_labor_ops["TECH-NO"].append(str(row[109:114].strip()))
                    dict_labor_ops["CR.CO"].append(str(row[115:118].strip()))
                    dict_labor_ops["LBR-SALE-CTRL-NO"].append(str(row[119:135].strip()))
                    dict_labor_ops["LBR-TYPE"].append(str(row[136:144].strip()))
                    dict_labor_ops["COST-AMT"].append(str(row[145:150].strip()))
                    dict_labor_ops["SALE-AMT"].append(str(row[151:].strip()))
           
            if row.strip() != "" and row.strip() != "SEQ            SEQ" and labor_ops_seq==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
             
                if len(row[0:8].strip())==0 :
                   
                    if  len(row[9:11].strip())==0:
                        if len(row[16:26].strip())>0:
                           dict_labor_ops_seq["OP-CODE"][(len(dict_labor_ops_seq["OP-CODE"])-1)] = (dict_labor_ops_seq["OP-CODE"][(len(dict_labor_ops_seq["OP-CODE"])-1)] + str(row[16:26].strip()))       
                        if  len(row[12:15].strip())==0: 
                            if len(row[16:26].strip())==0:
                                if len(row[27:].strip())>0:
                                   dict_labor_ops_seq["PTS-SEQ-NOS"][(len(dict_labor_ops_seq["PTS-SEQ-NOS"])-1)] = (dict_labor_ops_seq["PTS-SEQ-NOS"][(len(dict_labor_ops_seq["PTS-SEQ-NOS"])-1)]+"," + str(row[27:].strip()))  

                        
                else:
                    dict_labor_ops_seq["REFER#"].append(str(row[0:8].strip()))
                    dict_labor_ops_seq["LINE-CDS"].append(str(row[9:11].strip()))
                    dict_labor_ops_seq["SEQ-NO"].append(str(row[12:15].strip()))
                    dict_labor_ops_seq["OP-CODE"].append(str(row[16:26].strip()))
                    dict_labor_ops_seq["PTS-SEQ-NOS"].append(str(row[27:].strip()))
                     

            if row.strip() != "" and row.strip() != "CO                                                                   NO" and ro_mls==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
             
                if len(row[0:8].strip())==0 :
                   
                    if  len(row[9:11].strip())==0:
                        if len(row[23:58].strip())>0:
                           dict_ro_mls["SVC-DESC"][(len(dict_ro_mls["SVC-DESC"])-1)] = (dict_ro_mls["SVC-DESC"][(len(dict_ro_mls["SVC-DESC"])-1)] +' '+ str(row[23:58]))
                        if len(row[133:141].strip())>0:
                           dict_ro_mls["MLS-TYPE"][(len(dict_ro_mls["MLS-TYPE"])-1)] = (dict_ro_mls["MLS-TYPE"][(len(dict_ro_mls["MLS-TYPE"])-1)] + str(row[133:141].strip()))   
                        if len(row[12:22].strip())>0:
                           dict_ro_mls["OP-CODE"][(len(dict_ro_mls["OP-CODE"])-1)] = (dict_ro_mls["OP-CODE"][(len(dict_ro_mls["OP-CODE"])-1)] + str(row[12:22].strip()))       
                        if len(row[112:132].strip())>0:
                           dict_ro_mls["FEE.ID"][(len(dict_ro_mls["FEE.ID"])-1)] = (dict_ro_mls["FEE.ID"][(len(dict_ro_mls["FEE.ID"])-1)] + str(row[112:132].strip()))       
                        if len(row[142:147].strip())>0:
                           dict_ro_mls["COST-AMT"][(len(dict_ro_mls["COST-AMT"])-1)] = (dict_ro_mls["COST-AMT"][(len(dict_ro_mls["COST-AMT"])-1)] + str(row[142:147].strip()))
                        if len(row[148:158].strip())>0:
                           dict_ro_mls["SALE-AMT"][(len(dict_ro_mls["SALE-AMT"])-1)] = (dict_ro_mls["SALE-AMT"][(len(dict_ro_mls["SALE-AMT"])-1)] + str(row[148:158].strip()))
                        if len(row[12:22].strip())==0:
                            if len(row[159:162].strip())==0:
                               if len(row[163:170].strip())>0:
                                  dict_ro_mls["MCD-NOS"][(len(dict_ro_mls["MCD-NOS"])-1)] = (dict_ro_mls["MCD-NOS"][(len(dict_ro_mls["MCD-NOS"])-1)]+"," + str(row[163:170].strip()))
                        if len(row[171:].strip())>0:
                           dict_ro_mls["DISC.ID"][(len(dict_ro_mls["DISC.ID"])-1)] = (dict_ro_mls["DISC.ID"][(len(dict_ro_mls["DISC.ID"])-1)] + str(row[171:].strip()))       
                        
                else:
                    
                    dict_ro_mls["REFER#"].append(str(row[0:8].strip()))
                    dict_ro_mls["LINE-CDS"].append(str(row[9:11].strip()))
                    dict_ro_mls["OP-CODE"].append(str(row[12:22].strip()))
                    dict_ro_mls["SVC-DESC"].append(str(row[23:58]))
                    dict_ro_mls["LBR-SALE-ACCT"].append(str(row[59:72].strip()))
                    dict_ro_mls["COST$"].append(str(row[73:81].strip()))
                    dict_ro_mls["SALE$"].append(str(row[82:90].strip()))
                    dict_ro_mls["SALE.CO"].append(str(row[91:94].strip()))
                    dict_ro_mls["LBR-SALE-CTRL-NO"].append(str(row[95:111].strip()))
                    dict_ro_mls["FEE.ID"].append(str(row[112:132].strip()))
                    dict_ro_mls["MLS-TYPE"].append(str(row[133:141].strip()))
                    dict_ro_mls["COST-AMT"].append(str(row[142:147].strip()))
                    dict_ro_mls["SALE-AMT"].append(str(row[148:158].strip()))  
                    dict_ro_mls["MLS-NO"].append(str(row[159:162].strip())) 
                    dict_ro_mls["MCD-NOS"].append(str(row[163:170].strip()))
                    dict_ro_mls["DISC.ID"].append(str(row[171:].strip()))
                     
                           
                   

            if row.strip() != "" and row.strip() != "CO." and parts==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                if len(row[0:10].strip())==0 :
              
                    if  len(row[22:38].strip())>0:
                        dict_parts["PART-NO"][(len(dict_parts["PART-NO"])-1)] = (dict_parts["PART-NO"][(len(dict_parts["PART-NO"])-1)] + str(row[22:38].strip()))
                         
                    if len(row[39:57].strip())>0:
                        dict_parts["DESC"][(len(dict_parts["DESC"])-1)] = (dict_parts["DESC"][(len(dict_parts["DESC"])-1)] +' '+ str(row[39:57]))
                    
                    if len(row[126:156].strip())>0:
                        dict_parts["EPDE.PARTS.NOTE"][(len(dict_parts["EPDE.PARTS.NOTE"])-1)] = (dict_parts["EPDE.PARTS.NOTE"][(len(dict_parts["EPDE.PARTS.NOTE"])-1)] +' '+ str(row[126:156]))

                    if len(row[157:167].strip())>0:
                        dict_parts["COST-AMT"][(len(dict_parts["COST-AMT"])-1)] = (dict_parts["COST-AMT"][(len(dict_parts["COST-AMT"])-1)] + str(row[157:167].strip()))

                    if len(row[168:173].strip())>0:
                        dict_parts["SALE-AMT"][(len(dict_parts["SALE-AMT"])-1)] = (dict_parts["SALE-AMT"][(len(dict_parts["SALE-AMT"])-1)] + str(row[168:173].strip()))

                    if len(row[174:179].strip())>0:
                        dict_parts["LIST-AMT"][(len(dict_parts["LIST-AMT"])-1)] = (dict_parts["LIST-AMT"][(len(dict_parts["LIST-AMT"])-1)] + str(row[174:179].strip()))
                               
                else:
                  
                    dict_parts["REFER#"].append(str(row[0:10].strip()))
                    dict_parts["LINE-CDS"].append(str(row[11:21].strip()))
                    dict_parts["PART-NO"].append(str(row[22:38].strip()))
                    dict_parts["DESC"].append(str(row[39:57]))
                    dict_parts["Q.O."].append(str(row[58:62].strip()))
                    dict_parts["COST"].append(str(row[63:70].strip()))
                    dict_parts["SALE"].append(str(row[71:79].strip()))
                    dict_parts["LIST"].append(str(row[80:88].strip()))
                    dict_parts["T-SALE"].append(str(row[89:97].strip()))
                    dict_parts["TOTAL$"].append(str(row[98:106].strip()))
                    dict_parts['SALE-ACCT'].append(str(row[107:116].strip()))
                    dict_parts["SALE.CO"].append(str(row[117:120].strip()))
                    dict_parts["Q.B."].append(str(row[121:125].strip()))
                    dict_parts["EPDE.PARTS.NOTE"].append(str(row[126:156]))
                    dict_parts["COST-AMT"].append(str(row[157:167].strip()))
                    dict_parts["SALE-AMT"].append(str(row[168:173].strip()))
                    dict_parts["LIST-AMT"].append(str(row[174:179].strip()))
                    dict_parts["SEQ-NO"].append(str(row[180:].strip()))   

            if row.strip() != ""   and story ==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
             
                if len(row[0:8].strip())==0 :
                   
                    if len(row[12:57].strip())>0:
                        dict_story["TECH-STORY"][(len(dict_story["TECH-STORY"])-1)] = (dict_story["TECH-STORY"][(len(dict_story["TECH-STORY"])-1)] +' '+ str(row[12:57]))
                    if len(row[75:95].strip())>0:
                        dict_story["TECH-NAME"][(len(dict_story["TECH-NAME"])-1)] = (dict_story["TECH-NAME"][(len(dict_story["TECH-NAME"])-1)] + str(row[75:95]))   
                    if len(row[96:].strip())>0:
                        dict_story["TECH-EMP-NO"][(len(dict_story["TECH-EMP-NO"])-1)] = (dict_story["TECH-EMP-NO"][(len(dict_story["TECH-EMP-NO"])-1)] + str(row[96:].strip()))   
                         
                else:
                    dict_story["REFER#"].append(str(row[0:8].strip()))
                    dict_story["LINE-CDS"].append(str(row[9:11].strip()))
                    dict_story["TECH-STORY"].append(str(row[12:57]))
                    dict_story["STORY-TIME"].append(str(row[58:64].strip()))
                    dict_story["STORY-DATE"].append(str(row[65:74].strip()))
                    dict_story["TECH-NAME"].append(str(row[75:95]))
                    dict_story["TECH-EMP-NO"].append(str(row[96:].strip()))

            if row.strip() != ""   and estimate ==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
             
                    dict_est["REFER#"].append(str(row[0:8].strip()))
                    dict_est["LINE-CDS"].append(str(row[9:13].strip()))
                    dict_est["TOTAL"].append(str(row[14:]))
                    

            idx += 1

        #END WHILE
        if loglevel==logging.DEBUG:
            logger.debug("Parse Completed...dict_WIP_1 :"+str((dict_WIP_1)))
            logger.debug("Parse Completed...dict_WIP_2:"+str((dict_WIP_2)))
            logger.debug("Parse Completed...dict_WIP_3:"+str((dict_WIP_3)))
            logger.debug("Parse Completed...dict_WIP_4:"+str((dict_WIP_4)))
            logger.debug("Parse Completed...dict_labor:"+str((dict_labor)))
            logger.debug("Parse Completed...dict_rop:"+str((dict_rop)))
            logger.debug("Parse Completed...dict_labor_ops:"+str((dict_labor_ops)))
            logger.debug("Parse Completed...dict_labor_ops_seq:"+str((dict_labor_ops_seq)))
            logger.debug("Parse Completed...dict_ro_mls:"+str((dict_ro_mls)))
            logger.debug("Parse Completed...dict_parts:"+str((dict_parts)))
            logger.debug("Parse Completed...dict_story:"+str((dict_story)))
            logger.debug("Parse Completed...dict_est:"+str((dict_est)))
         #covert dict to dataframe
               
        dict_WIP_1 = pad.DataFrame(dict_WIP_1)        
        dict_WIP_2 = pad.DataFrame(dict_WIP_2)
        dict_WIP_3 = pad.DataFrame(dict_WIP_3)
        dict_WIP_4 = pad.DataFrame(dict_WIP_4)
        dict_labor = pad.DataFrame(dict_labor)
        dict_rop = pad.DataFrame(dict_rop)
        dict_labor_ops = pad.DataFrame(dict_labor_ops)
        dict_labor_ops_seq = pad.DataFrame(dict_labor_ops_seq)
        dict_ro_mls = pad.DataFrame(dict_ro_mls)

        dict_parts = pad.DataFrame(dict_parts) 
        dict_story = pad.DataFrame(dict_story) 
        dict_est = pad.DataFrame(dict_est) 
        df_final=pad.DataFrame([])
        #Merge dataframe together
        if len(dict_WIP_1)>0 and len(dict_WIP_2)>0:
            df_final = pad.merge(dict_WIP_1,dict_WIP_2, on='REFER#', how='left')
            if len(df_final)>0 and len(dict_WIP_3)>0:
                df_final = pad.merge(df_final,dict_WIP_3, on='REFER#', how='left')
                if len(df_final)>0 and len(dict_WIP_4)>0:
                   df_final = pad.merge(df_final,dict_WIP_4, on='REFER#', how='left')
       
        if len(dict_labor)>0 and len(dict_est)>0:
            dict_labor = pad.merge(dict_labor,dict_est,on=["REFER#","LINE-CDS"] , how='left')
       
        if len(dict_labor_ops)>0 and len(dict_labor_ops_seq)>0:
            dict_labor_ops = pad.merge(dict_labor_ops,dict_labor_ops_seq,on=["REFER#","LINE-CDS","OP-CODE"] , how='left')
        #df_final = df_merge_1_2__3_4.replace('\r\n',' ', regex=True) 
        #df_final = df_final.replace('\n',' ', regex=True)
        #df_final = df_final.replace('\r',' ', regex=True)
        
        df_final = df_final.replace(np.nan,'', regex=True) 


        dict_parts = dict_parts.replace(np.nan,'', regex=True)   
        dict_labor = dict_labor.replace(np.nan,'', regex=True)   
        dict_rop = dict_rop.replace(np.nan,'', regex=True)  

        dict_labor_ops = dict_labor_ops.replace(np.nan,'', regex=True)  
         

        dict_ro_mls = dict_ro_mls.replace(np.nan,'', regex=True) 
        dict_story = dict_story.replace(np.nan,'', regex=True)   
       
        logger.debug("Invoice Parsing done succesfully...")        
        return {"ro":df_final,"labor":dict_labor,"ro_rop":dict_rop,"labor_ops" :dict_labor_ops,"parts":dict_parts,'story':dict_story,"ro_mls":dict_ro_mls,'account_line':account_line}