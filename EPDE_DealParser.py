import numpy as np
import pandas as pad
import io
import logging 
import os

loglevel = int(os.environ['LOG_LEVEL'] )
logger = logging.getLogger('DealParser')
logger.setLevel(loglevel)
 
class DealParser():
    @classmethod
    def parse(self,fileDataAsStr):
        logger.debug("inside DealParser parse...")
        df_final=pad.DataFrame([]) 
        txtfil = io.StringIO(fileDataAsStr)           
        row = txtfil.readline()
        idx = 0
        #LIST FI-WIP STATUS SALETYPE SOLD 73 106 BUYER-BIRTH BUYER-CITY BUYER-NAME BUYER-NO. BUYER-PHONE1 BUYER-PHONE2 BUYER-STATE
        #FI-WIP.... S. SALE-TYPE. SOLD... FINANCE-CO. NAME.... APR-RATE-(FULL).... BIRTH.. CITY...... BUYER-NAME............... BUYER.... PHONE....... BUS.-PHONE.. ST
        fi_wip_1 : bool =False
        dict_FI_WIP_1={'FI-WIP':[],
                    'STATUS':[],
                    'SALE-TYPE':[],
                    'SOLD':[],
                    'FINANCE-CO-NAME':[],
                    'APR':[],
                    'BUYER-BIRTH':[],
                    'BUYER-CITY':[],
                    'BUYER-NAME':[], 
                    'BUYER-NO':[],
                    'BUYER-PHONE1':[],
                    'BUYER-PHONE2':[],
                    'BUYER-STATE':[]
                   } 
        #LIST FI-WIP CASH-D CASH-P COBUY-NAME COBUY-NO COBUY-PHONE1 COBUY-PHONE2 COLOR-NEW EMAIL1 EMAIL2 FI.MGR FINANCE-CHG FINANCE-TOTAL
        #FI-WIP.... CASH-DOWN CASH-PRICE CO-BUYER-NAME............ CO-BUYER. PHONE....... BUS.-PHONE.. COLOR..... EMAIL1.................................. EMAIL2.................................. FIMGR FINC-CHG. FINC-TOTL
        fi_wip_2 : bool =False
        dict_FI_WIP_2={'FI-WIP':[],
                    'CASH-D':[],
                    'CASH-P':[],
                    'COBUY-NAME':[],
                    'COBUY-NO':[], 
                    'COBUY-PHONE1':[], 
                    'COBUY-PHONE2':[], 
                    'COLOR-NEW':[], 
                    'EMAIL1':[], 
                    'EMAIL2':[], 
                    'FI-MGR':[], 
                    'FINANCE-CHG':[], 
                    'FINANCE-TOTAL':[] 
                    }
        #LIST FI-WIP 320.1 MONTHLY-PMT MSRPPL 52 ODOMETER-NEW SERIAL-NEW STOCK-NO. TERM TRADE1 TRADE2 320.1 BUYER-ZIP 114.1 104
        #FI-WIP.... DEAL TYPE PAYMENT.. MSRP......... NEW-USED MILES. SERIAL-NO........ VEHICLE-STOCK-NO... TERM STOCK-T1 STOCK-T2 DEAL TYPE ZIP.. TRADE1 NET VALUE TAX(1)-AMOUNT......
        fi_wip_3 : bool =False
        dict_FI_WIP_3={'FI-WIP':[],
                    'FIN-LSE':[],
                    'MONTHLY-PMT':[],
                    'MSRPPL':[],
                    'NEW-USED':[],
                    'ODOMETER-NEW':[],
                    'SERIAL-NEW':[],
                    'STOCK-NO':[],
                    'TERM':[],
                    'TRADE1':[],
                    'TRADE2':[],
                    'DEAL_TYPE':[],
                    'BUYER-ZIP':[],
                    'TRADE1-NET-VALUE':[],
                    'TAX-1-AMOUNT':[]                   
                   
                    }
        #LIST FI-WIP TRIMLVL VEH-MAKE VEH-MODEL YEAR-NEW BUYER-STREET BUYER-FIRST BUYER-LAST MIDDLE
        #FI-WIP.... TRIM........... MAKE...... MODEL..... YEAR STREET.............. FIRST NAME..... BUYER LAST NAME..... BUYER MIDDLE
        fi_wip_4 : bool =False
        dict_FI_WIP_4={'FI-WIP':[],
                    'TRIMLVL':[],
                    'VEH-MAKE':[],
                    'VEH-MODEL':[],
                    'YEAR-NEW':[],
                    'BUYER-STREET':[],
                    'BUYER-FIRST':[],
                    'BUYER-LAST':[],
                    'MIDDLE':[]
                    }
        #LIST FI-LEASE 8 13.1 62 64 67-Z 10 11
        #FI-LEASE.. APR-RATE-(FULL).... CASH CAP REDUCT. TOTAL-ADJ-CAP-COST. TOTAL-FIN.-CHARGE.. TOTAL-MONTHLY-PMTS. LEASE-END-VALUE.... LEASE-TERM.........
        fi_lease:bool=False
        dict_FI_LEASE={'FI-WIP':[],
                    'FI-WIP':[],
                    'APR-RATE-FULL':[],
                    'CASH-CAP-REDUCT':[],
                    'TOTAL-ADJ-CAP-COST':[],
                    'TOTAL-FIN-CHARGE':[],        
                    'TOTAL-MONTHLY-PMTS':[],
                    'LEASE-END-VALUE':[],
                    'LEASE-TERM':[],
                    'TOT-MNTH-PMT-W-TAX':[]                   
                    }
        #LIST FI.DAO DEAL WE.OWE.DESC WE.OWE.SALE WE.OWE.COST ID-SUPP
        #DEAL.... DESCRIPTION................... WE OWE SALE WE OWE COST
        fi_dao : bool =False
        dict_FI_DAO={'FI-WIP':[],
                     'DESCRIPTION':[],
                     'WE-OWE-SALE':[] ,
                     'WE-OWE-COST':[]                    
                   } 
        #LIST FI-AUX 15
        #FI-AUX.... IN SERVICE DATE....
        fi_aux : bool =False
        dict_FI_AUX={'FI-WIP':[],
                     'IN-SERVICE-DATE':[]              
                   } 
        #LIST CAR-INV STOCK-NO. STATUS 77 ENGINE VEH_STYLE SOLD-DATE ID-SUPP
        #STOCK-NO. STATUS COUNTRY. ENGINE......... VEH_STYLE...........  SOLD-DATE                                                                                                                                                     SALE $          
        car_inv : bool =False
        dict_car_inv={
                    'STOCK-NO':[],
                    'STATUS':[],
                    'COUNTRY':[],
                    'ENGINE':[],
                    'VEH_STYLE':[],
                    'SOLD-DATE':[],
                    'MILE':[]
                    }
        #LIST LABOR SERIAL STOCK-NO PROD-DATE CLOSE-DATE
        #LABOR..... SERIAL........... STOCK-NO.. PROD-DATE CLOSE-DATE
        labor : bool =False
        dict_labor={
                    'RO':[],
                    'SERIAL':[],
                    'STOCK-NO':[],
                    'PROD-DATE':[],
                    'CLOSE-DATE':[]
                    
                    }
        skip_line_startwith_list=[]
        skip_line_startwith_list.append('TERM')
        skip_line_startwith_list.append(':GET-LIST')
        skip_line_startwith_list.append('>SORT')
        skip_line_startwith_list.append('>LIST')
        skip_line_startwith_list.append(':LIST')
        skip_line_startwith_list.append(':')
        skip_line_startwith_list.append(':SSELECT')        
        skip_line_startwith_list.append('>')
        skip_line_startwith_list.append(':SSELECT') 
        skip_line_startwith_list.append('>SAVE-LIST') 
        skip_line_startwith_list.append(':MENU') 
       
        skip_line_startwith_list.append('FI-WIP.... S. SALE-TYPE. SOLD... FINANCE-CO. NAME.... APR-RATE-(FULL).... BIRTH..')
        skip_line_startwith_list.append('FI-WIP.... CASH-DOWN CASH-PRICE CO-BUYER-NAME............ CO-BUYER.')
        skip_line_startwith_list.append('FI-WIP.... DEAL TYPE PAYMENT.. MSRP......... NEW-USED MILES. SERIAL-NO........')
        skip_line_startwith_list.append('FI-WIP.... TRIM........... MAKE...... MODEL..... YEAR STREET....')
        skip_line_startwith_list.append('FI-LEASE.. APR-RATE-(FULL).... CASH CAP REDUCT. TOTAL-ADJ-CAP-COST. TOTAL-FIN.-CHARGE..')
        skip_line_startwith_list.append('DEAL.... DESCRIPTION................... WE OWE SALE')
        skip_line_startwith_list.append('FI-AUX.... IN SERVICE DATE....')
        skip_line_startwith_list.append('STOCK-NO. STATUS COUNTRY. ENGINE......... VEH_STYLE')
        skip_line_startwith_list.append('LABOR..... SERIAL........... STOCK-NO.. PROD-DATE')
         
        skip_line_contains_list=[]        
        skip_line_contains_list.append('ITEMS SELECTED')
        skip_line_contains_list.append('ITEM SELECTED')
        skip_line_contains_list.append('ITEM LISTED')
        skip_line_contains_list.append('ITEMS LISTED')
        skip_line_contains_list.append('FRAMES USED.')
        skip_line_contains_list.append('CATALOGED;')        
        #skip_line_contains_list.append(' PAGE    ')
        skip_line_contains_list.append("' NOT ON FILE")
        skip_line_contains_list.append("COUNT PRIVLIB")
        skip_line_contains_list.append("ITEMS COUNTED")
        skip_line_contains_list.append("ITEM COUNTED")
        skip_line_contains_list.append(' adp (') 
        
       
        line_end_text=['ITEMS LISTED.','ONE ITEM LISTED']        
        
        fi_wip_1_line=['FI-WIP.... S. SALE-TYPE. SOLD... FINANCE-CO. NAME.... APR-RATE-(FULL).... BIRTH..']
        fi_wip_2_line=['FI-WIP.... CASH-DOWN CASH-PRICE CO-BUYER-NAME............ CO-BUYER.']
        fi_wip_3_line=['FI-WIP.... DEAL TYPE PAYMENT.. MSRP......... NEW-USED MILES. SERIAL-NO........']
        fi_wip_4_line=['FI-WIP.... TRIM........... MAKE...... MODEL..... YEAR STREET....']
        fi_lease_line=['FI-LEASE.. APR-RATE-(FULL).... CASH CAP REDUCT. TOTAL-ADJ-CAP-COST. TOTAL-FIN.-CHARGE..']
        fi_aux_line=['FI-AUX.... IN SERVICE DATE....']
        fi_dao_line=['DEAL.... DESCRIPTION................... WE OWE SALE']
        car_inv_line=['STOCK-NO. STATUS COUNTRY. ENGINE......... VEH_STYLE']          
        labor_line=['LABOR..... SERIAL........... STOCK-NO.. PROD-DATE']       
        eof='ITEMS COUNTED'
        account_line=[] 
        while row:
            row = txtfil.readline()
            if row.__contains__('adp (') and row.strip().endswith(')'):
                account_line.append(row.strip())    
                                 
            if row.startswith(tuple(fi_wip_1_line)):
                fi_wip_1=True   
            if row.startswith(tuple(fi_wip_2_line)):
                fi_wip_2=True 
            if row.startswith(tuple(fi_wip_3_line)):
                fi_wip_3=True   
            if row.startswith(tuple(fi_wip_4_line)):
                fi_wip_4=True           
            if row.startswith(tuple(fi_lease_line)):
                fi_lease=True  
            if row.startswith(tuple(fi_dao_line)):
                fi_dao=True  
            if row.startswith(tuple(fi_aux_line)):
                fi_aux=True   
            if row.startswith(tuple(car_inv_line)):
                car_inv=True 
            if row.startswith(tuple(labor_line)):
                labor=True 
            if list(filter(row.__contains__, line_end_text)) != []:
                if(fi_wip_1==True):
                    fi_wip_1=False                    
                if(fi_wip_2==True):
                    fi_wip_2=False 
                if(fi_wip_3==True):
                    fi_wip_3=False     
                if(fi_wip_4==True):
                    fi_wip_4=False 
                if(fi_dao==True):
                    fi_dao=False
                if(fi_aux==True):
                    fi_aux=False  
                if(fi_lease==True):
                    fi_lease=False
                if(car_inv==True):
                    car_inv=False  
                if(labor==True):
                    labor=False  
           
            if row.strip() != "" and fi_wip_1==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
               
                if len(row[0:10].strip())==0:
                    if len(row[33:53].strip())>0:
                        dict_FI_WIP_1["FINANCE-CO-NAME"][(len(dict_FI_WIP_1["FINANCE-CO-NAME"])-1)] = (dict_FI_WIP_1["FINANCE-CO-NAME"][(len(dict_FI_WIP_1["FINANCE-CO-NAME"])-1)] + str(row[33:53]).rstrip()) 
                    if len(row[82:92].strip())>0:
                        dict_FI_WIP_1["BUYER-CITY"][(len(dict_FI_WIP_1["BUYER-CITY"])-1)] = (dict_FI_WIP_1["BUYER-CITY"][(len(dict_FI_WIP_1["BUYER-CITY"])-1)] + str(row[82:92]).rstrip()) 
                    if len(row[93:118].strip())>0:
                        dict_FI_WIP_1["BUYER-NAME"][(len(dict_FI_WIP_1["BUYER-NAME"])-1)] = (dict_FI_WIP_1["BUYER-NAME"][(len(dict_FI_WIP_1["BUYER-NAME"])-1)] + str(row[93:118]).rstrip()) 
                                     
                else:                    
                    dict_FI_WIP_1["FI-WIP"].append(str(row[0:10].strip()))
                    dict_FI_WIP_1["STATUS"].append(str(row[11:13].strip()))
                    dict_FI_WIP_1["SALE-TYPE"].append(str(row[14:24].strip()))
                    dict_FI_WIP_1["SOLD"].append(str(row[25:32].strip()))
                    dict_FI_WIP_1["FINANCE-CO-NAME"].append(str(row[33:53]))
                    dict_FI_WIP_1["APR"].append(str(row[54:73].strip()))
                    dict_FI_WIP_1["BUYER-BIRTH"].append(str(row[74:81].strip()))
                    dict_FI_WIP_1["BUYER-CITY"].append(str(row[82:92]))
                    dict_FI_WIP_1["BUYER-NAME"].append(str(row[93:118]))
                    dict_FI_WIP_1["BUYER-NO"].append(str(row[119:128].strip()))
                    dict_FI_WIP_1["BUYER-PHONE1"].append(str(row[129:141].strip()))
                    dict_FI_WIP_1["BUYER-PHONE2"].append(str(row[142:154].strip()))
                    dict_FI_WIP_1["BUYER-STATE"].append(str(row[155:].strip()))

            if row.strip() != "" and fi_wip_2==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
               
                if len(row[0:9].strip())==0:
                    if len(row[32:57].strip())>0:
                        dict_FI_WIP_2["COBUY-NAME"][(len(dict_FI_WIP_2["COBUY-NAME"])-1)] = (dict_FI_WIP_2["COBUY-NAME"][(len(dict_FI_WIP_2["COBUY-NAME"])-1)]+  str(row[32:57]).rstrip()) 
                    
                    if len(row[94:104].strip())>0:
                        dict_FI_WIP_2["COLOR-NEW"][(len(dict_FI_WIP_2["COLOR-NEW"])-1)] = (dict_FI_WIP_2["COLOR-NEW"][(len(dict_FI_WIP_2["COLOR-NEW"])-1)]+  str(row[94:104]).rstrip()) 
                       
                else:                    
                    dict_FI_WIP_2["FI-WIP"].append(str(row[0:9].strip())) 
                    dict_FI_WIP_2["CASH-D"].append(str(row[10:20].strip()))  
                    dict_FI_WIP_2["CASH-P"].append(str(row[21:31].strip()))                     
                    dict_FI_WIP_2["COBUY-NAME"].append(str(row[32:57]))  
                    dict_FI_WIP_2["COBUY-NO"].append(str(row[58:67].strip()))  
                    dict_FI_WIP_2["COBUY-PHONE1"].append(str(row[68:80].strip())) 
                    dict_FI_WIP_2["COBUY-PHONE2"].append(str(row[81:93].strip()))  
                    dict_FI_WIP_2["COLOR-NEW"].append(str(row[94:104]))  
                    dict_FI_WIP_2["EMAIL1"].append(str(row[105:145].strip())) 
                    dict_FI_WIP_2["EMAIL2"].append(str(row[146:186].strip()))  
                    dict_FI_WIP_2["FI-MGR"].append(str(row[187:191].strip()))  
                    dict_FI_WIP_2["FINANCE-CHG"].append(str(row[192:201].strip())) 
                    dict_FI_WIP_2["FINANCE-TOTAL"].append(str(row[202:].strip())) 
                 
            if row.strip() != "" and fi_wip_3==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                    dict_FI_WIP_3["FI-WIP"].append(str(row[0:10].strip()))    
                    dict_FI_WIP_3['FIN-LSE'].append(str(row[11:17].strip())) 
                    dict_FI_WIP_3['MONTHLY-PMT'].append(str(row[18:30].strip())) 
                    dict_FI_WIP_3['MSRPPL'].append(str(row[31:44].strip())) 
                    dict_FI_WIP_3['NEW-USED'].append(str(row[44:53].strip())) 
                    dict_FI_WIP_3['ODOMETER-NEW'].append(str(row[54:60].strip())) 
                    dict_FI_WIP_3['SERIAL-NEW'].append(str(row[61:78].strip())) 
                    dict_FI_WIP_3['STOCK-NO'].append(str(row[79:98].strip())) 
                    dict_FI_WIP_3['TERM'].append(str(row[99:103].strip())) 
                    dict_FI_WIP_3['TRADE1'].append(str(row[104:112].strip())) 
                    dict_FI_WIP_3['TRADE2'].append(str(row[113:121].strip())) 
                    dict_FI_WIP_3['DEAL_TYPE'].append(str(row[122:126].strip())) 
                    dict_FI_WIP_3['BUYER-ZIP'].append(str(row[127:137].strip()))   
                    dict_FI_WIP_3['TRADE1-NET-VALUE'].append(str(row[138:154].strip())) 
                    dict_FI_WIP_3['TAX-1-AMOUNT'].append(str(row[155:].strip())) 
                     

            if row.strip() != "" and fi_wip_4==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
               
                if len(row[0:10].strip())==0:
                    if len(row[27:37].strip())>0:
                        dict_FI_WIP_4["VEH-MAKE"][(len(dict_FI_WIP_4["VEH-MAKE"])-1)] = (dict_FI_WIP_4["VEH-MAKE"][(len(dict_FI_WIP_4["VEH-MAKE"])-1)]  + str(row[27:37]).rstrip()) 
                    if len(row[38:48].strip())>0:
                        dict_FI_WIP_4["VEH-MODEL"][(len(dict_FI_WIP_4["VEH-MODEL"])-1)] = (dict_FI_WIP_4["VEH-MODEL"][(len(dict_FI_WIP_4["VEH-MODEL"])-1)]  + str(row[38:48]).rstrip()) 
                    if len(row[54:74].strip())>0:
                        dict_FI_WIP_4["BUYER-STREET"][(len(dict_FI_WIP_4["BUYER-STREET"])-1)] = (dict_FI_WIP_4["BUYER-STREET"][(len(dict_FI_WIP_4["BUYER-STREET"])-1)]  + str(row[54:74]).rstrip()) 
                    if len(row[75:90].strip())>0:
                        dict_FI_WIP_4["BUYER-FIRST"][(len(dict_FI_WIP_4["BUYER-FIRST"])-1)] = (dict_FI_WIP_4["BUYER-FIRST"][(len(dict_FI_WIP_4["BUYER-FIRST"])-1)]  + str(row[75:90]).rstrip()) 
                    if len(row[91:111].strip())>0:
                        dict_FI_WIP_4["BUYER-LAST"][(len(dict_FI_WIP_4["BUYER-LAST"])-1)] = (dict_FI_WIP_4["BUYER-LAST"][(len(dict_FI_WIP_4["BUYER-LAST"])-1)]  + str(row[91:111]).rstrip()) 
                    if len(row[112:124].strip())>0:
                        dict_FI_WIP_4["MIDDLE"][(len(dict_FI_WIP_4["MIDDLE"])-1)] = (dict_FI_WIP_4["MIDDLE"][(len(dict_FI_WIP_4["MIDDLE"])-1)]  + str(row[112:124]).rstrip()) 
                        
                else:                    
                    dict_FI_WIP_4["FI-WIP"].append(str(row[0:10].strip()))    
                    dict_FI_WIP_4['TRIMLVL'].append(str(row[11:26].strip())) 
                    dict_FI_WIP_4['VEH-MAKE'].append(str(row[27:37])) 
                    dict_FI_WIP_4['VEH-MODEL'].append(str(row[38:48])) 
                    dict_FI_WIP_4['YEAR-NEW'].append(str(row[49:53].strip())) 
                    dict_FI_WIP_4['BUYER-STREET'].append(str(row[54:74])) 
                    dict_FI_WIP_4['BUYER-FIRST'].append(str(row[75:90])) 
                    dict_FI_WIP_4['BUYER-LAST'].append(str(row[91:111])) 
                    dict_FI_WIP_4['MIDDLE'].append(str(row[112:124])) 


            if row.strip() != "" and fi_lease==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
               
                 
                    dict_FI_LEASE["FI-WIP"].append(str(row[0:10].strip()))    
                    dict_FI_LEASE["APR-RATE-FULL"].append(str(row[11:30].strip())) 
                    dict_FI_LEASE["CASH-CAP-REDUCT"].append(str(row[31:47].strip())) 
                    dict_FI_LEASE["TOTAL-ADJ-CAP-COST"].append(str(row[48:67].strip())) 
                    dict_FI_LEASE["TOTAL-FIN-CHARGE"].append(str(row[68:87].strip()))        
                    dict_FI_LEASE["TOTAL-MONTHLY-PMTS"].append(str(row[88:107].strip())) 
                    dict_FI_LEASE["LEASE-END-VALUE"].append(str(row[108:127].strip())) 
                    dict_FI_LEASE["LEASE-TERM"].append(str(row[128:147].strip())) 
                    dict_FI_LEASE["TOT-MNTH-PMT-W-TAX"].append(str(row[148:].strip()))                              
                  

            if row.strip() != "" and fi_dao==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
               
                if len(row[0:8].strip())==0:
                    if len(row[9:39].strip())>0 :                        
                        if len(row[40:51].strip())==0 and len(row[52:63].strip())==0: 
                            dict_FI_DAO["DESCRIPTION"][(len(dict_FI_DAO["DESCRIPTION"])-1)] = (dict_FI_DAO["DESCRIPTION"][(len(dict_FI_DAO["DESCRIPTION"])-1)]+" "+ str(row[9:39]).strip()) 
                        else:
                            dict_FI_DAO["DESCRIPTION"][(len(dict_FI_DAO["DESCRIPTION"])-1)] = (dict_FI_DAO["DESCRIPTION"][(len(dict_FI_DAO["DESCRIPTION"])-1)]+"||"+ str(row[9:39]).strip()) 
                            dict_FI_DAO["WE-OWE-SALE"][(len(dict_FI_DAO["WE-OWE-SALE"])-1)] = (dict_FI_DAO["WE-OWE-SALE"][(len(dict_FI_DAO["WE-OWE-SALE"])-1)] +"||"+ str(row[40:51].strip()))    
                            dict_FI_DAO["WE-OWE-COST"][(len(dict_FI_DAO["WE-OWE-COST"])-1)] = (dict_FI_DAO["WE-OWE-COST"][(len(dict_FI_DAO["WE-OWE-COST"])-1)] +"||"+ str(row[52:63].strip()))    
                       
                    
                else:                    
                    dict_FI_DAO["FI-WIP"].append(str(row[0:8].strip()))
                    if len(row[9:39].strip())>0 :
                        dict_FI_DAO["DESCRIPTION"].append(str(row[9:39]).strip())
                    else:
                        dict_FI_DAO["DESCRIPTION"].append(str(row[9:39]).strip())
                    dict_FI_DAO["WE-OWE-SALE"].append(str(row[40:51].strip()))
                    dict_FI_DAO["WE-OWE-COST"].append(str(row[52:62].strip()))
            if row.strip() != "" and fi_aux==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
               
                    dict_FI_AUX["FI-WIP"].append(str(row[0:10].strip()))    
                    dict_FI_AUX["IN-SERVICE-DATE"].append(str(row[11:].strip())) 
                     
            if row.strip() != "" and car_inv==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                
                if len(row[0:9].strip())==0 :                   
                        if len(row[17:25].strip())>0:
                            dict_car_inv["COUNTRY"][(len(dict_car_inv["COUNTRY"])-1)] = (dict_car_inv["COUNTRY"][(len(dict_car_inv["COUNTRY"])-1)]  + str(row[17:25]).rstrip()) 
                        if len(row[26:41].strip())>0:
                            dict_car_inv["ENGINE"][(len(dict_car_inv["ENGINE"])-1)] = (dict_car_inv["ENGINE"][(len(dict_car_inv["ENGINE"])-1)]  + str(row[26:41]).rstrip()) 
                        if len(row[42:62].strip())>0:
                            dict_car_inv["VEH_STYLE"][(len(dict_car_inv["VEH_STYLE"])-1)] = (dict_car_inv["VEH_STYLE"][(len(dict_car_inv["VEH_STYLE"])-1)]  + str(row[42:62]).rstrip()) 
                       
                else:
                        dict_car_inv["STOCK-NO"].append(str(row[0:9].strip()))
                        dict_car_inv["STATUS"].append(str(row[10:16].strip()))
                        dict_car_inv["COUNTRY"].append(str(row[17:25])) 
                        dict_car_inv["ENGINE"].append(str(row[26:41] )) 
                        dict_car_inv["VEH_STYLE"].append(str(row[42:62] )) 
                        dict_car_inv["SOLD-DATE"].append(str(row[63:72] ).strip())
                        dict_car_inv["MILE"].append(str(row[73:] ).strip())


            if row.strip() != "" and labor==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                
                        dict_labor["RO"].append(str(row[0:10]).strip())
                        dict_labor["STOCK-NO"].append(str(row[11:28]).strip())
                        dict_labor["SERIAL"].append(str(row[29:39]).strip())
                        dict_labor["PROD-DATE"].append(str(row[40:49]).strip()) 
                        dict_labor["CLOSE-DATE"].append(str(row[50:] ).strip()) 
                       
            idx += 1

        #END WHILE
        if loglevel==logging.DEBUG:            
           
            logger.debug("Parse Completed...dict_FI_WIP_1:"+str((dict_FI_WIP_1)))
            logger.debug("Parse Completed...dict_FI_WIP_2:"+str((dict_FI_WIP_2)))
            logger.debug("Parse Completed...dict_FI_WIP_3:"+str((dict_FI_WIP_3)))
            logger.debug("Parse Completed...dict_FI_WIP_4:"+str((dict_FI_WIP_4)))
            logger.debug("Parse Completed...dict_FI_LEASE:"+str((dict_FI_LEASE))) 
            logger.debug("Parse Completed...dict_FI_DAO:"+str((dict_FI_DAO))) 
            logger.debug("Parse Completed...dict_FI_AUX:"+str((dict_FI_AUX))) 
            logger.debug("Parse Completed...dict_car_inv:"+str((dict_car_inv))) 
            logger.debug("Parse Completed...dict_labor:"+str((dict_labor)))        
            
        
         #covert dict to dataframe              
          
        df_FI_WIP_1 = pad.DataFrame(dict_FI_WIP_1)
        df_FI_WIP_2 = pad.DataFrame(dict_FI_WIP_2)
        df_FI_WIP_3 = pad.DataFrame(dict_FI_WIP_3)
        df_FI_WIP_4 = pad.DataFrame(dict_FI_WIP_4)
        df_FI_LEASE = pad.DataFrame(dict_FI_LEASE)
        df_FI_DAO = pad.DataFrame(dict_FI_DAO)
        df_FI_AUX = pad.DataFrame(dict_FI_AUX)
        df_car_inv = pad.DataFrame(dict_car_inv)    
        df_labor= pad.DataFrame(dict_labor) 
        #Merge dataframe together
       
        df_FI_WIP_1 = df_FI_WIP_1.replace(np.nan,'', regex=True) 
        df_FI_WIP_2 = df_FI_WIP_2.replace(np.nan,'', regex=True)
        df_FI_WIP_3 = df_FI_WIP_3.replace(np.nan,'', regex=True) 
        df_FI_WIP_4 = df_FI_WIP_4.replace(np.nan,'', regex=True)  
        df_FI_LEASE = df_FI_LEASE.replace(np.nan,'', regex=True) 
        df_FI_DAO = df_FI_DAO.replace(np.nan,'', regex=True) 
        df_FI_AUX = df_FI_AUX.replace(np.nan,'', regex=True) 

        df_FI_WIP_MERGE=pad.DataFrame([]) 
        df_FI_WIP_MERGE=df_FI_WIP_1
        if len(df_FI_WIP_MERGE)>0 and len(df_FI_WIP_2)>0:
            df_FI_WIP_MERGE = pad.merge(df_FI_WIP_MERGE,df_FI_WIP_2, on='FI-WIP', how='left')
        if len(df_FI_WIP_MERGE)>0 and len(df_FI_WIP_3)>0:
            df_FI_WIP_MERGE = pad.merge(df_FI_WIP_MERGE,df_FI_WIP_3, on='FI-WIP', how='left')
        if len(df_FI_WIP_MERGE)>0 and len(df_FI_WIP_4)>0:
            df_FI_WIP_MERGE = pad.merge(df_FI_WIP_MERGE,df_FI_WIP_4, on='FI-WIP', how='left')

        if len(df_FI_WIP_MERGE)>0 and len(df_FI_DAO)>0:
            df_FI_WIP_MERGE = pad.merge(df_FI_WIP_MERGE,df_FI_DAO, on='FI-WIP', how='left')
        
        if len(df_FI_WIP_MERGE)>0 and len(df_FI_LEASE)>0:
            df_FI_WIP_MERGE = pad.merge(df_FI_WIP_MERGE,df_FI_LEASE, on='FI-WIP', how='left')

        if len(df_FI_WIP_MERGE)>0 and len(df_FI_AUX)>0:
            df_FI_WIP_MERGE = pad.merge(df_FI_WIP_MERGE,df_FI_AUX, on='FI-WIP', how='left')
        
        df_FI_WIP_MERGE = df_FI_WIP_MERGE.replace(np.nan,'', regex=True) 
 
        df_car_inv = df_car_inv.replace(np.nan,'', regex=True)         
        df_labor = df_labor.replace(np.nan,'', regex=True)    
        logger.debug("DEAL Parsing done succesfully...")        
        return {"car_inv":df_car_inv,"fi_wip":df_FI_WIP_MERGE,'labor':df_labor,'account_line':account_line}