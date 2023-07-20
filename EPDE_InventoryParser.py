import numpy as np
import pandas as pad
import io
import logging 
import os

loglevel = int(os.environ['LOG_LEVEL'] )
logger = logging.getLogger('InventoryParser')
logger.setLevel(loglevel)
 
class InventoryParser():
    @classmethod
    def parse(self,fileDataAsStr):
        logger.debug("inside InventoryParser parse...")
        df_final=pad.DataFrame([]) 
        txtfil = io.StringIO(fileDataAsStr)           
        row = txtfil.readline()
        idx = 0
       
        #LIST CAR-INV SERIAL ENTRY YR MAKE MODEL COLOR STATUS STOCK STOCK-TYPE BALANCE I-COMPANY I-ACCT SVC-VEHID MILES DAYS DATE-SOLD ID-SUPP
        #SERIAL........... ENTRY.. YR MAKE.. MODEL..... COLOR..... STATUS STOCK STOCK-TYPE BALANCE.. CO. ACCOUNT SVC VEHID. MILES. DAYS  SOLD
        #                                                                                                                                                                                      SALE $          
        car_inv : bool =False
        dict_car_inv={
                    'VIN':[],
                    'ENTRY':[],
                    'YEAR':[],
                    'MAKE':[],
                    'MODEL':[],
                    'COLOR':[],
                    'STATUS':[],
                    'STOCK':[],
                    'STOCK-TYPE':[] ,
                    'BALANCE':[],
                    'I-COMPANY':[],
                    'I-ACCT':[],
                    'SVC-VEHID':[],
                    'MILES':[],
                    'DAYS':[],
                    'DATE-SOLD':[]
                    }

      
        # LIST FI-WIP 41 STATUS 4 SLVENTRY 33 5 8
        #FI-WIP.... SERIAL-NO-VEHICLE.. S. DATE-SOLD.......... ENTRY DT. VEHICLE-STOCK-NO... SALESPERSON NO. 1.. BUYER-NAME...............
        fi_wip : bool =False
        dict_FI_WIP={'FI-WIP-NO':[],
                     'VIN':[],
                     'STATUS':[],
                     'DATE-SOLD':[],
                     'ENTRY-DT':[],
                     'STOCK-NO':[],
                     'SALESPERSON-NO-1':[],
                     'BUYER-NAME':[]  ,
                     'FINANCE-CO-NAME':[] ,
                     'SALETYPE' :[]
                   } 

        #LIST FI.DAO WITH WE.OWE.DESC "FG3YR""FG5YR" DEAL STATUS STOCK-NO SLSP1N SOLD WE.OWE.DESC WE.OWE.SALE WE.OWE.COST ID-SUPP
        #DEAL.... STATUS STOCK NO SLSPERSON 1 NAME CONTRACT DT DESCRIPTION................... WE OWE SALE WE OWE COST
        fi_dao : bool =False
        dict_FI_DAO={'DEAL':[],
                     'DEAL-STATUS':[],
                     'STOCK-NO':[],
                     'SLSPERSON-1-NAME':[],
                     'CONTRACT-DT':[],
                     'DESCRIPTION':[],
                     'WE-OWE-SALE':[] ,
                     'WE-OWE-COST':[]                    
                   } 
        
        #LIST ACDB5/GL.JE.DTL BREAK-ON CNTL TOTAL POST.AMT.EXT ACCT DET-SUPP
        #ACDB5/GL.JE.DTL CNTL............. POST.AMT.EXT ACCT...
        acdb5_gl : bool =False
        dict_ACDB5_GL={'STOCK':[],
                     'POST.AMT.EXT':[],
                     'ACCT':[]                                     
                   } 
        #LIST ACDB5/GL.JE.DTL BREAK-ON CNTL TOTAL ACCT.TYPE POST.AMT.EXT ACCT DET-SUPP
        #ACDB5/GL.JE.DTL CNTL............. ACCT.TYPE POST.AMT.EXT ACCT...
        acdb5_gl_other : bool =False
        dict_ACDB5_GL_OTHER={'STOCK':[],
                     'POST.AMT.EXT-OTHER':[],
                     'ACCT-OTHER':[]                                     
                   } 
        #LIST WIP VIN OPEN-DATE CLOSE-DATE CP-TOTAL-COST$ CP-TOTAL-SALE$ TOTAL-COST TOTAL$ APPT-IDS APPT-DATE CUST-NAME RO-VEHID STATUS
        #WIP....... SERIAL NO........ OPEN-DATE CLOSE-DATE CP TOT.. CP TOT.. TOTAL COST TOTAL$... APPT-IDS...... APPT-DATE CUSTOMER NAME............ VEHID... STATUS..
        #                                                  COST $   SALE $                                                                                      
        wip_appt : bool =False
        dict_WIP_APPT={ 'REFER#':[],
                     'VIN':[],
                     'OPEN-DATE':[],
                     'CLOSE-DATE':[],
                     'CP-TOTAL-COST':[],
                     'CP-TOTAL-SALE':[],
                     'TOTAL-COST':[],
                     'TOTAL$':[],                  
                     'APPT-IDS':[],
                     'APPT-DATE':[],                    
                     'CUSTOMER-NAME':[],
                     'VEHID':[],
                     'STATUS':[]
                    }        
       
       
       
        #LIST LABOR SERIAL STOCK-NO LINE-CDS MILEAGE SVC-DESCS
        #LABOR..... SERIAL........... STOCK-NO.. LC MILEAGE SERVICE DESCRIPTION................
        labor_appt: bool =False
        dict_labor_appt={ 
                     'REFER#':[],                     
                     'VIN':[],
                     'STOCK-NO':[],
                     'LINE-CDS':[],
                     'MILEAGE':[],
                     'SVC-DESCS':[]                                         
                   }

        #LIST WIP VIN OPEN-DATE CLOSE-DATE CP-TOTAL-COST$ CP-TOTAL-SALE$ TOTAL-COST TOTAL$ CUST-NAME RO-VEHID STATUS
        #WIP....... SERIAL NO........ OPEN-DATE CLOSE-DATE CP TOT.. CP TOT.. TOTAL COST TOTAL$... CUSTOMER NAME............ VEHID... STATUS..
        #                                                  COST $   SALE $                                                             
        wip_labor : bool =False
        dict_WIP_LABOR={ 'REFER#':[],
                     'VIN':[],
                     'OPEN-DATE':[],
                     'CLOSE-DATE':[],
                     'CP-TOTAL-COST':[],
                     'CP-TOTAL-SALE':[],
                     'TOTAL-COST':[],
                     'TOTAL$':[],                  
                     'CUSTOMER-NAME':[],
                     'VEHID':[],
                     'STATUS':[]
                    }      
        
        #LIST LABOR SERIAL LINE-CDS STOCK-NO DEF-LBR-TYP MILEAGE SVC-DESCS
        #LABOR..... SERIAL........... LC STOCK-NO.. DEF....... MILEAGE SERVICE DESCRIPTION................
        labor_2: bool =False
        dict_labor_2={ 'REFER#':[],                     
                     'VIN':[],
                     'LINE-CDS':[],
                     'STOCK-NO':[],
                     'DEF-LBR-TYP':[],
                     'MILEAGE':[],                   
                     'SVC-DESCS':[]                         
                   }

        #LIST PO-ORDERS PO-NO PO-TYPE DESC RO-NO CREATE-DATE CLOSE-DATE AMOUNT STATUS VEND-NAME ID-SUPP
        #PO NUMBER. PO TYPE DESCRIPTION..... RO NO..... CREATED CLOSED. AMOUNT.... STATUS.. VENDOR NAME........................
        po_order: bool =False            
        dict_po_order={
                  'PO-NO':[],
                  'PO-TYPE':[],
                  'DESC':[],
                  'REFER#':[],
                  'CREATE-DATE':[],
                  'CLOSE-DATE':[],
                  'AMOUNT':[],
                  'STATUS':[],
                  'VEND-NAME':[]
        }
         
        skip_line_startwith_list=[]
        skip_line_startwith_list.append('TERM')
        skip_line_startwith_list.append(':GET-LIST')
        skip_line_startwith_list.append('>SORT')
        skip_line_startwith_list.append('>LIST')
        skip_line_startwith_list.append(':')
        skip_line_startwith_list.append(':SSELECT')        
        skip_line_startwith_list.append('>')
        skip_line_startwith_list.append(':SSELECT') 
        skip_line_startwith_list.append('>SAVE-LIST') 
        skip_line_startwith_list.append(':MENU') 
        skip_line_startwith_list.append(':MENU')
        skip_line_startwith_list.append('SERIAL........... ENTRY.. YR MAKE.. MODEL..... COLOR.....')
        skip_line_startwith_list.append('FI-WIP.... SERIAL-NO-VEHICLE.. S. DATE-SOLD.......... ENTRY DT.') 
        skip_line_startwith_list.append('DEAL.... STATUS STOCK NO SLSPERSON 1 NAME CONTRACT DT DESCRIPTION...............')
        skip_line_startwith_list.append('WIP....... SERIAL NO........ OPEN-DATE CLOSE-DATE CP TOT.. CP TOT.. TOTAL COST TOTAL$... APPT-IDS...... APPT-DATE')
        
        skip_line_startwith_list.append('LABOR..... SERIAL........... LC STOCK-NO.. MILEAGE SERVICE')

        skip_line_startwith_list.append('WIP....... SERIAL NO........ OPEN-DATE CLOSE-DATE CP TOT.. CP TOT.. TOTAL COST TOTAL$... CUSTOMER NAME...')
       
        skip_line_startwith_list.append('LABOR..... SERIAL........... LC STOCK-NO.. DEF....... MILEAGE SERVICE')
        skip_line_startwith_list.append('PO NUMBER. PO TYPE DESCRIPTION..... RO NO.') 
        skip_line_startwith_list.append('DEAL.... STATUS SLSPERSON 1 NAME CONTRACT')  
       
        skip_line_startwith_list.append('ACDB5/GL.JE.DTL CNTL............. POST.AMT.EXT') 
        
        skip_line_startwith_list.append('ACDB5/GL.JE.DTL CNTL............. ACCT.TYPE POST.AMT.EXT') 
        
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
        skip_line_contains_list.append('           COST $   SALE $     ')
        skip_line_contains_list.append('            LBRTYP') 
       
        line_end_text=['ITEMS LISTED.','ONE ITEM LISTED']
        
        car_inv_line=['SERIAL........... ENTRY.. YR MAKE.. MODEL..... COLOR..... STATUS STOCK']

        fi_wip_line=['FI-WIP.... SERIAL-NO-VEHICLE.. S. DATE-SOLD.......... ENTRY DT. VEHICLE-STOCK-NO... SALESPERSON NO. 1.. BUYER-NAME']
        fi_dao_line=['DEAL.... STATUS SLSPERSON 1 NAME CONTRACT DT DESCRIPTION']
         
       
        acdb5_gl_line=['ACDB5/GL.JE.DTL CNTL............. POST.AMT.EXT']

        acdb5_gl_line_other=['ACDB5/GL.JE.DTL CNTL............. ACCT.TYPE POST.AMT.EXT']
         

        wip_appt_line=['WIP....... SERIAL NO........ OPEN-DATE CLOSE-DATE CP TOT.. CP TOT.. TOTAL COST TOTAL$... APPT-IDS...... APPT-DATE']
        labor_appt_line=['LABOR..... SERIAL........... LC STOCK-NO.. MILEAGE SERVICE DESCRIPTION..']

        wip_labor_line=['WIP....... SERIAL NO........ OPEN-DATE CLOSE-DATE CP TOT.. CP TOT.. TOTAL COST TOTAL$... CUSTOMER']
        labor_2_line=['LABOR..... SERIAL........... LC STOCK-NO.. DEF.......']
        po_order_line=['PO NUMBER. PO TYPE DESCRIPTION..... RO NO..']
         
        eof='ITEMS COUNTED'
        account_line=[] 
        while row:
            row = txtfil.readline()
            if row.__contains__('adp (') and row.strip().endswith(')'):
                account_line.append(row.strip())   
                     
            if row.startswith(tuple(car_inv_line)):
                car_inv=True 
               
            if row.startswith(tuple(fi_wip_line)):
                fi_wip=True    
             
            if row.startswith(tuple(fi_dao_line)):
                fi_dao=True  
            if row.startswith(tuple(acdb5_gl_line)):
                acdb5_gl=True  

            if row.startswith(tuple(acdb5_gl_line_other)):
                acdb5_gl_other=True  

            if row.startswith(tuple(wip_appt_line)):
                wip_appt=True 

            if row.startswith(tuple(labor_appt_line)):
                labor_appt=True 

            if row.startswith(tuple(wip_labor_line)):
                wip_labor=True 
            if row.startswith(tuple(labor_2_line)):
                labor_2=True 
            if row.startswith(tuple(po_order_line)):
                po_order=True 

            if list(filter(row.__contains__, line_end_text)) != []:
                #logger.info("if line_end_text:True") 
                 
                   
                if(car_inv==True):
                    car_inv=False  
                    
                if(fi_wip==True):
                    fi_wip=False 

                if(fi_dao==True):
                    fi_dao=False

                if(acdb5_gl==True):
                    acdb5_gl=False

                if(acdb5_gl_other==True):
                    acdb5_gl_other=False

                if(wip_appt==True):
                    wip_appt=False   

                if(labor_appt==True): 
                    labor_appt=False 

                if(wip_labor==True):
                    wip_labor=False  

                if(labor_2==True): 
                    labor_2=False

                if(po_order==True):
                    po_order=False 
 
               
            if row.strip() != "" and car_inv==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                
                if len(row[0:17].strip())==0 :                   
                        if len(row[29:35].strip())>0 and len(dict_car_inv["MAKE"])>0:
                            dict_car_inv["MAKE"][(len(dict_car_inv["MAKE"])-1)] = (dict_car_inv["MAKE"][(len(dict_car_inv["MAKE"])-1)] + str(row[29:35].strip())) 
                        if len(row[36:46].strip())>0 and len(dict_car_inv["MODEL"])>0:
                            dict_car_inv["MODEL"][(len(dict_car_inv["MODEL"])-1)] = (dict_car_inv["MODEL"][(len(dict_car_inv["MODEL"])-1)] + str(row[36:46].strip()))
                        if len(row[47:57].strip())>0 and len(dict_car_inv["COLOR"])>0:
                            dict_car_inv["COLOR"][(len(dict_car_inv["COLOR"])-1)] = (dict_car_inv["COLOR"][(len(dict_car_inv["COLOR"])-1)] +' '+ str(row[47:57])) 
                        if len(row[71:81].strip())>0 and len(dict_car_inv["STOCK-TYPE"])>0:
                            dict_car_inv["STOCK-TYPE"][(len(dict_car_inv["STOCK-TYPE"])-1)] = (dict_car_inv["STOCK-TYPE"][(len(dict_car_inv["STOCK-TYPE"])-1)] + str(row[71:81].strip())) 
                        if len(row[81:91].strip())>0 and len(dict_car_inv["BALANCE"])>0:
                            dict_car_inv["BALANCE"][(len(dict_car_inv["BALANCE"])-1)] = (dict_car_inv["BALANCE"][(len(dict_car_inv["BALANCE"])-1)]  + str(row[81:91].strip())) 
                        if len(row[104:114].strip())>0 and len(dict_car_inv["SVC-VEHID"])>0:
                            dict_car_inv["SVC-VEHID"][(len(dict_car_inv["SVC-VEHID"])-1)] = (dict_car_inv["SVC-VEHID"][(len(dict_car_inv["SVC-VEHID"])-1)]  + str(row[104:114].strip())) 
                        if len(row[115:121].strip())>0 and len(dict_car_inv["MILES"])>0:
                            dict_car_inv["MILES"][(len(dict_car_inv["MILES"])-1)] = (dict_car_inv["MILES"][(len(dict_car_inv["MILES"])-1)]  + str(row[115:121].strip())) 
                        if len(row[127:].strip())>0 and len(dict_car_inv["DATE-SOLD"])>0:
                            dict_car_inv["DATE-SOLD"][(len(dict_car_inv["DATE-SOLD"])-1)] = (dict_car_inv["DATE-SOLD"][(len(dict_car_inv["DATE-SOLD"])-1)]  + str(row[127:].strip())) 
    
                else:
                        dict_car_inv["VIN"].append(str(row[0:17].strip()))
                        dict_car_inv["ENTRY"].append(str(row[18:25].strip()))
                        dict_car_inv["YEAR"].append(str(row[26:28].strip())) 
                        dict_car_inv["MAKE"].append(str(row[29:35].strip())) 
                        dict_car_inv["MODEL"].append(str(row[36:46].strip())) 
                        dict_car_inv["COLOR"].append(str(row[47:57]))                      
                        dict_car_inv["STATUS"].append(str(row[58:60].strip()))
                        dict_car_inv["STOCK"].append(str(row[61:70].strip()))
                        dict_car_inv["STOCK-TYPE"].append(str(row[71:80].strip()))
                        dict_car_inv["BALANCE"].append(str(row[81:91].strip()))
                        dict_car_inv["I-COMPANY"].append(str(row[92:95].strip())) 
                        dict_car_inv["I-ACCT"].append(str(row[96:103]).strip())                      
                        dict_car_inv["SVC-VEHID"].append(str(row[104:114].strip()))
                        dict_car_inv["MILES"].append(str(row[115:121].strip()))
                        dict_car_inv["DAYS"].append(str(row[122:126].strip()))
                        dict_car_inv["DATE-SOLD"].append(str(row[127:].strip()))
                   
                     
  
            if row.strip() != "" and fi_wip==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
               
                if len(row[0:10].strip())==0:
                    if len(row[84:103].strip())>0:
                        dict_FI_WIP["SALESPERSON-NO-1"][(len(dict_FI_WIP["SALESPERSON-NO-1"])-1)] = (dict_FI_WIP["SALESPERSON-NO-1"][(len(dict_FI_WIP["SALESPERSON-NO-1"])-1)]+" " + str(row[84:103])) 
                    if len(row[104:129].strip())>0:
                        dict_FI_WIP["BUYER-NAME"][(len(dict_FI_WIP["BUYER-NAME"])-1)] = (dict_FI_WIP["BUYER-NAME"][(len(dict_FI_WIP["BUYER-NAME"])-1)]+" " + str(row[104:129])) 
                    if len(row[130:150].strip())>0:
                        dict_FI_WIP["FINANCE-CO-NAME"][(len(dict_FI_WIP["FINANCE-CO-NAME"])-1)] = (dict_FI_WIP["FINANCE-CO-NAME"][(len(dict_FI_WIP["FINANCE-CO-NAME"])-1)]+ str(row[130:150]).strip()) 
                    if len(row[151:].strip())>0:
                        dict_FI_WIP["SALETYPE"][(len(dict_FI_WIP["SALETYPE"])-1)] = (dict_FI_WIP["SALETYPE"][(len(dict_FI_WIP["SALETYPE"])-1)]+ str(row[151:]).strip()) 
                        
                else:                    
                    dict_FI_WIP["FI-WIP-NO"].append(str(row[0:10].strip()))
                    dict_FI_WIP["VIN"].append(str(row[11:30].strip()))
                    dict_FI_WIP["STATUS"].append(str(row[31:33].strip()))            
                    dict_FI_WIP["DATE-SOLD"].append(str(row[34:53].strip()))
                    dict_FI_WIP["ENTRY-DT"].append(str(row[54:63].strip()))   
                    dict_FI_WIP["STOCK-NO"].append(str(row[64:83].strip()))
                    dict_FI_WIP["SALESPERSON-NO-1"].append(str(row[84:103]))  
                    dict_FI_WIP["BUYER-NAME"].append(str(row[104:129]))
                    dict_FI_WIP["FINANCE-CO-NAME"].append(str(row[130:150]).strip())
                    dict_FI_WIP["SALETYPE"].append(str(row[151:]).strip())

                    
                 

            if row.strip() != "" and fi_dao==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
               
                if len(row[0:8].strip())==0:
                    if len(row[16:32].strip())>0:
                        dict_FI_DAO["SLSPERSON-1-NAME"][(len(dict_FI_DAO["SLSPERSON-1-NAME"])-1)] = (dict_FI_DAO["SLSPERSON-1-NAME"][(len(dict_FI_DAO["SLSPERSON-1-NAME"])-1)]+" "+ str(row[16:32])) 
                    
                    if len(row[45:75].strip())>0 :                        
                        if len(row[76:87].strip())==0 and len(row[88:99].strip())==0: 
                            dict_FI_DAO["DESCRIPTION"][(len(dict_FI_DAO["DESCRIPTION"])-1)] = (dict_FI_DAO["DESCRIPTION"][(len(dict_FI_DAO["DESCRIPTION"])-1)]+" "+ str(row[45:75]).strip()) 
                        else:
                            dict_FI_DAO["DESCRIPTION"][(len(dict_FI_DAO["DESCRIPTION"])-1)] = (dict_FI_DAO["DESCRIPTION"][(len(dict_FI_DAO["DESCRIPTION"])-1)]+"||"+ str(row[45:75]).strip()) 
                            dict_FI_DAO["WE-OWE-SALE"][(len(dict_FI_DAO["WE-OWE-SALE"])-1)] = (dict_FI_DAO["WE-OWE-SALE"][(len(dict_FI_DAO["WE-OWE-SALE"])-1)] +"||"+ str(row[76:87].strip()))    
                            dict_FI_DAO["WE-OWE-COST"][(len(dict_FI_DAO["WE-OWE-COST"])-1)] = (dict_FI_DAO["WE-OWE-COST"][(len(dict_FI_DAO["WE-OWE-COST"])-1)] +"||"+ str(row[88:99].strip()))    

                        
                    
                else:                    
                    dict_FI_DAO["DEAL"].append(str(row[0:8].strip()))
                    dict_FI_DAO["DEAL-STATUS"].append(str(row[9:15].strip()))                  
                    dict_FI_DAO["SLSPERSON-1-NAME"].append(str(row[16:32]))
                    dict_FI_DAO["CONTRACT-DT"].append(str(row[33:44].strip()))
                    if len(row[45:75].strip())>0 :
                        dict_FI_DAO["DESCRIPTION"].append(str(row[45:75]).strip())
                    else:
                        dict_FI_DAO["DESCRIPTION"].append(str(row[45:75]).strip())
                    dict_FI_DAO["WE-OWE-SALE"].append(str(row[76:87].strip()))
                    dict_FI_DAO["WE-OWE-COST"].append(str(row[88:97].strip()))
                    dict_FI_DAO["STOCK-NO"].append(str(row[98:].strip()))

          
            if row.strip() != "" and acdb5_gl==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
               
                if len(row[0:33].strip())==0:
                    if len(row[34:46].strip())>0:
                        dict_ACDB5_GL["POST.AMT.EXT"][(len( dict_ACDB5_GL["POST.AMT.EXT"])-1)] = ( dict_ACDB5_GL["POST.AMT.EXT"][(len( dict_ACDB5_GL["POST.AMT.EXT"])-1)]  + str(row[34:46].strip())) 
                    if len(row[47:].strip())>0:
                        dict_ACDB5_GL["ACCT"][(len(dict_ACDB5_GL["ACCT"])-1)] = (dict_ACDB5_GL["ACCT"][(len(dict_ACDB5_GL["ACCT"])-1)] + str(row[47:].strip())) 
                    
                else:                    
                    dict_ACDB5_GL["STOCK"].append(str(row[0:33].strip()))
                    dict_ACDB5_GL["POST.AMT.EXT"].append(str(row[34:46].strip()))
                    dict_ACDB5_GL["ACCT"].append(str(row[47:].strip()))    

            if row.strip() != "" and acdb5_gl_other==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
               
                if len(row[0:33].strip())==0:
                    if len(row[43:56].strip())>0:
                        dict_ACDB5_GL_OTHER["POST.AMT.EXT-OTHER"][(len( dict_ACDB5_GL_OTHER["POST.AMT.EXT-OTHER"])-1)] = ( dict_ACDB5_GL_OTHER["POST.AMT.EXT-OTHER"][(len( dict_ACDB5_GL_OTHER["POST.AMT.EXT-OTHER"])-1)]  + str(row[43:56].strip())) 
                    if len(row[57:].strip())>0:
                        dict_ACDB5_GL_OTHER["ACCT-OTHER"][(len(dict_ACDB5_GL_OTHER["ACCT-OTHER"])-1)] = (dict_ACDB5_GL_OTHER["ACCT-OTHER"][(len(dict_ACDB5_GL_OTHER["ACCT-OTHER"])-1)] + str(row[57:].strip())) 
                    
                else:                    
                    dict_ACDB5_GL_OTHER["STOCK"].append(str(row[0:33].strip()))
                    dict_ACDB5_GL_OTHER["POST.AMT.EXT-OTHER"].append(str(row[43:56].strip()))
                    dict_ACDB5_GL_OTHER["ACCT-OTHER"].append(str(row[57:].strip()))            
      
            if row.strip() != "" and wip_appt==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
               
                if len(row[0:10].strip())==0:
                    if len(row[89:103].strip())>0:
                        dict_WIP_APPT["APPT-IDS"][(len(dict_WIP_APPT["APPT-IDS"])-1)] = (dict_WIP_APPT["APPT-IDS"][(len(dict_WIP_APPT["APPT-IDS"])-1)]+" " + str(row[89:103])) 
                    if len(row[114:139].strip())>0:
                        dict_WIP_APPT["CUSTOMER-NAME"][(len(dict_WIP_APPT["CUSTOMER-NAME"])-1)] = (dict_WIP_APPT["CUSTOMER-NAME"][(len(dict_WIP_APPT["CUSTOMER-NAME"])-1)]+" " + str(row[114:139])) 
                    if len(row[140:148].strip())>0:
                        dict_WIP_APPT["VEHID"][(len(dict_WIP_APPT["VEHID"])-1)] = (dict_WIP_APPT["VEHID"][(len(dict_WIP_APPT["VEHID"])-1)] + str(row[140:148].strip())) 
             
                else:                    
                    dict_WIP_APPT["REFER#"].append(str(row[0:10].strip()))
                    dict_WIP_APPT["VIN"].append(str(row[11:28].strip()))
                    dict_WIP_APPT["OPEN-DATE"].append(str(row[29:38].strip()))
                    dict_WIP_APPT["CLOSE-DATE"].append(str(row[39:49].strip()))
                    dict_WIP_APPT["CP-TOTAL-COST"].append(str(row[50:58].strip()))
                    dict_WIP_APPT["CP-TOTAL-SALE"].append(str(row[59:67].strip()))
                    dict_WIP_APPT["TOTAL-COST"].append(str(row[68:78].strip()))
                    dict_WIP_APPT["TOTAL$"].append(str(row[79:88]))
                    dict_WIP_APPT["APPT-IDS"].append(str(row[89:103].strip()))
                    dict_WIP_APPT["APPT-DATE"].append(str(row[104:113].strip()))
                    dict_WIP_APPT["CUSTOMER-NAME"].append(str(row[114:139]))
                    dict_WIP_APPT["VEHID"].append(str(row[140:148].strip()))
                    dict_WIP_APPT["STATUS"].append(str(row[149:].strip()))

                 
            if row.strip() != "" and labor_appt==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
             
                if len(row[0:10].strip())==0 :
                   
                    if  len(row[29:31].strip())==0:
                        if len(row[43:50].strip())>0:
                           dict_labor_appt["MILEAGE"][(len(dict_labor_appt["MILEAGE"])-1)] = (dict_labor_appt["MILEAGE"][(len(dict_labor_appt["MILEAGE"])-1)]  + str(row[43:50].strip()))
                        if len(row[51:86].strip())>0:
                           dict_labor_appt["SVC-DESCS"][(len(dict_labor_appt["SVC-DESCS"])-1)] = (dict_labor_appt["SVC-DESCS"][(len(dict_labor_appt["SVC-DESCS"])-1)]+" "  + str(row[51:86]))
                           
                    else:
                        dict_labor_appt["REFER#"].append(dict_labor_appt["REFER#"][(len(dict_labor_appt["REFER#"])-1)])
                        dict_labor_appt["VIN"].append(dict_labor_appt["VIN"][(len(dict_labor_appt["VIN"])-1)])
                        dict_labor_appt["LINE-CDS"].append(str(row[29:31].strip()))   
                        dict_labor_appt["STOCK-NO"].append(dict_labor_appt["STOCK-NO"][(len(dict_labor_appt["STOCK-NO"])-1)])
                        dict_labor_appt["MILEAGE"].append(dict_labor_appt["MILEAGE"][(len(["MILEAGE"])-1)])
                        dict_labor_appt["SVC-DESCS"].append(str(row[51:86]))  
                else:                     
                    dict_labor_appt["REFER#"].append(str(row[0:10].strip()))
                    dict_labor_appt["VIN"].append(str(row[11:28].strip()))
                    dict_labor_appt["LINE-CDS"].append(str(row[29:31].strip()))   
                    dict_labor_appt["STOCK-NO"].append(str(row[32:42].strip()))                                    
                    dict_labor_appt["MILEAGE"].append(str(row[43:50].strip()))                 
                    dict_labor_appt["SVC-DESCS"].append(str(row[51:86])) 
                    
            if row.strip() != "" and wip_labor==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
               
                if len(row[0:10].strip())==0:
                    if len(row[89:114].strip())>0:
                        dict_WIP_LABOR["CUSTOMER-NAME"][(len(dict_WIP_LABOR["CUSTOMER-NAME"])-1)] = (dict_WIP_LABOR["CUSTOMER-NAME"][(len(dict_WIP_LABOR["CUSTOMER-NAME"])-1)]+" " + str(row[89:114])) 
                    if len(row[115:123].strip())>0:
                        dict_WIP_LABOR["VEHID"][(len(dict_WIP_LABOR["VEHID"])-1)] = (dict_WIP_LABOR["VEHID"][(len(dict_WIP_LABOR["VEHID"])-1)] + str(row[115:123].strip())) 
             
                else:                    
                    dict_WIP_LABOR["REFER#"].append(str(row[0:10].strip()))
                    dict_WIP_LABOR["VIN"].append(str(row[11:28].strip()))
                    dict_WIP_LABOR["OPEN-DATE"].append(str(row[29:38].strip()))
                    dict_WIP_LABOR["CLOSE-DATE"].append(str(row[39:49].strip()))
                    dict_WIP_LABOR["CP-TOTAL-COST"].append(str(row[50:58].strip()))
                    dict_WIP_LABOR["CP-TOTAL-SALE"].append(str(row[59:67].strip()))
                    dict_WIP_LABOR["TOTAL-COST"].append(str(row[68:78].strip()))
                    dict_WIP_LABOR["TOTAL$"].append(str(row[79:88].strip()))                     
                    dict_WIP_LABOR["CUSTOMER-NAME"].append(str(row[89:114]))
                    dict_WIP_LABOR["VEHID"].append(str(row[115:123].strip()))
                    dict_WIP_LABOR["STATUS"].append(str(row[124:].strip()))
   
            if row.strip() != "" and labor_2==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
             
                if len(row[0:10].strip())==0 :
                   
                    if  len(row[29:31].strip())==0:
                        if len(row[43:53].strip())>0:
                           dict_labor_2["DEF-LBR-TYP"][(len(dict_labor_2["DEF-LBR-TYP"])-1)] = (dict_labor_2["DEF-LBR-TYP"][(len(dict_labor_2["DEF-LBR-TYP"])-1)]  + str(row[43:53].strip()))
                        if len(row[54:61].strip())>0:
                           dict_labor_2["MILEAGE"][(len(dict_labor_2["MILEAGE"])-1)] = (dict_labor_2["MILEAGE"][(len(dict_labor_2["MILEAGE"])-1)]   + str(row[54:61].strip()))
                        
                        if len(row[62:97].strip())>0:
                           dict_labor_2["SVC-DESCS"][(len(dict_labor_2["SVC-DESCS"])-1)] = (dict_labor_2["SVC-DESCS"][(len(dict_labor_2["SVC-DESCS"])-1)]+" "  + str(row[62:97]))
                               
                    else:
                           dict_labor_2["REFER#"].append(dict_labor_2["REFER#"][(len(dict_labor_2["REFER#"])-1)])
                           dict_labor_2["VIN"].append(dict_labor_2["VIN"][(len(dict_labor_2["VIN"])-1)])
                           dict_labor_2["LINE-CDS"].append(str(row[29:31].strip())) 
                           dict_labor_2["STOCK-NO"].append(dict_labor_2["STOCK-NO"][(len(dict_labor_2["STOCK-NO"])-1)])
                           dict_labor_2["DEF-LBR-TYP"].append(str(row[43:53].strip())) 
                           dict_labor_2["MILEAGE"].append(dict_labor_2["MILEAGE"][(len(["MILEAGE"])-1)])
                           dict_labor_2["SVC-DESCS"].append(str(row[62:97]))
                           

                else:                     
                    dict_labor_2["REFER#"].append(str(row[0:10].strip()))
                    dict_labor_2["VIN"].append(str(row[11:28].strip()))
                    dict_labor_2["LINE-CDS"].append(str(row[29:31].strip())) 
                    dict_labor_2["STOCK-NO"].append(str(row[32:42].strip()))                   
                    dict_labor_2["DEF-LBR-TYP"].append(str(row[43:53].strip())) 
                    dict_labor_2["MILEAGE"].append(str(row[54:61].strip()))                   
                    dict_labor_2["SVC-DESCS"].append(str(row[62:97])) 
                   
 
            if row.strip() != "" and po_order==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                if len(row[0:10].strip())==0 :                  
                    if len(row[11:18].strip())>0:
                        dict_po_order["PO-TYPE"][(len(dict_po_order["PO-TYPE"])-1)] = (dict_po_order["PO-TYPE"][(len(dict_po_order["PO-TYPE"])-1)]  + str(row[11:18]))
                    if len(row[19:35].strip())>0:
                        dict_po_order["DESC"][(len(dict_po_order["DESC"])-1)] = (dict_po_order["DESC"][(len(dict_po_order["DESC"])-1)]+" " + str(row[19:35]))
                    if len(row[74:82].strip())>0:
                        dict_po_order["STATUS"][(len(dict_po_order["STATUS"])-1)] = (dict_po_order["STATUS"][(len(dict_po_order["STATUS"])-1)]  + str(row[74:82]))
                    if len(row[83:118].strip())>0:
                        dict_po_order["VEND-NAME"][(len(dict_po_order["VEND-NAME"])-1)] = (dict_po_order["VEND-NAME"][(len(dict_po_order["VEND-NAME"])-1)]+" " + str(row[83:118]))
                   
                                   
                else:                     
                    dict_po_order["PO-NO"].append(str(row[0:10].strip()))
                    dict_po_order["PO-TYPE"].append(str(row[11:18].strip()))
                    dict_po_order["DESC"].append(str(row[19:35]))
                    dict_po_order["REFER#"].append(str(row[36:46].strip()))
                    dict_po_order["CREATE-DATE"].append(str(row[47:54].strip()))
                    dict_po_order["CLOSE-DATE"].append(str(row[55:62].strip()))
                    dict_po_order["AMOUNT"].append(str(row[63:73].strip()))
                    dict_po_order["STATUS"].append(str(row[74:82].strip()))
                    dict_po_order["VEND-NAME"].append(str(row[83:118]))
            
            idx += 1

        #END WHILE
        if loglevel==logging.DEBUG:
            
            logger.debug("Parse Completed...dict_car_inv:"+str((dict_car_inv)))
            logger.debug("Parse Completed...dict_FI_WIP:"+str((dict_FI_WIP)))
            logger.debug("Parse Completed...dict_FI_DAO:"+str((dict_FI_DAO))) 
            logger.debug("Parse Completed...dict_ACDB5_GL:"+str((dict_ACDB5_GL))) 
            logger.debug("Parse Completed...dict_ACDB5_GL_OTHER:"+str((dict_ACDB5_GL_OTHER)))        
            logger.debug("Parse Completed...dict_WIP_APPT:"+str((dict_WIP_APPT)))            
            logger.debug("Parse Completed...dict_labor_appt:"+str((dict_labor_appt)))
            logger.debug("Parse Completed...dict_WIP_LABOR:"+str((dict_WIP_LABOR)))
            logger.debug("Parse Completed...dict_labor_2:"+str((dict_labor_2)))
            logger.debug("Parse Completed...dict_po_order :"+str((dict_po_order)))
        
         #covert dict to dataframe
               
        df_car_inv = pad.DataFrame(dict_car_inv)        
        df_FI_WIP = pad.DataFrame(dict_FI_WIP)
        df_FI_DAO = pad.DataFrame(dict_FI_DAO)
        df_ACDB5_GL = pad.DataFrame(dict_ACDB5_GL)
        df_ACDB5_GL_OTHER = pad.DataFrame(dict_ACDB5_GL_OTHER)
        df_WIP_LABOR = pad.DataFrame(dict_WIP_LABOR)
        df_WIP_APPT = pad.DataFrame(dict_WIP_APPT)
        df_labor_appt = pad.DataFrame(dict_labor_appt)
        df_labor_2 = pad.DataFrame(dict_labor_2)
        df_po_order = pad.DataFrame(dict_po_order)
      
        #Merge dataframe together
        #df_FI_WIP_MERGE=pad.DataFrame([]) 
        df_FI_WIP = df_FI_WIP.replace(np.nan,'', regex=True) 
        df_FI_DAO = df_FI_DAO.replace(np.nan,'', regex=True) 
        df_ACDB5_GL = df_ACDB5_GL.replace(np.nan,'', regex=True) 
        df_ACDB5_GL_OTHER = df_ACDB5_GL_OTHER.replace(np.nan,'', regex=True) 
        
        df_FI_WIP_MERGE=df_FI_WIP
        if len(df_FI_WIP_MERGE)>0 and len(df_FI_DAO)>0:
          df_FI_WIP_MERGE = pad.merge(df_FI_WIP_MERGE,df_FI_DAO, on='STOCK-NO', how='left')
        
        df_FI_WIP_MERGE = df_FI_WIP_MERGE.replace(np.nan,'', regex=True) 

        df_WIP_APPT = df_WIP_APPT.replace(np.nan,'', regex=True) 
        df_WIP_LABOR = df_WIP_LABOR.replace(np.nan,'', regex=True)  
        df_labor_appt = df_labor_appt.replace(np.nan,'', regex=True)   
        df_labor_2 = df_labor_2.replace(np.nan,'', regex=True)   
        df_po_order = df_po_order.replace(np.nan,'', regex=True)   
        df_car_inv = df_car_inv.replace(np.nan,'', regex=True)  
        
        if len(df_car_inv)>0 and len(df_ACDB5_GL)>0:
          df_car_inv = pad.merge(df_car_inv,df_ACDB5_GL, on='STOCK', how='left')

        if len(df_car_inv)>0 and len(df_ACDB5_GL_OTHER)>0:
          df_car_inv = pad.merge(df_car_inv,df_ACDB5_GL_OTHER, on='STOCK', how='left')

        df_car_inv = df_car_inv.replace(np.nan,'', regex=True)  
       
        logger.debug(" INVENTORY Parsing done succesfully...")        
        return {"car_inv":df_car_inv,"fi_wip":df_FI_WIP_MERGE,"wip_appt":df_WIP_APPT,"labor_appt":df_labor_appt,"labor_2":df_labor_2,"po_orders" :df_po_order,"wip_labor":df_WIP_LABOR,'account_line':account_line}