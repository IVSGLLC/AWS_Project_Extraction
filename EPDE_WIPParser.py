import json
import numpy as np
import pandas as pad
import io
import logging 
import os

loglevel = int(os.environ['LOG_LEVEL'] )
logger = logging.getLogger('InvoiceParser')
logger.setLevel(loglevel)
 
class WIPParser():

    @classmethod
    def parse(self,fileDataAsStr): 
        
        logger.debug("inside WIPParser parse ...")

        txtfil = io.StringIO(fileDataAsStr)   
        
        row = txtfil.readline()
        idx = 0

        wip_ro : bool =False
        wip_cust : bool =False
        wip_status : bool =False
        wip_email : bool =False;   
  
        dict_ro={'REFER':[],'OPEN-DATE':[],'TOTAL$':[],'COMMENTS':[],'YR':[],'MAKE':[],'MODEL':[],'EPDE.TOTAL.ESTIMATE':[]}
        dict_cust={'REFER':[],'CUST':[],'CUSTOMER LINE1':[],'CUSTOMER LINE3':[],'CITY':[],'STATE':[],'ZIP':[],'SR-R':[],'SR-NAME':[],'SERIAL NO':[]}
        dict_status={'REFER':[],'STATUS':[],'PRINT-TIME':[],'CLOSED':[],'COMMENT':[],'HOME PHONE':[],'EPDE.SPC.INS':[]}
        dict_email={'REFER':[],'EMAIL':[],'PHONE':[],'TAG-NO':[],'WP':[]}

        #REFER#.. OPEN-DATE TOTAL$... COMMENTS........................... YR MAKE...... MODEL.....
        wip_ro_line=str('REFER#.. OPEN-DATE TOTAL$... COMMENTS....').split("^")
        #REFER#.. EPDE.CUST... CUSTOMER LINE1.......... CUSTOMER LINE3.......... CITY-STATE-ZIP..................... SA-R.. SA-NAME.................. SERIAL NO........
        wip_cust_line=str('REFER#.. EPDE.CUST... CUSTOMER LINE1.......... CUSTOMER LINE3.......... CITY-STATE-ZIP..................... SA-R.. SA-NAME.................. SERIAL NO........').split("^")
        #RONUM... EMAIL.............................. PHONE....... TAG-NO. WP$.........
        wip_email_line=str('RONUM... EMAIL.............................. PHONE....... TAG-NO. WP$.........').split("^")
        #REFER#.. STATUS PRINT-TIME............... CLOSED. COMMENT................. HOME PHONE
        #REFER#.. STATUS EPDE.PRINT.TIME CLOSED. COMMENT................. HOME PHONE SPECIAL-INSTRUCTIONS...............
        wip_status_line=str('REFER#.. STATUS EPDE.PRINT.TIME CLOSED. COMMENT................. HOME PHONE SPECIAL-INSTRUCTIONS.............').split("^")
        #ITEMS LISTED.
        line_end_text=str('ITEMS LISTED.^ITEM LISTED.').split("^")
        #ITEMS SELECTED^REFER# OPEN-DATE EPDE.TOTAL COMMENTS YEAR MAKE MODEL ID-SUPP^REFER# EPDE.CUST CUST-N1 CUST-N3 CITY-STATE-ZIP SA-R5 SA-NAME VIN ID-SUPP^LABOR RONUM EPDE.EMAIL EPDE.PHONE TAG-NO EPDE.WT.TOTAL ID-SUPP^REFER# RO-STATUS PRINT.TIME CLOSED COMMENT HPHONE ID-SUPP
        skip_line_contains_list=str("ITEMS SELECTED^REFER# OPEN-DATE EPDE.TOTAL COMMENTS YEAR MAKE MODEL ID-SUPP^REFER# EPDE.CUST CUST-N1 CUST-N3 CITY-STATE-ZIP SA-R5 SA-NAME VIN ID-SUPP^LABOR RONUM EPDE.EMAIL EPDE.PHONE TAG-NO EPDE.WT.TOTAL ID-SUPP^REFER# RO-STATUS EPDE.PRINT.TIME CLOSED COMMENT HPHONE EPDE.SPC.INS ID-SUPP^' NOT ON FILE").split("^")
        #>SORT^PAGE^:GET-LIST WIP^:SSELECT WIP WITH VIN^>SAVE-LIST WIP^login^password
        skip_line_startwith_list=str('>SORT^PAGE^:GET-LIST WIP^:SSELECT WIP WITH VIN^>SAVE-LIST WIP^login^password^REFER#.. OPEN-DATE TOTAL$... COMMENTS.....^REFER#.. EPDE.CUST... CUSTOMER LINE1.......... CUSTOMER LINE3.......... CITY-STATE-ZIP..................... SA-R.. SA-NAME.................. SERIAL NO........^REFER#.. STATUS EPDE.PRINT.TIME CLOSED. COMMENT................. HOME PHONE^RONUM... EMAIL.............................. PHONE....... TAG-NO. WP$.........').split("^")

       
        account_line=[]     
        while row:
            row = txtfil.readline()
            if row.__contains__('adp (') and row.strip().endswith(')'):
                account_line.append(row.strip())      

            #if row.S("REFER#") and  row.__contains__("OPEN-DATE") and row.__contains__("TOTAL$") and row.__contains__("MAKE") and row.__contains__("MODEL"):
            if row.startswith(tuple(wip_ro_line)):
                wip_ro=True            
            #if row.__contains__("REFER#") and  row.__contains__("CUST") and row.__contains__("CUSTOMER LINE1") and row.__contains__("CITY-STATE-ZIP") and row.__contains__("SERIAL NO") and row.__contains__("CUSTOMER LINE3"):
            if row.startswith(tuple(wip_cust_line)):
                wip_cust=True     
            if row.startswith(tuple(wip_status_line)):       
            #if  row.__contains__("REFER#")  and row.__contains__("STATUS")  and row.__contains__("PRINT-TIME")  and row.__contains__("CLOSED")  and row.__contains__("COMMENT") and row.__contains__("HOME PHONE"):
                wip_status=True    
            if row.startswith(tuple(wip_email_line)):           
            #if row.__contains__("RONUM")  and row.__contains__("PHONE") and row.__contains__("EMAIL") and row.__contains__("TAG-NO") and row.__contains__("WP$"):
                wip_email=True             
            if list(filter(row.__contains__, line_end_text)) != []:
                if(wip_ro==True): wip_ro=False  
                if(wip_cust==True): wip_cust=False       
                if(wip_status==True): wip_status=False     
                if(wip_email==True): wip_email=False 
        
            #if wip_ro==True and not row=="\n" and not row.startswith("PAGE") and not (row.__contains__("ITEMS SELECTED") ) and not row.startswith(">SORT") and not ( row.__contains__("REFER#") and  row.__contains__("OPEN-DATE") and row.__contains__("TOTAL$") and row.__contains__("MAKE") and row.__contains__("MODEL")):
            if wip_ro==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
                if len(row[0:8].strip())==0 :
                    if len(row[68:78].strip())>0:
                        dict_ro["MAKE"][(len(dict_ro["MAKE"])-1)] = (dict_ro["MAKE"][(len(dict_ro["MAKE"])-1)] + str(row[68:78].strip())) 
                    if len(row[79:90].strip())>0:
                        dict_ro["MODEL"][(len(dict_ro["MODEL"])-1)] = (dict_ro["MODEL"][(len(dict_ro["MODEL"])-1)] + str(row[79:90].strip()))
                    if len(row[29:64].strip())>0:
                        dict_ro["COMMENTS"][(len(dict_ro["COMMENTS"])-1)] = (dict_ro["COMMENTS"][(len(dict_ro["COMMENTS"])-1)] +' '+ str(row[29:64])) 
                else:
                    dict_ro["REFER"].append(str(row[0:8].strip()))
                    dict_ro["OPEN-DATE"].append(str(row[9:18].strip()))
                    dict_ro["TOTAL$"].append(str(row[19:28].strip()))
                    dict_ro["COMMENTS"].append(str(row[29:64]))
                    dict_ro["YR"].append(str(row[65:67].strip()))
                    dict_ro["MAKE"].append(str(row[68:78].strip()))
                    dict_ro["MODEL"].append(str(row[79:90].strip())) 
                    dict_ro["EPDE.TOTAL.ESTIMATE"].append(str(row[91:].strip())) 

            if wip_cust==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
            #if wip_cust==True and not row=="\n" and not row.startswith("PAGE") and not (row.__contains__("ITEMS SELECTED") ) and not row.startswith(">SORT")  and not (row.__contains__("REFER#") and  row.__contains__("EPDE.CUST") and row.__contains__("CUSTOMER LINE1") and row.__contains__("CITY-STATE-ZIP") and row.__contains__("SERIAL NO") and row.__contains__("CUSTOMER LINE3")):
                if len(row[0:8].strip())==0 and len(row[9:21].strip())==0:
                    if len(row[22:46].strip())>0:
                        dict_cust["CUSTOMER LINE1"][(len(dict_cust["CUSTOMER LINE1"])-1)] = (dict_cust["CUSTOMER LINE1"][(len(dict_cust["CUSTOMER LINE1"])-1)] + str(row[22:46]))
                    if len(row[47:71].strip())>0:
                        dict_cust["CUSTOMER LINE3"][(len(dict_cust["CUSTOMER LINE3"])-1)] = (dict_cust["CUSTOMER LINE3"][(len(dict_cust["CUSTOMER LINE3"])-1)] + str(row[47:71])) 
                    if len(row[72:107].strip())>0:
                        dict_cust["ZIP"][(len(dict_cust["ZIP"])-1)] = (dict_cust["ZIP"][(len(dict_cust["ZIP"])-1)] + str(row[72:107].strip())) 
                    if len(row[115:140].strip())>0:
                        dict_cust["SR-NAME"][(len(dict_cust["SR-NAME"])-1)] = (dict_cust["SR-NAME"][(len(dict_cust["SR-NAME"])-1)] + str(row[115:140])) 
                else:
                    dict_cust["REFER"].append(str(row[0:8].strip()))
                    dict_cust["CUST"].append(str(row[9:21].strip()))
                    dict_cust["CUSTOMER LINE1"].append(str(row[22:46]))
                    dict_cust["CUSTOMER LINE3"].append(str(row[47:71]))
                    #CITY-STATE-ZIP
                    city=""
                    state=""
                    zip=""
                    if len(row[72:107].strip())>0 :
                            csz=row[72:107].strip()
                            #if loglevel==logging.DEBUG:
                                #logger.debug("..csz:"+str(csz))
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
                                                          
                    dict_cust["CITY"].append(str(city))
                    dict_cust["STATE"].append(str(state))
                    dict_cust["ZIP"].append(str(zip)) 
                    #if loglevel==logging.DEBUG:
                                #logger.debug("city:"+str(city)+",state:"+str(state)+",zip:"+str(zip))
                    dict_cust["SR-R"].append(str(row[108:114].strip()))  
                    dict_cust["SR-NAME"].append(str(row[115:140]) ) 
                    dict_cust["SERIAL NO"].append(str(row[141:].strip())  )

            if wip_status==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
            #if wip_status==True and not row=="\n" and not row.startswith("PAGE") and not (row.__contains__("ITEMS SELECTED") ) and not row.startswith(">SORT") and not (row.__contains__("REFER#")  and row.__contains__("STATUS")  and row.__contains__("PRINT-TIME")  and row.__contains__("CLOSED")  and row.__contains__("COMMENT") and row.__contains__("HOME PHONE")):
                #print(row)
                if len(row[0:8].strip())==0 :
                    if len(row[16:31].strip())>0:
                        dict_status["PRINT-TIME"][(len(dict_status["PRINT-TIME"])-1)] = (dict_status["PRINT-TIME"][(len(dict_status["PRINT-TIME"])-1)] + str(row[16:31].strip()))
                    if len(row[40:64].strip())>0:
                        dict_status["COMMENT"][(len(dict_status["COMMENT"])-1)] = (dict_status["COMMENT"][(len(dict_status["COMMENT"])-1)]+' ' + str(row[40:64])) 
                    if len(row[9:15].strip())>0:
                        sts=dict_status["STATUS"][(len(dict_status["STATUS"])-1)]
                        if len(sts.strip())>0:
                          dict_status["STATUS"][(len(dict_status["STATUS"])-1)] = (dict_status["STATUS"][(len(dict_status["STATUS"])-1)]+"," + str(row[9:15].strip())) 
                        else:
                          dict_status["STATUS"][(len(dict_status["STATUS"])-1)] = (dict_status["STATUS"][(len(dict_status["STATUS"])-1)] + str(row[9:15].strip())) 
                    if len(row[76:].strip())>0:
                        dict_status["EPDE.SPC.INS"][(len(dict_status["EPDE.SPC.INS"])-1)] = (dict_status["EPDE.SPC.INS"][(len(dict_status["EPDE.SPC.INS"])-1)] +' '+ str(row[76:]))
                  
                
                else:
                    dict_status["REFER"].append(str(row[0:8].strip()))
                    dict_status["STATUS"].append(str(row[9:15].strip()))
                    dict_status["PRINT-TIME"].append(str(row[16:31].strip()))
                    dict_status["CLOSED"].append(str(row[32:39].strip()))
                    dict_status["COMMENT"].append(str(row[40:64]))
                    dict_status["HOME PHONE"].append(str(row[65:75].strip()))
                    dict_status["EPDE.SPC.INS"].append(str(row[76:]))
                   
                    
                    

            if wip_email==True and not row=="\n" and not row.startswith(tuple(skip_line_startwith_list)) and not (list(filter(row.__contains__, skip_line_contains_list)) != []):
            #if wip_email==True and not row=="\n" and not row.startswith("PAGE") and not (row.__contains__("ITEMS SELECTED") ) and not row.startswith(">SORT") and not (row.__contains__("RONUM")  and row.__contains__("PHONE") and row.__contains__("EMAIL") and row.__contains__("TAG-NO") and row.__contains__("WP$") ):
                
                if len(row[0:8].strip())==0 :
                    if len(row[9:44].strip())>0:
                        dict_email["EMAIL"][(len(dict_email["EMAIL"])-1)] = (dict_email["EMAIL"][(len(dict_email["EMAIL"])-1)] + str(row[9:44].strip()))
            
                else:
                    dict_email["REFER"].append(str(row[0:8].strip()))
                    dict_email["EMAIL"].append(str(row[9:44].strip()))
                    dict_email["PHONE"].append(str(row[45:57].strip()))
                    dict_email["TAG-NO"].append(str(row[58:65].strip()))
                    dict_email["WP"].append(str(row[66:].strip()))
            idx += 1
        if loglevel==logging.DEBUG:
            logger.debug("Parse Completed...dict_ro :"+str(dict_ro))
            logger.debug("Parse Completed...dict_cust:"+str(dict_cust))
            logger.debug("Parse Completed...dict_status:"+str(dict_status))
            logger.debug("Parse Completed...dict_email:"+str(dict_email))
         #covert dict to dataframe
        df_ro = pad.DataFrame(dict_ro)
        
        df_cust = pad.DataFrame(dict_cust)
        df_status = pad.DataFrame(dict_status)
        df_email = pad.DataFrame(dict_email)

         

        df_final=pad.DataFrame([])
        #Merge dataframe together
        if len(df_ro)>0 and len(df_cust)>0:
            df_merge_ro_cust = pad.merge(df_ro,df_cust, on='REFER', how='left')
            if len(df_merge_ro_cust)>0 and len(df_status)>0:
                df_merge_ro_cust_status = pad.merge(df_merge_ro_cust,df_status, on='REFER', how='left')
                if len(df_merge_ro_cust_status)>0 and len(df_email)>0:   
                    df_merge_ro_cust_status_email = pad.merge(df_merge_ro_cust_status,df_email, on='REFER', how='left')
                    #remove new line if any......
                   # df_final = df_merge_ro_cust_status_email.replace('\r\n',' ', regex=True)
                   # df_final = df_final.replace('\r',' ', regex=True)
                    #df_final = df_final.replace('\n',' ', regex=True)
                    df_final = df_merge_ro_cust_status_email.replace(np.nan,'', regex=True)          
                    #if loglevel==logging.DEBUG:
                        #logger.debug(str(df_final))

        logger.debug("WIP Parsing done succesfully...")
        return {'df_final':df_final,'account_line':account_line}
    @classmethod
    def parse_TK_RO(self,fileDataAsStr): 
        logger.debug("inside parse_TK_RO parse ...")        
        JSON_obj = json.loads(fileDataAsStr)         
        return JSON_obj
    #MR INTEGRATION
    @classmethod
    def parse_MR_RO(self,fileDataAsStr): 
        logger.debug("inside parse_MR_RO parse ...")        
        JSON_obj = json.loads(fileDataAsStr)         
        return JSON_obj  
    
