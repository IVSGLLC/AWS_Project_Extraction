import pandas as pad
import logging 
import os
from EPDE_DealParser import DealParser
from EPDE_InventoryParser import InventoryParser

from EPDE_InvoiceParser import InvoiceParser
from EPDE_PartsParser import PartsParser
from EPDE_ROHistoryParser import ROHistoryParser
from EPDE_SalesCustParser import SalesCustParser
from EPDE_WIPParser import WIPParser

loglevel = int(os.environ['LOG_LEVEL'] )
logger = logging.getLogger('ParseData')
logger.setLevel(loglevel)
 
class ParseData():
    @classmethod
    def parse_DMS_Parts_File(self,fileDataAsStr): 
        logger.info("inside parse_DMS_Parts_File...")        
        df_final=pad.DataFrame([]) 
        try:
            parser= PartsParser()
            df_final=parser.parse(fileDataAsStr)
            #if loglevel==logging.DEBUG:
                #logger.debug(str(df_final))    
            logger.info("Parts Parsing done succesfully...")
        except Exception as ex:
             logger.error("Error Occure in parse_DMS_Parts_File ..." , exc_info=True)
        return df_final
    @classmethod
    def parse_MR_Parts_File(self,fileDataAsStr): 
        logger.info("inside parse_MR_Parts_File...")        
        df_final=pad.DataFrame([]) 
        try:
            parser= PartsParser()
            df_final=parser.parse_MR_PARTS(fileDataAsStr)
            #if loglevel==logging.DEBUG:
                #logger.debug(str(df_final))    
            logger.info("MR Parts Parsing done succesfully...")
        except Exception as ex:
             logger.error("Error Occure in parse_MR_Parts_File ..." , exc_info=True)
        return df_final
    @classmethod
    def parse_TEKION_Parts_File(self,fileDataAsStr): 
        logger.info("inside parse_TEKION_Parts_File...")        
        df_final=pad.DataFrame([]) 
        try:
            parser= PartsParser()
            df_final=parser.parse_TK_PARTS(fileDataAsStr)
            #if loglevel==logging.DEBUG:
                #logger.debug(str(df_final))    
            logger.info("TEKION Parts Parsing done succesfully...")
        except Exception as ex:
             logger.error("Error Occure in parse_TEKION_Parts_File ..." , exc_info=True)
        return df_final
    @classmethod
    def parse_DMS_SalesCust_File(self,fileDataAsStr): 
        logger.info("inside parse_DMS_Invoice_File...")
        df_final=pad.DataFrame([]) 
        try:
            parser= SalesCustParser()
            df_final=parser.parse(fileDataAsStr)             
            logger.info("SalesCust Parsing done succesfully...")
        except Exception as ex:
             logger.error("Error Occure in parse_DMS_SalesCust_File ..." , exc_info=True)
        return df_final
    @classmethod
    def parse_MR_SalesCust_File(self,fileDataAsStr): 
        logger.info("inside parse_MR_SalesCust_File...")
        df_final=pad.DataFrame([]) 
        try:
            parser= SalesCustParser()
            df_final=parser.parse_MR_SALESCUST(fileDataAsStr)             
            logger.info("MR SalesCust Parsing done succesfully...")
        except Exception as ex:
             logger.error("Error Occure in parse_MR_SalesCust_File ..." , exc_info=True)
        return df_final    
    @classmethod
    def parse_TEKION_SalesCust_File(self,fileDataAsStr): 
        logger.info("inside parse_TEKION_SalesCust_File...")
        df_final=pad.DataFrame([]) 
        try:
            parser= SalesCustParser()
            df_final=parser.parse_TK_SALESCUST(fileDataAsStr)             
            logger.info("TEKION SalesCust Parsing done succesfully...")
        except Exception as ex:
             logger.error("Error Occure in parse_TEKION_SalesCust_File ..." , exc_info=True)
        return df_final    
    @classmethod
    def parse_DMS_Invoice_File(self,fileDataAsStr):
        logger.info("inside parse_DMS_Invoice_File...")
        df_final=pad.DataFrame([]) 
        try:
            invParser= InvoiceParser()
            df_final=invParser.parse(fileDataAsStr)            
            logger.info("Invoice Parsing done succesfully...")
        except Exception as ex:
             logger.error("Error Occure in parse_DMS_Invoice_File ..." , exc_info=True)
        return df_final
    
    @classmethod
    def parse_DMS_WIP_File(self,fileDataAsStr):         
        logger.info("inside parse_DMS_WIP_File...")        
        df_final=pad.DataFrame([]) 
        try:
            parser= WIPParser()
            df_final=parser.parse(fileDataAsStr)            
            logger.info("WIP Parsing done succesfully...")
        except Exception as ex:
             logger.error("Error Occure in parse_DMS_WIP_File ..." , exc_info=True)
        return df_final
    @classmethod
    def parse_TKION_WIP_File(self,fileDataAsStr):         
        logger.info("inside parse_TKION_WIP_File...")        
        df_final=pad.DataFrame([]) 
        try:
            parser= WIPParser()
            df_final=parser.parse_TK_RO(fileDataAsStr)            
            logger.info("TEKION WIP Parsing done succesfully...")
        except Exception as ex:
             logger.error("Error Occure in parse_TKION_WIP_File ..." , exc_info=True)
        return df_final
    @classmethod
    def parse_MR_WIP_File(self,fileDataAsStr):         
        logger.info("inside parse_MR_WIP_File...")        
        df_final=pad.DataFrame([]) 
        try:
            parser= WIPParser()
            df_final=parser.parse_MR_RO(fileDataAsStr)            
            logger.info("MR WIP Parsing done succesfully...")
        except Exception as ex:
             logger.error("Error Occure in parse_MR_WIP_File ..." , exc_info=True)
        return df_final
    @classmethod
    def parse_DMS_Inventory_File(self,fileDataAsStr): 
        logger.info("inside parse_DMS_Inventory_File...")        
        df_final=pad.DataFrame([]) 
        try:
            parser= InventoryParser()
            df_final=parser.parse(fileDataAsStr)
            #if loglevel==logging.DEBUG:
                #logger.debug(str(df_final))    
            logger.info("Inventory Parsing done succesfully...")
        except Exception as ex:
             logger.error("Error Occure in parse_DMS_Inventory_File ..." , exc_info=True)
        return df_final    
    @classmethod
    def parse_DMS_Deal_File(self,fileDataAsStr): 
        logger.info("inside parse_DMS_Deal_File...")        
        df_final=pad.DataFrame([]) 
        try:
            parser= DealParser()
            df_final=parser.parse(fileDataAsStr)
            logger.info("Deal Parsing done succesfully...")
        except Exception as ex:
             logger.error("Error Occure in parse_DMS_Deal_File ..." , exc_info=True)
        return df_final    
    @classmethod
    def parse_DMS_ROHistory_File(self,fileDataAsStr): 
        logger.info("inside parse_DMS_ROHistory_File...")        
        df_final=pad.DataFrame([]) 
        try:
            parser= ROHistoryParser()
            df_final=parser.parse(fileDataAsStr)
            logger.info("ROHistory Parsing done succesfully...")
        except Exception as ex:
             logger.error("Error Occure in parse_DMS_ROHistory_File ..." , exc_info=True)
        return df_final    
    @classmethod
    def parse_DMS_ROHistory_FileV1(self,fileDataAsStr): 
        logger.info("inside parse_DMS_ROHistory_FileV1...")        
        df_final=pad.DataFrame([]) 
        try:
            parser= ROHistoryParser()
            df_final=parser.parseV1(fileDataAsStr)
            logger.info("ROHistory Parsing v1 done succesfully...")
        except Exception as ex:
             logger.error("Error Occure in parse_DMS_ROHistory_FileV1 ..." , exc_info=True)
        return df_final    