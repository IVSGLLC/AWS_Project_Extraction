#This module is responsible to handle OAuth2.o Custom Session
from EPDE_Logging import LogManger
class ErrorHandler(object):
    logger=LogManger() 
    __error_codes={
        308:"Payment json is not valid or missing",
        309: "Invalid Amount Format: #args-1",
        310: "Missing DocumentId in Request",
        311: "Invalid Payment Type =#args-1" ,
        312: "DMS is not available. Please try again later or call the dealership for a status and ETA",
        313: "Internal API Error. Please try again later or call the IVSG Technical Support for a status and ETA",
        314: "Invalid Employee ID = #args-1",
        315: "Status Missing",
        316: "Requested wrong Status: #args-1",
        317: "Payment Type Missing",
        318: "Invalid Pos Payment Type = #args-1",
        319: "Amount Missing",
        320: "Employee ID Missing",
        321: "Invalid Document Type =#args-1",
        322: "Missing DocumentType in Request",
        323: "Invalid RO Number",
        324: "Invalid RO Number #args-1",
        325: "Invalid openDateTime Format : #args-1 ,Expected format is yyyy-MM-dd hh:mm:ss tt",
        327: "Missing Payment Type(s)",       
        328: "Invalid Payment Type = #args-1",
        329: "Customer json is not valid or missing",
        330: "Missing Customer Number in Request",
        331: "Your API subscription has been expired.",
        332: "Denied Access to EDPE...",
        333: "System maintenance in process between #args-1 UTC and #args-2 UTC.  Your request is denied.  Retry during non-maintenance hours.",
        334: "Amount Paid: #args-1 not equal to Amount Due: #args-2",
        335: "Invalid warrantyDue Format: #args-1",
        336: "RO with WarantyDue-#args-1 cannot close,it must be closed by Clerk.",
        337: "Unauthorized,Session not found.",
        338: "Invalid RO Number",
        339: "Your API subcription not exist.",
        340: "Payment Request not exist." ,
        341: "RO number not available",
        342: "RO number is not Ready" ,
        343: "Customer Number not found" ,
        344: "Response JSON not supported." ,
        345: "missing/invalid posting information",
        346: "Total deposit does not match detail",
        347: "Invalid Department",
        348: "Invalid Reference Number",
        349: "Post Accounting not allowed while xxxxx is running. ",
        350: "xxxxx could be month end in process",
        351: "A/R running",
        352: "Accounting out of balance",
        353: "Missing file id in Request",
        354: "Missing requestType in Request",
        355: "Invalid Request Type = #args-1",
        356: "Invalid file id.",
        357: "Invalid File Type = #args-1",
        358: "Missing File Type in Request",
        359: "Missing Parts Invoice Number in Request",
        360: "Parts Invoice Number not available",
        361: "unsupported grant type", 
        362: "Requested Wrong Status:#args-1,RO number is Closed or not Ready,Latest Status:#args-2", 
        401: "Unauthorized",   
        363: "Invalid Copy Type = #args-1",
        364: "Missing Copy Type in Request",  
        365: "Request Timeout, DMS is not available.",
        366: "VIN not available",
        367: "Missing VIN in Request",
        368: "Missing vin in Request",
        369: "Missing #args-1 in Request",
        370: "Missing #args-1 in Request",
        371: "Invalid  #args-1 format = #args-2,Expected format is #args-3",
        372: "Invalid Request Parameter :#args-1",      
        373: "#args-1 can not be less than #args-2",  
        374: "Invalid plan - #args-1", 
        375: "Invalid type - #args-1", 
        376: "Invalid status - #args-1", 
        377: "Missing Date Range [fromDate and toDate] in Request", 
        378: "Missing input in Request,Please provide at least one input [vin|plan with fromDate and toDate | fromDate and toDate|stockFilter with fromEntryDate and toEntryDate", 
        379: "Missing input in Request,Please provide at least one input [vin|plan/status/type with fromDate and toDate | fromDate and toDate", 
        380: "Missing input in Request,Please provide at least one input [ vin|customer|vin and customer|closeDate Range]" ,
        381: "AmountDue must be Zero",
        382: "No RO exist with [AmountDue = Zero  and Status = C97 ] to close",
        383: "BatchCloseRO is not setup for store.Please call the IVSG Technical Support.",
        384: "Missing #args-1 in Request",
        385: "Invalid partitionKey",
        386: "Missing input in Request,Please provide at least one input [vin|plan with fromDate and toDate | fromDate and toDate", 
       
    }
    @classmethod
    def GetErrorMessage(self,code):
        return self.__error_codes[code]

    def GetErrorFormattedMessage(self,code,args=[]):
        msg=self.__error_codes[code]
        index=1
        for arg in args:
            msg=msg.replace('#args-'+str(index),arg)
            index=index+1
        return msg

    @classmethod
    def HandleGeneralError(self,moduleNM="",functionNM=""):
        self.logger.error("General Error Occured in "+moduleNM+">>"+functionNM+":",True)
        return { "status":False,"error_code":313 ,"error_message":self.__error_codes[313]}  

    @classmethod
    def HandleAppError(self,code,moduleNM="",functionNM="",otherInfo=""):
        self.logger.error("Application Error Occured in "+moduleNM+">>"+functionNM+": Detail:"+otherInfo+", return code="+str(code)+",message="+self.__error_codes[code]+"",True)
        return { "status":False,"error_code":code ,"error_message":self.__error_codes[code]}  
    
    @classmethod
    def appInfo(self,moduleNM,functionNM,otherInfo=""):
        if otherInfo is None or otherInfo=='':
           self.logger.info("Calling .... ["+moduleNM+"]--["+functionNM+"] :")  
        else:
            self.logger.info("Calling .... ["+moduleNM+"]--["+functionNM+"] {"+otherInfo+"}")
    
           