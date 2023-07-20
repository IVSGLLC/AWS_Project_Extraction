import logging
import  os 
#This module is responsible to handle OAuth2.o Custom Session
def Get_LOG_LEVEL():
    try:
        return int(os.environ['LOG_LEVEL']) 
    except:
        return 10  
class LogManger(object):
    logger = logging.getLogger('LogManger')
    loglevel = Get_LOG_LEVEL()       
    logger.setLevel(loglevel)    
    
    @classmethod
    def debug(self,message):
        if self.loglevel!=-1 and self.loglevel== logging.DEBUG:
           self.logger.debug(str(message))
    @classmethod
    def info(self,message):
        if self.loglevel!=-1:
            self.logger.info(str(message))
    @classmethod
    def error(self,message,ex_info_flag):
        if self.loglevel!=-1:
            if ex_info_flag is None:
                ex_info_flag=False
            if self.loglevel!=-1:
                self.logger.error(str(message),exc_info=ex_info_flag)