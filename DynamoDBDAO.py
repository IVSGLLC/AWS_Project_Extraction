 
from DealDBHelper import DealDBHelper
from InventoryDBHelper import InventoryDBHelper
from InvoiceDBHelper import InvoiceDBHelper
from PartsDBHelper import PartsDBHelper
from RODBHelper import RODBHelper
from ROHistoryDBHelper import ROHistoryDBHelper
from SalesCustDBHelper import SalesCustDBHelper
 
class DynamoDBDAO():

    @classmethod
    def Save(self,store_code,df,client_id):        
        helper=RODBHelper()
        return helper.Save(store_code,df,client_id)
    @classmethod
    def SaveTKRO(self,store_code,df,client_id):
        helper=RODBHelper()
        return helper.Save_TKRO(store_code,df,client_id) 
    @classmethod
    def SaveMRRO(self,store_code,df,client_id):
        helper=RODBHelper()
        return helper.Save_MRRO(store_code,df,client_id) 
    @classmethod
    def SaveParts(self,store_code,df,client_id):
        helper=PartsDBHelper()
        return helper.SaveParts(store_code,df,client_id)
    @classmethod
    def SavePartsTK(self,store_code,df,client_id):   
        helper=PartsDBHelper()      
        return helper.Save_TKParts(store_code,df,client_id) 
    @classmethod
    def SavePartsMR(self,store_code,df,client_id):   
        helper=PartsDBHelper()      
        return helper.Save_MRParts(store_code,df,client_id) 
    @classmethod
    def SaveInvoice(self,store_code,df,client_id):
        helper=InvoiceDBHelper()
        return helper.SaveInvoice(store_code,df,client_id)
    
    @classmethod
    def SaveInventory(self,store_code,df,client_id):
        helper=InventoryDBHelper()
        return helper.SaveInventory(store_code=store_code,client_id=client_id,df=df)
    @classmethod
    def SaveDeal(self,store_code,df,client_id):
        helper=DealDBHelper()
        return helper.SaveDeal(store_code=store_code,client_id=client_id,df=df)
    @classmethod
    def SaveSalesCust(self,store_code,df,client_id):
        helper=SalesCustDBHelper()
        return helper.SaveSalesCust(store_code,df,client_id) 
    @classmethod
    def SaveSalesCustTK(self,store_code,df,client_id):
        helper=SalesCustDBHelper()
        return helper.Save_TKSalesCust(store_code,df,client_id) 
    @classmethod
    def SaveROHistory(self,store_code,df,client_id):
        helper=ROHistoryDBHelper()
        return helper.SaveROHistory(store_code,df,client_id)      
    @classmethod
    def SaveSalesCustMR(self,store_code,df,client_id):
        helper=SalesCustDBHelper()
        return helper.Save_MRSalesCust(store_code,df,client_id)       
    @classmethod
    def SaveROHistoryNew(self,store_code,df,client_id):
        helper=ROHistoryDBHelper()
        return helper.SaveROHistoryNew(store_code,df,client_id)      
    
   
   