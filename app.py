from cmath import nan
import numpy as np
from pandas import DataFrame

def display(word_list):
    counter=1
    for word in word_list:
        print("-------------display---START------------"+str(counter))
        for line in word:
            line_st= " ".join(line)
            print(line_st)
        
        counter=counter+1
def displayMatrix(word_list):
    print("-------------display---MATRIX START------------")   
    for line in word_list:        
            line_st= " ".join(line)
            print(line_st)  
            
        
       
def processASCIIArt(ascii_art):
    if ascii_art is not None and len(ascii_art.strip())>0:
        l=ascii_art.split('\n')
        l_2d=[]
        for line in l:
            arr=[]
            for c in line:
                arr.append(c)
            l_2d.append(arr)    
        df=DataFrame(l_2d)
        df = df.replace(nan,'', regex=True) 
      
        mat = df.values.tolist()     
        displayMatrix(mat) 
        rot_mat= np.rot90(mat)  
        displayMatrix(rot_mat) 
        word_list=[]
        word=[]
        for line in rot_mat:
            line_st= "".join(line) 
            if len(line_st.strip())==0:      
            #if len(line_st.strip())==2 and (line_st.strip().__contains__("\\") and line_st.strip().__contains__("/")  ):            
                #word.append(line)	
                if len(word)>0:
                    word_list.append(word)
                word=[]
            else:
                word.append(line)	
        if len(word)>0:
                    word_list.append(word)     
        final_word_list=[]	 
        for word in word_list:	
            rot_mat= np.rot90(word) 
            rot_mat= np.rot90(rot_mat) 
            rot_mat= np.rot90(rot_mat) 		 
            final_word=[]
            for line in rot_mat:
                line_st= "".join(line)
                final_word.append(line) 
            final_word_list.append(final_word)
        final_word_list.reverse()
        display(final_word_list)
        return final_word_list
    else:
         return []
def processASCIIArtFile(ascii_art_file):
    if ascii_art_file is not None and len(ascii_art_file.strip())>0:
        print("fileName="+ascii_art_file)
        try:
            file1 = open(ascii_art_file,"r")
            ascii_art=file1.read()  
            file1.close()  
            return processASCIIArt(ascii_art=ascii_art)     
        except: 
            if not file1.closed:   
                file1.close()    
            print("errr")   
    return []    
   
if __name__ == '__main__':
    l_2d=[]
    
    ascii_art      = None
     

    #file1 = open("D:\ssahu\CodeCommit\SOW\\DMS Security\\test.txt","r")
   # ascii_art=str(file1.readlines())
    #file1.close() 
    print("------------ascii_art----------------")
    #print(ascii_art )    
    final_word_list=processASCIIArtFile("D:\\ssahu\\CodeCommit\\SOW\DMS Security\\test.txt")

    
