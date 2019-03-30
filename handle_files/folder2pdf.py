
# encoding:utf-8

import os
#import os.path
import zipfile  
import img2pdf
import shutil
import datetime
import psycopg2
from tkinter import * 
from tkinter.filedialog import askdirectory
import hashlib
from PyPDF2 import PdfFileReader as reader,PdfFileWriter as writer


conn=psycopg2.connect(database="JY_test",user="postgres",password="w19790307",host="localhost",port="5432")
cur=conn.cursor()

def selectPath():
    path_ = askdirectory()
    path.set(path_)
    rootdir = path.get()
    for parent, dirnames, filenames in os.walk(rootdir):
        dir_str=parent
        name_str=dir_str.split("/")[-1].split("\\")[-1]+"."+"pdf"
        file_name_str=[]
        file_name_ss=[]
        file_name=""
        file_dir=""
        file_type=""
        target_name=""
        complete_type=False
        for filename in filenames:
            if(dir_str==parent): 
                file_dir=parent         
                oldname= os.path.join(parent+"/"+filename)
                if(filename[-3:]=="jpg"):
                    file_type="pdf"
                    file_name_str.append(oldname)
                if(filename[-3:]=="tif"):
                    file_type="pdf"
                    file_name_str.append(oldname)
                else:            
                    file_name_ss.append(oldname)      
   
        with open(name_str,"wb") as f:
            f.write(img2pdf.convert(file_name_str))
            file_name=name_str
        f.close()
        dstfile=os.path.join(u"y:/"+parent[3:]+".pdf")
        target_name=os.path.join(u"y:/"+parent[3:])

        fpath,fname=os.path.split(dstfile)
        if not os.path.exists(fpath):
            os.makedirs(fpath)
        shutil.move(os.getcwd()+"\\"+name_str,dstfile)
        complete_type=True
        
        name_zip=name_str[:-4]+".zip"
        if(len(file_name_ss)>0):
            file_type="zip"
            z=zipfile.ZipFile(name_zip,'w',zipfile.ZIP_DEFLATED)
            for v in file_name_ss:
                z.write(v)
                complete_type=True
            z.close()

        dt = datetime.datetime.now()
        cur.execute("INSERT INTO gys.mytable(file_name,file_dir,file_type,target_name,complete_time,complete_type) \
        VALUES (%s,%s,%s,%s,%s,%s)",(file_name,file_dir,file_type,target_name,dt,complete_type));
        # cur.execute("SELECT * FROM test")
        conn.commit()
        conn.close()

        

root = Tk()
root.title("封包导出")
path = StringVar()
hash = hashlib.md5()

Label(root,text = "目标路径:").grid(row = 0, column = 0)
Entry(root, textvariable = path).grid(row = 0, column = 1)
Button(root, text = "路径选择", command = selectPath).grid(row = 0, column = 2)
root.geometry("800x500")
root.mainloop()
