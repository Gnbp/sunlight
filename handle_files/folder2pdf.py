import psycopg2
from tkinter.filedialog import askdirectory
import os
import img2pdf
from PyPDF2 import PdfFileReader as pdfreader, PdfFileWriter as pdfwriter
import shutil
import zipfile
import datetime
from tkinter import *
from tkinter import messagebox
import configparser
import imghdr
import hashlib

import time

import gevent
from gevent.queue import Queue
from gevent.pool import Pool

from gevent import monkey
monkey.patch_all()


# 文件处理队列
pdf_file_queue = Queue()
zip_file_queue = Queue()
# 数据库插入队列
sql_queue = Queue()


DIR_PATH = os.path.split(os.path.realpath(__file__))[0]

conf = configparser.ConfigParser()
conf_path = os.path.join(DIR_PATH, 'script_config.txt')
conf.read(conf_path)
drive_name = conf.get('onedrive', 'netdrive_name')



class MySQLFile(object):
    def __init__(self, netdrive_name):
        self.file_name = ''
        self.file_dir = ''
        self.file_type = ''
        self.target_username = netdrive_name
        self.target_name = ''
        self.start_time = datetime.datetime.now()
        self.complete_time = datetime.datetime.now()
        self.complete_type = False
        self.remark1 = ''
        self.remark2 = ''
        

class MyOriginFile(object):
    def __init__(self):
        self.p_name = ''
        self.file_name = ''
        self.file_dir = ''
        self.file_size = ''
        self.last_change_time = ''
        self.file_type = ''
        self.file_md5 = ''
        self.remark1 = ''
        self.remark2 = ''


class MyPdfZip(object):
    def __init__(self):
        self.py_windows = Tk()
        self.py_windows.title('封包导出')
        self.path1 = StringVar()
        self.path2 = StringVar()
        self.path3 = StringVar()
        self.len_files = 0
        self.fail_file_obj = []

    def shutil_move(self, file_root, obj):
        # 要移动的文件位置路径
        # Ubuntu 下环境路径选项配置
        # dspath = os.path.join(u'' + self.path2.get() + '/'+file_root.split('/', 4)[4] + '.' +obj.file_type )
        # Window 下环境路径选项配置
        dspath = os.path.join(u'' + self.path2.get() + file_root[3:] + '.' + obj.file_type)
        
        # 文件入库的 target_name
        # Ubuntu 下环境路径选项配置
        # obj.target_name = os.path.join(u'' + self.path2.get() + '/'+file_root.split('/', 4)[4])
        # Window 下环境路径选项配置
        obj.target_name = os.path.join(u'' + self.path2.get() + file_root[3:])
        
        # print('dspath:{}'.format(dspath))
        
        # 是否存在要移动的目录，如果不存在，就创建
        fpath, fname = os.path.split(dspath)
        if not os.path.exists(fpath):
            # print('fpath:{}'.format(fpath))
            os.makedirs(fpath)

        # 移动
        shutil.move(os.getcwd() + '/'  + obj.file_name, dspath)

        # 标记完成
        obj.complete_type = True

    def chose_folder1(self):
        self._path1 = askdirectory()
        self.path1.set(self._path1)

    def chose_folder2(self):
        self._path2 = askdirectory()
        self.path2.set(self._path2)

    def run(self):
        self.init_window()
        # self.conn_postgresql()

    def init_window(self):
        self.py_windows.geometry('800x500')
        Label(self.py_windows, text='起始路径').grid(row=0, column=0)
        Entry(self.py_windows, textvariable=self.path1).grid(row=0, column=1)
        Button(self.py_windows, text='路径选择', command=self.chose_folder1).grid(row=0, column=2)
        Label(self.py_windows, text='目标路径').grid(row=1, column=0)
        Entry(self.py_windows, textvariable=self.path2).grid(row=1, column=1)
        Button(self.py_windows, text='路径选择', command=self.chose_folder2).grid(row=1, column=2)
        Button(self.py_windows, text='执行', command=self.conn_postgresql).grid(row=1, column=3)
        Listbox(self.py_windows, listvariable=self.path3).grid(row=2, column=0)

        # 进入消息循环
        self.py_windows.mainloop()


    def recode_origin_file(self, obj, file_root, pdf_names):
         # 记录源文件的父名字，名字，大小，md5，类型等等
        for origin_file in pdf_names:
            # 初始化源文件 文件对象
            o_obj = MyOriginFile()
            o_obj.p_name = obj.file_name
            o_obj.file_dir = origin_file
            o_obj.file_name = origin_file[len(file_root)+1:]
            o_obj.file_size = os.path.getsize(origin_file) / 1024   # 大小的单位是KB
            o_obj.last_change_time = datetime.datetime.fromtimestamp(os.path.getmtime(origin_file))
            o_obj.file_type = imghdr.what(origin_file)
            with open(origin_file, 'rb') as f:
                contents = f.read()
            o_obj.file_md5 = hashlib.md5(contents).hexdigest()
            # 将源文件插入源文件的数据库
            self.insert_originfile2sql(o_obj)



    def composite_files(self):

        rootdir = self.path1.get()
        for parent, dirnames, filenames in os.walk(rootdir):
            
            self.len_files += len(dirnames)*2
            name_str = parent.split('/')[-1].split('\\')[-1]
            print('name_str:{}'.format(name_str))
            
            file_name_pdf = []
            file_name_zip = []
            
            for filename in filenames:

                oldfilepath = os.path.join(parent + '/' + filename)
                # oldfilepath = oldfilepath.replace('//', '/')
                # print('oldfilepath:{}'.format(oldfilepath))
                if filename[-3:] == 'jpg':
                    # 判断文件是否能打开，不能打开则不处理整个文件夹，且记录一下
                    if imghdr.what(oldfilepath) == 'jpeg':
                        file_name_pdf.append(oldfilepath)
                    else:
                        # 整个文件不做处理，标记起来
                        # logging.error('{} 这个目录因为{}文件打不开，不做处理，请查看原因'.format(parent, filename))
                        break
                elif filename[-3:] == 'tif':
                    if imghdr.what(oldfilepath) == 'tiff':
                        file_name_pdf.append(oldfilepath)
                    else:
                        # 整个文件不做处理，标记起来
                        # logging.error('{} 这个目录因为{}文件打不开，不做处理，请查看原因'.format(parent, filename))
                        break
                else:
                    file_name_zip.append(oldfilepath)
            # # 验证源文件的完整性，不完整则不生成PDF    
            # file_name_pdf = self.check_files(file_name_pdf)
            if file_name_pdf != []:
                task_pdf = {
                    'file_root': parent,
                    'base_name': name_str,
                    'pdf_names': file_name_pdf,
                }
                pdf_file_queue.put(task_pdf)
            
            
            
            task_zip = {
                    'file_root': parent,
                    'base_name': name_str,
                    'pdf_names': file_name_zip,
                }
            zip_file_queue.put(task_zip)
            
            

        # 显示失败的文件列表
        self.sql_fail_file_name()
        print('total_files:{}'.format(self.len_files))
        
        # 执行完关闭窗口
        self.py_windows.destroy()
            
                

    def pdf_task_func(self):
        task = pdf_file_queue.get()
        file_root = task.get('file_root')
        base_name = task.get('base_name')
        pdf_names = task.get('pdf_names')
        # 初始化PDF 文件对象
        f_obj = MySQLFile(drive_name)
        f_obj.file_dir = file_root
        f_obj.file_type = 'pdf'
        f_obj.file_name = base_name  + '.pdf'
        if not self.query_sql_exist(f_obj) and pdf_names != []:
            # 记录源文件
            self.recode_origin_file(f_obj, file_root, pdf_names)

            f_obj.start_time = datetime.datetime.now()
            try:
                # 开始将jpg 和 tif 转换成pdf
                self.generate_pdffile(f_obj, pdf_names)
                # 开始为PDF 文件添加书签
                self.pdf_add_bookmark(f_obj, pdf_names)
                # 移动文件
                self.shutil_move(file_root, f_obj)
            except:
                # 错误信息写入日志
                # logging.error('文件：{} 生成或移动失败'.format(f_obj.file_name))
                pass
            finally:
                f_obj.complete_time = datetime.datetime.now()
                # self.insert_pdffile2sql(f_obj)

                # 将sql 语句插入消息队列
                self.sql_queue_task_func(f_obj)
                
    def sql_queue_task_func(self, obj):
        # 将sql 语句插入消息队列
        SQL = "INSERT INTO jy.handler_pdf (file_name, file_dir, file_type, target_username, target_name, start_time, complete_time, complete_type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        SQL_DATA = (
        obj.file_name, obj.file_dir, obj.file_type,  obj.target_username, obj.target_name, obj.start_time, obj.complete_time, obj.complete_type)
        sql_task = (SQL, SQL_DATA)
        sql_queue.put(sql_task)
          

    def zip_task_func(self):
        task = zip_file_queue.get()
        file_root = task.get('file_root')
        base_name = task.get('base_name')
        zip_names = task.get('pdf_names')
        # 初始化ZIP 文件对象
        f_obj = MySQLFile(drive_name)
        f_obj.file_dir = file_root
        f_obj.file_type = 'zip'
        f_obj.file_name = base_name  + '.zip'
        # if len(zip_names) > 0:
        if not self.query_sql_exist(f_obj) and len(zip_names) > 0:

            f_obj.start_time = datetime.datetime.now()
            try:
                # 生成ZIP 文件
                self.generate_zipfile(f_obj, zip_names)
                # 移动文件
                self.shutil_move(file_root, f_obj)
            except:
                # 错误信息写入日志
                # logging.error('文件：{} 生成或移动失败'.format(f_obj.file_name))
                pass
            finally:
                f_obj.complete_time = datetime.datetime.now()
                # # 数据插入
                # self.insert_pdffile2sql(f_obj)

                # 将sql 语句插入消息队列
                self.sql_queue_task_func(f_obj)
            
                    

    def generate_pdffile(self, obj, pdf_names):
        # 生成PDF 文件
        with open(obj.file_name, 'wb') as f:
            f.write(img2pdf.convert(pdf_names))
        # 打印日志信息
        # logging.info('{}转换PDF完成'.format(obj.file_name))
        # print('{}转换PDF完成'.format(obj.file_name))
    
    def pdf_add_bookmark(self, obj, pdf_names):
        # 为PDF 文件增加书签
        reader_obj = pdfreader(obj.file_name)
        pdf_writer = pdfwriter()
        pdf_writer.cloneDocumentFromReader(reader_obj)
        for i, v in enumerate(pdf_names):
            pdf_writer.addBookmark(u'' + v.split('\\')[-1], i)
        with open(obj.file_name, 'wb') as font:
            pdf_writer.write(font)
            # 打印日志信息
            # logging.info('{}增加书签完成'.format(obj.file_name))
        
        


    def generate_zipfile(self, obj, zip_names):
        # 压缩其他文件
        z = zipfile.ZipFile(obj.file_name, 'w', zipfile.ZIP_DEFLATED)
        for zip_file in zip_names:
            z.write(zip_file)
        z.close()
        # 打印日志信息
        # logging.info('{}生成ZIP完成'.format(obj.file_name))
        # print('{}生成ZIP完成'.format(obj.file_name))

        
    def conn_postgresql(self):
        self.conn = psycopg2.connect(database='test_class', user='postgres', password='123456we')
        self.cur = self.conn.cursor()
        self.composite_files()
        pool = Pool(20)
        """
        # 文件处理队列
        pdf_file_queue = Queue()
        zip_file_queue = Queue()
        # 数据库插入队列
        sql_queue = Queue()
        """
        while True:
            if not pdf_file_queue.empty():
                pool.spawn(self.pdf_task_func) 
            elif not zip_file_queue.empty():
                pool.spawn(self.zip_task_func)
            elif not sql_queue.empty():
                pool.spawn(self.insert_task_func)
            else:
                break 

        self.conn.close()

    def insert_task_func(self):
        task = sql_queue.get()
        SQL = task[0]
        SQL_DATA = task[1]
        self.cur.execute(SQL, SQL_DATA)
        self.conn.commit()

    def insert_pdffile2sql(self, obj):
        # 连接数据库

        if obj.file_name:
            SQL = "INSERT INTO jy.handler_pdf (file_name, file_dir, file_type, target_username, target_name, start_time, complete_time, complete_type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            SQL_DATA = (
            obj.file_name, obj.file_dir, obj.file_type,  obj.target_username, obj.target_name, obj.start_time, obj.complete_time, obj.complete_type)
            self.cur.execute(SQL, SQL_DATA)
            self.conn.commit()
            # 打印日志信息
            # logging.info('格式为{} 的文件：{} 在{} 到{} 耗时{} ，由{}位置，生成到{}，生成状态是否成功：{}'.format(obj.file_type, obj.file_name, obj.start_time, obj.complete_time, 
            # (obj.complete_time-obj.start_time), obj.file_dir, obj.target_name, obj.complete_type))

    
    def query_sql_exist(self, obj):
        print("=="*80)
        SQL = 'SELECT id,file_name,file_dir FROM jy.handler_pdf WHERE complete_type=True;'
        self.cur.execute(SQL)
        sql_i = 0
        for sql_file_obj in self.cur.fetchall():
            sql_i += 1
            if sql_file_obj[1] == obj.file_name and sql_file_obj[2] == obj.file_dir:
                print('file:{} has been exist:sql_id {},file_id {}'.format(obj.file_name, sql_i, sql_file_obj[0]))
                return True
    
    def sql_fail_file_name(self):
        SQL = 'SELECT id, file_name, file_dir FROM jy.handler_pdf WHERE complete_type=False;'
        self.cur.execute(SQL)


        fail_count = self.cur.fetchall()
        # 查询失败文件的对象
        for sql_fail_file in fail_count:
            ff_obj = MySQLFile(drive_name) 
            ff_obj.file_name = sql_fail_file[1]
            ff_obj.file_dir = sql_fail_file[2]
            # 打印日志信息
            # logging.info('数据库目前未完成状态的文件数为{}，id 为{} 的文件{} 是未完成状态'.format(len(fail_count), sql_fail_file[0], sql_fail_file[1]))
            self.fail_file_obj.append(ff_obj)
        
        
        # 设置显示失败的内容
        path3_data = tuple()
        if self.fail_file_obj != []:
            for ff1_obj in self.fail_file_obj:
                path3_data += (ff1_obj.file_name,)
        else:
            path3_data = ('no file generate fail',)
            # logging.info('恭喜你，没有数据失败')

        self.path3.set(path3_data)

    def insert_originfile2sql(self, obj):
        if obj.file_name:
            
            SQL = "INSERT INTO jy.origin_file (p_name, file_name, file_dir, file_type, file_size, file_md5, last_change_time) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            SQL_DATA = (
            obj.p_name, obj.file_name, obj.file_dir, obj.file_type,  obj.file_size, obj.file_md5, obj.last_change_time)
            self.cur.execute(SQL, SQL_DATA)
            self.conn.commit()
            # 打印日志信息
            # logging.info('格式为{} 的文件：{}, 它的大小为：{} kb，MD5值为：{}， 最后一次修改的时间为：{}'.format(obj.file_type, obj.file_name, obj.file_size, obj.file_md5, obj.last_change_time))

# def main():
#     root = Tk()
#     root.title('登录验证')
    
#     Label(root,text='会员名称:').grid(row=0,column=0)
#     Label(root,text='会员代号:').grid(row=1,column=0)
    
#     u1 = StringVar()
#     p1 = StringVar()

#     e1 = Entry(root,textvariable=u1)    # Entry 是 Tkinter 用来接收字符串等输入的控件.
#     e2 = Entry(root,textvariable=p1,show='#')
#     e1.grid(row=0,column=1,padx=10,pady=5)  #设置输入框显示的位置，以及长和宽属性
#     e2.grid(row=1,column=1,padx=10,pady=5)

#     Button(root,text='验证信息',width=10,command=lambda :auth(root, e1.get(), e2.get()))\
#     .grid(row=2,column=0,sticky=W,padx=10,pady=5)

#     Button(root,text='退出',width=10,command=root.quit)\
#     .grid(row=2,column=1,sticky=E,padx=10,pady=5)

#     root.mainloop()
    
# def auth(root, username, password):
#     if username == '123' and password == '321':
#         root.destroy()
#         a = MyPdfZip()
#         a.run()
        
#     else:
#         messagebox.showinfo(title='警告',message='输入信息错误')
#         print('验证失败')


if __name__ == "__main__":
    # main()
    start_time = time.time()
    a = MyPdfZip()
    a.run()
    end_time = time.time()
    print('use time :{}'.format(end_time-start_time))