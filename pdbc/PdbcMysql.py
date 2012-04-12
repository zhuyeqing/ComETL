#-*-coding:UTF-8 -*-
import MySQLdb
import commands

import PdbcUtility
from Pdbc import pdbc

class PdbcMysql(pdbc):
    def __init__(self,logger):
        self.logger = logger
    
    def connect(self,ip,port,db,user,passwd):
        try:
            conn = MySQLdb.connect(ip,user,passwd,db,port)
            cursor = conn.cursor()
            self.conn = conn
            self.cursor = cursor
            self.ip = ip
            self.user = user
            self.passwd = passwd
            self.db = db
            self.port = port
            return True
        except MySQLdb.Error, e:
            self.logger("PdbcMysql %s" % (str(e)),'error')
            self.conn = None
            self.cursor = None
            return False

    def execute(self,sql,**params):
        self.cursor.execute(sql)
        return True
    
    def save(self,outfile,args):
        if PdbcUtility.__dict__.has_key(args['data_save_type']):
            return PdbcUtility.__dict__[args['data_save_type']](self,outfile,args,self.logger)
        else:
            udf = __import__('PdbcUDF/%s' % args['data_save_type'])
            return udf.__dict__[args['data_save_type']](self,outfile,args,self.logger)
    
    def getone(self):
        return self.cursor.fetchone()
    
    def getall(self):
        return self.cursor.fetchall()
    
    def getn(self,n):
        return self.cursor.fetchmany(n)
    
    def load(self,infile,args):
        tmp_args = {"port":self.port,'host':self.ip,'user':self.user,'passwd':self.passwd,'db':self.db}
        tmp_args.update(args)
        tmp_args["dest_table"] = args['db_table']
        tmp_args['data_field'] = args['data_field']
        tmp_args['tmp_dir'] = args['tmp_dir']
        
        if PdbcUtility.__dict__.has_key(args['data_load_type']):
            return PdbcUtility.__dict__[args['data_load_type']](self,infile,tmp_args,self.logger)
        else:
            udf = __import__('PdbcUDF/%s' % args['data_load_type'])
            return udf.__dict__[args['data_load_type']](self,infile,tmp_args,self.logger)         

    
    def close(self):
        self.cursor.close()
        self.conn.close()
        
if __name__=='__main__':
    #test ok at 2012-03-16 10:31:00 213server home path
    #输出格式修改为tab键分割：为了支持json数据
    def log(x,y):
        print x,y
    pdm = PdbcMysql(log)
    pdm.connect('192.168.1.50',3306,'test','jobs','hhxxttxs')
    pdm.load('/home/zhaoxiuxiang/ComETL2.0/pdbc/mysqltest.txt',{"dest_table":'a2','data_field':'a|b','data_load_type':'SplitLoad','tmp_dir':'/home/zhaoxiuxiang/ComETL2.0/pdbc'})
    pdm.execute('select * from test.a2 limit 20',officialsql=True)
    print pdm.getone()
    print pdm.getn(2)
    pdm.save('/tmp/test/test.log',{"data_save_type":'SimpleOutput'})
