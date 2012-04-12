#-*-coding:UTF-8 -*-
import time
import commands
import PdbcUtility
from Pdbc import pdbc

class PdbcShell(pdbc):
    """ PdbcMapred db driver"""
    def __init__(self,logger):
        self.logger = logger
    
    def connect(self,ip,port,db,user='',passwd=''):
        try:
            self.ip = ip
            self.port = port
            self.db = db
            self.user = user
            self.passwd = passwd
            return True
        except Exception, e: 
            self.logger('pdbc PdbcShell error: %s' % str(e),'error')  
            return False   
        
    def execute(self,sql,**params):
        res = commands.getstatusoutput(""" %s """ % (sql))
        if res[0] != 0:
            self.logger(res[1],'error')
            return False            
        return True

    def save(self,outfile,args):
        if PdbcUtility.__dict__.has_key(args['data_save_type']):
            return PdbcUtility.__dict__[args['data_save_type']](self,outfile,args,self.logger)
        else:
            udf = __import__('PdbcUDF/%s' % args['data_save_type'])
            return udf.__dict__[args['data_save_type']](self,outfile,args,self.logger)
    
    def load(self,infile,args):
        return False
        
    def close(self):
        return True
        

if __name__=="__main__":
    #etl_op['db_path'] 为 shell 命令的最终输出目录，etl框架从该目录中获取结果
    def log(x,y):
        print x,y    
    tbh = PdbcShell(log)
    if tbh.connect('',0,"",'',''):
        tbh.execute("/opt/modules/hadoop/hadoop-0.20.203.0/bin/hadoop fs -put /home/zhaoxiuxiang/ComETL2.0/pdbc/hivetest122.txt /tmp ")
        tbh.close()
    
        