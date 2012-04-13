#-*-coding:UTF-8 -*-
import time
import commands
import PdbcUtility
import pdbc_conf
from Pdbc import pdbc

mrjob_error = ['Output path already exists','Exception in thread "Main Thread"']

class PdbcMapred(pdbc):
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
            self.logger('pdbc PdbcMapred error: %s' % str(e),'error')  
            return False   
        
    def execute(self,sql,**params):
        if params.get('officialsql',False):
            self.output_dir = sql[sql.find('-output') + 7:].split(' ')[1]
            #commands.getstatusoutput("/opt/modules/hadoop/hadoop-0.20.203.0/bin/hadoop fs -rmr %s" % self.output_dir)
        res = commands.getstatusoutput(""" %s """ % (sql))
        if res[0] != 0:
            self.logger(res[1],'error')
            return False            
        for err in mrjob_error:
            if res[1].find(err) != -1:
                self.logger(res[1],'error')
                return False
        if params.get('officialsql',False):
            suc_check_cmd = "%s/bin/hadoop fs -test -e %s/_SUCCESS" % (pdbc_conf.HADOOP_HOME,self.output_dir)
            suc_check_res = commands.getstatusoutput(suc_check_cmd)
            if int(suc_check_res[0]) != 0:
                self.logger(res[1],'error')
                return False    
    
            cmd = "%s/bin/hadoop fs -rmr %s/_logs %s/_SUCCESS" % (pdbc_conf.HADOOP_HOME,self.output_dir,self.output_dir)
            res = commands.getstatusoutput(cmd)
        return True

    def save(self,outfile,args):
        if PdbcUtility.__dict__.has_key(args['data_save_type']):
            return PdbcUtility.__dict__[args['data_save_type']](self,outfile,args,self.logger)
        else:
            udf = __import__('PdbcUDF/%s' % args['data_save_type'])
            return udf.__dict__[args['data_save_type']](self,outfile,args,self.logger)
        #return PdbcUtility.MapredOutput(self,outfile,args,self.logger)
    
    def load(self,infile,args):
        if PdbcUtility.__dict__.has_key(args['data_load_type']):
            return PdbcUtility.__dict__[args['data_load_type']](self,infile,args,self.logger)
        else:
            udf = __import__('PdbcUDF/%s' % args['data_load_type'])
            return udf.__dict__[args['data_load_type']](self,infile,args,self.logger)
        
    def close(self):
        return True
        

if __name__=="__main__":
    def log(x,y):
        print x,y    
    tbh = PdbcMapred(log)
    if tbh.connect('',0,"",'',''):
        tbh.execute("/opt/modules/hadoop/hadoop-0.20.203.0/bin/hadoop fs -put /home/zhaoxiuxiang/ComETL2.0/pdbc/hivetest122.txt /tmp ")
        tbh.close()
    
        