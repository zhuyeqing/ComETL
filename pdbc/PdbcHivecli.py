#-*-coding:UTF-8 -*-
import os
import time
import commands
import pdbc_conf
import PdbcUtility
from Pdbc import pdbc

class PdbcHivecli(pdbc):
    """ hive db driver"""
    def __init__(self,logger):
        self.logger = logger
    
    def connect(self,ip,port,db,user='',passwd=''):
        try:
            self.ip = ip
            self.port = port
            self.db = db
            self.user = user
            self.passwd = passwd
            self.hiveinitq = "hiveinitq_%d.q" % int(time.time())
            self.hiveinitfile = ''
            self.execute('use %s' % self.db,presql = True)
            return True
        except Exception, tx: 
            self.logger('pdbc hivecli error: %s' % str(tx),'error')  
            return False   
        
    def execute(self,sql,**params):
        if params.get('presql',False):
            PdbcUtility.HiveInitQWrite(params.get("tmpdir",'/tmp'),self.hiveinitq,sql,self.logger)
            self.hiveinitfile = params.get("tmpdir",'/tmp') + "/" + self.hiveinitq
            return True
        elif params.get('officialsql',False):
            self.outfile = params["outfile"]
            if os.path.isfile(self.hiveinitfile):
                res = commands.getstatusoutput("""%s/bin/hive -i %s -S -e "%s" > %s """ % (pdbc_conf.HIVE_HOME,self.hiveinitfile,sql,self.outfile))
            else:
                res = commands.getstatusoutput("""%s/bin/hive -S -e "%s" > %s """ % (pdbc_conf.HIVE_HOME,sql,self.outfile))
            if res[0] != 0:
                self.logger(str(res),'error')
                return False             
            if res[1].find("FAILED:") != -1:
                self.logger(res[1],'error')
                return False
            return True
        else:
            #PdbcUtility.HiveInitQWrite(params.get("tmpdir",'/tmp'),"")
            if os.path.isfile(self.hiveinitfile):
                res = commands.getstatusoutput("""%s/bin/hive -i %s -S -e "%s" """ % (pdbc_conf.HIVE_HOME,self.hiveinitfile,sql))
            else:
                res = commands.getstatusoutput("""%s/bin/hive -S -e "%s" """ % (pdbc_conf.HIVE_HOME,sql))
            if res[0] != 0:
                self.logger(str(res),'error')
                return False             
            if res[1].find("FAILED:") != -1:
                self.logger(res[1],'error')
                return False
            return True

    def save(self,outfile,args):
        if PdbcUtility.__dict__.has_key(args['data_save_type']):
            return PdbcUtility.__dict__[args['data_save_type']](self,outfile,args,self.logger)
        else:
            udf = __import__('PdbcUDF/%s' % args['data_save_type'])
            return udf.__dict__[args['data_save_type']](self,outfile,args,self.logger)
        #return PdbcUtility.HivecliOutput('',self.outfile,'',self.logger)
    
    def load(self,infile,args):
        if PdbcUtility.__dict__.has_key(args['data_load_type']):
            return PdbcUtility.__dict__[args['data_load_type']](self,infile,args,self.logger)
        else:
            udf = __import__('PdbcUDF/%s' % args['data_load_type'])
            return udf.__dict__[args['data_load_type']](self,infile,args,self.logger)        
        
        
        
    def close(self):
        commands.getstatusoutput("rm -f %s" % self.hiveinitfile)
        return True
        

if __name__=="__main__":
    def log(x,y):
        print x,y
    tbh = PdbcHivecli(log)
    if tbh.connect('192.168.1.43', 10000, "test",'',''):
        tbh.load('/home/zhaoxiuxiang/ComETL2.0/pdbc/hivetest122.txt',{'data_load_type':'Internalload',"db_table":'u_data','db_table_partition':''})
        tbh.execute('set mapred.job.queue.name=ETL',presql=True)
        tbh.execute('select * from u_data limit 20',officialsql=True,outfile='/tmp/test/hivecli1.txt')
        tbh.save('/tmp/test/hivetest.log',{"data_save_type":'SimpleOutput'})
        ##假设表已经存在，如果不存在，需要在post sql中进行创建
        #tbh.load('/tmp/test/hivetest.log',{'data_load_type':'ExternalLoad',"db_path":'/tmp/cometl1'})
        #tbh.load('/tmp/test/hivetest.log',{'data_load_type':'ExternalLzoLoad',"db_path":'/tmp/cometl1'})
        
        tbh.close()         
    
        