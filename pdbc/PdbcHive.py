#-*-coding:UTF-8 -*-
import PdbcUtility
from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from Pdbc import pdbc

class PdbcHive(pdbc):
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
            transport = TSocket.TSocket(ip,port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
        
            client = ThriftHive.Client(protocol)
            transport.open()  
            client.execute('use %s' % db)
            #client.execute('add jar /opt/modules/hive/HivePlugin.jar')
            #client.execute("create temporary function getpid as 'com.baofeng.data.hive.UDFGetPid'")
            #if mapred_queue != "":
            #    client.execute('set mapred.job.queue.name=%s' % mapred_queue)
            self.transport = transport
            self.client = client
            return True
        except Thrift.TException, tx:
            self.transport = None
            self.client = None
            self.logger('pdbc hive error: %s' % (tx.message),'error') 
            return False   
        
    def execute(self,sql,**params):
        if self.client:
            self.client.execute(sql)
            return True
        
    def getone(self):
        if self.client:
            res = self.client.fetchOne()
            if res[-1] == '\n':
                res = res[:-1]
            if len(res) == 0:
                return None
            return tuple(res.split('\t'))
    
    def getn(self,n):
        if self.client:
            re_res = []
            res = self.client.fetchN(n)
            for row in res:
                if len(row) == 0:
                    continue                 
                if row[-1] == '\n':
                    row = row[:-1]
                re_res.append(tuple(row.split('\t')))
            return re_res    
    
    def getall(self):
        if self.client:
            re_res = []
            res = self.client.fetchAll()
            for row in res:
                if len(row) == 0:
                    continue                 
                if row[-1] == '\n':
                    row = row[:-1]
                re_res.append(tuple(row.split('\t')))
            return re_res

    def save(self,outfile,args):
        if PdbcUtility.__dict__.has_key(args['data_save_type']):
            return PdbcUtility.__dict__[args['data_save_type']](self,outfile,args,self.logger)
        else:
            udf = __import__('PdbcUDF/%s' % args['data_save_type'])
            return udf.__dict__[args['data_save_type']](self,outfile,args,self.logger)
    
    def load(self,infile,args):
        if PdbcUtility.__dict__.has_key(args['data_load_type']):
            return PdbcUtility.__dict__[args['data_load_type']](self,infile,args,self.logger)
        else:
            udf = __import__('PdbcUDF/%s' % args['data_load_type'])
            return udf.__dict__[args['data_load_type']](self,infile,args,self.logger)        
        #return True
        
    def close(self):
        self.transport.close()
        

if __name__=="__main__":
    #test ok at 2012-03-16 12:02:00 213server home path
    def log(x,y):
        print x,y    
    tbh = PdbcHive(log)
    if tbh.connect('192.168.1.43', 10000, "test",'',''):
        tbh.load('/home/zhaoxiuxiang/ComETL2.0/pdbc/hivetest.txt',{'data_load_type':'Internalload',"db_table":'u_data','db_table_partition':''})
        tbh.execute('select * from u_data limit 10',officialsql=True)
        oneres = tbh.getone()
        print oneres
        nres = tbh.getn(3)
        print nres
        tbh.save('/tmp/test/hivetest.log',{"data_save_type":'SimpleOutput'})
        ##假设表已经存在，如果不存在，需要在post sql中进行创建
        tbh.load('/tmp/test/hivetest.log',{'data_load_type':'ExternalLoad',"db_path":'/tmp/cometl1'})
        tbh.load('/tmp/test/hivetest.log',{'data_load_type':'ExternalLzoLoad',"db_path":'/tmp/cometl1'})
        
        tbh.close()
    
        