#-*-coding:UTF-8 -*-
import re
import os
import copy
import EtlUtility

from EtlTb import Etltraceback

class ExtractionEtl(object):
    def __init__(self,etl_op,db_object_dict,logger,name='ETL',debug=False):
        self.logger = logger
        self.name = name + "_" + etl_op['job_name'] + '_' +etl_op['step_name']
        self.conf_name = name
        self.debug = debug
        #self.etl_op = etl_op
        #self.pattern = re.compile(r'\<\![\w\s\S]*\!\>')
        self.pattern = re.compile(r'\<\![\w\(\),\s]*\!\>')
        self.exec_namespace = {}
        EtlUtility.SqlExecCode(etl_op,self.exec_namespace)
        self.EtlopClean(etl_op)
        self.db_object_dict = db_object_dict
        #etl_op['tmpdir'] == conf.tmpdir + '/' + name
        self.outfiledir = "%s/%s" % (etl_op['tmpdir'],etl_op['date'])
        self.outfilename = '%s_%s_%s_%s_%s' % (name,etl_op['job_name'],etl_op['step_name'],etl_op['date'],etl_op['hour'])
        os.system('mkdir -p %s' % self.outfiledir)

    def ArgRepace(self,strinfo):
        new_strinfo = copy.deepcopy(strinfo)
        for arg in self.pattern.findall(strinfo):
            new_arg = "tmp=" + arg.replace("<!",'').replace("!>",'')
            exec(new_arg) in self.exec_namespace
            new_strinfo=new_strinfo.replace(arg,self.exec_namespace['tmp'])
        return new_strinfo
    
    def EtlopClean(self,etl_op):
        self.etl_op = {}
        for key in etl_op.keys():
            if key in ["db_path","sql"]:
                tmp_strinfo = etl_op[key] % etl_op
                self.etl_op[key] = self.ArgRepace(tmp_strinfo)
            elif key in ["post_sql",'pre_sql']:
                self.etl_op[key] = []
                tmp_list = copy.deepcopy(etl_op[key])
                for tmp_strinfo in tmp_list:
                    self.etl_op[key].append(self.ArgRepace(tmp_strinfo % etl_op))
            else:
                self.etl_op[key] = copy.deepcopy(etl_op[key])
    
    def DbConnect(self,etl_op,db_object_dict):
        try:
            self.logger("[%s] db connect started" % self.name,'info')

            if not db_object_dict['Extraction'].has_key(etl_op['db_type']):
                self.logger('[%s] ComEtl not supported database [%s]' % (self.name,etl_op['src_db_type']),'error')
                return False      
            self.db_obj = db_object_dict['Extraction'][etl_op['db_type']]

            connum = len(etl_op["db_coninfo"])
            for coninfo in etl_op["db_coninfo"]:
                if not self.db_obj.connect(coninfo['db_ip'],coninfo['db_port'],coninfo['db_db'],\
                           coninfo['db_user'],coninfo['db_passwd']):
                    connum -= 1
            if connum == 0:
                self.logger("[%s] src_db connect failed" % self.name,'error')
                return False                    
            
            self.logger("[%s] db connect successfully" % self.name,'info')             
            return True              
        except Exception,e:
            self.logger("[%s] %s" % ("ExtractionEtl DbConnect",str(e)),'error')
            self.logger("[%s] %s" % ("ExtractionEtl DbConnect",Etltraceback()),'error')
            return False       

    def DbPreSql(self,etl_op,db_object_dict):
        try:
            self.logger("[%s] db presql exec started" % self.name,'info')
            
            for args in etl_op["pre_sql"]:
                self.logger("[%s] db presql [%s]" % (self.name,args),'info')
                if not self.db_obj.execute(args,presql=True):
                    self.logger("[%s] db presql [%s] failed" % (self.name,args),'error')
                    return False                   
            
            self.logger("[%s] db presql exec successfully" % self.name,'info')
            return True
        except Exception,e:
            self.logger("[%s] %s" % ("ExtractionEtl DbPreSql",str(e)),'error')
            self.logger("[%s] %s" % ("ExtractionEtl DbPreSql",Etltraceback()),'error')
            return False

    def DbExecSql(self,etl_op,db_object_dict):
        try:
            self.logger("[%s] db execsql started" % self.name,'info')
            
            if EtlUtility.__dict__.has_key(etl_op['sql_assemble']):
                sql = EtlUtility.__dict__[etl_op['sql_assemble']](etl_op,self.logger)
            else:
                udf = __import__('EtlUDF/%s' % etl_op['sql_assemble'])
                sql = udf.__dict__[etl_op['sql_assemble']](etl_op,self.logger)
                                
            self.logger("[%s] sql is [%s]" % (self.name,sql),'info')
            
            if not self.db_obj.execute(sql,officialsql=True,outfile = self.outfiledir + '/' + self.outfilename):
                self.logger("[%s] db execsql failed" % self.name,'error')
                return False           
            else:
                self.logger("[%s] db execsql successfully" % self.name,'info')
                return True
        except Exception,e:
            self.logger("[%s] %s" % ("ExtractionEtl DbExecSql",str(e)),'error')
            self.logger("[%s] %s" % ("ExtractionEtl DbExecSql",Etltraceback()),'error')
            return False

    def GetSqlRes(self,etl_op,db_object_dict):
        try:
            self.logger("[%s] db getsqlres started" % self.name,'info')
            
            args = copy.deepcopy(etl_op)
            args["tmpdir"] = etl_op['tmpdir']
            if etl_op['db_type'] == "mapred":
                args["mapred_output_dir"] = etl_op['sql'][etl_op["sql"].find('-output') + 7:].split(' ' ,2)[1] 
            if etl_op['db_type'] == "hive":
                args['fixedfield'] = etl_op.get('fixedfield',0)
                args['dynamicfield'] = etl_op.get('dynamicfield',0)
                try:
                    args['tablefiledname'] = '|'.join(str(x['tablefiledname']) for x in etl_op['sql_xml_deal'])
                except:
                    pass
                
            
            if self.db_obj.save(self.outfiledir + '/' + self.outfilename,args):
                self.logger("[%s] db getsqlres successfully" % self.name,'info')
                return True
            else:
                self.logger("[%s] db getsqlres failed" % self.name,'error')
                return False             
        except Exception,e:
            self.logger("[%s] %s" % ("ExtractionEtl GetSqlRes",str(e)),'error')
            self.logger("[%s] %s" % ("ExtractionEtl GetSqlRes",Etltraceback()),'error')
            return False
    
    def LoadRes(self,etl_op,db_object_dict):
        try:
            self.logger("[%s] db LoadRes started" % self.name,'info')
            
            #SplitLoad(self.src_db,etl_op,self.outfile,self.logger)
            
            self.logger("[%s] db LoadRes exec started" % self.name,'info')
            return True
        except Exception,e:
            self.logger("[%s] %s" % ("ExtractionEtl LoadRes",str(e)),'error')
            self.logger("[%s] %s" % ("ExtractionEtl LoadRes",Etltraceback()),'error')
            return False

    def DbPostSql(self,etl_op,db_object_dict):
        try:
            self.logger("[%s] DbPostSql started" % self.name,'info')
            
            for args in etl_op["post_sql"]:
                self.logger("[%s] db postsql [%s]" % (self.name,args),'info')
                if not self.src_db.execute(args,postsql=True):
                    self.logger("[%s] postsql is [%s] failed" % (self.name,args),'error')
                    return False                  
            
            self.logger("[%s] DbPostSql successfully" % self.name,'info')
            return True
        except Exception,e:
            self.logger("[%s] %s" % ('ExtractionEtl DbPostSql',str(e)),'error')
            self.logger("[%s] %s" % ("ExtractionEtl DbPostSql",Etltraceback()),'error')
            return False

    def run(self):
        try:
            self.logger("[%s] etl started<-><-><-><-><->" % self.name,'info')
            if self.DbConnect(self.etl_op,self.db_object_dict):
                if self.DbPreSql(self.etl_op,self.db_object_dict):
                    if self.DbExecSql(self.etl_op,self.db_object_dict):
                        if self.GetSqlRes(self.etl_op,self.db_object_dict):
                            if self.DbPostSql(self.etl_op,self.db_object_dict):
                                self.logger("[%s] etl successfully #############" % self.name,'info')
                                self.db_obj.close()
                                return True
            self.logger("[%s] etl failed" % self.name,'error')
            self.db_obj.close()
            return False
        except Exception,e:
            self.logger("[%s] %s" % ('ExtractionEtl run',str(e)),'error')
            self.logger("[%s] %s" % ("ExtractionEtl run",Etltraceback()),'error')
            return False
         
                
        