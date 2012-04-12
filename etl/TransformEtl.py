#-*-coding:UTF-8 -*-
import re
import os
import imp
import copy
import EtlUtility

from EtlTb import Etltraceback

class TransformEtl(object):
    def __init__(self,etl_op,db_object_dict,logger,name='ETL',debug=False):
        self.logger = logger
        self.name = name + "_" + etl_op['job_name'] + '_' +etl_op['step_name']
        self.conf_name = name
        self.debug = debug
        self.etl_op = etl_op
        self.outfiledir = "%s/%s" % (etl_op['tmpdir'],etl_op['date'])
        self.outfilename = '%s_%s_%s_%s_%s' % (name,etl_op['job_name'],etl_op['step_name'],etl_op['date'],etl_op['hour'])        

    def Transform(self,etl_op,db_object_dict):
        try:
            self.logger("[%s] Transform started" % self.name,'info')
            
            source_files = {}
            for source in etl_op['data_source']:
                if source.get('path','').strip() != '':
                    source_files[source['path']] = source['data_field']
                else:
                    tmp_file = '%s/%s_%s_%s_%s_%s' % (self.outfiledir,self.conf_name,source.get('job_name',etl_op['job_name']),source['step_name'],etl_op['date'],etl_op['hour'])
                    source_files[tmp_file] = source['data_field']
            
            args = copy.deepcopy(etl_op)
            
            if EtlUtility.__dict__.has_key(etl_op['data_transform_type']):
                if not EtlUtility.__dict__[etl_op['data_transform_type']](source_files,self.outfiledir + '/' + self.outfilename,self.logger,args):
                    self.logger("[%s] Transform failed" % self.name,'error')
                    return False
            else:
                #udf = imp.load_source('udf','EtlUDF/%s.py' % etl_op['data_transform_type'])
                udf = __import__('EtlUDF/%s' % etl_op['data_transform_type'])
                if not udf.__dict__[etl_op['data_transform_type']](source_files,self.outfiledir + '/' + self.outfilename,self.logger,args):
                    self.logger("[%s] Transform failed" % self.name,'error')
                    return False                       
            
            self.logger("[%s] Transform successfully" % self.name,'info')
            return True
        except Exception,e:
            self.logger("[%s] %s" % ("TransformEtl Transform",str(e)),'error')
            self.logger("[%s] %s" % ("TransformEtl Transform",Etltraceback()),'error')
            return False

    def run(self):
        try:
            self.logger("[%s] TransformEtl started<-><-><-><-><->" % self.name,'info')
            
            if self.Transform(self.etl_op,''):
                self.logger("[%s] TransformEtl successfully #############" % self.name,'info')
                return True
        
            self.logger("[%s] TransformEtl failed" % self.name,'error')
            return False
        except Exception,e:
            self.logger("[%s] %s" % ("TransformEtl run",str(e)),'error')
            self.logger("[%s] %s" % ("TransformEtl run",Etltraceback()),'error')
            return False
         
                
        