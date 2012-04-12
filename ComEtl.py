#-*-coding:UTF-8 -*-
import os
import sys
import imp
import copy
import time
import pickle
import commands
import conf
import DqsAlarm

from Utility.log import *
from optparse import OptionParser

global log

usage = 'usage: ComEtl.py -c arg1:conffile -j arg2:job_name -p arg3:process -t arg4:timerange -d arg5:debugmode -e arg1:etlclass'
parser = OptionParser(usage=usage,version="%ComEtl 1.0")
parser.add_option('-e','--etlclass',dest='etlclass',help="etl class name",default='SimpleEtl')
parser.add_option('-c','--conf',dest='conffile',help="conf file for etl")
parser.add_option('-j','--job_name',dest='job_name',help="conf file job name")
parser.add_option('-p','--process',dest='process',help="conf file step name")
parser.add_option('-t','--timerange',dest='timerange',help="etl_time_range")
parser.add_option('-d','--debugmode',dest='debugmode',help="etl_debug_mode")

(options, args) = parser.parse_args()

def ProTimeRange(timerange):
    if timerange == None:
        return [time.strftime('%Y%m%d',time.localtime(time.time() - 24*60*60))]
    flags = timerange.split('-')
    if len(flags) == 1:
        return [flags[0]]
    
    res = []
    start_time = int(time.mktime(time.strptime(flags[0],'%Y%m%d')))
    end_time = int(time.mktime(time.strptime(flags[1],'%Y%m%d')))
    for i in range(start_time,end_time,24*60*60):
        res.append(time.strftime('%Y%m%d',time.localtime(i)))
    return res

def ProTimeRangeHour(timerange,delay_hour=2):
    if timerange == None:
        return [time.strftime('%Y%m%d%H',time.localtime(time.time() - delay_hour*60*60))]
    flags = timerange.split('-')
    if len(flags) == 1:
        return [flags[0]]
    
    res = []
    start_time = int(time.mktime(time.strptime(flags[0],'%Y%m%d%H')))
    end_time = int(time.mktime(time.strptime(flags[1],'%Y%m%d%H')))
    for i in range(start_time,end_time,60*60):
        res.append(time.strftime('%Y%m%d%H',time.localtime(i)))
    return res

def FailedRecover(conffile,runtime,run_log):
    try:
        str_op_conf = pickle.dumps(run_log)
        
        failed_file = "failedrecover_%s_%s" % (conffile,runtime)
        folder = conf.basedir + '/failedrecover/'
        os.system("mkdir -p %s" % folder)
        f = open("%s%s" % (folder,failed_file),'w')
        f.write(str_op_conf)
        f.close()
    except Exception,e:
        log('unknown error at FailedRecover--%s' % str(e),'error')
        return False 

def GetConf(conf_file):
    try:
        f = open("%s" % (conf_file),'r')
        str_op_conf = f.read()
        op_conf = pickle.loads(str_op_conf)
        f.close()
        return op_conf
    except Exception,e:
        log('=========unknown error at GetConf--%s' % str(e),'error')
        return None

def RunLogInit(run_log,etl_op,runtime,conffile):
    for job in etl_op['jobs']:
        run_log[job['job_name']] = {}
        for action in job['analysis']:
            run_log[job['job_name']][action['step_name']] = 'undo'
        for action in job['transform']:
            run_log[job['job_name']][action['step_name']] = 'undo'
        for action in job['loading']:
            run_log[job['job_name']][action['step_name']] = 'undo'
    failed_file = "%s/failedrecover/failedrecover_%s_%s" % (conf.basedir,conffile,runtime)
    if os.path.isfile(failed_file):
        run_log.update(GetConf(failed_file))
    commands.getstatusoutput("rm -f %s" % failed_file)
    return True
              

def EtlSched(etlclass,conffile,timerange,debugmode,job_name,process):
    conf_model_path = conf.basedir + '/etl_conf'
    etl_model_path = conf.basedir + '/etl'
    pdbc_model_path = conf.basedir + '/pdbc'

    if sys.path.count(conf_model_path) == 0:
        sys.path.append(conf_model_path)
    if sys.path.count(etl_model_path) == 0:
        sys.path.append(etl_model_path)
    if sys.path.count(pdbc_model_path) == 0:
        sys.path.append(pdbc_model_path)        
        
    db_object_dict = {'loading':{},'Extraction':{}}
    for db_type in conf.db_info.keys():
        tmp_db = imp.load_source('tmp_db',pdbc_model_path + '/' + conf.db_info[db_type]['model'])
        if tmp_db.__dict__.has_key(conf.db_info[db_type]['classname']):
            tmp_db_class = tmp_db.__dict__[conf.db_info[db_type]['classname']]
            if conf.db_info[db_type]['support'] == 'Loading':
                db_object_dict['loading'][db_type] = tmp_db_class(log)
            elif conf.db_info[db_type]['support'] == 'Extraction':
                db_object_dict['Extraction'][db_type] = tmp_db_class(log)
            else:
                db_object_dict['Extraction'][db_type] = tmp_db_class(log)
                db_object_dict['loading'][db_type] = tmp_db_class(log)
                
    etl_conf = imp.load_source('etl_conf',conf_model_path + '/' + conffile + '.py')
    timelist = []
    if etl_conf.etl_op.get('run_mode','day') == 'day':
        timelist = ProTimeRange(timerange)
    elif etl_conf.etl_op.get('run_mode','day') == 'hour':
        timelist = ProTimeRangeHour(timerange,etl_conf.etl_op.get('delay_hours',2))
    else:
        log('[%s] unknown run_mode %s' % (conffile,etl_conf.etl_op.get('run_mode','day')),'error')
        return False
    
    etl_res = True
    for runtime in timelist:
        etl_op = copy.deepcopy(etl_conf.etl_op)
        global_args = {}
        global_args['conffile'] = conffile
        global_args['tmpdir'] = conf.tmp_dir + "/" + conffile
        global_args['tmp_dir'] = global_args['tmpdir']
        global_args['date'] = runtime[:8]
        global_args['hour'] =  runtime[8:10] or '23'

        run_log = {}
        RunLogInit(run_log,etl_op,runtime,conffile)
        
        for job in etl_op['jobs']:
            if job_name:
                if job['job_name'] != job_name:
                    continue
                
            for i in range(len(job['analysis'])):
                if job['analysis'][i]['db_type'] in ['hive','hivecli']:
                    job['analysis'][i]['pre_sql'].insert(0,"set mapred.job.queue.name=%s" % conf.mapred_queue)
                if job['analysis'][i]['db_type'] in ['mapred']:
                    job['analysis'][i]['sql'] = job['analysis'][i]['sql'] + " -jobconf mapred.job.queue.name=%s " % conf.mapred_queue
            
            action_list = []
            action_list.extend(job['analysis'])
            action_list.extend(job['transform'])
            action_list.extend(job['loading'])
            for action in action_list:
                action['job_name'] = job['job_name']
                action.update(global_args)
                etlclass = action.get('etl_class_name','')
                if process:
                    if action["step_name"] == process:
                        etlmodel = imp.load_source('cometl',etl_model_path + '/' + etlclass + '.py')
                        cometl = etlmodel.__dict__[etlclass](action,db_object_dict,log,conffile,debugmode)
                        if cometl.run():
                            log('[%s-%s-%s-%s] success' % (conffile,job['job_name'],action["step_name"],runtime),'info')
                            return (True,'')
                        else:
                            log('[%s-%s-%s-%s] failed' % (conffile,job['job_name'],action["step_name"],runtime),'error')
                            #op_conf={"etlclass":etlclass,"conffile":conffile,"timerange":runtime,"process":action["step_name"],"debugmode":debugmode}
                            #FailedRecover(op_conf)
                            #dqsvalue = {'type':'job','title':'ETL','program':'%s_%s_%s' % (conffile,action["step_name"],runtime),'state':0,'msg':'%s_%s_%s_etl_failed' % (conffile,action["step_name"],runtime)}
                            #DqsAlarm.DQSAlarm('job',dqsvalue)                                          
                            return (False,'%s_%s_%s_%s' % (conffile,job['job_name'],action["step_name"],runtime))
                else:
                    if run_log[job['job_name']][action["step_name"]] == 'ok':
                        continue
                    etlmodel = imp.load_source('cometl',etl_model_path + '/' + etlclass + '.py')
                    cometl = etlmodel.__dict__[etlclass](action,db_object_dict,log,conffile,debugmode)
                    if cometl.run():
                        log('[%s-%s-%s-%s] success' % (conffile,job['job_name'],action["step_name"],runtime),'info')
                        run_log[job['job_name']][action["step_name"]] = 'ok'
                    else:
                        run_log[job['job_name']][action["step_name"]] = 'failed'
                        log('[%s-%s-%s-%s] failed' % (conffile,job['job_name'],action["step_name"],runtime),'error')
                        #op_conf={"etlclass":etlclass,"conffile":conffile,"timerange":runtime,"process":action["step_name"],"debugmode":debugmode}
                        if job_name:
                            return (False,'%s_%s_%s_%s' % (conffile,job['job_name'],action["step_name"],runtime))
                        else:
                            FailedRecover(conffile,runtime,run_log)
                            dqsvalue = {'type':'job','title':'ETL','program':'%s_%s_%s_%s' % (conffile,job['job_name'],action["step_name"],runtime),'state':0,'msg':'%s_%s_%s_etl_failed' % (conffile,action["step_name"],runtime)}
                            DqsAlarm.DQSAlarm('job',dqsvalue)                                          
                            return (False,'%s_%s_%s_%s' % (conffile,job['job_name'],action["step_name"],runtime))
    
    return (True,'')

if __name__=="__main__":
    if options.conffile == None:
        print 'etl conf must appointed'
        print usage
    else:
        debug = False
        if options.debugmode in ['true','True']:
            debug = True
        #loginit("ComETL.log",debug)
        logfile = conf.basedir + "/LogComEtl2.0.log"
        etllog = EtlLog(logfile,debug)
        log = etllog.log
        res = EtlSched(options.etlclass,options.conffile,options.timerange,options.debugmode,options.job_name,options.process)
        if not res[0]:
            raise res[1]
        