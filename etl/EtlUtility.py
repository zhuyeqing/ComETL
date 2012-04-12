#-*-coding:UTF-8 -*-
import os
import re
import time
import commands

def SqlExecCode(etl_op,exec_namespace):
    exec("import time") in exec_namespace
    exec("import calendar") in exec_namespace
    exec("adddate = lambda x,y:time.strftime('%Y%m%d',time.gmtime(int(time.mktime(time.strptime(x,'%Y%m%d'))) + y*24*3600 + 8*3600))") in exec_namespace
    exec("subdate = lambda x,y:time.strftime('%Y%m%d',time.gmtime(int(time.mktime(time.strptime(x,'%Y%m%d'))) - y*24*3600 + 8*3600))") in exec_namespace
    exec("getyear = lambda x:x[:4]") in exec_namespace
    exec("getmonth = lambda x:x[:4]") in exec_namespace
    exec("lastmonth = lambda x:time.strftime('%m',time.gmtime(int(time.mktime(time.strptime(x,'%Y%m%d'))) - int(x[6:8])*24*3600 + 8*3600))")  in exec_namespace
    exec("lastmonthday = lambda x,y:time.strftime('%Y%m',time.gmtime(int(time.mktime(time.strptime(x,'%Y%m%d'))) - int(x[6:8])*24*3600 + 8*3600)) + '%02d' % y")  in exec_namespace
    exec("curweek = lambda x,y:subdate(x,calendar.weekday(int(x[:4]),int(x[4:6]),int(x[6:])) - y)") in exec_namespace
    exec("lastweek = lambda x,y:subdate(x,calendar.weekday(int(x[:4]),int(x[4:6]),int(x[6:])) - y + 7)") in exec_namespace
    exec("addhour = lambda x,y:int(x)+y") in exec_namespace
    exec("subhour = lambda x,y:int(x)-y") in exec_namespace
    exec("date = '%s'" % etl_op['date']) in exec_namespace
    exec("hour = '%s'" % etl_op['hour']) in exec_namespace     

def SimpleAssemble(etl_op,logger):
    return etl_op['sql']

def OptXmlAssemble(etl_op,logger):
    tmp_etl_op = {}
    tmp_etl_op['xmlnodeprocess'] = ','.join(str(x['processing']) for x in etl_op['sql_xml_deal'])
    #tmp_etl_op['xmlnodelist'] = '|'.join(str(x['xmlnodename']) for x in etl_op['src_xml_deal'])
    tmp_xmlnodelist = []
    for x in etl_op['sql_xml_deal']:
        tmp_xmlnodelist.append(x['xmlnodename'])
    xmlnodelist = list(set(tmp_xmlnodelist))
    tmp_etl_op['xmlnodelist'] = '|'.join(str(x) for x in xmlnodelist)
    tmp_etl_op['xmlnodedefine'] = ' int,'.join(str(x) for x in xmlnodelist) + ' int' 
    tmp_etl_op['xmlattrlist'] = '|'.join(str(x['xmlnodeattr']) for x in etl_op['sql_xml_deal'])
    sql = etl_op['sql']%tmp_etl_op
    return sql

def MapredRCAssemble(etl_op,logger):
    etl_runtime_conf_dir = "%s/%s" % (etl_op['tmpdir'],etl_op['date'])
    commands.getstatusoutput("mkdir -p %s" % etl_runtime_conf_dir)
    f = open('%s/etl_runtime_conf.py' % etl_runtime_conf_dir,'w')
    f.write('date="%s"\n' % etl_op['date'])
    f.write('hour="%s"\n' % etl_op['hour'])
    f.close()
    sql = etl_op['sql'] + ' -file %s/etl_runtime_conf.py' % etl_runtime_conf_dir
    return sql

def SimpleTransform(source_files,dest_file,logger,args):
    commands.getstatusoutput("rm -f %s" % (dest_file))
    for k,v in source_files.items():
        res = commands.getstatusoutput("cat %s >> %s" % (k,dest_file))
        if res[0] != 0:
            logger('SimpleTransform %s' % res[1],'error')
            return False
    return True

def ConvertTransform(source_files,dest_file,logger,args):
    return True

if __name__=="__main__":
    etl_op = {'sql_xml_deal':[  {'xmlnodename':'BF_VVSUCC',\
                             'xmlnodeattr':'count',\
                             'tablefiledname':'BF_VVSUCC',\
                             'processing':"sum(if(tmp.BF_VVSUCC>=1,1,0)),sum(tmp.BF_VVSUCC)"},\
                            {'xmlnodename':'BF_DAYS',\
                             'xmlnodeattr':'count',\
                             'tablefiledname':'BF_DAYS',\
                             'processing':"sum(if(tmp.BF_DAYS>=1,1,0)),sum(tmp.BF_DAYS)"},\
                            {'xmlnodename':'SF_OPENBYSTART',\
                             'xmlnodeattr':'count',\
                             'tablefiledname':'SF_OPENBYSTART',\
                             'processing':"sum(if(tmp.SF_OPENBYSTART>=1,1,0)),sum(tmp.SF_OPENBYSTART)"},\
                          ],
          'sql':"select tmp.STAT_DATE,tmp.VERSION,%(xmlnodeprocess)s from (select TRANSFORM(xml,'%(xmlnodelist)s','%(xmlattrlist)s',stat_date,version) USING './XmlFlat.py' as (%(xmlnodedefine)s,STAT_DATE string,VERSION string) from test.test) tmp group by tmp.STAT_DATE,tmp.VERSION",
          }
    print OptXmlAssemble(etl_op,'')