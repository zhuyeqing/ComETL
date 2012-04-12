#-*-coding:UTF-8 -*-
import os
import re
import time
import copy
import commands

#-----------------com pdbc functions-----------------#
HADOOP_HOME = '/opt/modules/hadoop/hadoop-0.20.203.0'
LOAD_CMD_BASE = '%s/bin/hadoop fs -put' % HADOOP_HOME

def FakeOutput(src_db,out_file,args,logger):
    return True

def FakeLoad(*args):
    return True

def CreateLzoIndex(hfile,logger):
    cmd = '%s/bin/hadoop jar /opt/hadoopgpl/lib/hadoop-lzo.jar com.hadoop.compression.lzo.LzoIndexer %s'
    res = commands.getstatusoutput(cmd % (HADOOP_HOME,hfile))
    if res[0] != 0:
        logger("CreateLzoIndex %s" % res[1],'error')
        return False
    return True

def LzofileToHdfsdir(localfile,hdfsdir,logger):
    filename = os.path.split(localfile)[1]
    commands.getstatusoutput("rm -f %s.lzo" % localfile)
    res = commands.getstatusoutput("/usr/local/bin/lzop -1U %s" % localfile)
    if res[0] != 0:
        logger("LzofileToHdfsdir %s" % res[1],'error')
        return False
    else:
        if LocalfileToHdfsdir(localfile + ".lzo",hdfsdir,logger):
            if CreateLzoIndex(hdfsdir + "/" + filename + ".lzo",logger):
                return True
        logger("LzofileToHdfsdir ",'error')
        return False
            
        

def LocalfileToHdfsdir(localfile,hdfsdir,logger):
    filename = os.path.split(localfile)[1]
    hdfsdir_check_cmd = "%s/bin/hadoop fs -test -d %s" % (HADOOP_HOME,hdfsdir)
    hdfsdir_check_res = commands.getstatusoutput(hdfsdir_check_cmd)
    if int(hdfsdir_check_res[0]) != 0:
        commands.getstatusoutput("%s/bin/hadoop fs -mkdir %s" % (HADOOP_HOME,hdfsdir))
    
    res = commands.getstatusoutput('%s %s %s/' % (LOAD_CMD_BASE,localfile,hdfsdir))
    if os.system("%s/bin/hadoop fs -test -e %s/%s" % (HADOOP_HOME,hdfsdir,filename)) != 0 or int(res[0]) != 0:
        logger("LocalfileToHdfsdir %s" % res[1],'error')
        return False
    return True
#-----------------com pdbc functions-----------------#


#-----------------mysql pdbc functions-----------------#
InfobLoadCmd = 'mysql -h%(host)s -P%(port)d -u%(user)s -p%(passwd)s %(db)s --local-infile=1 -e "%(sql)s"'
LoadSqlBase = "load data local infile \'%s\' replace into table %s FIELDS TERMINATED BY \',\' (%s)"
re_loaderror = re.compile("Row: \d+")

def SimpleOutput(src_db,out_file,args,logger):
    commands.getstatusoutput("mkdir -p %s" % os.path.split(out_file)[0])
    tmp_outfile = open(out_file,'w')
    while True:
        tmp_data = src_db.getn(10000)
        if tmp_data == None:
            break
        if len(tmp_data) == 0:
            break        
        tmp_outfile.write('\n'.join('\t'.join(str(x) for x in row) for row in tmp_data))
        tmp_outfile.write('\n')
    tmp_outfile.close()
    return True

def batchinsert(src_db,etl_op,datafile):
    SplitLoad(src_db,etl_op,datafile)

def SimpleLoad(dest_db,datafile,args,logger):
    tmp_sql = "load data local infile \'%s\' replace into table " % datafile + tmp['dest_table'] + " FIELDS TERMINATED BY \'\t\' (%s)" % tmp['data_field'].replace('|',',')
    return dest_db.execute(tmp_sql)

def UpdateLoad(dest_db,datafile,args,logger):
    #tmp_sql = "load data local infile \'%s\' replace into table " % datafile + tmp['dest_table'] + " FIELDS TERMINATED BY \'\t\' (%s)" % tmp['data_field'].replace('|',',')
    #return dest_db.execute(tmp_sql)
    data_field_map = {}
    i = 0
    for field in args['data_field'].split('|'):
        data_field_map[field] = i
        i += 1
        
    update_data_field_map = {}
    for field in args['update_data_field'].split('|'):
        update_data_field_map[field] = data_field_map[field]

    update_where_field_map = {}
    for field in args['update_where_field'].split('|'):
        update_where_field_map[field] = data_field_map[field]
    
    sqlbase = 'update %s set %s where %s'
    f = open(datafile,'r')
    for l in f:
        frags = l.replace('\n','').split('\t')
        #sql = 'update %s set %s where %s' % args['db_table']
        data_sql = []
        where_sql = []
        for k,v in update_data_field_map.items():
            data_sql.append('%s="%s"' % (k,frags[v]))
            
        for k,v in update_where_field_map.items():
            where_sql.append('%s="%s"' % (k,frags[v]))
            
        sql = sqlbase % (args['db_table'],','.join(str(x) for x in data_sql),','.join(str(x) for x in where_sql))
        res = dest_db.execute(sql)
        if not res:
            logger("[%s] update failed" % sql,'error')
    return True
   
def SplitLoad(dest_db,datafile,args,logger):
    tmp = copy.deepcopy(args)
    out_file = datafile
    
    if os.path.isfile(datafile) == False:
        return False
    
    sql = ""
    os.system('split -C 32m %s %s_part_' % (out_file,out_file))
    data_folder = os.path.split(out_file)[0]
    for f in os.listdir(data_folder):
        tmp_f = "%s/%s" % (data_folder,f)
        if tmp_f.startswith('%s_part_' % out_file):
            tmp_sql = "load data local infile \'%s\' replace into table " % tmp_f + tmp['dest_table'] + " FIELDS TERMINATED BY \'\t\' (%s)" % tmp['data_field'].replace('|',',')            
            #sql += "load data local infile \'%s\' replace into table " % tmp_f + etl_op['dest_table'] + " FIELDS TERMINATED BY \',\' (%s)" % etl_op['dest_data_field'].replace('|',',')
            tmp['sql'] = sql + tmp_sql
            while True:
                res = commands.getstatusoutput(InfobLoadCmd % tmp)
                logger("SplitLoad sql [%s]" % (InfobLoadCmd % tmp),'info')
                #ERROR 2 (HY000) at line 1: Wrong data or column definition. Row: 234224, field: 5.
                if res[1].find('ERROR') == -1:
                    os.system("rm -f %s" % tmp_f)
                    break
                else:
                    try:
                        error_line = int(re_loaderror.findall(res[1])[0][5:])
                    except:
                        #print "unknow error:%s" % res[1]
                        logger("unknow error:%s" % res[1],'error')
                        break
                    #print 'delete error data line:',res[1]
                    logger("'delete error data line:%d" % error_line,'info')
                    new_tmp_file = tmp_f + '_seded'
                    os.system("sed '%dd' %s >> %s " % (error_line,tmp_f,new_tmp_file))
                    os.system("rm -f %s" % tmp_f)
                    os.system("mv %s %s" % (new_tmp_file,tmp_f))
    return True
    #os.system("rm -f %s*" % out_file)

#-----------------mysql pdbc functions-----------------#    



#-----------------hive pdbc functions-----------------#
HIVE_LOAD_CMD = "LOAD DATA INPATH \'%s\' INTO TABLE %s %s"

def ExternalLoad(dest_db,datafile,args,logger):
    tabledatapath = args["db_path"]
    if LocalfileToHdfsdir(datafile,tabledatapath,logger):
        return True
    logger("pdbc hive ",'error')
    return False

def ExternalLzoLoad(dest_db,datafile,args,logger):
    tabledatapath = args["db_path"]
    if LzofileToHdfsdir(datafile,tabledatapath,logger):
        return True
    logger("pdbc hive ",'error')
    return False

def Internalload(dest_db,datafile,args,logger):
    tmp_hdfsdir = "/tmp/etl_%s" % str(int(time.time()))
    if not LocalfileToHdfsdir(datafile,tmp_hdfsdir,logger):
        logger("pdbc hive Internalload",'error')
        return False
    tmp_hdfsfile = tmp_hdfsdir + "/" + os.path.split(datafile)[1]
    tmp_sql = HIVE_LOAD_CMD % (tmp_hdfsfile,args["db_table"],args["db_table_partition"])
    logger("Internalload sql [%s]" % tmp_sql,'info')
    res = dest_db.execute(tmp_sql)
    commands.getstatusoutput("%s/bin/hadoop fs -rmr %s" % (HADOOP_HOME,tmp_hdfsdir))
    return res

def MatrixTransform(matrix,tablefiledname,fixedfield,dynamicfield):
    res = []
    filed_list = tablefiledname.split('|')
    for row in matrix:
        i = 0
        for filed in filed_list:
           res.append(list(row[:fixedfield]) + list(row[fixedfield+i*dynamicfield:fixedfield+(i+1)*dynamicfield]) + [filed])
           i += 1
    return res

def ConvertOutput(src_db,out_file,args,logger):
    tmp_outfile = open(out_file,'w')
    while True:
        tmp_data = src_db.getn(10000)
        if tmp_data == None:
            break
        if len(tmp_data) == 0:
            break
        out_data = MatrixTransform(tmp_data,args['tablefiledname'],args['fixedfield'],args['dynamicfield'])
        tmp_outfile.write('\n'.join('\t'.join(str(x) for x in row) for row in out_data))
        tmp_outfile.write('\n')
    tmp_outfile.close()
    return True
#-----------------hive pdbc functions-----------------# 


#-----------------hivecli pdbc functions-----------------#
def HiveInitQWrite(dir,file,sql,logger):
    f = open("%s/%s" % (dir,file),'a')
    if not sql.endswith(';'):
        f.write(sql+';')
    else:
        f.write(sql)
    f.write("\n")
    f.close()
    
def HivecliOutput(src_db,out_file,args,logger):
    return True
    tmp_outfile = out_file + "_tmp"
    outf = open(out_file,'r')
    tmp_outf = open(tmp_outfile,'w')
    for line in outf:
        new_line = line.replace('\t',',')
        tmp_outf.write(new_line)
    outf.close()
    tmp_outf.close()
    commands.getstatusoutput("mv %s %s" % (tmp_outfile,out_file))
#-----------------hivecli pdbc functions-----------------# 


#-----------------hivemapred pdbc functions-----------------#
def MapredOutput(src_db,out_file,args,logger):
    tmp_dir = args["tmpdir"] + "/pdbcmapred_" + str(int(time.time())) 
    commands.getstatusoutput('mkdir -p %s' % tmp_dir)
    commands.getstatusoutput('rm -f %s' % out_file)
    res = commands.getstatusoutput("%s/bin/hadoop fs -copyToLocal %s/* %s" % (HADOOP_HOME,args["mapred_output_dir"],tmp_dir))
    if res[0] != 0:
        logger("pdbc mapred %s" % res[1],'error')
        return False
    else:
        for f in os.listdir(tmp_dir):
            tmp_f = tmp_dir + "/" + f
            if os.path.isfile(tmp_f):
                in_f = open(tmp_f,'r')
                out_f = open(out_file,'a')
                for line in in_f:
                    out_f.write(line)
                    if line[-1] != "\n":
                        out_f.write("\n")
                in_f.close()
                out_f.close()
        return True
    
def MapredLoad(dest_db,datafile,args,logger):
    ExternalLoad(dest_db,datafile,args,logger)

def MapredLzoLoad(dest_db,datafile,args,logger):
    ExternalLzoLoad(dest_db,datafile,args,logger)  
#-----------------hivemapred pdbc functions-----------------#


#-----------------hiveshell pdbc functions-----------------#
def ShellSimpleOutput(src_db,out_file,args,logger):
    commands.getstatusoutput('rm -f %s' % out_file)
    for f in os.listdir(args['db_path']):
        tmp_f = args['db_path'] + "/" + f
        if os.path.isfile(tmp_f):
            in_f = open(tmp_f,'r')
            out_f = open(out_file,'a')
            for line in in_f:
                out_f.write(line)
                if line[-1] != "\n":
                    out_f.write("\n")
            in_f.close()
            out_f.close()
    return True      
#-----------------hiveshell pdbc functions-----------------#