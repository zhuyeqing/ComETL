#-*-coding:UTF-8 -*-
basedir = '/opt/hadoopbusiness/ComEtl2.0'
tmp_dir = '/data1/tmp'

db_info = {'hive':{'model':'PdbcHive.py','classname':'PdbcHive','support':'all'},
           'hivecli':{'model':'PdbcHivecli.py','classname':'PdbcHivecli','support':'all'},
           'mapred':{'model':"PdbcMapred.py",'classname':'PdbcMapred','support':'all'},
           'mysql':{'model':"PdbcMysql.py",'classname':'PdbcMysql','support':'all'},
           'shell':{'model':"PdbcShell.py",'classname':'PdbcShell','support':'Extraction'}}

mapred_queue = "ETL"