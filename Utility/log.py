#-*-coding:UTF-8 -*-
import logging
from logging.handlers import RotatingFileHandler

class EtlLog(object):
    def __init__(self,logfile,debug=False):
        logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=logfile,
                        filemode='a')
        
        #定义一个RotatingFileHandler，最多备份5个日志文件，每个日志文件最大10M
        Rthandler = RotatingFileHandler(logfile, maxBytes=20*1024*1024,backupCount=5)
        Rthandler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
        Rthandler.setFormatter(formatter)
        logging.getLogger('').addHandler(Rthandler)
        
        if debug:
            console = logging.StreamHandler()
            console.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
            console.setFormatter(formatter)
            logging.getLogger('').addHandler(console)        
    
    def log(self,mes,type):
        if type == "debug":
            logging.debug(mes)
        elif type == "info":
            logging.info(mes)
        elif type == "warn":
            logging.warning(mes)
        elif type == "error":
            logging.error(mes)
        else:
            logging.warning("unknown type:%s %s" % (type,mes))


if __name__=="__main__":
    logfile = "log_mod_test.log"
    etllog = EtlLog(logfile,True)
    #loginit(logfile)
    for i in range(5):
        etllog.log("%d ---- %d -----------------------------" % (i,i),"debug")

