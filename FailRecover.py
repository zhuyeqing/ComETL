#-*-coding:UTF-8 -*-
import os
import time
import commands

import conf
    
if __name__=="__main__":
    folder = conf.basedir + '/failedrecover/'
    f_list = os.listdir(folder)
    for f in f_list:
        conffile = f.rsplit('_',1)[0].split('_',1)[1]
        runtime = f.rsplit('_',1)[1]
        commands.getstatusoutput('python %s/ComEtl.py -c %s -t %s' % (conf.basedir,conffile,runtime))