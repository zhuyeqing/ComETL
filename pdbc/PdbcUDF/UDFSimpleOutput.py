#-*-coding:UTF-8 -*-
import os
import commands

def UDFSimpleOutput(src_db,out_file,args,logger):
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
