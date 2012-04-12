#-*-coding:UTF-8 -*-

def EtlUDFtest(source_files,dest_file,logger,args):
    logger('EtlUDFtest','info')
    for filepath,filefield in source_files.items():
        f = open(filepath,'r')
        f.close()
    return True