#-*-coding:UTF-8 -*-
import traceback
import StringIO

def Etltraceback():
    fp = StringIO.StringIO()
    traceback.print_exc(file=fp)
    return fp.getvalue()    