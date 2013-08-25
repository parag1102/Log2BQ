import os
import re
import sys

# Run this python file as -:
'''python2.7 cmdline.py <project_id> <dataset_id> <table_id> <GS_csv_name> 
    <appengine project destination_path>'''

###############################################################################
NUM         =  0
COL         = []
TYPE        = []
###############################################################################

# writes the schema of the table to file
def schema_to_file(file):
    proj_id=sys.argv[1]
    dset_id=sys.argv[2]
    tbl_id =sys.argv[3]
    gs_csv =sys.argv[4]
    
    commd="bq show "+proj_id+":"+dset_id+"."+tbl_id+" >> "+file
    os.system(commd)

###############################################################################

# writes the IDs(project id, dataset id etc) and the json schema string to our
# library
def ids_schema_to_lib(lib, schema):
    proj_id=sys.argv[1]
    dset_id=sys.argv[2]
    tbl_id =sys.argv[3]
    gs_csv =sys.argv[4]
    
    f1=open(lib,'r')
    s=f1.read()
    pos=s.find("files.gs = gs")+13
    str1=s[:pos]
    str2=s[pos:]
    f1.close()
    
    str1=str1+"""
        
###############################################################################
GS_CSV      = '"""+gs_csv+"""'
TABLE_ID    = '"""+tbl_id+"""'
PROJECT_ID  = '"""+proj_id+"""'
DATASET_ID  = '"""+dset_id+"""'
SCHEMA      = '''"""+schema+"""'''
###############################################################################
"""
    
    f2=open(lib,'w')
    f2.write(str1+str2)
    f2.close()
    
###############################################################################

# reads the schema from the file and returns the same as a json string
def get_schema(file):
    
    f=open(file,'r')
    str=f.read()
    
    str=re.sub(' ','',str)
    str=re.sub('\n','',str)
    
    ind=str.count('|-')
    
    pos1=str.find('|-')
    s=str[pos1+2:]
    
    for i in range(ind):
        if (i != ind-1):
            pos=s.find(':')
            COL.append(s[:pos])
            tpos=s.find('|-')
            TYPE.append(s[pos+1:tpos])
            s=re.sub(s[:tpos],'',s)
            s=s[2:]
        
        else:
            pos=s.find(':')
            COL.append(s[:pos])
            TYPE.append(s[pos+1:])
            s=re.sub(s[:],'',s)
    
    ind=len(COL)
    global NUM
    NUM=ind
    schema=''' {"fields": [
        '''
    
    for i in range(ind):
        if (i != ind-1):
            schema+='''{"name": "'''+COL[i]+'''", "type": "'''+TYPE[i]+'''"},'''
        
        else:
            schema+='''{"name": "'''+COL[i]+'''", "type": "'''+TYPE[i]+'''"}
                ]
                }'''
    
    return schema

###############################################################################

# generates our logging class LogClass() in the file logclass.py
def generate_class():
    
    class_file=sys.argv[5]+"/logclass.py"
    
    f=open(class_file,'w')
    
    s0="""import re
import loglib
import logging
"""
    
    s1="""
class LogClass():"""    
    
    s2="""
    def glog(self, """
    
    for i in range(NUM):
        if (i != NUM-1):
            s2+=COL[i]+", "
        else:
            s2+=COL[i]+"""):
"""
    
    for j in range(NUM):
        s2+="""         """+COL[j]+"""=re.sub(',',' ',"""+COL[j]+""")
"""
    
    s2+="""         str='%lg%'+"""
    
    for k in range(NUM):
        if (k != NUM-1):
            s2+=COL[k]+"""+", "+"""
        else:
            s2+=COL[k]+"""
"""
    
    s2+="""         logging.info(str)
"""
    
    f.write(s0+s1+s2)

###############################################################################

# gives the complete control flow of our cmdline.py script
def whole_process():
    
    schema_file=sys.argv[5]+"/schema.txt"
    lib=sys.argv[5]+"/loglib.py"
    
    schema_to_file(schema_file)         # writes the schema to schema.txt
    schema=get_schema(schema_file)
    
    ids_schema_to_lib(lib, schema)      # writes the ids and json schema string
                                        # to loglib.py
    
    generate_class()
###############################################################################

whole_process()

###############################################################################
