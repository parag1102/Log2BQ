import re
import logging

class LogClass():
    def glog(self, level, message):
         level=re.sub(',',' ',level)
         message=re.sub(',',' ',message)
         str='%lg%'+level+", "+message
         logging.info(str)
