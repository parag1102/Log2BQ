from __future__ import with_statement

import re
import os
import cgi
import math
import time
import string
import urllib
import pickle
import pprint
import logging
import httplib2
import datetime
import functions
import wsgiref.handlers

try:
    import json                # Python 2.7.
except ImportError:
    import simplejson as json  # Python 2.5.


from google.appengine.ext import db
from bigqueryv2 import BigQueryClient
from apiclient.discovery import build
from apiclient.errors import HttpError
from google.appengine.api import files
from google.appengine.api import users
from google.appengine.ext import webapp
from google.appengine.api import channel
from google.appengine.ext import deferred
from google.appengine.api import memcache
from google.appengine.api import urlfetch
from google.appengine.api import taskqueue
from google.appengine.api import app_identity
from google.appengine.ext.webapp import template
from google.appengine.api.logservice import logservice
from google.appengine.ext.webapp.util import run_wsgi_app
from oauth2client.appengine import AppAssertionCredentials

try:
    files.gs
except AttributeError:
    import gs
    files.gs = gs

###############################################################################
GS_CSV      = 'log_bucket/logging.csv'
TABLE_ID    = 'logs'
PROJECT_ID  = '774982414180'
DATASET_ID  = 'my_dataset'
SCHEMA      = ''' {"fields": [
    {"name": "level", "type": "string"},{"name": "message", "type": "string"}
    ]
    }'''
###############################################################################

# Pushes the task of sending to GS into App Engine TaskQueue
def sendToGSUsingTaskQueue(csv_name):
    
    csv_file=re.sub('.csv','',csv_name)
    log_count=30
    loop_count=functions.calculateLoopCount()
                
    for i in range(0,loop_count):
        offset=i*log_count
        csv_name=csv_file+str(i)+'.csv'
        deferred.defer(functions.writeToGS,csv_name,offset,log_count)
        
        print '<br />Logs Succesfully written to gs://'+csv_name+'<br />'

###############################################################################

# Pushes the task of sending to BQ into App Engine TaskQueue
def sendToBQUsingTaskQueue(project_id,dataset_id,table_id,csv_name,schema):

    csv_file=re.sub('.csv','',csv_name)
    loop_count=functions.calculateLoopCount()
    
    for i in range(0,loop_count):
        csv_name=csv_file+str(i)+'.csv'
        print '<br />Sending to Google BigQuery from '+csv_name
        
        deferred.defer(functions.sendToBQ,project_id,dataset_id,table_id,csv_name,schema,_countdown=60)
        
        print '<br />Logs Successfully sent from '+csv_name+' to Google BigQuery<br />'


###############################################################################

# Gives the control flow of the process of sending the logs to BigQuery
def send():
    print '<br />#####Pushing the Tasks into App Angine TaskQueue#####<br />'
    sendToGSUsingTaskQueue(GS_CSV)
    sendToBQUsingTaskQueue(PROJECT_ID,DATASET_ID,TABLE_ID,GS_CSV,SCHEMA)
    print '<br />#####Successfully Completed the Background Process#####<br />'

###############################################################################
send()
###############################################################################
